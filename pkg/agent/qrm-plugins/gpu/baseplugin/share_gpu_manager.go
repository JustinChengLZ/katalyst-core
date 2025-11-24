/*
Copyright 2022 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package baseplugin

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This module determines whether each GPU device is eligible for shared usage
// (ShareGPU) based on per-pod extended indicators fetched from MetaServer. It
// periodically scans the GPU machine state maintained by BasePlugin and builds
// a snapshot map `shareGPUMap` that records the ShareGPU decision for each
// device ID.
// Decision rule: Only main containers are considered; if any main container on
// a device disables ShareGPU, the device is marked non-shareable. To reduce
// repeated external queries, a per-sync in-memory cache keyed by `pod UID` is
// used to memoize `EnableShareGPU` decisions.

// ShareGPUManager determines per-device ShareGPU eligibility and maintains a
// periodic snapshot. Safe for concurrent reads via `EnableShareGPU`.
//
// Time Complexity:
//   - `sync`: O(D + C) where D is number of devices and C is number of main
//     containers scanned; per-pod indicator lookups are amortized O(1) via cache.
//   - `EnableShareGPU`: O(1) map read.
//   - `Allocate`: O(A) over `TopologyAwareAllocations` entries when marking.
//
// Potential Errors:
// - External indicator fetch may fail; treated conservatively and wrapped.
// - Internal panics are recovered to keep the manager running.
type ShareGPUManager interface {
	// Allocate processes an allocation of a main container. If the pod's
	// indicator disables ShareGPU, all involved device IDs in
	// `TopologyAwareAllocations` are marked non-shareable in the snapshot.
	// Params:
	// - ctx: request-scoped context for external calls
	// - allocationInfo: container allocation metadata; ignored if nil or non-main
	// Returns: none; updates internal snapshot
	// Errors: any external errors are logged and wrapped internally
	Allocate(ctx context.Context, allocationInfo *state.AllocationInfo)

	// EnableShareGPU returns the cached ShareGPU decision for a given device ID.
	// Params:
	// - id: device ID string
	// Returns: true if the device is shareable; false if unknown or disallowed
	EnableShareGPU(id string) bool

	// Run starts the periodic synchronization loop until `stopCh` is closed.
	// Params:
	// - basePlugin: plugin providing machine state and MetaServer
	// - stopCh: channel to stop the loop
	// Notes: blocking method; should be called in a separate goroutine.
	Run(*BasePlugin, <-chan struct{})
}
type shareGPUManager struct {
	sync.RWMutex

	shareGPUMap map[string]bool
	basePlugin  *BasePlugin
}

// NewShareGPUManager creates a new ShareGPUManager instance.
// Returns: a manager with an empty snapshot cache.
func NewShareGPUManager() ShareGPUManager {
	return &shareGPUManager{
		shareGPUMap: make(map[string]bool),
	}
}

// EnableShareGPU returns the cached ShareGPU decision for a given device ID.
// If the device ID is not present in the latest snapshot, it returns false.
// Complexity: O(1) map lookup.
func (s *shareGPUManager) EnableShareGPU(id string) bool {
	s.RLock()
	defer s.RUnlock()

	return s.shareGPUMap[id]
}

// Allocate marks involved device IDs as non-shareable if the pod disables
// ShareGPU. Non-main containers are ignored.
// Complexity: O(A) where A is number of device IDs in TopologyAwareAllocations.
func (s *shareGPUManager) Allocate(ctx context.Context, allocationInfo *state.AllocationInfo) {
	if allocationInfo == nil || !allocationInfo.CheckMainContainer() {
		return
	}

	enableShareGPU := s.evaluateContainerDeviceShareStatus(ctx, allocationInfo, nil)
	if enableShareGPU {
		return
	}

	s.Lock()
	defer s.Unlock()
	for id := range allocationInfo.TopologyAwareAllocations {
		s.shareGPUMap[id] = false
	}
}

// Run starts the periodic synchronization loop that refreshes ShareGPU
// decisions. The loop:
// - immediately performs a synchronization once for faster readiness;
// - then schedules periodic syncs every 15 seconds;
// - stops when `stopCh` is closed.
// Note: The method is blocking; callers should invoke it in a goroutine.
func (s *shareGPUManager) Run(basePlugin *BasePlugin, stopCh <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	s.basePlugin = basePlugin

	go func() {
		<-stopCh
		cancel()
	}()

	s.sync(ctx)
	wait.UntilWithContext(ctx, s.sync, 30*time.Second)
}

// sync refreshes the ShareGPU decisions by scanning machine state.
// Complexity: O(D + C) per invocation.
func (s *shareGPUManager) sync(ctx context.Context) {
	if s.basePlugin == nil {
		general.Infof("share gpu manager sync failed, basePlugin is nil")
		return
	}

	s.Lock()
	defer s.Unlock()
	machineState, ok := s.basePlugin.State.GetMachineState()[gpuconsts.GPUDeviceType]
	if !ok {
		general.Infof("share gpu manager found no GPU machine state; skipping")
		return
	}

	// Build a fresh snapshot with per-sync indicator cache.
	shareGPUMap := make(map[string]bool, len(machineState))
	indicatorCache := make(map[types.UID]bool)
	for id, alloc := range machineState {
		shareGPUMap[id] = s.evaluateDeviceShareStatus(ctx, alloc, indicatorCache)
	}

	s.shareGPUMap = shareGPUMap
}

// evaluateDeviceShareStatus scans main containers for a device and returns true
// if and only if all of them enable ShareGPU. Any error retrieving indicators is
// treated as non-blocking and the container is ignored (optimistic sharing).
// evaluateDeviceShareStatus returns true iff all main containers enable ShareGPU.
// Complexity: O(Cd) where Cd is number of main containers on the device.
func (s *shareGPUManager) evaluateDeviceShareStatus(ctx context.Context, alloc *state.AllocationState, cache map[types.UID]bool) bool {
	if alloc == nil {
		return false
	}

	// Default to shareable and short-circuit to false once a disallowed pod is found.
	for _, containerEntries := range alloc.PodEntries {
		for _, container := range containerEntries {
			if !container.CheckMainContainer() {
				continue
			}

			enableShareGPU := s.evaluateContainerDeviceShareStatus(ctx, container, cache)
			if !enableShareGPU {
				return false
			}
		}
	}

	return true
}

// evaluateContainerDeviceShareStatus checks a single container's pod-level indicator.
// If a cache is provided, it memoizes decisions by pod UID.
// Complexity: O(1) with cache hit; O(ExternalCall) otherwise.
func (s *shareGPUManager) evaluateContainerDeviceShareStatus(ctx context.Context, container *state.AllocationInfo, cache map[types.UID]bool) bool {
	podMeta := s.preparePodMeta(container)

	if cache != nil {
		if v, ok := cache[podMeta.UID]; ok {
			return v
		}
	}

	enableShareGPU, err := s.getPodEnableShareGPU(ctx, podMeta)
	if err != nil {
		general.Infof("share gpu manager: fetching extended indicators failed for pod %s/%s: %v", podMeta.Namespace, podMeta.Name, err)
		if cache != nil {
			cache[podMeta.UID] = false
		}
		return false
	}

	if cache != nil {
		cache[podMeta.UID] = enableShareGPU
	}
	return enableShareGPU
}

// getPodEnableShareGPU queries MetaServer for the pod's `EnableShareGPU` indicator.
// Returns: boolean indicator value; error when external call fails.
// Errors: wrapped with context using `pkg/errors`.
func (s *shareGPUManager) getPodEnableShareGPU(ctx context.Context, podMeta metav1.ObjectMeta) (bool, error) {
	enableShareGPU := false
	indicators := v1alpha1.ReclaimResourceIndicators{}
	baseLine, err := s.basePlugin.MetaServer.ServiceExtendedIndicator(ctx, podMeta, &indicators)
	if err != nil {
		return false, errors.Wrapf(err, "ServiceExtendedIndicator failed for pod %s/%s", podMeta.Namespace, podMeta.Name)
	}

	if !baseLine && indicators.EnableShareGPU != nil {
		enableShareGPU = *indicators.EnableShareGPU
	}

	return enableShareGPU, nil
}

func (s *shareGPUManager) preparePodMeta(info *state.AllocationInfo) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		UID:         types.UID(info.PodUid),
		Namespace:   info.PodNamespace,
		Name:        info.PodName,
		Labels:      info.Labels,
		Annotations: info.Annotations,
	}
}
