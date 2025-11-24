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
	"fmt"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	pluginapi "k8s.io/kubelet/pkg/apis/resourceplugin/v1alpha1"

	"github.com/kubewharf/katalyst-api/pkg/apis/config/v1alpha1"
	workloadapis "github.com/kubewharf/katalyst-api/pkg/apis/workload/v1alpha1"
	commonstate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/commonstate"
	gpuconsts "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/consts"
	gpustate "github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/state"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/spd"
)

// fakeSPM is a lightweight ServiceProfilingManager for tests.
type fakeSPM struct {
	behaviors map[types.UID]struct {
		baseline bool
		enable   *bool
		err      error
	}
	calls int
}

func (f *fakeSPM) ServiceExtendedIndicator(_ context.Context, podMeta metav1.ObjectMeta, indicators interface{}) (bool, error) {
	f.calls++
	b := f.behaviors[podMeta.UID]
	// fill EnableShareGPU if indicators is of proper type
	if ind, ok := indicators.(*v1alpha1.ReclaimResourceIndicators); ok {
		ind.EnableShareGPU = b.enable
	}
	if b.err != nil {
		return false, b.err
	}
	return b.baseline, nil
}

// Stubs for unused interface methods
func (f *fakeSPM) ServiceBusinessPerformanceLevel(context.Context, metav1.ObjectMeta) (spd.PerformanceLevel, error) {
	return spd.PerformanceLevelPerfect, nil
}

func (f *fakeSPM) ServiceBusinessPerformanceScore(context.Context, metav1.ObjectMeta) (float64, error) {
	return spd.MaxPerformanceScore, nil
}

func (f *fakeSPM) ServiceSystemPerformanceTarget(context.Context, metav1.ObjectMeta) (spd.IndicatorTarget, error) {
	return spd.IndicatorTarget{}, nil
}

func (f *fakeSPM) ServiceBaseline(context.Context, metav1.ObjectMeta) (bool, error) {
	return false, nil
}

func (f *fakeSPM) ServiceAggregateMetrics(context.Context, metav1.ObjectMeta, v1.ResourceName, bool, workloadapis.Aggregator, workloadapis.Aggregator) ([]resource.Quantity, error) {
	return nil, nil
}
func (f *fakeSPM) Run(context.Context) {}

// fakeState provides minimal state for tests.
type fakeState struct {
	machine gpustate.AllocationResourcesMap
}

func (f *fakeState) GetMachineState() gpustate.AllocationResourcesMap {
	return f.machine
}
func (f *fakeState) GetPodResourceEntries() gpustate.PodResourceEntries             { return nil }
func (f *fakeState) GetPodEntries(resourceName v1.ResourceName) gpustate.PodEntries { return nil }
func (f *fakeState) GetAllocationInfo(resourceName v1.ResourceName, podUID, containerName string) *gpustate.AllocationInfo {
	return nil
}
func (f *fakeState) SetMachineState(gpustate.AllocationResourcesMap, bool)          {}
func (f *fakeState) SetResourceState(v1.ResourceName, gpustate.AllocationMap, bool) {}
func (f *fakeState) SetPodResourceEntries(gpustate.PodResourceEntries, bool)        {}
func (f *fakeState) SetAllocationInfo(v1.ResourceName, string, string, *gpustate.AllocationInfo, bool) {
}
func (f *fakeState) Delete(v1.ResourceName, string, string, bool) {}
func (f *fakeState) ClearState()                                  {}
func (f *fakeState) StoreState() error                            { return nil }

func makeContainer(podUID, podNS, podName, containerName string, main bool, ids ...string) *gpustate.AllocationInfo {
	t := pluginapi.ContainerType_SIDECAR.String()
	if main {
		t = pluginapi.ContainerType_MAIN.String()
	}
	topo := make(map[string]gpustate.Allocation)
	for _, id := range ids {
		topo[id] = gpustate.Allocation{Quantity: 1}
	}
	return &gpustate.AllocationInfo{
		AllocationMeta: gpustate.AllocationInfo{AllocationMeta: commonstate.AllocationMeta{
			PodUid:        podUID,
			PodNamespace:  podNS,
			PodName:       podName,
			ContainerName: containerName,
			ContainerType: t,
		}}.AllocationMeta,
		AllocatedAllocation:      gpustate.Allocation{Quantity: 1},
		TopologyAwareAllocations: topo,
	}
}

func Test_EnableShareGPU_DefaultFalse(t *testing.T) {
	t.Parallel()
	m := NewShareGPUManager().(*shareGPUManager)
	if got := m.EnableShareGPU("non-existent"); got {
		t.Fatalf("expected false for unknown id, got true")
	}
}

func Test_Allocate_MarkFalse_OnIndicatorFalse(t *testing.T) {
	t.Parallel()
	spm := &fakeSPM{behaviors: map[types.UID]struct {
		baseline bool
		enable   *bool
		err      error
	}{
		types.UID("u1"): {baseline: false, enable: ptrBool(false)},
	}}
	bp := &BasePlugin{MetaServer: &metaserver.MetaServer{ServiceProfilingManager: spm}}
	m := NewShareGPUManager().(*shareGPUManager)
	m.basePlugin = bp

	alloc := makeContainer("u1", "ns", "pod", "c", true, "GPU-1", "GPU-2")
	m.Allocate(context.Background(), alloc)

	if m.EnableShareGPU("GPU-1") || m.EnableShareGPU("GPU-2") {
		t.Fatalf("expected devices marked false after Allocate")
	}
}

func Test_Sync_BuildsSnapshot_WithCache(t *testing.T) {
	t.Parallel()
	enable := ptrBool(true)
	spm := &fakeSPM{behaviors: map[types.UID]struct {
		baseline bool
		enable   *bool
		err      error
	}{
		types.UID("u1"): {baseline: false, enable: enable},
	}}
	bp := &BasePlugin{MetaServer: &metaserver.MetaServer{ServiceProfilingManager: spm}}

	// Build machine state: one device with two main containers of same pod
	ce := gpustate.ContainerEntries{
		"c1": makeContainer("u1", "ns", "pod", "c1", true, "GPU-1"),
		"c2": makeContainer("u1", "ns", "pod", "c2", true, "GPU-1"),
	}
	allocState := &gpustate.AllocationState{PodEntries: gpustate.PodEntries{"u1": ce}}
	machine := gpustate.AllocationResourcesMap{v1.ResourceName(gpuconsts.GPUDeviceType): gpustate.AllocationMap{"GPU-1": allocState}}

	m := NewShareGPUManager().(*shareGPUManager)
	m.basePlugin = &BasePlugin{MetaServer: bp.MetaServer, State: &fakeState{machine: machine}}
	m.sync(context.Background())

	if !m.EnableShareGPU("GPU-1") {
		t.Fatalf("expected GPU-1 shareable")
	}
	if spm.calls != 1 {
		t.Fatalf("expected 1 indicator call due to cache, got %d", spm.calls)
	}
}

func Test_Concurrency_Safety(t *testing.T) {
	t.Parallel()
	enable := ptrBool(true)
	spm := &fakeSPM{behaviors: map[types.UID]struct {
		baseline bool
		enable   *bool
		err      error
	}{
		types.UID("u1"): {baseline: false, enable: enable},
	}}
	bp := &BasePlugin{MetaServer: &metaserver.MetaServer{ServiceProfilingManager: spm}}
	m := NewShareGPUManager().(*shareGPUManager)
	m.basePlugin = bp

	alloc := makeContainer("u1", "ns", "pod", "c", true, "GPU-1")

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			m.Allocate(ctx, alloc)
			time.Sleep(time.Millisecond)
		}
	}()

	// Reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = m.EnableShareGPU("GPU-1")
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
}

func Test_Run_StartsAndStops(t *testing.T) {
	t.Parallel()
	enable := ptrBool(true)
	spm := &fakeSPM{behaviors: map[types.UID]struct {
		baseline bool
		enable   *bool
		err      error
	}{
		types.UID("u1"): {baseline: false, enable: enable},
	}}
	bp := &BasePlugin{MetaServer: &metaserver.MetaServer{ServiceProfilingManager: spm}, State: &fakeState{machine: gpustate.AllocationResourcesMap{v1.ResourceName(gpuconsts.GPUDeviceType): gpustate.AllocationMap{"GPU-1": &gpustate.AllocationState{PodEntries: gpustate.PodEntries{"u1": gpustate.ContainerEntries{"c": makeContainer("u1", "ns", "pod", "c", true, "GPU-1")}}}}}}}
	m := NewShareGPUManager()

	stopCh := make(chan struct{})
	go m.Run(bp, stopCh)
	time.Sleep(50 * time.Millisecond)
	close(stopCh)
}

func Test_Allocate_Ignores_NonMain(t *testing.T) {
	t.Parallel()
	spm := &fakeSPM{behaviors: map[types.UID]struct {
		baseline bool
		enable   *bool
		err      error
	}{}}
	bp := &BasePlugin{MetaServer: &metaserver.MetaServer{ServiceProfilingManager: spm}}
	m := NewShareGPUManager().(*shareGPUManager)
	m.basePlugin = bp

	alloc := makeContainer("u2", "ns", "pod", "c", false, "GPU-1")
	m.Allocate(context.Background(), alloc)
	if m.EnableShareGPU("GPU-1") {
		t.Fatalf("non-main container should not mark device")
	}
}

func ptrBool(b bool) *bool { v := b; return &v }

func BenchmarkSync_WithIndicatorCache(b *testing.B) {
	enable := ptrBool(true)
	spm := &fakeSPM{behaviors: map[types.UID]struct {
		baseline bool
		enable   *bool
		err      error
	}{
		types.UID("u1"): {baseline: false, enable: enable},
	}}
	bp := &BasePlugin{MetaServer: &metaserver.MetaServer{ServiceProfilingManager: spm}}

	// Build a large machine state: N devices, each with M main containers from the same pod
	const N, M = 50, 20
	am := make(gpustate.AllocationMap, N)
	for i := 0; i < N; i++ {
		ce := make(gpustate.ContainerEntries, M)
		for j := 0; j < M; j++ {
			cname := fmt.Sprintf("c%c", 'A'+j)
			ce[cname] = makeContainer("u1", "ns", "pod", cname, true, fmt.Sprintf("GPU-%c", 'A'+i))
		}
		am[fmt.Sprintf("GPU-%c", 'A'+i)] = &gpustate.AllocationState{PodEntries: gpustate.PodEntries{"u1": ce}}
	}
	machine := gpustate.AllocationResourcesMap{v1.ResourceName(gpuconsts.GPUDeviceType): am}

	m := NewShareGPUManager().(*shareGPUManager)
	m.basePlugin = &BasePlugin{MetaServer: bp.MetaServer, State: &fakeState{machine: machine}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		spm.calls = 0
		m.sync(context.Background())
	}

	// Expect only 1 external call per sync due to cache (one unique pod UID)
	if spm.calls != 1 {
		b.Fatalf("expected 1 indicator call per sync, got %d", spm.calls)
	}
}
