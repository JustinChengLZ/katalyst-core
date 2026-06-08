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

package allocation

import (
	"time"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
)

// emitDuration emits the elapsed time since startTime as a raw float64
// millisecond metric and logs it. Safe to call with a nil emitter.
func emitDuration(emitter metrics.MetricEmitter, metricName string,
	startTime time.Time, tags ...metrics.MetricTag,
) {
	elapsed := time.Since(startTime)
	if emitter != nil {
		_ = emitter.StoreFloat64(metricName, float64(elapsed/time.Millisecond),
			metrics.MetricTypeNameRaw, tags...)
	}
	general.InfoS("finished", "metric", metricName, "duration", elapsed)
}

// timedFilteringStrategy wraps a FilteringStrategy and emits its Filter duration.
type timedFilteringStrategy struct {
	allocate.FilteringStrategy
	emitter    metrics.MetricEmitter
	metricName string
	tags       []metrics.MetricTag
}

// Filter overrides the promoted Filter() to add timing/metrics.
func (t *timedFilteringStrategy) Filter(ctx *allocate.AllocationContext, devices []string) ([]string, error) {
	start := time.Now()
	tags := append(t.tags, metrics.MetricTag{Key: "strategyName", Val: t.FilteringStrategy.Name()})
	defer emitDuration(t.emitter, t.metricName, start, tags...)
	return t.FilteringStrategy.Filter(ctx, devices)
}

// timedSortingStrategy wraps a SortingStrategy and emits its Sort duration.
type timedSortingStrategy struct {
	allocate.SortingStrategy
	emitter    metrics.MetricEmitter
	metricName string
	tags       []metrics.MetricTag
}

func (t *timedSortingStrategy) Sort(ctx *allocate.AllocationContext, devices []string) ([]string, error) {
	start := time.Now()
	tags := append(t.tags, metrics.MetricTag{Key: "strategyName", Val: t.SortingStrategy.Name()})
	defer emitDuration(t.emitter, t.metricName, start, tags...)
	return t.SortingStrategy.Sort(ctx, devices)
}

// timedBindingStrategy wraps a BindingStrategy and emits its Bind duration.
type timedBindingStrategy struct {
	allocate.BindingStrategy
	emitter    metrics.MetricEmitter
	metricName string
	tags       []metrics.MetricTag
}

func (t *timedBindingStrategy) Bind(ctx *allocate.AllocationContext, devices []string) (*allocate.AllocationResult, error) {
	start := time.Now()
	tags := append(t.tags, metrics.MetricTag{Key: "strategyName", Val: t.BindingStrategy.Name()})
	defer emitDuration(t.emitter, t.metricName, start, tags...)
	return t.BindingStrategy.Bind(ctx, devices)
}

// withFilterTiming returns a FilteringStrategy that emits filter duration metrics.
func withFilterTiming(fs allocate.FilteringStrategy, emitter metrics.MetricEmitter, tags ...metrics.MetricTag) allocate.FilteringStrategy {
	return &timedFilteringStrategy{
		FilteringStrategy: fs,
		emitter:           emitter,
		metricName:        util.MetricNameAllocateFilterDuration,
		tags:              tags,
	}
}

// withSortTiming returns a SortingStrategy that emits sort duration metrics.
func withSortTiming(ss allocate.SortingStrategy, emitter metrics.MetricEmitter, tags ...metrics.MetricTag) allocate.SortingStrategy {
	return &timedSortingStrategy{
		SortingStrategy: ss,
		emitter:         emitter,
		metricName:      util.MetricNameAllocateSortDuration,
		tags:            tags,
	}
}

// withBindTiming returns a BindingStrategy that emits bind duration metrics.
func withBindTiming(bs allocate.BindingStrategy, emitter metrics.MetricEmitter, tags ...metrics.MetricTag) allocate.BindingStrategy {
	return &timedBindingStrategy{
		BindingStrategy: bs,
		emitter:         emitter,
		metricName:      util.MetricNameAllocateBindDuration,
		tags:            tags,
	}
}
