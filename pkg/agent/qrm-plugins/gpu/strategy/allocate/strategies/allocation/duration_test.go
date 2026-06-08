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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/gpu/strategy/allocate"
	"github.com/kubewharf/katalyst-core/pkg/agent/qrm-plugins/util"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
)

// recordedSample captures a single StoreFloat64 call.
type recordedSample struct {
	name  string
	value float64
	tags  []metrics.MetricTag
}

// recordingEmitter is an in-test MetricEmitter that records StoreFloat64 calls.
type recordingEmitter struct {
	metrics.MetricEmitter
	samples []recordedSample
}

func newRecordingEmitter() *recordingEmitter {
	return &recordingEmitter{MetricEmitter: metrics.DummyMetrics{}}
}

func (r *recordingEmitter) StoreFloat64(name string, val float64, _ metrics.MetricTypeName, tags ...metrics.MetricTag) error {
	r.samples = append(r.samples, recordedSample{name: name, value: val, tags: tags})
	return nil
}

// stub strategies used to drive the wrappers without external dependencies.

type stubFilter struct {
	name string
	out  []string
	err  error
}

func (s *stubFilter) Name() string { return s.name }
func (s *stubFilter) Filter(_ *allocate.AllocationContext, _ []string) ([]string, error) {
	return s.out, s.err
}

type stubSort struct {
	name string
	out  []string
	err  error
}

func (s *stubSort) Name() string { return s.name }
func (s *stubSort) Sort(_ *allocate.AllocationContext, _ []string) ([]string, error) {
	return s.out, s.err
}

type stubBind struct {
	name string
	out  *allocate.AllocationResult
	err  error
}

func (s *stubBind) Name() string { return s.name }
func (s *stubBind) Bind(_ *allocate.AllocationContext, _ []string) (*allocate.AllocationResult, error) {
	return s.out, s.err
}

func TestEmitDuration_NilEmitter(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		emitDuration(nil, "x", time.Now())
	})
}

func TestEmitDuration_EmitsAndLogs(t *testing.T) {
	t.Parallel()

	emitter := newRecordingEmitter()
	tags := []metrics.MetricTag{{Key: "k", Val: "v"}}

	emitDuration(emitter, "test_metric", time.Now(), tags...)

	assert.Len(t, emitter.samples, 1)
	assert.Equal(t, "test_metric", emitter.samples[0].name)
	assert.Equal(t, tags, emitter.samples[0].tags)
}

func TestTimedFilteringStrategy_Filter(t *testing.T) {
	t.Parallel()

	emitter := newRecordingEmitter()
	inner := &stubFilter{name: "stub-filter", out: []string{"a", "b"}}
	tags := []metrics.MetricTag{{Key: "resourceName", Val: "gpu"}}

	wrapped := withFilterTiming(inner, emitter, tags...)
	assert.Equal(t, "stub-filter", wrapped.Name())

	got, err := wrapped.Filter(&allocate.AllocationContext{}, []string{"x"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, got)

	assert.Len(t, emitter.samples, 1)
	assert.Equal(t, util.MetricNameAllocateFilterDuration, emitter.samples[0].name)
	assert.Equal(t, append(tags, metrics.MetricTag{Key: "strategyName", Val: "stub-filter"}), emitter.samples[0].tags)
}

func TestTimedFilteringStrategy_Filter_Error(t *testing.T) {
	t.Parallel()

	emitter := newRecordingEmitter()
	wantErr := errors.New("filter failed")
	inner := &stubFilter{name: "stub-filter", err: wantErr}

	wrapped := withFilterTiming(inner, emitter)
	_, err := wrapped.Filter(&allocate.AllocationContext{}, nil)

	assert.ErrorIs(t, err, wantErr)
	assert.Len(t, emitter.samples, 1)
	assert.Equal(t, util.MetricNameAllocateFilterDuration, emitter.samples[0].name)
}

func TestTimedSortingStrategy_Sort(t *testing.T) {
	t.Parallel()

	emitter := newRecordingEmitter()
	inner := &stubSort{name: "stub-sort", out: []string{"a", "b"}}
	tags := []metrics.MetricTag{{Key: "strategy", Val: "generic"}}

	wrapped := withSortTiming(inner, emitter, tags...)
	assert.Equal(t, "stub-sort", wrapped.Name())

	got, err := wrapped.Sort(&allocate.AllocationContext{}, []string{"x"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, got)

	assert.Len(t, emitter.samples, 1)
	assert.Equal(t, util.MetricNameAllocateSortDuration, emitter.samples[0].name)
	assert.Equal(t, append(tags, metrics.MetricTag{Key: "strategyName", Val: "stub-sort"}), emitter.samples[0].tags)
}

func TestTimedSortingStrategy_Sort_Error(t *testing.T) {
	t.Parallel()

	emitter := newRecordingEmitter()
	wantErr := errors.New("sort failed")
	inner := &stubSort{name: "stub-sort", err: wantErr}

	wrapped := withSortTiming(inner, emitter)
	_, err := wrapped.Sort(&allocate.AllocationContext{}, nil)

	assert.ErrorIs(t, err, wantErr)
	assert.Len(t, emitter.samples, 1)
	assert.Equal(t, util.MetricNameAllocateSortDuration, emitter.samples[0].name)
}

func TestTimedBindingStrategy_Bind(t *testing.T) {
	t.Parallel()

	emitter := newRecordingEmitter()
	want := &allocate.AllocationResult{Success: true, AllocatedDevices: []string{"gpu-1"}}
	inner := &stubBind{name: "stub-bind", out: want}
	tags := []metrics.MetricTag{{Key: "resourceName", Val: "gpu"}}

	wrapped := withBindTiming(inner, emitter, tags...)
	assert.Equal(t, "stub-bind", wrapped.Name())

	got, err := wrapped.Bind(&allocate.AllocationContext{}, []string{"x"})
	assert.NoError(t, err)
	assert.Equal(t, want, got)

	assert.Len(t, emitter.samples, 1)
	assert.Equal(t, util.MetricNameAllocateBindDuration, emitter.samples[0].name)
	assert.Equal(t, append(tags, metrics.MetricTag{Key: "strategyName", Val: "stub-bind"}), emitter.samples[0].tags)
}

func TestTimedBindingStrategy_Bind_Error(t *testing.T) {
	t.Parallel()

	emitter := newRecordingEmitter()
	wantErr := errors.New("bind failed")
	inner := &stubBind{name: "stub-bind", err: wantErr}

	wrapped := withBindTiming(inner, emitter)
	_, err := wrapped.Bind(&allocate.AllocationContext{}, nil)

	assert.ErrorIs(t, err, wantErr)
	assert.Len(t, emitter.samples, 1)
	assert.Equal(t, util.MetricNameAllocateBindDuration, emitter.samples[0].name)
}
