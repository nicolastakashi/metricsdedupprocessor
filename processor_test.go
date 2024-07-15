package metricsdedupprocessor

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestDedupNoDelayedReplica(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &Config{
		ReplicaLabel: "replica",
		SwapTimeout:  1 * time.Minute,
	}

	testCases := []struct {
		name string
	}{
		{
			name: "no_replica_label",
		},
		{
			name: "same_replica",
		},
		{
			name: "different_replica_active_replica",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			next := &consumertest.MetricsSink{}
			factory := NewFactory()
			mgp, err := factory.CreateMetricsProcessor(
				ctx,
				processortest.NewNopSettings(),
				config,
				next,
			)

			require.NoError(t, err)
			dir := filepath.Join("testdata", tc.name)

			md, err := golden.ReadMetrics(filepath.Join(dir, "input.yaml"))
			require.NoError(t, err)

			// Test that ConsumeMetrics works
			err = mgp.ConsumeMetrics(ctx, md)
			require.NoError(t, err)

			allMetrics := next.AllMetrics()
			require.Len(t, allMetrics, 1)

			expectedNextData, err := golden.ReadMetrics(filepath.Join(dir, "next.yaml"))
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedNextData, allMetrics[0]))
		})
	}
}

func TestDedupDelayedReplica(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &Config{
		ReplicaLabel: "replica",
		SwapTimeout:  1 * time.Second,
	}

	testCases := []struct {
		name string
	}{
		{
			name: "swap_replica",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			next := &consumertest.MetricsSink{}
			factory := NewFactory()
			mgp, err := factory.CreateMetricsProcessor(
				ctx,
				processortest.NewNopSettings(),
				config,
				next,
			)

			require.NoError(t, err)
			dir := filepath.Join("testdata", tc.name)

			md, err := golden.ReadMetrics(filepath.Join(dir, "first.yaml"))
			require.NoError(t, err)

			// Test that ConsumeMetrics works
			err = mgp.ConsumeMetrics(ctx, md)
			require.NoError(t, err)

			time.Sleep(3 * time.Second)

			md, err = golden.ReadMetrics(filepath.Join(dir, "second.yaml"))
			require.NoError(t, err)

			err = mgp.ConsumeMetrics(ctx, md)
			require.NoError(t, err)

			allMetrics := next.AllMetrics()
			require.Len(t, allMetrics, 2)

			expectedNextData, err := golden.ReadMetrics(filepath.Join(dir, "next.yaml"))
			require.NoError(t, err)
			require.NoError(t, pmetrictest.CompareMetrics(expectedNextData, allMetrics[1]))
		})
	}
}
