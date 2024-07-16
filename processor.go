package metricsdedupprocessor

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type Processor struct {
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger

	md pmetric.Metrics

	nextConsumer consumer.Metrics
	replicaLabel string
	swapTimeout  time.Duration
	replica      atomic.Value
}

func newProcessor(config *Config, log *zap.Logger, nextConsumer consumer.Metrics) *Processor {
	ctx, cancel := context.WithCancel(context.Background())

	p := &Processor{
		ctx:          ctx,
		cancel:       cancel,
		logger:       log,
		md:           pmetric.NewMetrics(),
		nextConsumer: nextConsumer,
		replicaLabel: config.ReplicaLabel,
		swapTimeout:  config.SwapTimeout,
	}

	p.replica.Store(&replicaInfo{replica: pcommon.NewValueEmpty(), timestamp: -1})

	return p
}

func (p *Processor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}
func (p *Processor) Start(_ context.Context, _ component.Host) error {
	return nil
}

func (p *Processor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	now := time.Now()
	var replica pcommon.Value
	var ok bool

	shouldRemove := func(attributes pcommon.Map) bool {
		if replica, ok = attributes.Get(p.replicaLabel); !ok {
			// if the replica is not set, we keep the Metric
			return false
		}

		if replicaInUse := p.getInUseReplica(replica, now); replicaInUse == replica {
			// if the replica is in use, we keep the Metric
			return false
		}

		return true
	}

	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				switch m.Type() {
				case pmetric.MetricTypeGauge:
					m.Gauge().DataPoints().
						RemoveIf(func(ndp pmetric.NumberDataPoint) bool {
							return shouldRemove(ndp.Attributes())
						})
					return m.Gauge().DataPoints().Len() == 0
				case pmetric.MetricTypeSum:
					m.Sum().DataPoints().
						RemoveIf(func(ndp pmetric.NumberDataPoint) bool {
							return shouldRemove(ndp.Attributes())
						})
					return m.Sum().DataPoints().Len() == 0
				case pmetric.MetricTypeHistogram:
					m.Histogram().DataPoints().
						RemoveIf(func(hdp pmetric.HistogramDataPoint) bool {
							return shouldRemove(hdp.Attributes())
						})
					return m.Histogram().DataPoints().Len() == 0
				case pmetric.MetricTypeSummary:
					m.Summary().DataPoints().
						RemoveIf(func(sdp pmetric.SummaryDataPoint) bool {
							return shouldRemove(sdp.Attributes())
						})
					return m.Summary().DataPoints().Len() == 0
				case pmetric.MetricTypeExponentialHistogram:
					m.ExponentialHistogram().DataPoints().
						RemoveIf(func(ehdp pmetric.ExponentialHistogramDataPoint) bool {
							return shouldRemove(ehdp.Attributes())
						})
					return m.ExponentialHistogram().DataPoints().Len() == 0
				}
				return false
			})
			return sm.Metrics().Len() == 0
		})

		return rm.ScopeMetrics().Len() == 0
	})

	if err := p.nextConsumer.ConsumeMetrics(ctx, md); err != nil {
		// we fail fast: if we get an error from the next, we break the processing for this batch
		return err
	}
	return nil
}

func (p *Processor) Shutdown(_ context.Context) error {
	p.cancel()
	return nil
}

type replicaInfo struct {
	replica   pcommon.Value
	timestamp int64
}

func (p *Processor) getInUseReplica(replica pcommon.Value, now time.Time) pcommon.Value {
	current := p.replica.Load().(*replicaInfo)

	// if we don't have a replica, we just set it
	if current.replica.AsString() == "" {
		p.replica.CompareAndSwap(current, &replicaInfo{replica, now.UnixNano()})
		return replica
	}

	// if the replica is the same, we just update the timestamp
	if current.replica.AsString() == replica.AsString() {
		p.replica.CompareAndSwap(current, &replicaInfo{replica, now.UnixNano()})
		return replica
	}

	// if the replica is different, we check if the timestamp is older than 10 seconds
	// if it is, we update the replica
	if now.UnixNano()-current.timestamp > p.swapTimeout.Nanoseconds() {
		p.replica.CompareAndSwap(current, &replicaInfo{replica, now.UnixNano()})
		return replica
	}

	// if the replica is different and the timestamp is newer than 10 seconds, we return the current replica
	return current.replica
}
