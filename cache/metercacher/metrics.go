// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metercacher

import (
	"github.com/luxfi/metric"
)

const (
	resultLabel = "result"
	hitResult   = "hit"
	missResult  = "miss"
)

var (
	resultLabels = []string{resultLabel}
	hitLabels    = metric.Labels{
		resultLabel: hitResult,
	}
	missLabels = metric.Labels{
		resultLabel: missResult,
	}
)

type cacheMetrics struct {
	getCount metric.CounterVec
	getTime  metric.GaugeVec

	putCount metric.Counter
	putTime  metric.Gauge

	len           metric.Gauge
	portionFilled metric.Gauge
}

func newMetrics(
	namespace string,
	registry metric.Registry,
) (*cacheMetrics, error) {
	metricsInstance := metric.NewWithRegistry(namespace, registry)

	m := &cacheMetrics{
		getCount: metricsInstance.NewCounterVec(
			"get_count",
			"number of get calls",
			resultLabels,
		),
		getTime: metricsInstance.NewGaugeVec(
			"get_time",
			"time spent (ns) in get calls",
			resultLabels,
		),
		putCount: metricsInstance.NewCounter(
			"put_count",
			"number of put calls",
		),
		putTime: metricsInstance.NewGauge(
			"put_time",
			"time spent (ns) in put calls",
		),
		len: metricsInstance.NewGauge(
			"len",
			"number of entries",
		),
		portionFilled: metricsInstance.NewGauge(
			"portion_filled",
			"fraction of cache filled",
		),
	}
	return m, nil
}