package overrides

import (
	"io"
	"net/http"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/tempo/pkg/sharedconfig"
	"github.com/grafana/tempo/pkg/spanfilter/config"
	"github.com/grafana/tempo/tempodb/backend"
)

type Service interface {
	services.Service
	Interface
}

type Interface interface {
	prometheus.Collector

	// Config
	IngestionRateStrategy() string
	MaxLocalTracesPerUser(userID string) int
	MaxGlobalTracesPerUser(userID string) int
	MaxBytesPerTrace(userID string) int
	Forwarders(userID string) []string
	MaxBytesPerTagValuesQuery(userID string) int
	MaxBlocksPerTagValuesQuery(userID string) int
	IngestionRateLimitBytes(userID string) float64
	IngestionBurstSizeBytes(userID string) int
	MetricsGeneratorIngestionSlack(userID string) time.Duration
	MetricsGeneratorRingSize(userID string) int
	MetricsGeneratorProcessors(userID string) map[string]struct{}
	MetricsGeneratorMaxActiveSeries(userID string) uint32
	MetricsGeneratorCollectionInterval(userID string) time.Duration
	MetricsGeneratorDisableCollection(userID string) bool
	MetricsGeneratorForwarderQueueSize(userID string) int
	MetricsGeneratorForwarderWorkers(userID string) int
	MetricsGeneratorProcessorServiceGraphsHistogramBuckets(userID string) []float64
	MetricsGeneratorProcessorServiceGraphsDimensions(userID string) []string
	MetricsGeneratorProcessorServiceGraphsPeerAttributes(userID string) []string
	MetricsGeneratorProcessorServiceGraphsEnableClientServerPrefix(userID string) bool
	MetricsGeneratorProcessorServiceGraphsEnableClientMetrics(userID string) bool
	MetricsGeneratorProcessorSpanMetricsHistogramBuckets(userID string) []float64
	MetricsGeneratorProcessorSpanMetricsDimensions(userID string) []string
	MetricsGeneratorProcessorSpanMetricsIntrinsicDimensions(userID string) map[string]bool
	MetricsGeneratorProcessorSpanMetricsFilterPolicies(userID string) []config.FilterPolicy
	MetricsGeneratorProcessorSpanMetricsDimensionMappings(userID string) []sharedconfig.DimensionMappings
	MetricsGeneratorProcessorSpanMetricsEnableTargetInfo(userID string) bool
	MetricsGeneratorProcessorSpanMetricsTargetInfoExcludedDimensions(userID string) []string
	MetricsGeneratorProcessorLocalBlocksMaxLiveTraces(userID string) uint64
	MetricsGeneratorProcessorLocalBlocksMaxBlockDuration(userID string) time.Duration
	MetricsGeneratorProcessorLocalBlocksMaxBlockBytes(userID string) uint64
	MetricsGeneratorProcessorLocalBlocksTraceIdlePeriod(userID string) time.Duration
	MetricsGeneratorProcessorLocalBlocksFlushCheckPeriod(userID string) time.Duration
	MetricsGeneratorProcessorLocalBlocksCompleteBlockTimeout(userID string) time.Duration
	BlockRetention(userID string) time.Duration
	MaxSearchDuration(userID string) time.Duration
	DedicatedColumns(userID string) backend.DedicatedColumns

	// API
	WriteStatusRuntimeConfig(w io.Writer, r *http.Request) error
}
