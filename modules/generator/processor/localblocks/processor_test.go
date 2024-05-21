package localblocks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/wal"
	"github.com/stretchr/testify/require"
)

type mockOverrides struct{}

var _ ProcessorOverrides = (*mockOverrides)(nil)

func (m *mockOverrides) DedicatedColumns(string) backend.DedicatedColumns {
	return nil
}

func (m *mockOverrides) MaxBytesPerTrace(string) int {
	return 0
}

func (m *mockOverrides) UnsafeQueryHints(string) bool {
	return false
}

var _ tempodb.Writer = (*mockWriter)(nil)

type mockWriter struct {
	blocks []*backend.BlockMeta
}

func (m *mockWriter) WriteBlock(_ context.Context, b tempodb.WriteableBlock) error {
	m.blocks = append(m.blocks, b.BlockMeta())
	return nil
}

func (m *mockWriter) CompleteBlock(context.Context, common.WALBlock) (common.BackendBlock, error) {
	return nil, nil
}

func (m *mockWriter) CompleteBlockWithBackend(context.Context, common.WALBlock, backend.Reader, backend.Writer) (common.BackendBlock, error) {
	return nil, nil
}

func (m *mockWriter) WAL() *wal.WAL { return nil }

func TestProcessorDoesNotRace(t *testing.T) {
	wal, err := wal.New(&wal.Config{
		Filepath: t.TempDir(),
		Version:  encoding.DefaultEncoding().Version(),
	})
	require.NoError(t, err)

	var (
		ctx    = context.Background()
		tenant = "fake"
		cfg    = Config{
			FlushCheckPeriod:     10 * time.Millisecond,
			TraceIdlePeriod:      time.Second,
			CompleteBlockTimeout: time.Minute,
			Block: &common.BlockConfig{
				BloomShardSizeBytes: 100_000,
				BloomFP:             0.05,
				Version:             encoding.DefaultEncoding().Version(),
			},
			Metrics: MetricsConfig{
				ConcurrentBlocks:  10,
				TimeOverlapCutoff: 0.2,
			},
		}
		overrides = &mockOverrides{}
	)

	p, err := New(cfg, tenant, wal, &mockWriter{}, overrides)
	require.NoError(t, err)

	var (
		end = make(chan struct{})
		wg  = sync.WaitGroup{}
	)

	concurrent := func(f func()) {
		wg.Add(1)
		defer wg.Done()

		for {
			select {
			case <-end:
				return
			default:
				f()
			}
		}
	}

	go concurrent(func() {
		tr := test.MakeTrace(10, nil)
		for _, b := range tr.Batches {
			for _, ss := range b.ScopeSpans {
				for _, s := range ss.Spans {
					s.Kind = v1.Span_SPAN_KIND_SERVER
				}
			}
		}

		req := &tempopb.PushSpansRequest{
			Batches: tr.Batches,
		}
		p.PushSpans(ctx, req)
	})

	go concurrent(func() {
		err := p.cutIdleTraces(true)
		require.NoError(t, err, "cutting idle traces")
	})

	go concurrent(func() {
		err := p.cutBlocks(true)
		require.NoError(t, err, "cutting blocks")
	})

	go concurrent(func() {
		err := p.completeBlock()
		require.NoError(t, err, "completing block")
	})

	go concurrent(func() {
		err := p.flushBlock(uuid.New())
		require.NoError(t, err, "flushing blocks")
	})

	go concurrent(func() {
		err := p.deleteOldBlocks()
		require.NoError(t, err, "deleting old blocks")
	})

	// Run multiple queries
	go concurrent(func() {
		_, err := p.GetMetrics(ctx, &tempopb.SpanMetricsRequest{
			Query:   "{}",
			GroupBy: "status",
		})
		require.NoError(t, err)
	})

	go concurrent(func() {
		_, err := p.GetMetrics(ctx, &tempopb.SpanMetricsRequest{
			Query:   "{}",
			GroupBy: "status",
		})
		require.NoError(t, err)
	})

	go concurrent(func() {
		_, err := p.QueryRange(ctx, &tempopb.QueryRangeRequest{
			Query: "{} | rate() by (resource.service.name)",
			Start: uint64(time.Now().Add(-5 * time.Minute).UnixNano()),
			End:   uint64(time.Now().UnixNano()),
			Step:  uint64(30 * time.Second),
		})
		require.NoError(t, err)
	})

	// Run for a bit
	time.Sleep(2000 * time.Millisecond)

	// Cleanup
	close(end)
	wg.Wait()
	p.Shutdown(ctx)
}

func TestReplicationFactor(t *testing.T) {
	wal, err := wal.New(&wal.Config{
		Filepath: t.TempDir(),
		Version:  encoding.DefaultEncoding().Version(),
	})
	require.NoError(t, err)

	cfg := Config{
		FlushCheckPeriod:     time.Minute,
		TraceIdlePeriod:      time.Minute,
		CompleteBlockTimeout: time.Minute,
		Block: &common.BlockConfig{
			BloomShardSizeBytes: 100_000,
			BloomFP:             0.05,
			Version:             encoding.DefaultEncoding().Version(),
		},
		Metrics: MetricsConfig{
			ConcurrentBlocks:  10,
			TimeOverlapCutoff: 0.2,
		},
		FilterServerSpans: false,
	}

	mockWriter := &mockWriter{}

	p, err := New(cfg, "fake", wal, mockWriter, &mockOverrides{})
	require.NoError(t, err)

	tr := test.MakeTrace(10, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	p.PushSpans(context.TODO(), &tempopb.PushSpansRequest{
		Batches: tr.Batches,
	})

	require.NoError(t, p.cutIdleTraces(true))
	verifyReplicationFactor(t, p.headBlock)

	require.NoError(t, p.cutBlocks(true))
	for _, b := range p.walBlocks {
		verifyReplicationFactor(t, b)
	}

	require.NoError(t, p.completeBlock())
	for _, b := range p.completeBlocks {
		verifyReplicationFactor(t, b)
	}

	require.Eventually(t, func() bool {
		return len(mockWriter.blocks) == 1
	}, time.Second, 10*time.Millisecond)
	verifyReplicationFactor(t, &mockBlock{meta: mockWriter.blocks[0]})
}

func verifyReplicationFactor(t *testing.T, b common.BackendBlock) {
	require.Equal(t, 1, int(b.BlockMeta().ReplicationFactor))
}

var _ common.BackendBlock = (*mockBlock)(nil)

type mockBlock struct {
	meta *backend.BlockMeta
}

func (m *mockBlock) FindTraceByID(context.Context, common.ID, common.SearchOptions) (*tempopb.Trace, error) {
	return nil, nil
}

func (m *mockBlock) Search(context.Context, *tempopb.SearchRequest, common.SearchOptions) (*tempopb.SearchResponse, error) {
	return nil, nil
}

func (m *mockBlock) SearchTags(context.Context, traceql.AttributeScope, common.TagCallback, common.SearchOptions) error {
	return nil
}

func (m *mockBlock) SearchTagValues(context.Context, string, common.TagCallback, common.SearchOptions) error {
	return nil
}

func (m *mockBlock) SearchTagValuesV2(context.Context, traceql.Attribute, common.TagCallbackV2, common.SearchOptions) error {
	return nil
}

func (m *mockBlock) Fetch(context.Context, traceql.FetchSpansRequest, common.SearchOptions) (traceql.FetchSpansResponse, error) {
	return traceql.FetchSpansResponse{}, nil
}

func (m *mockBlock) FetchTagValues(context.Context, traceql.AutocompleteRequest, traceql.AutocompleteCallback, common.SearchOptions) error {
	return nil
}

func (m *mockBlock) BlockMeta() *backend.BlockMeta { return m.meta }
