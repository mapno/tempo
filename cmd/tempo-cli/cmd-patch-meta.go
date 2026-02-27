package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"

	tempo_io "github.com/grafana/tempo/pkg/io"
	pq "github.com/grafana/tempo/pkg/parquetquery"

	"github.com/grafana/tempo/pkg/boundedwaitgroup"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vparquet4"
)

type patchMetaCmd struct {
	backendOptions

	TenantID string `arg:"" help:"tenant ID to patch blocks for"`
	Start    string `arg:"" help:"start time in RFC3339 (e.g. 2006-01-02T15:04:05Z07:00) or relative (e.g. now-1h) format"`
	End      string `arg:"" help:"end time in RFC3339 (e.g. 2006-01-02T15:04:05Z07:00) or relative (e.g. now) format"`
}

func (cmd *patchMetaCmd) Run(opts *globalOptions) error {
	r, _, _, err := loadBackend(&cmd.backendOptions, opts)
	if err != nil {
		return err
	}

	startTime, err := parseTime(cmd.Start)
	if err != nil {
		return err
	}
	endTime, err := parseTime(cmd.End)
	if err != nil {
		return err
	}

	ctx := context.Background()

	blockIDs, _, err := r.Blocks(ctx, cmd.TenantID)
	if err != nil {
		return err
	}

	fmt.Println("Total blocks:", len(blockIDs))

	// Load block metas in parallel and filter by time range
	wg := boundedwaitgroup.New(20)
	resultsCh := make(chan *backend.BlockMeta, len(blockIDs))
	for _, id := range blockIDs {
		wg.Add(1)

		go func(id2 uuid.UUID) {
			defer wg.Done()

			meta, err := r.BlockMeta(ctx, id2, cmd.TenantID)
			if errors.Is(err, backend.ErrDoesNotExist) {
				return
			}
			if err != nil {
				fmt.Println("Error reading block meta:", err)
				return
			}

			if meta.StartTime.Unix() <= endTime.Unix() &&
				meta.EndTime.Unix() >= startTime.Unix() {
				resultsCh <- meta
			}
		}(id)
	}

	wg.Wait()
	close(resultsCh)

	var blocks []*backend.BlockMeta
	for meta := range resultsCh {
		blocks = append(blocks, meta)
	}

	fmt.Println("Blocks in range:", len(blocks))

	// Filter to blocks with fewer than 10 dedicated columns per scope (faulty blocks)
	const minDedicatedColumnsPerScope = 10
	var faulty []*backend.BlockMeta
	for _, meta := range blocks {
		if maybeFaultyBlock(meta, minDedicatedColumnsPerScope) {
			faulty = append(faulty, meta)
		}
	}

	fmt.Println("Faulty blocks:", len(faulty))

	// Check each faulty block's parquet data for populated columns beyond what meta declares
	for _, meta := range faulty {
		populated, err := countPopulatedDedicatedColumns(ctx, r, meta)
		if err != nil {
			fmt.Printf("  %s error: %v\n", meta.BlockID, err)
			continue
		}

		metaCounts := dedicatedColumnCountsByScope(meta)
		needsFix := false
		for scope, popCount := range populated {
			if popCount > metaCounts[scope] {
				needsFix = true
				break
			}
		}

		if needsFix {
			fmt.Printf("  %s NEEDS FIX meta=%v populated=%v\n", meta.BlockID, metaCounts, populated)
		} else {
			fmt.Printf("  %s OK meta=%v populated=%v\n", meta.BlockID, metaCounts, populated)
		}
	}

	return nil
}

// dedicatedColumnCountsByScope returns a map of scope -> count for the block's dedicated columns.
func dedicatedColumnCountsByScope(meta *backend.BlockMeta) map[backend.DedicatedColumnScope]int {
	counts := make(map[backend.DedicatedColumnScope]int)
	for _, col := range meta.DedicatedColumns {
		counts[col.Scope]++
	}
	return counts
}

// maybeFaultyBlock returns true if any scope in the block has fewer than minPerScope dedicated columns.
func maybeFaultyBlock(meta *backend.BlockMeta, minPerScope int) bool {
	counts := dedicatedColumnCountsByScope(meta)
	for _, count := range counts {
		if count < minPerScope {
			return true
		}
	}
	// Also faulty if no dedicated columns at all
	return len(counts) == 0
}

// dedicatedColumnPaths returns the scope->type->paths mapping for vParquet4 blocks.
func dedicatedColumnPaths() map[backend.DedicatedColumnScope]map[backend.DedicatedColumnType][]string {
	return vparquet4.DedicatedResourceColumnPaths
}

// countPopulatedDedicatedColumns opens the parquet file for a block and counts how many
// dedicated column slots per scope actually contain non-null data.
func countPopulatedDedicatedColumns(ctx context.Context, r backend.Reader, meta *backend.BlockMeta) (map[backend.DedicatedColumnScope]int, error) {
	paths := dedicatedColumnPaths()

	rr := vparquet4.NewBackendReaderAt(ctx, r, vparquet4.DataFileName, meta)
	br := tempo_io.NewBufferedReaderAt(rr, int64(meta.Size_), 2*1024*1024, 64)
	pf, err := parquet.OpenFile(br, int64(meta.Size_), parquet.SkipBloomFilters(true))
	if err != nil {
		return nil, fmt.Errorf("opening parquet file: %w", err)
	}

	populated := make(map[backend.DedicatedColumnScope]int)

	for scope, byType := range paths {
		for _, columnPaths := range byType {
			for _, colPath := range columnPaths {
				if hasNonNullData(pf, colPath) {
					populated[scope]++
				}
			}
		}
	}

	return populated, nil
}

// hasNonNullData checks if a parquet column has any non-null data by inspecting
// the column index across all row groups.
func hasNonNullData(pf *parquet.File, colPath string) bool {
	colIdx, _, _ := pq.GetColumnIndexByPath(pf, colPath)
	if colIdx < 0 {
		return false
	}

	for _, rg := range pf.RowGroups() {
		cc := rg.ColumnChunks()[colIdx]
		ci, err := cc.ColumnIndex()
		if err != nil || ci == nil {
			continue
		}
		for i := 0; i < ci.NumPages(); i++ {
			if !ci.NullPage(i) {
				return true
			}
		}
	}

	return false
}
