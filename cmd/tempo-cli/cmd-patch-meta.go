package main

import (
	"context"
	"encoding/json"
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

	TenantID        string `arg:"" help:"tenant ID to patch blocks for"`
	Start           string `arg:"" help:"start time in RFC3339 (e.g. 2006-01-02T15:04:05Z07:00) or relative (e.g. now-1h) format"`
	End             string `arg:"" help:"end time in RFC3339 (e.g. 2006-01-02T15:04:05Z07:00) or relative (e.g. now) format"`
	DedicatedColCfg string `arg:"" help:"dedicated columns config as JSON array"`
}

func (cmd *patchMetaCmd) Run(opts *globalOptions) error {
	var newDedicatedColumns backend.DedicatedColumns
	if err := json.Unmarshal([]byte(cmd.DedicatedColCfg), &newDedicatedColumns); err != nil {
		return fmt.Errorf("parsing dedicated columns JSON: %w", err)
	}
	fmt.Println("New dedicated columns config:", len(newDedicatedColumns), "columns")
	b, _ := newDedicatedColumns.Marshal()
	fmt.Println(string(b))
	
	r, w, _, err := loadBackend(&cmd.backendOptions, opts)
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
		needsFix, err := hasUndeclaredPopulatedColumns(ctx, r, meta)
		if err != nil {
			fmt.Printf("  %s error: %v\n", meta.BlockID, err)
			continue
		}

		metaCounts := dedicatedColumnCountsByScope(meta)
		if !needsFix {
			fmt.Printf("  %s OK start=%s end=%s meta=%v\n", meta.BlockID, meta.StartTime.String(), meta.EndTime.String(), metaCounts)
			continue
		}

		fmt.Printf("  %s NEEDS FIX start=%s end=%s meta=%v\n", meta.BlockID, meta.StartTime.String(), meta.EndTime.String(), metaCounts)

		// Backup the existing meta.json
		if err := backupBlockMeta(ctx, r, w, meta); err != nil {
			fmt.Printf("  %s backup error: %v\n", meta.BlockID, err)
			continue
		}
		fmt.Printf("  %s backed up meta.json -> meta.json.backup\n", meta.BlockID)

		// Patch the dedicated columns and write the updated meta
		meta.DedicatedColumns = newDedicatedColumns
		if err := w.WriteBlockMeta(ctx, meta); err != nil {
			fmt.Printf("  %s write error: %v\n", meta.BlockID, err)
			continue
		}
		fmt.Printf("  %s patched meta.json with %d dedicated columns\n", meta.BlockID, len(newDedicatedColumns))
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

type scopeType struct {
	scope  backend.DedicatedColumnScope
	colTyp backend.DedicatedColumnType
}

// dedicatedColumnCountsByScopeAndType returns a map of (scope, type) -> count.
func dedicatedColumnCountsByScopeAndType(meta *backend.BlockMeta) map[scopeType]int {
	counts := make(map[scopeType]int)
	for _, col := range meta.DedicatedColumns {
		counts[scopeType{col.Scope, col.Type}]++
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

// hasUndeclaredPopulatedColumns opens the parquet file for a block and checks whether any
// dedicated column slots beyond what the meta declares contain non-null data.
// For example, if the meta declares 5 resource-string columns (String01-String05),
// this checks if String06-String10 have any non-null data.
func hasUndeclaredPopulatedColumns(ctx context.Context, r backend.Reader, meta *backend.BlockMeta) (bool, error) {
	rr := vparquet4.NewBackendReaderAt(ctx, r, vparquet4.DataFileName, meta)
	br := tempo_io.NewBufferedReaderAt(rr, int64(meta.Size_), 2*1024*1024, 64)
	pf, err := parquet.OpenFile(br, int64(meta.Size_), parquet.SkipBloomFilters(true))
	if err != nil {
		return false, fmt.Errorf("opening parquet file: %w", err)
	}

	metaCounts := dedicatedColumnCountsByScopeAndType(meta)

	for scope, byType := range vparquet4.DedicatedResourceColumnPaths {
		for colType, columnPaths := range byType {
			declared := metaCounts[scopeType{scope, colType}]
			// Only check columns beyond what the meta declares
			for i := declared; i < len(columnPaths); i++ {
				if hasNonNullData(pf, columnPaths[i]) {
					return true, nil
				}
			}
		}
	}

	return false, nil
}

const backupMetaName = "meta.json.backup"

// backupBlockMeta reads the raw meta.json and writes it as meta.json.backup.
func backupBlockMeta(ctx context.Context, r backend.Reader, w backend.Writer, meta *backend.BlockMeta) error {
	metaBytes, err := r.Read(ctx, backend.MetaName, uuid.UUID(meta.BlockID), meta.TenantID, nil)
	if err != nil {
		return fmt.Errorf("reading meta.json: %w", err)
	}

	err = w.Write(ctx, backupMetaName, uuid.UUID(meta.BlockID), meta.TenantID, metaBytes, nil)
	if err != nil {
		return fmt.Errorf("writing meta.json.backup: %w", err)
	}

	return nil
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
