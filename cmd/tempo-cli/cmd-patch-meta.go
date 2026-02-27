package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

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

	Start           string `arg:"" help:"start time in RFC3339 (e.g. 2006-01-02T15:04:05Z07:00) or relative (e.g. now-1h) format"`
	End             string `arg:"" help:"end time in RFC3339 (e.g. 2006-01-02T15:04:05Z07:00) or relative (e.g. now) format"`
	TenantID        string `help:"tenant ID to patch blocks for (requires --dedicated-col-cfg)" short:"t"`
	DedicatedColCfg string `help:"dedicated columns config as JSON array (requires --tenant-id)" short:"d"`
	PatchCfgFile    string `help:"path to JSON config file mapping tenant IDs to dedicated columns" short:"p" type:"existingfile"`
}

// tenantDedicatedColumns maps tenant ID -> dedicated columns config.
type tenantDedicatedColumns map[string]backend.DedicatedColumns

func (cmd *patchMetaCmd) Run(opts *globalOptions) error {
	tenantConfigs, err := cmd.loadTenantConfigs()
	if err != nil {
		return err
	}

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

	for tenantID, newDedicatedColumns := range tenantConfigs {
		fmt.Printf("\n=== Tenant: %s (%d dedicated columns) ===\n", tenantID, len(newDedicatedColumns))

		if err := cmd.processTenant(ctx, r, w, tenantID, startTime, endTime, newDedicatedColumns); err != nil {
			fmt.Printf("  tenant %s error: %v\n", tenantID, err)
			continue
		}
	}

	return nil
}

// loadTenantConfigs parses tenant -> dedicated columns from either CLI args or a config file.
func (cmd *patchMetaCmd) loadTenantConfigs() (tenantDedicatedColumns, error) {
	hasInline := cmd.TenantID != "" || cmd.DedicatedColCfg != ""
	hasFile := cmd.PatchCfgFile != ""

	if hasInline && hasFile {
		return nil, fmt.Errorf("use either --tenant-id/--dedicated-col-cfg or --patch-cfg-file, not both")
	}
	if !hasInline && !hasFile {
		return nil, fmt.Errorf("provide either --tenant-id and --dedicated-col-cfg, or --patch-cfg-file")
	}

	if hasFile {
		return loadPatchCfgFile(cmd.PatchCfgFile)
	}

	// Inline mode
	if cmd.TenantID == "" {
		return nil, fmt.Errorf("--tenant-id is required when using --dedicated-col-cfg")
	}
	if cmd.DedicatedColCfg == "" {
		return nil, fmt.Errorf("--dedicated-col-cfg is required when using --tenant-id")
	}

	var cols backend.DedicatedColumns
	if err := json.Unmarshal([]byte(cmd.DedicatedColCfg), &cols); err != nil {
		return nil, fmt.Errorf("parsing dedicated columns JSON: %w", err)
	}

	return tenantDedicatedColumns{cmd.TenantID: cols}, nil
}

func loadPatchCfgFile(path string) (tenantDedicatedColumns, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg tenantDedicatedColumns
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	if len(cfg) == 0 {
		return nil, fmt.Errorf("config file contains no tenant entries")
	}

	return cfg, nil
}

func (cmd *patchMetaCmd) processTenant(
	ctx context.Context,
	r backend.Reader,
	w backend.Writer,
	tenantID string,
	startTime, endTime time.Time,
	newDedicatedColumns backend.DedicatedColumns,
) error {
	blockIDs, _, err := r.Blocks(ctx, tenantID)
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

			meta, err := r.BlockMeta(ctx, id2, tenantID)
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
