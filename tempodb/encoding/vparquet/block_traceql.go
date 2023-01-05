package vparquet

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"

	"github.com/grafana/tempo/pkg/parquetquery"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

// Helper function to create an iterator,
// that abstracts away context like file and rowgroups.
type makeIterFn func(columnName string, predicate parquetquery.Predicate, selectAs string) parquetquery.Iterator

const (
	columnPathTraceID                  = "TraceID"
	columnPathStartTimeUnixNano        = "StartTimeUnixNano"
	columnPathDurationNanos            = "DurationNanos"
	columnPathRootSpanName             = "RootSpanName"
	columnPathRootServiceName          = "RootServiceName"
	columnPathResourceAttrKey          = "rs.Resource.Attrs.Key"
	columnPathResourceAttrString       = "rs.Resource.Attrs.Value"
	columnPathResourceAttrInt          = "rs.Resource.Attrs.ValueInt"
	columnPathResourceAttrDouble       = "rs.Resource.Attrs.ValueDouble"
	columnPathResourceAttrBool         = "rs.Resource.Attrs.ValueBool"
	columnPathResourceServiceName      = "rs.Resource.ServiceName"
	columnPathResourceCluster          = "rs.Resource.Cluster"
	columnPathResourceNamespace        = "rs.Resource.Namespace"
	columnPathResourcePod              = "rs.Resource.Pod"
	columnPathResourceContainer        = "rs.Resource.Container"
	columnPathResourceK8sClusterName   = "rs.Resource.K8sClusterName"
	columnPathResourceK8sNamespaceName = "rs.Resource.K8sNamespaceName"
	columnPathResourceK8sPodName       = "rs.Resource.K8sPodName"
	columnPathResourceK8sContainerName = "rs.Resource.K8sContainerName"

	columnPathSpanID        = "rs.ils.Spans.ID"
	columnPathSpanName      = "rs.ils.Spans.Name"
	columnPathSpanStartTime = "rs.ils.Spans.StartUnixNanos"
	columnPathSpanEndTime   = "rs.ils.Spans.EndUnixNanos"
	// columnPathSpanDuration       = "rs.ils.Spans.DurationNanos"
	columnPathSpanStatusCode     = "rs.ils.Spans.StatusCode"
	columnPathSpanAttrKey        = "rs.ils.Spans.Attrs.Key"
	columnPathSpanAttrString     = "rs.ils.Spans.Attrs.Value"
	columnPathSpanAttrInt        = "rs.ils.Spans.Attrs.ValueInt"
	columnPathSpanAttrDouble     = "rs.ils.Spans.Attrs.ValueDouble"
	columnPathSpanAttrBool       = "rs.ils.Spans.Attrs.ValueBool"
	columnPathSpanHTTPStatusCode = "rs.ils.Spans.HttpStatusCode"
	columnPathSpanHTTPMethod     = "rs.ils.Spans.HttpMethod"
	columnPathSpanHTTPURL        = "rs.ils.Spans.HttpUrl"
)

var intrinsicColumnLookups = map[traceql.Intrinsic]struct {
	scope      traceql.AttributeScope
	typ        traceql.StaticType
	columnPath string
}{
	traceql.IntrinsicName:     {traceql.AttributeScopeSpan, traceql.TypeString, columnPathSpanName},
	traceql.IntrinsicStatus:   {traceql.AttributeScopeSpan, traceql.TypeStatus, columnPathSpanStatusCode},
	traceql.IntrinsicDuration: {traceql.AttributeScopeSpan, traceql.TypeDuration, ""},
}

// Lookup table of all well-known attributes with dedicated columns
var wellKnownColumnLookups = map[string]struct {
	columnPath string                 // path.to.column
	level      traceql.AttributeScope // span or resource level
	typ        traceql.StaticType     // Data type
}{
	// Resource-level columns
	LabelServiceName:      {columnPathResourceServiceName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelCluster:          {columnPathResourceCluster, traceql.AttributeScopeResource, traceql.TypeString},
	LabelNamespace:        {columnPathResourceNamespace, traceql.AttributeScopeResource, traceql.TypeString},
	LabelPod:              {columnPathResourcePod, traceql.AttributeScopeResource, traceql.TypeString},
	LabelContainer:        {columnPathResourceContainer, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sClusterName:   {columnPathResourceK8sClusterName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sNamespaceName: {columnPathResourceK8sNamespaceName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sPodName:       {columnPathResourceK8sPodName, traceql.AttributeScopeResource, traceql.TypeString},
	LabelK8sContainerName: {columnPathResourceK8sContainerName, traceql.AttributeScopeResource, traceql.TypeString},

	// Span-level columns
	LabelHTTPStatusCode: {columnPathSpanHTTPStatusCode, traceql.AttributeScopeSpan, traceql.TypeInt},
	LabelHTTPMethod:     {columnPathSpanHTTPMethod, traceql.AttributeScopeSpan, traceql.TypeString},
	LabelHTTPUrl:        {columnPathSpanHTTPURL, traceql.AttributeScopeSpan, traceql.TypeString},
}

// Fetch spansets from the block for the given TraceQL FetchSpansRequest. The request is checked for
// internal consistencies:  operand count matches the operation, all operands in each condition are identical
// types, and the operand type is compatible with the operation.
func (b *backendBlock) Fetch(ctx context.Context, req traceql.FetchSpansRequest, opts common.SearchOptions) (traceql.FetchSpansResponse, error) {

	err := checkConditions(req.Conditions)
	if err != nil {
		return traceql.FetchSpansResponse{}, errors.Wrap(err, "conditions invalid")
	}

	pf, rr, err := b.openForSearch(ctx, opts)
	if err != nil {
		return traceql.FetchSpansResponse{}, err
	}

	iter, err := fetch(ctx, req, pf, opts)
	if err != nil {
		return traceql.FetchSpansResponse{}, errors.Wrap(err, "creating fetch iter")
	}

	return traceql.FetchSpansResponse{
		Results: iter,
		Bytes:   func() uint64 { return rr.TotalBytesRead.Load() },
	}, nil
}

func checkConditions(conditions []traceql.Condition) error {
	for _, cond := range conditions {
		opCount := len(cond.Operands)

		switch cond.Op {

		case traceql.OpNone:
			if opCount != 0 {
				return fmt.Errorf("operanion none must have 0 arguments. condition: %+v", cond)
			}

		case traceql.OpEqual, traceql.OpNotEqual,
			traceql.OpGreater, traceql.OpGreaterEqual,
			traceql.OpLess, traceql.OpLessEqual,
			traceql.OpRegex:
			if opCount != 1 {
				return fmt.Errorf("operation %v must have exactly 1 argument. condition: %+v", cond.Op, cond)
			}

		default:
			return fmt.Errorf("unknown operation. condition: %+v", cond)
		}

		// Verify all operands are of the same type
		if opCount == 0 {
			continue
		}

		for i := 1; i < opCount; i++ {
			if reflect.TypeOf(cond.Operands[0]) != reflect.TypeOf(cond.Operands[i]) {
				return fmt.Errorf("operands must be of the same type. condition: %+v", cond)
			}
		}
	}

	return nil
}

func operandType(operands traceql.Operands) traceql.StaticType {
	if len(operands) > 0 {
		return operands[0].Type
	}
	return traceql.TypeNil
}

// mergeSpansetIterator iterates through a slice of spansetIterators exhausting them
// in order
type mergeSpansetIterator struct {
	iters []*genIterator[traceql.Spanset]
	cur   int
}

var _ traceql.SpansetIterator = (*mergeSpansetIterator)(nil)

func (i *mergeSpansetIterator) Next(ctx context.Context) (*traceql.Spanset, error) {
	if i.cur >= len(i.iters) {
		return nil, nil
	}

	iter := i.iters[i.cur]
	spanset, err := iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	if spanset == nil {
		i.cur++
		return i.Next(ctx)
	}

	return spanset, nil
}

// fetch is the core logic for executing the given conditions against the parquet columns. The algorithm
// can be summarized as a hiearchy of iterators where we iterate related columns together and collect the results
// at each level into attributes, spans, and spansets.  Each condition (.foo=bar) is pushed down to the one or more
// matching columns using parquetquery.Predicates.  Results are collected The final return is an iterator where each result is 1 Spanset for each trace.
//
// Diagram:
//
//	Span attribute iterator: key    -----------------------------
//	                         ...    --------------------------  |
//	Span attribute iterator: valueN ----------------------|  |  |
//	                                                      |  |  |
//	                                                      V  V  V
//	                                                   -------------
//	                                                   | attribute |
//	                                                   | collector |
//	                                                   -------------
//	                                                          |
//	                                                          | List of attributes
//	                                                          |
//	                                                          |
//	Span column iterator 1    ---------------------------     |
//	                    ...   ------------------------  |     |
//	Span column iterator N    ---------------------  |  |     |
//	  (ex: name, status)                          |  |  |     |
//	                                              V  V  V     V
//	                                          ------------------
//	                                          | span collector |
//	                                          ------------------
//	                                                          |
//	                                                          | List of Spans
//	Resource attribute                                        |
//	 iterators:                                               |
//	   key     -----------------------------------------      |
//	   ...     --------------------------------------  |      |
//	   valueN  -----------------------------------  |  |      |
//	                                             |  |  |      |
//	                                             V  V  V      |
//	                                          -------------   |
//	                                          | attribute |   |
//	                                          | collector |   |
//	                                          -------------   |
//	                                                    |     |
//	                                                    |     |
//	                                                    |     |
//	                                                    |     |
//
// Resource column iterator 1  --------------------     |     |
//
//	...    -----------------  |     |     |
//
// Resource column iterator N  --------------  |  |     |     |
//
//	(ex: service.name)                    |  |  |     |     |
//	                                      V  V  V     V     V
//	                                     ----------------------
//	                                     |   batch collector  |
//	                                     ----------------------
//	                                                        |
//	                                                        | List of Spansets
//
// Trace column iterator 1  --------------------------        |
//
//	... -----------------------  |        |
//
// Trace column iterator N  --------------------  |  |        |
//
//	(ex: trace ID)                           |  |  |        |
//	                                         V  V  V        V
//	                                       -------------------
//	                                       | trace collector |
//	                                       -------------------
//	                                                        |
//	                                                        | Final Spanset
//	                                                        |
//	                                                        V
func fetch(ctx context.Context, req traceql.FetchSpansRequest, pf *parquet.File, opts common.SearchOptions) (*genIterator[traceql.Spanset], error) {

	// Categorize conditions into span-level or resource-level
	var (
		mingledConditions  bool
		spanConditions     []traceql.Condition
		resourceConditions []traceql.Condition
	)
	for _, cond := range req.Conditions {

		// If no-scoped intrinsic then assign default scope
		scope := cond.Attribute.Scope
		if cond.Attribute.Scope == traceql.AttributeScopeNone {
			if lookup, ok := intrinsicColumnLookups[cond.Attribute.Intrinsic]; ok {
				scope = lookup.scope
			}
		}

		switch scope {

		case traceql.AttributeScopeNone:
			mingledConditions = true
			spanConditions = append(spanConditions, cond)
			resourceConditions = append(resourceConditions, cond)

		case traceql.AttributeScopeSpan:
			spanConditions = append(spanConditions, cond)

		case traceql.AttributeScopeResource:
			resourceConditions = append(resourceConditions, cond)

		default:
			return nil, fmt.Errorf("unsupported traceql scope: %s", cond.Attribute)
		}
	}

	rgs := pf.RowGroups()
	if opts.TotalPages > 0 {
		// Read UP TO TotalPages.  The sharding calculations
		// are just estimates, so it may not line up with the
		// actual number of pages in this file.
		if opts.StartPage+opts.TotalPages > len(rgs) {
			opts.TotalPages = len(rgs) - opts.StartPage
		}
		rgs = rgs[opts.StartPage : opts.StartPage+opts.TotalPages]
	}
	makeIter := makeIterFunc(ctx, rgs, pf)

	// Global state
	// Span-filtering behavior changes depending on the resource-filtering in effect,
	// and vice-versa.  For example consider the query { span.a=1 }.  If no spans have a=1
	// then it generate the empty spanset.
	// However once we add a resource condition: { span.a=1 || resource.b=2 }, now the span
	// filtering must return all spans, even if no spans have a=1, because they might be
	// matched upstream to a resource.
	// TODO - After introducing AllConditions it seems like some of this logic overlaps.
	//        Determine if it can be generalized or simplified.
	var (
		// If there are only span conditions, then don't return a span upstream
		// unless it matches at least 1 span-level condition.
		spanRequireAtLeastOneMatch = len(spanConditions) > 0 && len(resourceConditions) == 0

		// If there are only resource conditions, then don't return a resource upstream
		// unless it matches at least 1 resource-level condition.
		batchRequireAtLeastOneMatch = len(spanConditions) == 0 && len(resourceConditions) > 0

		// Don't return the final spanset upstream unless it matched at least 1 condition
		// anywhere, except in the case of the empty query: {}
		batchRequireAtLeastOneMatchOverall = len(req.Conditions) > 0

		// Optimization for queries like {resource.x... && span.y ...}
		// Requires no mingled scopes like .foo=x, which could be satisfied
		// one either resource or span.
		allConditions = req.AllConditions && !mingledConditions
	)

	spanIter, err := createSpanIterator(makeIter, spanConditions, req.StartTimeUnixNanos, req.EndTimeUnixNanos, spanRequireAtLeastOneMatch, allConditions)
	if err != nil {
		return nil, errors.Wrap(err, "creating span iterator")
	}

	resourceIter, err := createResourceIterator(makeIter, spanIter, resourceConditions, batchRequireAtLeastOneMatch, batchRequireAtLeastOneMatchOverall, allConditions)
	if err != nil {
		return nil, errors.Wrap(err, "creating resource iterator")
	}

	traceIter := createTraceIterator(makeIter, resourceIter)

	return &genIterator[traceql.Spanset]{traceIter}, nil
}

// createSpanColumnIterators creates a column iterator for each span-level condition.
// It also returns duration predicates, which are handled specially.
func createSpanColumnIterators[T parquetquery.GroupPredicate](makeIter makeIterFn, conditions []traceql.Condition, withAllWellKnownColumns bool) ([]parquetquery.Iterator, []*parquetquery.GenericPredicate[int64], error) {
	var (
		columnSelectAs     = map[string]string{}
		columnPredicates   = map[string][]parquetquery.Predicate{}
		iters              []parquetquery.Iterator
		genericConditions  []traceql.Condition
		durationPredicates []*parquetquery.GenericPredicate[int64]
	)

	addPredicate := func(columnPath string, p parquetquery.Predicate) {
		columnPredicates[columnPath] = append(columnPredicates[columnPath], p)
	}

	for _, cond := range conditions {
		// Intrinsic?
		switch cond.Attribute.Intrinsic {
		case traceql.IntrinsicName:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, durationPredicates, err
			}
			addPredicate(columnPathSpanName, pred)
			columnSelectAs[columnPathSpanName] = columnPathSpanName
			continue

		case traceql.IntrinsicDuration:
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, durationPredicates, err
			}
			durationPredicates = append(durationPredicates, pred)
			continue

		case traceql.IntrinsicStatus:
			pred, err := createStatusPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, durationPredicates, err
			}
			addPredicate(columnPathSpanStatusCode, pred)
			columnSelectAs[columnPathSpanStatusCode] = columnPathSpanStatusCode
			continue
		}

		// Well-known attribute?
		if entry, ok := wellKnownColumnLookups[cond.Attribute.Name]; ok && entry.level != traceql.AttributeScopeResource {
			if cond.Op == traceql.OpNone {
				addPredicate(entry.columnPath, nil) // No filtering
				columnSelectAs[entry.columnPath] = cond.Attribute.Name
				continue
			}

			// Compatible type?
			if entry.typ == operandType(cond.Operands) {
				pred, err := createPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, durationPredicates, errors.Wrap(err, "creating predicate")
				}
				addPredicate(entry.columnPath, pred)
				columnSelectAs[entry.columnPath] = cond.Attribute.Name
				continue
			}
		}

		// Else: generic attribute lookup
		genericConditions = append(genericConditions, cond)
	}

	attrIter, err := createAttributeIterator[T](makeIter, genericConditions, DefinitionLevelResourceSpansILSSpanAttrs,
		columnPathSpanAttrKey, columnPathSpanAttrString, columnPathSpanAttrInt, columnPathSpanAttrDouble, columnPathSpanAttrBool)
	if err != nil {
		return nil, durationPredicates, errors.Wrap(err, "creating span attribute iterator")
	}
	if attrIter != nil {
		iters = append(iters, attrIter)
	}

	// Add predicates for well-known columns that were not explicitly selected in the query.
	// These are used for collecting the values of the columns.
	if withAllWellKnownColumns {
		for name, column := range wellKnownColumnLookups {
			if _, ok := columnPredicates[name]; column.level == traceql.AttributeScopeSpan && !ok {
				pred := parquetquery.NewGenericPredicate(
					func(v string) bool { return len(v) > 0 },
					func(min, max string) bool { return min != "" || max != "" },
					func(v parquet.Value) string { return v.String() },
				)
				addPredicate(column.columnPath, pred)
				columnSelectAs[column.columnPath] = name
			}
		}
	}

	for columnPath, predicates := range columnPredicates {
		iters = append(iters, makeIter(columnPath, parquetquery.NewOrPredicate(predicates...), columnSelectAs[columnPath]))
	}

	return iters, durationPredicates, nil
}

// createSpanIterator iterates through all span-level columns, groups them into rows representing one span each.
// Spans are returned that match any of the given conditions.
func createSpanIterator(makeIter makeIterFn, conditions []traceql.Condition, start, end uint64, requireAtLeastOneMatch, allConditions bool) (parquetquery.Iterator, error) {
	iters, durationPredicates, err := createSpanColumnIterators[*attributeCollector](makeIter, conditions, false)
	if err != nil {
		return nil, err
	}

	// Time range filtering?
	var startFilter, endFilter parquetquery.Predicate
	if start > 0 && end > 0 {
		// Here's how we detect the span overlaps the time window:
		// Span start <= req.End
		// Span end >= req.Start
		startFilter = parquetquery.NewIntBetweenPredicate(0, int64(end))
		endFilter = parquetquery.NewIntBetweenPredicate(int64(start), math.MaxInt64)
	}

	var required []parquetquery.Iterator

	minCount := 0
	if requireAtLeastOneMatch {
		minCount = 1
	}
	if allConditions {
		// The final number of expected attributes.
		distinct := map[string]struct{}{}
		for _, cond := range conditions {
			distinct[cond.Attribute.Name] = struct{}{}
		}
		minCount = len(distinct)
	}
	spanCol := &spanCollector{
		minAttributes:   minCount,
		durationFilters: durationPredicates,
	}

	// This is an optimization for when all the span conditions must be met.
	// We simply move all iterators into the required list.
	if allConditions {
		required = append(required, iters...)
		iters = nil
	}

	// This is an optimization for cases when allConditions is false, and
	// only span conditions are present, and we require at least one of them to match.
	// Wrap up the individual conditions with a union and move it into the required list.
	// This skips over static columns like ID that are omnipresent. This is also only
	// possible when there isn't a duration filter because it's computed from start/end.
	if requireAtLeastOneMatch && len(iters) > 0 && len(durationPredicates) == 0 {
		required = append(required, parquetquery.NewUnionIterator(DefinitionLevelResourceSpansILSSpan, iters, nil))
		iters = nil
	}

	// Static columns that are always loaded
	// Since these are always present, they are put at the end and will only
	// be read once the core conditions are met.
	required = append(required, makeIter(columnPathSpanStartTime, startFilter, columnPathSpanStartTime))
	required = append(required, makeIter(columnPathSpanEndTime, endFilter, columnPathSpanEndTime))
	required = append(required, makeIter(columnPathSpanID, nil, columnPathSpanID))

	// Left join here means the span id/start/end iterators + 1 are required,
	// and all other conditions are optional. Whatever matches is returned.
	return parquetquery.NewLeftJoinIterator(DefinitionLevelResourceSpansILSSpan, required, iters, spanCol), nil
}

// createResourceColumnIterators creates a list of iterators for all resource-level columns.
func createResourceColumIterators[T parquetquery.GroupPredicate](makeIter makeIterFn, conditions []traceql.Condition, withAllWellKnownColumns bool) ([]parquetquery.Iterator, error) {
	var (
		columnSelectAs    = map[string]string{}
		columnPredicates  = map[string][]parquetquery.Predicate{}
		iters             []parquetquery.Iterator
		genericConditions []traceql.Condition
	)

	addPredicate := func(columnPath string, p parquetquery.Predicate) {
		columnPredicates[columnPath] = append(columnPredicates[columnPath], p)
	}

	for _, cond := range conditions {
		// Well-known selector?
		if entry, ok := wellKnownColumnLookups[cond.Attribute.Name]; ok && entry.level != traceql.AttributeScopeSpan {
			if cond.Op == traceql.OpNone {
				addPredicate(entry.columnPath, nil) // No filtering
				columnSelectAs[entry.columnPath] = cond.Attribute.Name
				continue
			}

			// Compatible type?
			if entry.typ == operandType(cond.Operands) {
				pred, err := createPredicate(cond.Op, cond.Operands)
				if err != nil {
					return nil, errors.Wrap(err, "creating predicate")
				}
				iters = append(iters, makeIter(entry.columnPath, pred, cond.Attribute.Name))
				continue
			}
		}

		// Else: generic attribute lookup
		genericConditions = append(genericConditions, cond)
	}

	attrIter, err := createAttributeIterator[T](makeIter, genericConditions, DefinitionLevelResourceAttrs,
		columnPathResourceAttrKey, columnPathResourceAttrString, columnPathResourceAttrInt, columnPathResourceAttrDouble, columnPathResourceAttrBool)
	if err != nil {
		return nil, errors.Wrap(err, "creating span attribute iterator")
	}
	if attrIter != nil {
		iters = append(iters, attrIter)
	}

	for columnPath, predicates := range columnPredicates {
		iters = append(iters, makeIter(columnPath, parquetquery.NewOrPredicate(predicates...), columnSelectAs[columnPath]))
	}

	// Add predicates for well-known columns that were not explicitly selected in the query.
	// These are used for collecting the values of the columns.
	if withAllWellKnownColumns {
		for name, column := range wellKnownColumnLookups {
			if _, ok := columnPredicates[name]; column.level == traceql.AttributeScopeResource && !ok {
				pred := parquetquery.NewGenericPredicate(
					func(v string) bool { return len(v) > 0 },
					func(min, max string) bool { return min != "" || max != "" },
					func(v parquet.Value) string { return v.String() },
				)
				addPredicate(column.columnPath, pred)
				columnSelectAs[column.columnPath] = name
			}
		}
	}

	for columnPath, predicates := range columnPredicates {
		iters = append(iters, makeIter(columnPath, parquetquery.NewOrPredicate(predicates...), columnSelectAs[columnPath]))
	}

	return iters, nil
}

// createResourceIterator iterates through all resourcespans-level (batch-level) columns, groups them into rows representing
// one batch each. It builds on top of the span iterator, and turns the groups of spans and resource-level values into
// spansets.  Spansets are returned that match any of the given conditions.
func createResourceIterator(makeIter makeIterFn, spanIterator parquetquery.Iterator, conditions []traceql.Condition, requireAtLeastOneMatch, requireAtLeastOneMatchOverall, allConditions bool) (parquetquery.Iterator, error) {
	columnIters, err := createResourceColumIterators[*attributeCollector](makeIter, conditions, false)
	if err != nil {
		return nil, err
	}

	minCount := 0
	if requireAtLeastOneMatch {
		minCount = 1
	}
	if allConditions {
		// The final number of expected attributes
		distinct := map[string]struct{}{}
		for _, cond := range conditions {
			distinct[cond.Attribute.Name] = struct{}{}
		}
		minCount = len(distinct)
	}
	batchCol := &batchCollector{
		requireAtLeastOneMatchOverall,
		minCount,
	}

	var required []parquetquery.Iterator

	// This is an optimization for when all the resource conditions must be met.
	// We simply move all iterators into the required list.
	if allConditions {
		required = append(required, columnIters...)
		columnIters = nil
	}

	// This is an optimization for cases when only resource conditions are present,
	// and we require at least one of them to match.
	// Wrap up the individual conditions with a union and move it into the required list.
	if requireAtLeastOneMatch && len(columnIters) > 0 {
		required = append(required, parquetquery.NewUnionIterator(DefinitionLevelResourceSpans, columnIters, nil))
		columnIters = nil
	}

	// Put span iterator last, so it is only read when the resource conditions are met.
	required = append(required, spanIterator)

	// Left join here means the span iterator + 1 are required,
	// and all other resource conditions are optional.
	// Whatever matches is returned.
	return parquetquery.NewLeftJoinIterator(DefinitionLevelResourceSpans,
		required, columnIters, batchCol), nil
}

func createTraceIterator(makeIter makeIterFn, resourceIter parquetquery.Iterator) parquetquery.Iterator {
	traceIters := []parquetquery.Iterator{
		resourceIter,
		// Add static columns that are always return
		makeIter(columnPathTraceID, nil, columnPathTraceID),
		makeIter(columnPathStartTimeUnixNano, nil, columnPathStartTimeUnixNano),
		makeIter(columnPathDurationNanos, nil, columnPathDurationNanos),
		makeIter(columnPathRootSpanName, nil, columnPathRootSpanName),
		makeIter(columnPathRootServiceName, nil, columnPathRootServiceName),
	}

	// Final trace iterator
	// Join iterator means it requires matching resources to have been found
	// traceCollector adds trace-level data to the spansets
	return parquetquery.NewJoinIterator(DefinitionLevelTrace, traceIters, &traceCollector{})
}

func createPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	switch operands[0].Type {
	case traceql.TypeString:
		return createStringPredicate(op, operands)
	case traceql.TypeInt:
		return createIntPredicate(op, operands)
	case traceql.TypeFloat:
		return createFloatPredicate(op, operands)
	case traceql.TypeBoolean:
		return createBoolPredicate(op, operands)
	default:
		return nil, fmt.Errorf("cannot create predicate for operand: %v", operands[0])
	}
}

func createStringPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	for _, op := range operands {
		if op.Type != traceql.TypeString {
			return nil, fmt.Errorf("operand is not string: %+v", op)
		}
	}

	s := operands[0].S

	switch op {
	case traceql.OpEqual:
		return parquetquery.NewStringInPredicate([]string{s}), nil

	case traceql.OpNotEqual:
		return parquetquery.NewGenericPredicate(
			func(v string) bool {
				return v != s
			},
			func(min, max string) bool {
				return min != s || max != s
			},
			func(v parquet.Value) string {
				return v.String()
			},
		), nil

	case traceql.OpRegex:
		return parquetquery.NewRegexInPredicate([]string{s})

	default:
		return nil, fmt.Errorf("operand not supported for strings: %+v", op)
	}

}

func createIntPredicate(op traceql.Operator, operands traceql.Operands) (*parquetquery.GenericPredicate[int64], error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	var i int64
	switch operands[0].Type {
	case traceql.TypeInt:
		i = int64(operands[0].N)
	case traceql.TypeDuration:
		i = operands[0].D.Nanoseconds()
	default:
		return nil, fmt.Errorf("operand is not int or duration: %+v", operands[0])
	}

	var fn func(v int64) bool
	var rangeFn func(min, max int64) bool

	switch op {
	case traceql.OpEqual:
		fn = func(v int64) bool { return v == i }
		rangeFn = func(min, max int64) bool { return min <= i && i <= max }
	case traceql.OpNotEqual:
		fn = func(v int64) bool { return v != i }
		rangeFn = func(min, max int64) bool { return min != i || max != i }
	case traceql.OpGreater:
		fn = func(v int64) bool { return v > i }
		rangeFn = func(min, max int64) bool { return max > i }
	case traceql.OpGreaterEqual:
		fn = func(v int64) bool { return v >= i }
		rangeFn = func(min, max int64) bool { return max >= i }
	case traceql.OpLess:
		fn = func(v int64) bool { return v < i }
		rangeFn = func(min, max int64) bool { return min < i }
	case traceql.OpLessEqual:
		fn = func(v int64) bool { return v <= i }
		rangeFn = func(min, max int64) bool { return min <= i }
	default:
		return nil, fmt.Errorf("operand not supported for integers: %+v", op)
	}

	return parquetquery.NewIntPredicate(fn, rangeFn), nil
}

func createStatusPredicate(op traceql.Operator, operands traceql.Operands) (*parquetquery.GenericPredicate[int64], error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	var i int64
	switch operands[0].Type {
	case traceql.TypeInt:
		i = int64(operands[0].N)
	case traceql.TypeStatus:
		i = int64(StatusCodeMapping[operands[0].Status.String()])
	default:
		return nil, fmt.Errorf("operand is not int or status: %+v", operands[0])
	}

	var fn func(v int64) bool
	var rangeFn func(min, max int64) bool

	switch op {
	case traceql.OpEqual:
		fn = func(v int64) bool { return v == i }
		rangeFn = func(min, max int64) bool { return min <= i && i <= max }
	case traceql.OpNotEqual:
		fn = func(v int64) bool { return v != i }
		rangeFn = func(min, max int64) bool { return min != i || max != i }
	case traceql.OpGreater:
		fn = func(v int64) bool { return v > i }
		rangeFn = func(min, max int64) bool { return max > i }
	case traceql.OpGreaterEqual:
		fn = func(v int64) bool { return v >= i }
		rangeFn = func(min, max int64) bool { return max >= i }
	case traceql.OpLess:
		fn = func(v int64) bool { return v < i }
		rangeFn = func(min, max int64) bool { return min < i }
	case traceql.OpLessEqual:
		fn = func(v int64) bool { return v <= i }
		rangeFn = func(min, max int64) bool { return min <= i }
	default:
		return nil, fmt.Errorf("operand not supported for integers: %+v", op)
	}

	return parquetquery.NewIntPredicate(fn, rangeFn), nil
}

func createFloatPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	// Ensure operand is float
	if operands[0].Type != traceql.TypeFloat {
		return nil, fmt.Errorf("operand is not float: %+v", operands[0])
	}

	i := operands[0].F

	var fn func(v float64) bool
	var rangeFn func(min, max float64) bool

	switch op {
	case traceql.OpEqual:
		fn = func(v float64) bool { return v == i }
		rangeFn = func(min, max float64) bool { return min <= i && i <= max }
	case traceql.OpNotEqual:
		fn = func(v float64) bool { return v != i }
		rangeFn = func(min, max float64) bool { return min != i || max != i }
	case traceql.OpGreater:
		fn = func(v float64) bool { return v > i }
		rangeFn = func(min, max float64) bool { return max > i }
	case traceql.OpGreaterEqual:
		fn = func(v float64) bool { return v >= i }
		rangeFn = func(min, max float64) bool { return max >= i }
	case traceql.OpLess:
		fn = func(v float64) bool { return v < i }
		rangeFn = func(min, max float64) bool { return min < i }
	case traceql.OpLessEqual:
		fn = func(v float64) bool { return v <= i }
		rangeFn = func(min, max float64) bool { return min <= i }
	default:
		return nil, fmt.Errorf("operand not supported for floats: %+v", op)
	}

	return parquetquery.NewFloatPredicate(fn, rangeFn), nil
}

func createBoolPredicate(op traceql.Operator, operands traceql.Operands) (parquetquery.Predicate, error) {
	if op == traceql.OpNone {
		return nil, nil
	}

	// Ensure operand is bool
	if operands[0].Type != traceql.TypeBoolean {
		return nil, fmt.Errorf("operand is not bool: %+v", operands[0])
	}

	switch op {
	case traceql.OpEqual:
		return parquetquery.NewBoolPredicate(operands[0].B), nil

	case traceql.OpNotEqual:
		return parquetquery.NewBoolPredicate(!operands[0].B), nil

	default:
		return nil, fmt.Errorf("operand not supported for booleans: %+v", op)
	}
}

func createAttributeIterator[T parquetquery.GroupPredicate](
	makeIter makeIterFn, conditions []traceql.Condition,
	definitionLevel int,
	keyPath, strPath, intPath, floatPath, boolPath string,
) (parquetquery.Iterator, error) {
	// TODO(mapno): Verify this has same performance
	var (
		attrKeys                                               []string
		attrStringPreds, attrIntPreds, attrFltPreds, boolPreds []parquetquery.Predicate
	)
	for _, cond := range conditions {

		attrKeys = append(attrKeys, cond.Attribute.Name)

		if cond.Op == traceql.OpNone {
			// This means we have to scan all values, we don't know what type
			// to expect
			attrStringPreds = append(attrStringPreds, nil)
			attrIntPreds = append(attrIntPreds, nil)
			attrFltPreds = append(attrFltPreds, nil)
			boolPreds = append(boolPreds, nil)
			continue
		}

		switch cond.Operands[0].Type {

		case traceql.TypeString:
			pred, err := createStringPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, errors.Wrap(err, "creating attribute predicate")
			}
			attrStringPreds = append(attrStringPreds, pred)

		case traceql.TypeInt:
			pred, err := createIntPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, errors.Wrap(err, "creating attribute predicate")
			}
			attrIntPreds = append(attrIntPreds, pred)

		case traceql.TypeFloat:
			pred, err := createFloatPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, errors.Wrap(err, "creating attribute predicate")
			}
			attrFltPreds = append(attrFltPreds, pred)

		case traceql.TypeBoolean:
			pred, err := createBoolPredicate(cond.Op, cond.Operands)
			if err != nil {
				return nil, errors.Wrap(err, "creating attribute predicate")
			}
			boolPreds = append(boolPreds, pred)
		}
	}

	var valueIters []parquetquery.Iterator
	if len(attrStringPreds) > 0 {
		valueIters = append(valueIters, makeIter(strPath, parquetquery.NewOrPredicate(attrStringPreds...), "string"))
	}
	if len(attrIntPreds) > 0 {
		valueIters = append(valueIters, makeIter(intPath, parquetquery.NewOrPredicate(attrIntPreds...), "int"))
	}
	if len(attrFltPreds) > 0 {
		valueIters = append(valueIters, makeIter(floatPath, parquetquery.NewOrPredicate(attrFltPreds...), "float"))
	}
	if len(boolPreds) > 0 {
		valueIters = append(valueIters, makeIter(boolPath, parquetquery.NewOrPredicate(boolPreds...), "bool"))
	}

	if len(valueIters) > 0 {
		// LeftJoin means only look at rows where the key is what we want.
		// Bring in any of the typed values as needed.
		return parquetquery.NewLeftJoinIterator(definitionLevel,
			[]parquetquery.Iterator{makeIter(keyPath, parquetquery.NewStringInPredicate(attrKeys), "key")},
			valueIters,
			*new(T)), nil
	}

	return nil, nil
}

// This turns groups of span values into Span objects
type spanCollector struct {
	minAttributes   int
	durationFilters []*parquetquery.GenericPredicate[int64]
}

var _ parquetquery.GroupPredicate = (*spanCollector)(nil)

func (c *spanCollector) KeepGroup(res *parquetquery.IteratorResult) bool {

	span := &traceql.Span{
		Attributes: make(map[traceql.Attribute]traceql.Static),
	}

	for _, e := range res.OtherEntries {
		span.Attributes[newSpanAttr(e.Key)] = e.Value.(traceql.Static)
	}

	// Merge all individual columns into the span
	for _, kv := range res.Entries {
		switch kv.Key {
		case columnPathSpanID:
			span.ID = kv.Value.ByteArray()
		case columnPathSpanStartTime:
			span.StartTimeUnixNanos = kv.Value.Uint64()
		case columnPathSpanEndTime:
			span.EndtimeUnixNanos = kv.Value.Uint64()
		case columnPathSpanName:
			span.Attributes[traceql.NewIntrinsic(traceql.IntrinsicName)] = traceql.NewStaticString(kv.Value.String())
		// case columnPathSpanDuration:
		// 	span.Attributes[traceql.NewIntrinsic(traceql.IntrinsicDuration)] = traceql.NewStaticDuration(time.Duration(kv.Value.Uint64()))
		case columnPathSpanStatusCode:
			// Map OTLP status code back to TraceQL enum.
			// For other values, use the raw integer.
			var status traceql.Status
			switch kv.Value.Uint64() {
			case uint64(v1.Status_STATUS_CODE_UNSET):
				status = traceql.StatusUnset
			case uint64(v1.Status_STATUS_CODE_OK):
				status = traceql.StatusOk
			case uint64(v1.Status_STATUS_CODE_ERROR):
				status = traceql.StatusError
			default:
				status = traceql.Status(kv.Value.Uint64())
			}
			span.Attributes[traceql.NewIntrinsic(traceql.IntrinsicStatus)] = traceql.NewStaticStatus(status)
		default:
			// TODO - This exists for span-level dedicated columns like http.status_code
			// Are nils possible here?
			switch kv.Value.Kind() {
			case parquet.Boolean:
				span.Attributes[newSpanAttr(kv.Key)] = traceql.NewStaticBool(kv.Value.Boolean())
			case parquet.Int32, parquet.Int64:
				span.Attributes[newSpanAttr(kv.Key)] = traceql.NewStaticInt(int(kv.Value.Int64()))
			case parquet.Float:
				span.Attributes[newSpanAttr(kv.Key)] = traceql.NewStaticFloat(kv.Value.Double())
			case parquet.ByteArray:
				span.Attributes[newSpanAttr(kv.Key)] = traceql.NewStaticString(kv.Value.String())
			}
		}
	}

	// Save computed duration if any filters present and at least one is passed.
	if len(c.durationFilters) > 0 {
		duration := span.EndtimeUnixNanos - span.StartTimeUnixNanos
		for _, f := range c.durationFilters {
			if f == nil || f.Fn(int64(duration)) {
				span.Attributes[traceql.NewIntrinsic(traceql.IntrinsicDuration)] = traceql.NewStaticDuration(time.Duration(duration))
				break
			}
		}
	}

	if c.minAttributes > 0 {
		count := 0
		for _, v := range span.Attributes {
			if v.Type != traceql.TypeNil {
				count++
			}
		}
		if count < c.minAttributes {
			return false
		}
	}

	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue("span", span)

	return true
}

// batchCollector receives rows of matching resource-level
// This turns groups of batch values and Spans into SpanSets
type batchCollector struct {
	requireAtLeastOneMatchOverall bool
	minAttributes                 int
}

var _ parquetquery.GroupPredicate = (*batchCollector)(nil)

func (c *batchCollector) KeepGroup(res *parquetquery.IteratorResult) bool {

	// TODO - This wraps everything up in a spanset per batch.
	// We probably don't need to do this, since the traceCollector
	// flattens it into 1 spanset per trace.  All we really need
	// todo is merge the resource-level attributes onto the spans
	// and filter out spans that didn't match anything.

	resAttrs := make(map[traceql.Attribute]traceql.Static)
	spans := make([]traceql.Span, 0, len(res.OtherEntries))

	for _, kv := range res.OtherEntries {
		switch kv.Value.(type) {
		case *traceql.Span:
			spans = append(spans, *kv.Value.(*traceql.Span))
		case traceql.Static:
			resAttrs[newResAttr(kv.Key)] = kv.Value.(traceql.Static)
		}
	}

	// Throw out batches without any spans
	if len(spans) == 0 {
		return false
	}

	// Gather Attributes from dedicated resource-level columns
	for _, e := range res.Entries {
		switch e.Value.Kind() {
		case parquet.Int64:
			resAttrs[newResAttr(e.Key)] = traceql.NewStaticInt(int(e.Value.Int64()))
		case parquet.ByteArray:
			resAttrs[newResAttr(e.Key)] = traceql.NewStaticString(e.Value.String())
		}
	}

	if c.minAttributes > 0 {
		if len(resAttrs) < c.minAttributes {
			return false
		}
	}

	// Copy resource-level attributes to the individual spans now
	for k, v := range resAttrs {
		for _, span := range spans {
			if _, alreadyExists := span.Attributes[k]; !alreadyExists {
				span.Attributes[k] = v
			}
		}
	}

	// Remove unmatched attributes
	for _, span := range spans {
		for k, v := range span.Attributes {
			if v.Type == traceql.TypeNil {
				delete(span.Attributes, k)
			}
		}
	}

	sp := &traceql.Spanset{
		Spans: make([]traceql.Span, 0, len(spans)),
	}

	// Copy over only spans that met minimum criteria
	if c.requireAtLeastOneMatchOverall {
		for _, span := range spans {
			if len(span.Attributes) > 0 {
				sp.Spans = append(sp.Spans, span)
			}
		}
	} else {
		sp.Spans = spans
	}

	// Throw out batches without any spans
	if len(sp.Spans) == 0 {
		return false
	}

	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue("spanset", sp)

	return true
}

// traceCollector receives rows from the resource-level matches.
// It adds trace-level attributes into the spansets before
// they are returned
type traceCollector struct {
}

var _ parquetquery.GroupPredicate = (*traceCollector)(nil)

func (c *traceCollector) KeepGroup(res *parquetquery.IteratorResult) bool {
	finalSpanset := &traceql.Spanset{}

	for _, e := range res.Entries {
		switch e.Key {
		case columnPathTraceID:
			finalSpanset.TraceID = e.Value.ByteArray()
		case columnPathStartTimeUnixNano:
			finalSpanset.StartTimeUnixNanos = e.Value.Uint64()
		case columnPathDurationNanos:
			finalSpanset.DurationNanos = e.Value.Uint64()
		case columnPathRootSpanName:
			finalSpanset.RootSpanName = e.Value.String()
		case columnPathRootServiceName:
			finalSpanset.RootServiceName = e.Value.String()
		}
	}

	for _, e := range res.OtherEntries {
		if spanset, ok := e.Value.(*traceql.Spanset); ok {
			finalSpanset.Spans = append(finalSpanset.Spans, spanset.Spans...)
		}
	}

	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue("spanset", finalSpanset)

	return true
}

// attributeCollector receives rows from the individual key/string/int/etc
// columns and joins them together into map[key]value entries with the
// right type.
type attributeCollector struct {
}

var _ parquetquery.GroupPredicate = (*attributeCollector)(nil)

func (c *attributeCollector) KeepGroup(res *parquetquery.IteratorResult) bool {

	var key string
	var val traceql.Static

	for _, e := range res.Entries {
		// Ignore nulls, this leaves val as the remaining found value,
		// or nil if the key was found but no matching values
		if e.Value.Kind() < 0 {
			continue
		}

		switch e.Key {
		case "key":
			key = e.Value.String()
		case "string":
			val = traceql.NewStaticString(e.Value.String())
		case "int":
			val = traceql.NewStaticInt(int(e.Value.Int64()))
		case "float":
			val = traceql.NewStaticFloat(e.Value.Double())
		case "bool":
			val = traceql.NewStaticBool(e.Value.Boolean())
		}
	}

	res.Entries = res.Entries[:0]
	res.OtherEntries = res.OtherEntries[:0]
	res.AppendOtherValue(key, val)

	return true
}

func newSpanAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, name)
}

func newResAttr(name string) traceql.Attribute {
	return traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, name)
}
