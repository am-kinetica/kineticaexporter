package kineticaotelexporter

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// kineticaTracesExporter
type kineticaTracesExporter struct {
	logger *zap.Logger

	writer *KiWriter
}

type kineticaTraceRecord struct {
	span              *Span
	spanAttribute     []SpanAttribute
	resourceAttribute []ResourceAttribute
	scopeAttribute    []ScopeAttribute
	eventAttribute    []EventAttribute
	linkAttribute     []LinkAttribute
}

// newTracesExporter
//
//	@param logger
//	@param cfg
//	@return *kineticaTracesExporter
//	@return error
func newTracesExporter(logger *zap.Logger, cfg *Config) (*kineticaTracesExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	writer := NewKiWriter(context.TODO(), *cfg)
	tracesExp := &kineticaTracesExporter{
		logger: logger,
		writer: writer,
	}
	return tracesExp, nil
}

// pushTraceData
//
//	@receiver e
//	@param ctx
//	@param td
//	@return error
func (e *kineticaTracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	var errs []error
	var traceRecords []kineticaTraceRecord
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		resourceSpan := resourceSpans.At(i)
		resource := resourceSpan.Resource()
		scopeSpans := resourceSpan.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			spans := scopeSpans.At(j).Spans()
			scope := scopeSpans.At(j).Scope()
			for k := 0; k < spans.Len(); k++ {
				if kiTraceRecord, err := e.createTraceRecord(ctx, resource, scope, spans.At(k)); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}
					errs = append(errs, err)
				} else {
					traceRecords = append(traceRecords, *kiTraceRecord)
				}
			}
		}
	}

	if err := e.writer.persistTraceRecord(traceRecords); err != nil {
		errs = append(errs, err)
	}

	return multierr.Combine(errs...)
}

// createTraceRecord //
//
//	@receiver e
//	@param ctx
//	@param resource
//	@param scope
//	@param span
//	@return error
func (e *kineticaTracesExporter) createTraceRecord(ctx context.Context, resource pcommon.Resource, scope pcommon.InstrumentationScope, spanRecord ptrace.Span) (*kineticaTraceRecord, error) {
	var errs []error

	tags := make(map[string]string)
	fields := make(map[string]interface{})

	traceID := spanRecord.TraceID()
	if traceID.IsEmpty() {
		return nil, errors.New("span has no trace ID")
	}
	spanID := spanRecord.SpanID()
	if spanID.IsEmpty() {
		return nil, errors.New("span has no span ID")
	}

	tags[AttributeTraceID] = hex.EncodeToString(traceID[:])
	tags[AttributeSpanID] = hex.EncodeToString(spanID[:])

	if traceState := spanRecord.TraceState().AsRaw(); traceState != "" {
		fields[AttributeTraceState] = traceState
	}
	if parentSpanID := spanRecord.ParentSpanID(); !parentSpanID.IsEmpty() {
		fields[AttributeParentSpanID] = hex.EncodeToString(parentSpanID[:])
	}
	if name := spanRecord.Name(); name != "" {
		fields[AttributeName] = name
	}
	if kind := spanRecord.Kind(); kind != ptrace.SpanKindUnspecified {
		fields[AttributeSpanKind] = kind.String()
	}

	ts := spanRecord.StartTimestamp().AsTime()
	if ts.IsZero() {
		return nil, errors.New("span has no timestamp")
	}

	endTime := spanRecord.EndTimestamp().AsTime()
	if endTime.IsZero() {
		fields[AttributeEndTimeUnixNano] = endTime.UnixNano()
		fields[AttributeDurationNano] = endTime.Sub(ts).Nanoseconds()
	}

	droppedAttributesCount := uint64(spanRecord.DroppedAttributesCount())
	if spanRecord.Attributes().Len() > 0 {
		marshalledAttributes, err := json.Marshal(spanRecord.Attributes().AsRaw())
		if err != nil {
			e.logger.Debug("failed to marshal attributes to JSON %s", zap.String("", err.Error()))
			droppedAttributesCount += uint64(spanRecord.Attributes().Len())
		} else {
			fields[AttributeAttributes] = string(marshalledAttributes)
		}
	}
	if droppedAttributesCount > 0 {
		fields[AttributeDroppedAttributesCount] = droppedAttributesCount
	}

	droppedEventsCount := spanRecord.DroppedEventsCount()

	kiTraceRecord := new(kineticaTraceRecord)
	span := NewSpan(uuid.New().String(), uuid.New().String(), uuid.New().String(), uuid.New().String(), tags[AttributeTraceID], tags[AttributeSpanID], fields[AttributeParentSpanID].(string), fields[AttributeTraceState].(string), fields[AttributeName].(string), fields[AttributeSpanKind].(int8), strconv.Itoa(int(ts.UnixNano())), strconv.Itoa(int(fields[AttributeEndTimeUnixNano].(uint64))), int(droppedAttributesCount), int(droppedEventsCount), 0, "", 0)
	kiTraceRecord.span = span

	var spanAttribute []SpanAttribute
	spanAttributes := make(map[string]ValueTypePair)
	spanRecord.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			droppedAttributesCount++
			e.logger.Debug("log record attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err != nil {
			droppedAttributesCount++
			e.logger.Debug("invalid log record attribute value", zap.String("Error", err.Error()))
		} else {
			spanAttributes[k] = v
		}
		return true
	})

	for key := range spanAttributes {
		vtPair := spanAttributes[key]
		sa := newSpanAttributeValue(span.ID, key, vtPair)
		spanAttribute = append(spanAttribute, *sa)
	}

	copy(kiTraceRecord.spanAttribute, spanAttribute)

	var resourceAttribute []ResourceAttribute
	resourceAttributes := make(map[string]ValueTypePair)
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("resource attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err != nil {
			e.logger.Debug("Invalid resource attribute value", zap.String("Error", err.Error()))
		} else {
			resourceAttributes[k] = v
		}
		return true
	})

	for key := range resourceAttributes {
		vtPair := resourceAttributes[key]
		ra := newResourceAttributeValue(span.ResourceID, key, vtPair)
		resourceAttribute = append(resourceAttribute, *ra)
	}

	copy(kiTraceRecord.resourceAttribute, resourceAttribute)

	// Insert scope attributes
	var scopeAttribute []ScopeAttribute
	scopeAttributes := make(map[string]ValueTypePair)
	scopeName := scope.Name()
	scopeVersion := scope.Version()
	scope.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("scope attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err != nil {
			e.logger.Debug("invalid scope attribute value", zap.String("Error", err.Error()))
		} else {
			scopeAttributes[k] = v
		}
		return true
	})

	for key := range scopeAttributes {
		vtPair := scopeAttributes[key]
		sa := newScopeAttributeValue(span.ScopeID, key, scopeName, scopeVersion, vtPair)
		scopeAttribute = append(scopeAttribute, *sa)

	}

	copy(kiTraceRecord.scopeAttribute, scopeAttribute)

	//Insert event attributes
	var eventAttribute []EventAttribute
	eventAttributes := make(map[string]ValueTypePair)
	spanEvents := spanRecord.Events()

	for i := 0; i < spanEvents.Len(); i++ {
		event := spanEvents.At(i)
		event.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				e.logger.Debug("Event attribute key is empty")
			} else if v, err := AttributeValueToKineticaFieldValue(v); err != nil {
				e.logger.Debug("invalid event attribute value", zap.String("Error", err.Error()))
			} else {
				eventAttributes[k] = v
				ea := newEventAttributeValue(span.EventID, event.Name(), k, v)
				eventAttribute = append(eventAttribute, *ea)
			}
			return true
		})
	}

	copy(kiTraceRecord.eventAttribute, eventAttribute)

	//Insert link attributes
	var linkAttribute []LinkAttribute
	linkAttributes := make(map[string]ValueTypePair)
	spanLinks := spanRecord.Links()

	for i := 0; i < spanLinks.Len(); i++ {
		link := spanLinks.At(i)
		link.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				e.logger.Debug("Event attribute key is empty")
			} else if v, err := AttributeValueToKineticaFieldValue(v); err != nil {
				e.logger.Debug("invalid event attribute value", zap.String("Error", err.Error()))
			} else {
				linkAttributes[k] = v
				la := newLinkAttributeValue(span.LinkID, span.TraceID, span.SpanID, k, v)
				linkAttribute = append(linkAttribute, *la)
			}
			return true
		})
	}

	copy(kiTraceRecord.linkAttribute, linkAttribute)

	return kiTraceRecord, multierr.Combine(errs...)
}

// newLinkAttributeValue
//
//	@param linkID
//	@param traceID
//	@param spanID
//	@param key
//	@param vtPair
//	@return *LinkAttribute
func newLinkAttributeValue(linkID string, traceID, spanID, key string, vtPair ValueTypePair) *LinkAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		la := NewLinkAttribute(linkID, traceID, spanID, key, *av)
		return la
	}

	return nil
}

// newEventAttributeValue
//
//	@param eventID
//	@param eventName
//	@param key
//	@param vtPair
//	@return *EventAttribute
func newEventAttributeValue(eventID string, eventName string, key string, vtPair ValueTypePair) *EventAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := NewEventAttribute(eventID, eventName, key, *av)
		return sa
	}

	return nil
}

// newSpanAttributeValue
//
//	@param spanID
//	@param key
//	@param vtPair
//	@return *SpanAttribute
func newSpanAttributeValue(spanID string, key string, vtPair ValueTypePair) *SpanAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := NewSpanAttribute(spanID, key, *av)
		return sa
	}

	return nil
}
