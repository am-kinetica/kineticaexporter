package kineticaotelexporter

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
)

const (
	MeasurementSpans     = "spans"
	MeasurementSpanLinks = "span-links"
	MeasurementLogs      = "logs"

	// These attribute key names are influenced by the proto message keys.
	AttributeTime                   = "time"
	AttributeStartTimeUnixNano      = "start_time_unix_nano"
	AttributeTraceID                = "trace_id"
	AttributeSpanID                 = "span_id"
	AttributeTraceState             = "trace_state"
	AttributeParentSpanID           = "parent_span_id"
	AttributeParentServiceName      = "parent_service_name"
	AttributeChildServiceName       = "child_service_name"
	AttributeCallCount              = "call_count"
	AttributeSpansQueueDepth        = "spans_queue_depth"
	AttributeSpansDropped           = "spans_dropped"
	AttributeName                   = "name"
	AttributeSpanKind               = "kind"
	AttributeEndTimeUnixNano        = "end_time_unix_nano"
	AttributeDurationNano           = "duration_nano"
	AttributeDroppedAttributesCount = "dropped_attributes_count"
	AttributeDroppedEventsCount     = "dropped_events_count"
	AttributeDroppedLinksCount      = "dropped_links_count"
	AttributeAttributes             = "attributes"
	AttributeLinkedTraceID          = "linked_trace_id"
	AttributeLinkedSpanID           = "linked_span_id"
	AttributeSeverityNumber         = "severity_number"
	AttributeSeverityText           = "severity_text"
	AttributeBody                   = "body"

	LogTable                  = "log"
	LogAttributeTable         = "log_attribute"
	LogResourceAttributeTable = "log_resource_attribute"
	LogScopeAttributeTable    = "log_scope_attribute"

	TraceSpanTable              = "trace_span"
	TraceSpanAttributeTable     = "trace_span_attribute"
	TraceResourceAttributeTable = "trace_resource_attribute"
	TraceScopeAttributeTable    = "trace_scope_attribute"
	TraceEventAttributeTable    = "trace_event_attribute"
	TraceLinkAttributeTable     = "trace_link_attribute"

	GaugeTable = "metric_gauge"
	// GaugeAttributeTable                  = "metric_gauge_attribute"
	GaugeDatapointTable                  = "metric_gauge_datapoint"
	GaugeDatapointAttributeTable         = "metric_gauge_datapoint_attribute"
	GaugeDatapointExemplarTable          = "metric_gauge_datapoint_exemplar"
	GaugeDatapointExemplarAttributeTable = "metric_gauge_datapoint_exemplar_attribute"
	GaugeResourceAttributeTable          = "metric_gauge_metric_resource_attribute"
	GaugeScopeAttributeTable             = "metric_gauge_scope_attribute"

	SumTable = "metric_sum"
	// SumAttributeTable                  = "metric_sum_attribute"
	SumResourceAttributeTable          = "metric_sum_resource_attribute"
	SumScopeAttributeTable             = "metric_sum_scope_attribute"
	SumDatapointTable                  = "metric_sum_datapoint"
	SumDatapointAttributeTable         = "metric_sum_datapoint_attribute"
	SumDatapointExemplarTable          = "metric_sum_datapoint_exemplar"
	SumDataPointExemplarAttributeTable = "metric_sum_datapoint_exemplar_attribute"

	HistogramTable                           = "metric_histogram"
	HistogramAttributeTable                  = "metric_histogram_attribute"
	HistogramResourceAttributeTable          = "metric_histogram_resource_attribute"
	HistogramScopeAttributeTable             = "metric_histogram_scope_attribute"
	HistogramDatapointTable                  = "metric_histogram_datapoint"
	HistogramDatapointAttributeTable         = "metric_histogram_datapoint_attribute"
	HistogramBucketCountsTable               = "metric_histogram_datapoint_bucket_count"
	HistogramExplicitBoundsTable             = "metric_histogram_datapoint_explicit_bound"
	HistogramDatapointExemplarTable          = "metric_histogram_datapoint_exemplar"
	HistogramDataPointExemplarAttributeTable = "metric_histogram_datapoint_exemplar_attribute"

	ExpHistogramTable                           = "metric_exp_histogram"
	ExpHistogramAttributeTable                  = "metric_exp_histogram_attribute"
	ExpHistogramResourceAttributeTable          = "metric_exp_histogram_resource_attribute"
	ExpHistogramScopeAttributeTable             = "metric_exp_histogram_scope_attribute"
	ExpHistogramDatapointTable                  = "metric_exp_histogram_datapoint"
	ExpHistogramDatapointAttributeTable         = "metric_exp_histogram_datapoint_attribute"
	ExpHistogramPositiveBucketCountsTable       = "metric_exp_histogram_datapoint_bucket_positive_count"
	ExpHistogramNegativeBucketCountsTable       = "metric_exp_histogram_datapoint_bucket_negative_count"
	ExpHistogramDatapointExemplarTable          = "metric_exp_histogram_datapoint_exemplar"
	ExpHistogramDataPointExemplarAttributeTable = "metric_exp_histogram_datapoint_exemplar_attribute"

	SummaryTable                       = "metric_summary"
	SummaryAttributeTable              = "metric_summary_attribute"
	SummaryResourceAttributeTable      = "metric_summary_resource_attribute"
	SummaryScopeAttributeTable         = "metric_summary_scope_attribute"
	SummaryDatapointTable              = "metric_summary_datapoint"
	SummaryDatapointAttributeTable     = "metric_summary_datapoint_attribute"
	SummaryDatapointQuantileValueTable = "metric_summary_datapoint_quantile_values"

	ChunkSize = 10000
)

// AggregationTemporality - Metrics
type AggregationTemporality int

// const - Metrics
//
//	@param AggregationTemporalityUnspecified
const (
	AggregationTemporalityUnspecified AggregationTemporality = iota
	AggregationTemporalityDelta
	AggregationTemporalityCumulative
)

// ValueTypePair
type ValueTypePair struct {
	value     interface{}
	valueType pcommon.ValueType
}

// AttributeValueToKineticaTagValue
//
//	@param value
//	@return string
//	@return error
func AttributeValueToKineticaTagValue(value pcommon.Value) (string, error) {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return value.Str(), nil
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(value.Int(), 10), nil
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(value.Double(), 'f', -1, 64), nil
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(value.Bool()), nil
	case pcommon.ValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.Map())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pcommon.ValueTypeSlice:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.Slice())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pcommon.ValueTypeEmpty:
		return "", nil
	default:
		return "", fmt.Errorf("unknown value type %d", value.Type())
	}
}

// AttributeValueToKineticaFieldValue //
//
//	@param value
//	@return ValueTypePair
//	@return error
func AttributeValueToKineticaFieldValue(value pcommon.Value) (ValueTypePair, error) {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return ValueTypePair{value.Str(), pcommon.ValueTypeStr}, nil
	case pcommon.ValueTypeInt:
		return ValueTypePair{value.Int(), pcommon.ValueTypeInt}, nil
	case pcommon.ValueTypeDouble:
		return ValueTypePair{value.Double(), pcommon.ValueTypeDouble}, nil
	case pcommon.ValueTypeBool:
		return ValueTypePair{value.Bool(), pcommon.ValueTypeBool}, nil
	case pcommon.ValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.Map())); err != nil {
			return ValueTypePair{nil, pcommon.ValueTypeEmpty}, err
		} else {
			return ValueTypePair{string(jsonBytes), pcommon.ValueTypeStr}, nil
		}
	case pcommon.ValueTypeSlice:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.Slice())); err != nil {
			return ValueTypePair{nil, pcommon.ValueTypeEmpty}, err
		} else {
			return ValueTypePair{string(jsonBytes), pcommon.ValueTypeStr}, nil
		}
	case pcommon.ValueTypeEmpty:
		return ValueTypePair{nil, pcommon.ValueTypeEmpty}, nil
	default:
		return ValueTypePair{nil, pcommon.ValueTypeEmpty}, fmt.Errorf("unknown value type %v", value)
	}
}

// otlpKeyValueListToMap
//
//	@param kvList
//	@return map
func otlpKeyValueListToMap(kvList pcommon.Map) map[string]interface{} {
	m := make(map[string]interface{}, kvList.Len())
	kvList.Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			m[k] = v.Str()
		case pcommon.ValueTypeInt:
			m[k] = v.Int()
		case pcommon.ValueTypeDouble:
			m[k] = v.Double()
		case pcommon.ValueTypeBool:
			m[k] = v.Bool()
		case pcommon.ValueTypeMap:
			m[k] = otlpKeyValueListToMap(v.Map())
		case pcommon.ValueTypeSlice:
			m[k] = otlpArrayToSlice(v.Slice())
		case pcommon.ValueTypeEmpty:
			m[k] = nil
		default:
			m[k] = fmt.Sprintf("<invalid map value> %v", v)
		}
		return true
	})
	return m
}

// otlpArrayToSlice
//
//	@param arr
//	@return []interface{}
func otlpArrayToSlice(arr pcommon.Slice) []interface{} {
	s := make([]interface{}, 0, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		v := arr.At(i)
		switch v.Type() {
		case pcommon.ValueTypeStr:
			s = append(s, v.Str())
		case pcommon.ValueTypeInt:
			s = append(s, v.Int())
		case pcommon.ValueTypeDouble:
			s = append(s, v.Double())
		case pcommon.ValueTypeBool:
			s = append(s, v.Bool())
		case pcommon.ValueTypeEmpty:
			s = append(s, nil)
		default:
			s = append(s, fmt.Sprintf("<invalid array value> %v", v))
		}
	}
	return s
}

func convertResourceTags(resource pcommon.Resource) map[string]string {
	tags := make(map[string]string, resource.Attributes().Len())
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		tags[k] = v.AsString()
		return true
	})
	// TODO dropped attributes counts
	return tags
}

func convertResourceFields(resource pcommon.Resource) map[string]interface{} {
	fields := make(map[string]interface{}, resource.Attributes().Len())
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		fields[k] = v.AsRaw()
		return true
	})
	// TODO dropped attributes counts
	return fields
}

func convertScopeFields(is pcommon.InstrumentationScope) map[string]interface{} {
	fields := make(map[string]interface{}, is.Attributes().Len()+2)
	is.Attributes().Range(func(k string, v pcommon.Value) bool {
		fields[k] = v.AsRaw()
		return true
	})
	if name := is.Name(); name != "" {
		fields[semconv.AttributeTelemetrySDKName] = name
	}
	if version := is.Version(); version != "" {
		fields[semconv.AttributeTelemetrySDKVersion] = version
	}
	// TODO dropped attributes counts
	return fields
}

// getAttributeValue
//
//	@param vtPair
//	@return *AttributeValue
//	@return error
func getAttributeValue(vtPair ValueTypePair) (*AttributeValue, error) {
	var av *AttributeValue
	var err error
	switch vtPair.valueType {
	case pcommon.ValueTypeStr:
		value := vtPair.value.(string)
		av = new(AttributeValue)
		av.SetStringValue(value)
	case pcommon.ValueTypeInt:
		value := vtPair.value.(int)
		av = new(AttributeValue)
		av.SetIntValue(value)
	case pcommon.ValueTypeDouble:
		value := vtPair.value.(float64)
		av = new(AttributeValue)
		av.SetDoubleValue(value)
	case pcommon.ValueTypeBool:
		value := vtPair.value.(int8)
		av = new(AttributeValue)
		av.SetBoolValue(value)
	case pcommon.ValueTypeMap:
		value := vtPair.value.(string)
		av = new(AttributeValue)
		av.SetStringValue(value)
	case pcommon.ValueTypeSlice:
		value := vtPair.value.(string)
		av = new(AttributeValue)
		av.SetStringValue(value)
	default:
		err = fmt.Errorf("unknown value type %v", vtPair.valueType)
	}

	if err != nil {
		return av, nil
	}

	return nil, err

}

// ValidateStruct
//
//	@param s
//	@return err
func ValidateStruct(s interface{}) (err error) {
	// first make sure that the input is a struct
	// having any other type, especially a pointer to a struct,
	// might result in panic
	structType := reflect.TypeOf(s)
	if structType.Kind() != reflect.Struct {
		return errors.New("input param should be a struct")
	}

	// now go one by one through the fields and validate their value
	structVal := reflect.ValueOf(s)
	fieldNum := structVal.NumField()

	for i := 0; i < fieldNum; i++ {
		// Field(i) returns i'th value of the struct
		field := structVal.Field(i)
		fieldName := structType.Field(i).Name

		// CAREFUL! IsZero interprets empty strings and int equal 0 as a zero value.
		// To check only if the pointers have been initialized,
		// you can check the kind of the field:
		// if field.Kind() == reflect.Pointer { // check }

		// IsZero panics if the value is invalid.
		// Most functions and methods never return an invalid Value.
		isSet := field.IsValid() && !field.IsZero()

		if !isSet {
			err = errors.New(fmt.Sprintf("%v%s in not set; ", err, fieldName))
		}

	}

	return err
}

// ChunkBySize - Splits a slice into multiple slices of the given size
//
//	@param items
//	@param chunkSize
//	@return [][]T
func ChunkBySize[T any](items []T, chunkSize int) [][]T {
	var _chunks = make([][]T, 0, (len(items)/chunkSize)+1)
	for chunkSize < len(items) {
		items, _chunks = items[chunkSize:], append(_chunks, items[0:chunkSize:chunkSize])
	}
	return append(_chunks, items)
}

// ConvertToInterfaceSlice
//
//	@param items
//	@return []interface{}
func ConvertToInterfaceSlice[T any](items []T) []interface{} {
	interfaceSlice := make([]interface{}, len(items))
	for i := range items {
		interfaceSlice[i] = items[i]
	}
	return interfaceSlice
}
