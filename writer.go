package kineticaotelexporter

import (
	"context"
	"fmt"
	"sync"

	"github.com/am-kinetica/gpudb-api-go/gpudb"
	"github.com/google/uuid"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// AttributeValue
type AttributeValue struct {
	IntValue    int     `avro:"int_value"`
	StringValue string  `avro:"string_value"`
	BoolValue   int8    `avro:"bool_value"`
	DoubleValue float64 `avro:"double_value"`
	BytesValue  []byte  `avro:"bytes_value"`
}

// NewAttributeValue Constructor for AttributeValue
//
//	@param intValue
//	@param stringValue
//	@param boolValue
//	@param doubleValue
//	@param bytesValue
//	@return *AttributeValue
func NewAttributeValue(intValue int, stringValue string, boolValue int8, doubleValue float64, bytesValue []byte) *AttributeValue {
	o := new(AttributeValue)
	o.IntValue = intValue
	o.StringValue = stringValue
	o.BoolValue = boolValue
	o.DoubleValue = doubleValue
	o.BytesValue = bytesValue
	return o
}

// GetIntValue
//
//	@receiver attributevalue
//	@return int
func (attributevalue *AttributeValue) GetIntValue() int {
	return attributevalue.IntValue
}

// GetStringValue
//
//	@receiver attributevalue
//	@return string
func (attributevalue *AttributeValue) GetStringValue() string {
	return attributevalue.StringValue
}

// GetBoolValue
//
//	@receiver attributevalue
//	@return int8
func (attributevalue *AttributeValue) GetBoolValue() int8 {
	return attributevalue.BoolValue
}

// GetDoubleValue
//
//	@receiver attributevalue
//	@return float64
func (attributevalue *AttributeValue) GetDoubleValue() float64 {
	return attributevalue.DoubleValue
}

// GetBytesValue
//
//	@receiver attributevalue
//	@return []byte
func (attributevalue *AttributeValue) GetBytesValue() []byte {
	return attributevalue.BytesValue
}

// SetIntValue
//
//	@receiver attributevalue
//	@param IntValue
//	@return *AttributeValue
func (attributevalue *AttributeValue) SetIntValue(IntValue int) *AttributeValue {
	attributevalue.IntValue = IntValue
	return attributevalue
}

// SetStringValue
//
//	@receiver attributevalue
//	@param StringValue
//	@return *AttributeValue
func (attributevalue *AttributeValue) SetStringValue(StringValue string) *AttributeValue {
	attributevalue.StringValue = StringValue
	return attributevalue
}

// SetBoolValue
//
//	@receiver attributevalue
//	@param BoolValue
//	@return *AttributeValue
func (attributevalue *AttributeValue) SetBoolValue(BoolValue int8) *AttributeValue {
	attributevalue.BoolValue = BoolValue
	return attributevalue
}

// SetDoubleValue
//
//	@receiver attributevalue
//	@param DoubleValue
//	@return *AttributeValue
func (attributevalue *AttributeValue) SetDoubleValue(DoubleValue float64) *AttributeValue {
	attributevalue.DoubleValue = DoubleValue
	return attributevalue
}

// SetBytesValue
//
//	@receiver attributevalue
//	@param BytesValue
//	@return *AttributeValue
func (attributevalue *AttributeValue) SetBytesValue(BytesValue []byte) *AttributeValue {
	attributevalue.BytesValue = BytesValue
	return attributevalue
}

// BEGIN Log Handling

// Log
type Log struct {
	LogID                string `mapstructure:"log_id" avro:"log_id"`
	ResourceID           string `mapstructure:"resource_id" avro:"resource_id"`
	ScopeID              string `mapstructure:"scope_id" avro:"scope_id"`
	TraceID              string `mapstructure:"trace_id" avro:"trace_id"`
	SpanID               string `mapstructure:"span_id" avro:"span_id"`
	TimeUnixNano         string `mapstructure:"time_unix_nano" avro:"time_unix_nano"`
	ObservedTimeUnixNano string `mapstructure:"observerd_time_unix_nano" avro:"observerd_time_unix_nano"`
	SeverityID           int8   `mapstructure:"severity_id" avro:"severity_id"`
	SeverityText         string `mapstructure:"severity_text" avro:"severity_text"`
	Body                 string `mapstructure:"body" avro:"body"`
	Flags                int    `mapstructure:"flags" avro:"flags"`
}

// NewLog Constructor for Logs
//
//	@param LogID
//	@param ResourceID
//	@param ScopeID
//	@param TraceID
//	@param SpanID
//	@param TimeUnixNano
//	@param ObservedTimeUnixNano
//	@param SeverityID
//	@param SeverityText
//	@param Body
//	@param Flags
//	@return *Logs
func NewLog(LogID string, ResourceID string, ScopeID string, TraceID string, SpanID string, TimeUnixNano string, ObservedTimeUnixNano string, SeverityID int8, SeverityText string, Body string, Flags int) *Log {
	o := new(Log)

	o.LogID = LogID
	o.ResourceID = ResourceID
	o.ScopeID = ScopeID
	o.TraceID = TraceID
	o.SpanID = SpanID
	o.TimeUnixNano = TimeUnixNano
	o.ObservedTimeUnixNano = ObservedTimeUnixNano
	o.SeverityID = SeverityID
	o.SeverityText = SeverityText
	o.Body = Body
	o.Flags = Flags

	return o
}

// GetLogID
//
//	@receiver logs
//	@return uuid.UUID
func (logs *Log) GetLogID() string {
	return logs.LogID
}

// GetResourceID
//
//	@receiver logs
//	@return uuid.UUID
func (logs *Log) GetResourceID() string {
	return logs.ResourceID
}

// GetScopeID
//
//	@receiver logs
//	@return uuid.UUID
func (logs *Log) GetScopeID() string {
	return logs.ScopeID
}

// GetTraceID
//
//	@receiver logs
//	@return string
func (logs *Log) GetTraceID() string {
	return logs.TraceID
}

// GetSpanID
//
//	@receiver logs
//	@return string
func (logs *Log) GetSpanID() string {
	return logs.SpanID
}

// GetTimeUnixNano
//
//	@receiver logs
//	@return uint64
func (logs *Log) GetTimeUnixNano() string {
	return logs.TimeUnixNano
}

// GetObservedTimeUnixNano
//
//	@receiver logs
//	@return uint64
func (logs *Log) GetObservedTimeUnixNano() string {
	return logs.ObservedTimeUnixNano
}

// GetSeverityID
//
//	@receiver logs
//	@return int8
func (logs *Log) GetSeverityID() int8 {
	return logs.SeverityID
}

// GetSeverityText
//
//	@receiver logs
//	@return string
func (logs *Log) GetSeverityText() string {
	return logs.SeverityText
}

// GetBody
//
//	@receiver logs
//	@return string
func (logs *Log) GetBody() string {
	return logs.Body
}

// GetFlags
//
//	@receiver logs
//	@return int
func (logs *Log) GetFlags() int {
	return logs.Flags
}

// SetLogID
//
//	@receiver logs
//	@param LogID
//	@return *Logs
func (logs *Log) SetLogID(LogID string) *Log {
	logs.LogID = LogID
	return logs
}

// SetResourceID
//
//	@receiver logs
//	@param ResourceID
//	@return *Logs
func (logs *Log) SetResourceID(ResourceID string) *Log {
	logs.ResourceID = ResourceID
	return logs
}

// SetScopeID
//
//	@receiver logs
//	@param ScopeID
//	@return *Logs
func (logs *Log) SetScopeID(ScopeID string) *Log {
	logs.ScopeID = ScopeID
	return logs
}

// SetTraceID
//
//	@receiver logs
//	@param TraceID
//	@return *Logs
func (logs *Log) SetTraceID(TraceID string) *Log {
	logs.TraceID = TraceID
	return logs
}

// SetSpanID
//
//	@receiver logs
//	@param SpanID
//	@return *Logs
func (logs *Log) SetSpanID(SpanID string) *Log {
	logs.SpanID = SpanID
	return logs
}

// SetTimeUnixNano
//
//	@receiver logs
//	@param TimeUnixNano
//	@return *Logs
func (logs *Log) SetTimeUnixNano(TimeUnixNano string) *Log {
	logs.TimeUnixNano = TimeUnixNano
	return logs
}

// SetObservedTimeUnixNano
//
//	@receiver logs
//	@param ObservedTimeUnixNano
//	@return *Logs
func (logs *Log) SetObservedTimeUnixNano(ObservedTimeUnixNano string) *Log {
	logs.ObservedTimeUnixNano = ObservedTimeUnixNano
	return logs
}

// SetSeverityID
//
//	@receiver logs
//	@param SeverityID
//	@return *Logs
func (logs *Log) SetSeverityID(SeverityID int8) *Log {
	logs.SeverityID = SeverityID
	return logs
}

// SetSeverityText
//
//	@receiver logs
//	@param SeverityText
//	@return *Logs
func (logs *Log) SetSeverityText(SeverityText string) *Log {
	logs.SeverityText = SeverityText
	return logs
}

// SetBody
//
//	@receiver logs
//	@param Body
//	@return *Logs
func (logs *Log) SetBody(Body string) *Log {
	logs.Body = Body
	return logs
}

// SetFlags
//
//	@receiver logs
//	@param Flags
//	@return *Logs
func (logs *Log) SetFlags(Flags int) *Log {
	logs.Flags = Flags
	return logs
}

// LogAttribute
type LogAttribute struct {
	LogID          string `avro:"log_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// NewLogAttribute Constructor for LogsAttribute
//
//	@param key
//	@param attributes
//	@return *LogsAttribute
func NewLogAttribute(logID string, key string, attributes AttributeValue) *LogAttribute {
	o := new(LogAttribute)
	o.LogID = logID
	o.Key = key
	o.AttributeValue = attributes
	return o
}

// END LogAttribute

// ResourceAttribute
type ResourceAttribute struct {
	ResourceID     string `avro:"resource_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// NewResourceAttribute Constructor for LogsResourceAttribute
//
//	@param resourceID
//	@param key
//	@param attributes
//	@return *LogsResourceAttribute
func NewResourceAttribute(resourceID string, key string, attributes AttributeValue) *ResourceAttribute {
	o := new(ResourceAttribute)
	o.ResourceID = resourceID
	o.Key = key
	o.AttributeValue = attributes
	return o
}

// End LogResourceAttribute

// ScopeAttribute
type ScopeAttribute struct {
	ScopeID        string `avro:"scope_id"`
	ScopeName      string `avro:"scope_name"`
	ScopeVersion   string `avro:"scope_version"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// NewScopeAttribute Constructor for LogsScopeAttribute
//
//	@param scopeID
//	@param key
//	@param scopeName
//	@param scopeVersion
//	@param attributes
//	@return *LogsScopeAttribute
func NewScopeAttribute(scopeID string, key string, scopeName string, scopeVersion string, attributes AttributeValue) *ScopeAttribute {
	o := new(ScopeAttribute)
	o.ScopeID = uuid.New().String()
	o.Key = key
	o.ScopeName = scopeName
	o.ScopeVersion = scopeVersion
	o.AttributeValue = attributes
	return o
}

// END LogScopeAttribute

// KiWriter
type KiWriter struct {
	Db      gpudb.Gpudb
	Options gpudb.GpudbOptions
	cfg     Config
	logger  *zap.Logger
}

// GetDb
//
//	@receiver kiwriter
//	@return gpudb.Gpudb
func (kiwriter *KiWriter) GetDb() gpudb.Gpudb {
	return kiwriter.Db
}

// GetOptions
//
//	@receiver kiwriter
//	@return gpudb.GpudbOptions
func (kiwriter *KiWriter) GetOptions() gpudb.GpudbOptions {
	return kiwriter.Options
}

// GetCfg
//
//	@receiver kiwriter
//	@return Config
func (kiwriter *KiWriter) GetCfg() Config {
	return kiwriter.cfg
}

// SetDb
//
//	@receiver kiwriter
//	@param Db
//	@return *kiwriter
func (kiwriter *KiWriter) SetDb(Db gpudb.Gpudb) *KiWriter {
	kiwriter.Db = Db
	return kiwriter
}

// SetOptions
//
//	@receiver kiwriter
//	@param Options
//	@return *kiwriter
func (kiwriter *KiWriter) SetOptions(Options gpudb.GpudbOptions) *KiWriter {
	kiwriter.Options = Options
	return kiwriter
}

// SetCfg
//
//	@receiver kiwriter
//	@param cfg
//	@return *kiwriter
func (kiwriter *KiWriter) SetCfg(cfg Config) *KiWriter {
	kiwriter.cfg = cfg
	return kiwriter
}

// Writer - global pointer to kiwriter struct initialized in the init func
var Writer *KiWriter

// init
func init() {
	ctx := context.TODO()
	cfg := CreateDefaultConfig()
	config := cfg.(*Config)
	options := gpudb.GpudbOptions{Username: config.Username, Password: config.Password, ByPassSslCertCheck: config.BypassSslCertCheck}
	gpudbInst := gpudb.NewWithOptions(ctx, config.Host, &options)
	Writer = &KiWriter{*gpudbInst, options, *config, nil}
}

// NewKiWriter
//
//	@param ctx
//	@param cfg
//	@return *KiWriter
func NewKiWriter(ctx context.Context, cfg Config, logger *zap.Logger) *KiWriter {
	options := gpudb.GpudbOptions{Username: cfg.Username, Password: cfg.Password, ByPassSslCertCheck: cfg.BypassSslCertCheck}
	gpudbInst := gpudb.NewWithOptions(ctx, cfg.Host, &options)
	return &KiWriter{*gpudbInst, options, cfg, logger}
}

// GetGpuDbInst
//
//	@param cfg
//	@return *gpudb.Gpudb
func GetGpuDbInst(cfg *Config) *gpudb.Gpudb {
	ctx := context.TODO()
	options := gpudb.GpudbOptions{Username: cfg.Username, Password: cfg.Password, ByPassSslCertCheck: cfg.BypassSslCertCheck}
	// fmt.Println("Options", options)
	gpudbInst := gpudb.NewWithOptions(ctx, cfg.Host, &options)

	return gpudbInst

}

// insertLog //
//
//	@receiver logs
//	@return *Logs
//	@return error
func (logs *Log) insertLog() (*Log, error) {
	if logs.LogID == "" {
		logs.LogID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertLog, Writer.cfg.Schema, logs.LogID, logs.ResourceID, logs.ScopeID, logs.TraceID, logs.SpanID, logs.TimeUnixNano, logs.ObservedTimeUnixNano, logs.SeverityID, logs.SeverityText, logs.Body, logs.Flags)
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return nil, err
	}
	return logs, nil
}

// insertLogAttribute //
//
//	@receiver logAttribute
//	@param logID
//	@return error
func (logAttribute *LogAttribute) insertLogAttribute() error {

	if logAttribute.LogID == "" {
		logAttribute.LogID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertLogAttribute, Writer.cfg.Schema, logAttribute.LogID, logAttribute.Key, logAttribute.AttributeValue.StringValue, logAttribute.BoolValue, logAttribute.AttributeValue.IntValue, logAttribute.AttributeValue.DoubleValue, logAttribute.AttributeValue.BytesValue)
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return err
	}
	return nil
}

// insertLogResourceAttribute //
//
//	@receiver logResourceAttribute
//	@return error
func (logResourceAttribute *ResourceAttribute) insertLogResourceAttribute() error {

	if logResourceAttribute.ResourceID == "" {
		logResourceAttribute.ResourceID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertLogResourceAttribute, Writer.cfg.Schema, logResourceAttribute.ResourceID, logResourceAttribute.Key, logResourceAttribute.AttributeValue.StringValue, logResourceAttribute.GetBoolValue(), logResourceAttribute.GetIntValue(), logResourceAttribute.GetDoubleValue(), logResourceAttribute.GetBytesValue())
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return err
	}

	return nil
}

// insertLogScopeAttribute //
//
//	@receiver logScopeAttribute
//	@return error
func (logScopeAttribute *ScopeAttribute) insertLogScopeAttribute() error {
	if logScopeAttribute.ScopeID == "" {
		logScopeAttribute.ScopeID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertLogScopeAttribute, Writer.cfg.Schema, logScopeAttribute.ScopeID, logScopeAttribute.ScopeName, logScopeAttribute.ScopeVersion, logScopeAttribute.Key, logScopeAttribute.GetStringValue(), logScopeAttribute.GetBoolValue(), logScopeAttribute.GetIntValue(), logScopeAttribute.GetDoubleValue(), logScopeAttribute.GetBytesValue())
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return err
	}

	return nil
}

// END Log Handling

// BEGIN Trace Handling

// Span
type Span struct {
	ID                     string `mapstructure:"id" avro:"id" `
	ResourceID             string `mapstructure:"resource_id" avro:"resource_id" `
	ScopeID                string `mapstructure:"scope_id" avro:"scope_id" `
	EventID                string `mapstructure:"event_id" avro:"event_id" `
	LinkID                 string `mapstructure:"link_id" avro:"link_id" `
	TraceID                string `mapstructure:"trace_id" avro:"trace_id"`
	SpanID                 string `mapstructure:"span_id" avro:"span_id"`
	ParentSpanID           string `mapstructure:"parent_span_id" avro:"parent_span_id"`
	TraceState             string `mapstructure:"trace_state" avro:"trace_state"`
	Name                   string `mapstructure:"name" avro:"name"`
	SpanKind               int8   `mapstructure:"span_kind" avro:"span_kind"`
	StartTimeUnixNano      string `mapstructure:"start_time_unix_nano" avro:"start_time_unix_nano"`
	EndTimeUnixNano        string `mapstructure:"end_time_unix_nano" avro:"end_time_unix_nano"`
	DroppedAttributesCount int    `mapstructure:"dropped_attributes_count" avro:"dropped_attributes_count"`
	DroppedEventsCount     int    `mapstructure:"dropped_events_count" avro:"dropped_events_count"`
	DroppedLinksCount      int    `mapstructure:"dropped_links_count" avro:"dropped_links_count"`
	Message                string `mapstructure:"message" avro:"message"`
	StatusCode             int8   `mapstructure:"status_code" avro:"status_code"`
}

// NewSpan Constructor for Span
//
//	@param resourceID
//	@param scopeID
//	@param eventID
//	@param linkID
//	@param traceID
//	@param spanID
//	@param parentSpanID
//	@param traceState
//	@param name
//	@param spanKind
//	@param startTimeUnixNano
//	@param endTimeUnixNano
//	@param droppedAttributeCount
//	@param droppedEventCount
//	@param droppedLinkCount
//	@param message
//	@param statusCode
//	@return *Span
func NewSpan(resourceID string, scopeID string, eventID string, linkID string, traceID string, spanID string, parentSpanID string, traceState string, name string, spanKind int8, startTimeUnixNano string, endTimeUnixNano string, droppedAttributeCount int, droppedEventCount int, droppedLinkCount int, message string, statusCode int8) *Span {
	o := new(Span)
	o.ID = uuid.New().String()
	o.ResourceID = resourceID
	o.ScopeID = scopeID
	o.EventID = eventID
	o.LinkID = linkID
	o.TraceID = traceID
	o.SpanID = spanID
	o.ParentSpanID = parentSpanID
	o.TraceState = traceState
	o.Name = name
	o.SpanKind = spanKind
	o.StartTimeUnixNano = startTimeUnixNano
	o.EndTimeUnixNano = endTimeUnixNano
	o.DroppedAttributesCount = droppedAttributeCount
	o.DroppedEventsCount = droppedEventCount
	o.DroppedLinksCount = droppedLinkCount
	o.Message = message
	o.StatusCode = statusCode
	return o
}

// GetID
//
//	@receiver span
//	@return uuid.UUID
func (span *Span) GetID() string {
	return span.ID
}

// GetResourceID
//
//	@receiver span
//	@return uuid.UUID
func (span *Span) GetResourceID() string {
	return span.ResourceID
}

// GetScopeID
//
//	@receiver span
//	@return uuid.UUID
func (span *Span) GetScopeID() string {
	return span.ScopeID
}

// GetEventID
//
//	@receiver span
//	@return uuid.UUID
func (span *Span) GetEventID() string {
	return span.EventID
}

// GetLinkID
//
//	@receiver span
//	@return uuid.UUID
func (span *Span) GetLinkID() string {
	return span.LinkID
}

// GetTraceID
//
//	@receiver span
//	@return string
func (span *Span) GetTraceID() string {
	return span.TraceID
}

// GetSpanID
//
//	@receiver span
//	@return string
func (span *Span) GetSpanID() string {
	return span.SpanID
}

// GetParentSpanID
//
//	@receiver span
//	@return string
func (span *Span) GetParentSpanID() string {
	return span.ParentSpanID
}

// GetTraceState
//
//	@receiver span
//	@return string
func (span *Span) GetTraceState() string {
	return span.TraceState
}

// GetName
//
//	@receiver span
//	@return string
func (span *Span) GetName() string {
	return span.Name
}

// GetSpanKind
//
//	@receiver span
//	@return int8
func (span *Span) GetSpanKind() int8 {
	return span.SpanKind
}

// GetStartTimeUnixNano
//
//	@receiver span
//	@return uint64
func (span *Span) GetStartTimeUnixNano() string {
	return span.StartTimeUnixNano
}

// GetEndTimeUnixNano
//
//	@receiver span
//	@return uint64
func (span *Span) GetEndTimeUnixNano() string {
	return span.EndTimeUnixNano
}

// GetDroppedAttributeCount
//
//	@receiver span
//	@return int
func (span *Span) GetDroppedAttributeCount() int {
	return span.DroppedAttributesCount
}

// GetDroppedEventCount
//
//	@receiver span
//	@return int
func (span *Span) GetDroppedEventCount() int {
	return span.DroppedEventsCount
}

// GetDroppedLinkCount
//
//	@receiver span
//	@return int
func (span *Span) GetDroppedLinkCount() int {
	return span.DroppedLinksCount
}

// GetMessage
//
//	@receiver span
//	@return string
func (span *Span) GetMessage() string {
	return span.Message
}

// GetStatusCode
//
//	@receiver span
//	@return int8
func (span *Span) GetStatusCode() int8 {
	return span.StatusCode
}

// SetID
//
//	@receiver span
//	@param ID
//	@return *Span
func (span *Span) SetID(ID string) *Span {
	span.ID = ID
	return span
}

// SetResourceID
//
//	@receiver span
//	@param resourceID
//	@return *Span
func (span *Span) SetResourceID(resourceID string) *Span {
	span.ResourceID = resourceID
	return span
}

// SetScopeID
//
//	@receiver span
//	@param scopeID
//	@return *Span
func (span *Span) SetScopeID(scopeID string) *Span {
	span.ScopeID = scopeID
	return span
}

// SetEventID
//
//	@receiver span
//	@param eventID
//	@return *Span
func (span *Span) SetEventID(eventID string) *Span {
	span.EventID = eventID
	return span
}

// SetLinkID
//
//	@receiver span
//	@param linkID
//	@return *Span
func (span *Span) SetLinkID(linkID string) *Span {
	span.LinkID = linkID
	return span
}

// SetTraceID
//
//	@receiver span
//	@param traceID
//	@return *Span
func (span *Span) SetTraceID(traceID string) *Span {
	span.TraceID = traceID
	return span
}

// SetSpanID
//
//	@receiver span
//	@param spanID
//	@return *Span
func (span *Span) SetSpanID(spanID string) *Span {
	span.SpanID = spanID
	return span
}

// SetParentSpanID
//
//	@receiver span
//	@param parentSpanID
//	@return *Span
func (span *Span) SetParentSpanID(parentSpanID string) *Span {
	span.ParentSpanID = parentSpanID
	return span
}

// SetTraceState
//
//	@receiver span
//	@param traceState
//	@return *Span
func (span *Span) SetTraceState(traceState string) *Span {
	span.TraceState = traceState
	return span
}

// SetName
//
//	@receiver span
//	@param name
//	@return *Span
func (span *Span) SetName(name string) *Span {
	span.Name = name
	return span
}

// SetSpanKind
//
//	@receiver span
//	@param spanKind
//	@return *Span
func (span *Span) SetSpanKind(spanKind int8) *Span {
	span.SpanKind = spanKind
	return span
}

// SetStartTimeUnixNano
//
//	@receiver span
//	@param startTimeUnixNano
//	@return *Span
func (span *Span) SetStartTimeUnixNano(startTimeUnixNano string) *Span {
	span.StartTimeUnixNano = startTimeUnixNano
	return span
}

// SetEndTimeUnixNano
//
//	@receiver span
//	@param endTimeUnixNano
//	@return *Span
func (span *Span) SetEndTimeUnixNano(endTimeUnixNano string) *Span {
	span.EndTimeUnixNano = endTimeUnixNano
	return span
}

// SetDroppedAttributeCount
//
//	@receiver span
//	@param droppedAttributeCount
//	@return *Span
func (span *Span) SetDroppedAttributeCount(droppedAttributeCount int) *Span {
	span.DroppedAttributesCount = droppedAttributeCount
	return span
}

// SetDroppedEventCount
//
//	@receiver span
//	@param droppedEventCount
//	@return *Span
func (span *Span) SetDroppedEventCount(droppedEventCount int) *Span {
	span.DroppedEventsCount = droppedEventCount
	return span
}

// SetDroppedLinkCount
//
//	@receiver span
//	@param droppedLinkCount
//	@return *Span
func (span *Span) SetDroppedLinkCount(droppedLinkCount int) *Span {
	span.DroppedLinksCount = droppedLinkCount
	return span
}

// SetMessage
//
//	@receiver span
//	@param message
//	@return *Span
func (span *Span) SetMessage(message string) *Span {
	span.Message = message
	return span
}

// SetStatusCode
//
//	@receiver span
//	@param statusCode
//	@return *Span
func (span *Span) SetStatusCode(statusCode int8) *Span {
	span.StatusCode = statusCode
	return span
}

// insertTraceSpan //
//
//	@receiver span
//	@return uuid.UUID
//	@return error
func (span *Span) insertTraceSpan() (string, error) {
	if span.ID == "" {
		span.ID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertTraceSpan, Writer.cfg.Schema, span.ID, span.ResourceID, span.ScopeID, span.EventID, span.LinkID, span.TraceID, span.SpanID, span.ParentSpanID, span.TraceState, span.Name, span.SpanKind, span.StartTimeUnixNano, span.EndTimeUnixNano, span.DroppedAttributesCount, span.DroppedEventsCount, span.DroppedLinksCount, span.Message, span.StatusCode)
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return "", err
	}
	return span.ID, nil

}

// SpanAttribute
type SpanAttribute struct {
	SpanID         string `avro:"span_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// NewSpanAttribute Constructor for SpanAttribute
//
//	@param spanID
//	@param key
//	@param atributeValue
//	@return *SpanAttribute
func NewSpanAttribute(spanID string, key string, attributeValue AttributeValue) *SpanAttribute {
	o := new(SpanAttribute)
	o.SpanID = spanID
	o.Key = key
	o.AttributeValue = attributeValue
	return o
}

// insertSpanAttribute //
//
//	@receiver spanAttribute
//	@return uuid.UUID
//	@return error
func (spanAttribute *SpanAttribute) insertSpanAttribute() (string, error) {
	if spanAttribute.SpanID == "" {
		spanAttribute.SpanID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertTraceSpanAttribute, Writer.cfg.Schema, spanAttribute.SpanID, spanAttribute.Key, spanAttribute.GetStringValue(), spanAttribute.BoolValue, spanAttribute.GetIntValue(), spanAttribute.GetDoubleValue(), spanAttribute.GetBytesValue())
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return "", err
	}
	return spanAttribute.SpanID, nil
}

// TraceResourceAttribute
type TraceResourceAttribute struct {
	ResourceID     string `avro:"resource_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:"",squash`
}

// NewTraceResourceAttribute Constructor for TraceResourceAttribute
//
//	@param key
//	@param attributes
//	@return *TraceResourceAttribute
func NewTraceResourceAttribute(key string, attributes AttributeValue) *TraceResourceAttribute {
	o := new(TraceResourceAttribute)
	o.ResourceID = uuid.New().String()
	o.Key = key
	o.AttributeValue = attributes
	return o
}

// insertTraceResourceAttribute //
//
//	@receiver resourceAttribute
//	@return uuid.UUID
//	@return error
func (resourceAttribute *ResourceAttribute) insertTraceResourceAttribute() (string, error) {
	if resourceAttribute.ResourceID == "" {
		resourceAttribute.ResourceID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertTraceResourceAttribute, Writer.cfg.Schema, resourceAttribute.ResourceID, resourceAttribute.Key, resourceAttribute.GetStringValue(), resourceAttribute.BoolValue, resourceAttribute.GetIntValue(), resourceAttribute.GetDoubleValue(), resourceAttribute.GetBytesValue())
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return "", err
	}

	return resourceAttribute.ResourceID, nil
}

// End TraceResourceAttribute

// TraceScopeAttribute
type TraceScopeAttribute struct {
	ScopeID        string `avro:"scope_id"`
	ScopeName      string `avro:"scope_name"`
	ScopeVersion   string `avro:"scope_version"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// NewtraceScopeAttribute Constructor for TraceScopeAttribute
//
//	@param key
//	@param scopeName
//	@param scopeVersion
//	@param attributes
//	@return *TraceScopeAttribute
func NewtraceScopeAttribute(key string, scopeName string, scopeVersion string, attributes AttributeValue) *TraceScopeAttribute {
	o := new(TraceScopeAttribute)
	o.ScopeID = uuid.New().String()
	o.Key = key
	o.ScopeName = scopeName
	o.ScopeVersion = scopeVersion
	o.AttributeValue = attributes
	return o
}

// insertTraceScopeAttribute //
//
//	@receiver traceScopeAttribute
//	@return uuid.UUID
//	@return error
func (traceScopeAttribute *ScopeAttribute) insertTraceScopeAttribute() (string, error) {
	if traceScopeAttribute.ScopeID == "" {
		traceScopeAttribute.ScopeID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertTraceScopeAttribute, Writer.cfg.Schema, traceScopeAttribute.ScopeID, traceScopeAttribute.ScopeName, traceScopeAttribute.ScopeVersion, traceScopeAttribute.Key, traceScopeAttribute.GetStringValue(), traceScopeAttribute.GetBoolValue(), traceScopeAttribute.GetIntValue(), traceScopeAttribute.GetDoubleValue(), traceScopeAttribute.GetBytesValue())
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return "", err
	}

	return traceScopeAttribute.ScopeID, nil
}

// END TraceScopeAttribute

// EventAttribute
type EventAttribute struct {
	EventID        string `avro:"event_id"`
	EventName      string `avro:"event_name"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// NewEventAttribute Constructor for TraceEventAttribute
//
//	@param key
//	@param eventName
//	@param attributes
//	@return *TraceEventAttribute
func NewEventAttribute(eventID string, eventName string, key string, attributes AttributeValue) *EventAttribute {
	o := new(EventAttribute)
	o.EventID = eventID
	o.Key = key
	o.EventName = eventName
	o.AttributeValue = attributes
	return o
}

// insertTraceEventAttribute insertTraceScopeAttribute insertLogScopeAttribute //
//
//	@receiver traceEventAttribute
//	@return uuid.UUID
//	@return error
func (traceEventAttribute *EventAttribute) insertTraceEventAttribute() (string, error) {
	if traceEventAttribute.EventID == "" {
		traceEventAttribute.EventID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertTraceEventAttribute, Writer.cfg.Schema, traceEventAttribute.EventID, traceEventAttribute.EventName, traceEventAttribute.Key, traceEventAttribute.GetStringValue(), traceEventAttribute.GetBoolValue(), traceEventAttribute.GetIntValue(), traceEventAttribute.GetDoubleValue(), traceEventAttribute.GetBytesValue())
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return "", err
	}

	return traceEventAttribute.EventID, err
}

// END TraceEventAttribute

// LinkAttribute
type LinkAttribute struct {
	LinkID         string `avro:"link_id"`
	TraceID        string `avro:"trace_id"`
	SpanID         string `avro:"span_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// NewLinkAttribute Constructor for LinkAttribute
//
//	@param linkID
//	@param key
//	@param traceID
//	@param spanID
//	@param attributes
//	@return *LinkAttribute
func NewLinkAttribute(linkID string, key string, traceID string, spanID string, attributes AttributeValue) *LinkAttribute {
	o := new(LinkAttribute)
	o.LinkID = linkID
	o.Key = key
	o.TraceID = traceID
	o.SpanID = spanID
	o.AttributeValue = attributes
	return o
}

// insertTraceLinkAttribute  //
//
//	@receiver traceLinkAttribute
//	@return uuid.UUID
func (traceLinkAttribute *LinkAttribute) insertTraceLinkAttribute() (string, error) {
	if traceLinkAttribute.LinkID == "" {
		traceLinkAttribute.LinkID = uuid.New().String()
	}

	statement := fmt.Sprintf(InsertTraceLinkAttribute, Writer.cfg.Schema, traceLinkAttribute.LinkID, traceLinkAttribute.TraceID, traceLinkAttribute.SpanID, traceLinkAttribute.Key, traceLinkAttribute.GetStringValue(), traceLinkAttribute.GetBoolValue(), traceLinkAttribute.GetIntValue(), traceLinkAttribute.GetDoubleValue(), traceLinkAttribute.GetBytesValue())
	gpudb := Writer.Db
	_, err := gpudb.ExecuteSqlRaw(context.TODO(), statement, 0, 0, "", nil)
	if err != nil {
		return "", err
	}

	return traceLinkAttribute.LinkID, nil
}

// END LinkAttribute

// END Trace Handling

// Metrics Handling

// Gauge
type Gauge struct {
	GaugeID     string `avro:"gauge_id"`
	ResourceID  string `avro:"resource_id"`
	ScopeID     string `avro:"scope_id"`
	MetricName  string `avro:"metric_name"`
	Description string `avro:"metric_description"`
	Unit        string `avro:"metric_unit"`
}

// GaugeDatapoint
type GaugeDatapoint struct {
	GaugeID       string  `avro:"gauge_id"`
	ID            string  `avro:"id"`
	StartTimeUnix string  `mapstructure:"start_time_unix" avro:"start_time_unix"`
	TimeUnix      string  `mapstructure:"time_unix" avro:"time_unix"`
	GaugeValue    float64 `mapstructure:"gauge_value" avro:"gauge_value"`
	Flags         int     `mapstructure:"flags" avro:"flags"`
}

// GaugeDatapointAttribute
type GaugeDatapointAttribute struct {
	GaugeID        string `avro:"gauge_id"`
	DatapointID    string `avro:"datapoint_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// GaugeDatapointExemplar
type GaugeDatapointExemplar struct {
	GaugeID     string  `avro:"gauge_id"`
	DatapointID string  `avro:"datapoint_id"`
	ExemplarID  string  `avro:"exemplar_id"`
	TimeUnix    string  `mapstructure:"time_unix" avro:"time_unix"`
	GaugeValue  float64 `mapstructure:"gauge_value" avro:"gauge_value"`
	TraceID     string  `mapstructure:"trace_id" avro:"trace_id"`
	SpanID      string  `mapstructure:"span_id" avro:"span_id"`
}

// GaugeDataPointExemplarAttribute
type GaugeDataPointExemplarAttribute struct {
	GaugeID        string `avro:"gauge_id"`
	DatapointID    string `avro:"datapoint_id"`
	ExemplarID     string `avro:"exemplar_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// GaugeResourceAttribute
type GaugeResourceAttribute struct {
	ResourceID     string `avro:"resource_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// GaugeScopeAttribute
type GaugeScopeAttribute struct {
	ScopeID        string `avro:"scope_id"`
	ScopeName      string `avro:"scope_name"`
	ScopeVersion   string `avro:"scope_version"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// END Gauge

// Sum

// Sum
type Sum struct {
	SumID                  string `avro:"sum_id"`
	ResourceID             string `avro:"resource_id"`
	ScopeID                string `avro:"scope_id"`
	MetricName             string `avro:"metric_name"`
	Description            string `avro:"metric_description"`
	Unit                   string `avro:"metric_unit"`
	AggregationTemporality int8   `avro:"aggregation_temporality"`
	IsMonotonic            int8   `avro:"is_monotonic"`
}

// SumDatapoint
type SumDatapoint struct {
	SumID         string  `avro:"sum_id"`
	ID            string  `avro:"id"`
	StartTimeUnix string  `mapstructure:"start_time_unix" avro:"start_time_unix"`
	TimeUnix      string  `mapstructure:"time_unix" avro:"time_unix"`
	SumValue      float64 `mapstructure:"sum_value" avro:"sum_value"`
	Flags         int     `mapstructure:"flags" avro:"flags"`
}

// SumDataPointAttribute
type SumDataPointAttribute struct {
	SumID          string `avro:"sum_id"`
	DatapointID    string `avro:"datapoint_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// SumDatapointExemplar
type SumDatapointExemplar struct {
	SumID       string  `avro:"sum_id"`
	DatapointID string  `avro:"datapoint_id"`
	ExemplarID  string  `avro:"exemplar_id"`
	TimeUnix    string  `mapstructure:"time_unix" avro:"time_unix"`
	SumValue    float64 `mapstructure:"sum_value" avro:"sum_value"`
	TraceID     string  `mapstructure:"trace_id" avro:"trace_id"`
	SpanID      string  `mapstructure:"span_id" avro:"span_id"`
}

type SumDataPointExemplarAttribute struct {
	SumID          string `avro:"sum_id"`
	DatapointID    string `avro:"datapoint_id"`
	ExemplarID     string `avro:"exemplar_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// SumResourceAttribute
type SumResourceAttribute struct {
	ResourceID     string `avro:"resource_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// SumScopeAttribute
type SumScopeAttribute struct {
	ScopeID        string `avro:"scope_id"`
	ScopeName      string `avro:"scope_name"`
	ScopeVersion   string `avro:"scope_version"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// END Sum

// Histogram

// Histogram
type Histogram struct {
	HistogramID            string `avro:"histogram_id"`
	ResourceID             string `avro:"resource_id"`
	ScopeID                string `avro:"scope_id"`
	MetricName             string `avro:"metric_name"`
	Description            string `avro:"metric_description"`
	Unit                   string `avro:"metric_unit"`
	AggregationTemporality int8   `avro:"aggregation_temporality"`
}

// HistogramDatapoint
type HistogramDatapoint struct {
	HistogramID   string  `avro:"histogram_id"`
	ID            string  `avro:"id"`
	StartTimeUnix string  `avro:"start_time_unix"`
	TimeUnix      string  `avro:"time_unix"`
	Count         uint64  `avro:"count"`
	Sum           float64 `avro:"data_sum"`
	Min           float64 `avro:"data_min"`
	Max           float64 `avro:"data_max"`
	Flags         int     `avro:"flags"`
}

// HistogramDataPointAttribute
type HistogramDataPointAttribute struct {
	HistogramID    string `avro:"histogram_id"`
	DatapointID    string `avro:"datapoint_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

type HistogramDatapointBucketCount struct {
	HistogramID string `avro:"histogram_id"`
	DatapointID string `avro:"datapoint_id"`
	CountID     string `avro:"count_id"`
	Count       uint64 `avro:"count"`
}

type HistogramDatapointExplicitBound struct {
	HistogramID   string  `avro:"histogram_id"`
	DatapointID   string  `avro:"datapoint_id"`
	BoundID       string  `avro:"bound_id"`
	ExplicitBound float64 `avro:"explicit_bound"`
}

// HistogramDatapointExemplar
type HistogramDatapointExemplar struct {
	HistogramID    string  `avro:"histogram_id"`
	DatapointID    string  `avro:"datapoint_id"`
	ExemplarID     string  `avro:"exemplar_id"`
	TimeUnix       string  `avro:"time_unix"`
	HistogramValue float64 `avro:"histogram_value"`
	TraceID        string  `mapstructure:"trace_id" avro:"trace_id"`
	SpanID         string  `mapstructure:"span_id" avro:"span_id"`
}

// HistogramDataPointExemplarAttribute
type HistogramDataPointExemplarAttribute struct {
	HistogramID    string `avro:"histogram_id"`
	DatapointID    string `avro:"datapoint_id"`
	ExemplarID     string `avro:"exemplar_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// HistogramResourceAttribute
type HistogramResourceAttribute struct {
	ResourceID     string `avro:"resource_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// HistogramScopeAttribute
type HistogramScopeAttribute struct {
	ScopeID        string `avro:"scope_id"`
	ScopeName      string `avro:"scope_name"`
	ScopeVersion   string `avro:"scope_version"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// End Histogram

// Exponential Histogram

// ExponentialHistogram
type ExponentialHistogram struct {
	HistogramID            string `avro:"histogram_id"`
	ResourceID             string `avro:"resource_id"`
	ScopeID                string `avro:"scope_id"`
	MetricName             string `avro:"metric_name"`
	Description            string `avro:"metric_description"`
	Unit                   string `avro:"metric_unit"`
	AggregationTemporality int8   `avro:"aggregation_temporality"`
}

type ExponentialHistogramDatapoint struct {
	HistogramID           string  `avro:"histogram_id"`
	ID                    string  `avro:"id"`
	StartTimeUnix         string  `avro:"start_time_unix"`
	TimeUnix              string  `avro:"time_unix"`
	Count                 uint64  `avro:"count"`
	Sum                   float64 `avro:"data_sum"`
	Min                   float64 `avro:"data_min"`
	Max                   float64 `avro:"data_max"`
	Flags                 int     `avro:"flags"`
	Scale                 int     `avro:"scale"`
	ZeroCount             uint64  `avro:"zero_count"`
	BucketsPositiveOffset int     `avro:"buckets_positive_offset"`
	BucketsNegativeOffset int     `avro:"buckets_negative_offset"`
}

type ExponentialHistogramDataPointAttribute struct {
	HistogramID    string `avro:"histogram_id"`
	DatapointID    string `avro:"datapoint_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

type ExponentialHistogramBucketNegativeCount struct {
	HistogramID string `avro:"histogram_id"`
	DatapointID string `avro:"datapoint_id"`
	CountID     string `avro:"count_id"`
	Count       uint64 `avro:"count"`
}

type ExponentialHistogramBucketPositiveCount struct {
	HistogramID string `avro:"histogram_id"`
	DatapointID string `avro:"datapoint_id"`
	CountID     string `avro:"count_id"`
	Count       uint64 `avro:"count"`
}

type ExponentialHistogramDatapointExemplar struct {
	HistogramID    string  `avro:"histogram_id"`
	DatapointID    string  `avro:"datapoint_id"`
	ExemplarID     string  `avro:"exemplar_id"`
	TimeUnix       string  `avro:"time_unix"`
	HistogramValue float64 `avro:"histogram_value"`
	TraceID        string  `mapstructure:"trace_id" avro:"trace_id"`
	SpanID         string  `mapstructure:"span_id" avro:"span_id"`
}

// HistogramDataPointExemplarAttribute
type ExponentialHistogramDataPointExemplarAttribute struct {
	HistogramID    string `avro:"histogram_id"`
	DatapointID    string `avro:"datapoint_id"`
	ExemplarID     string `avro:"exemplar_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// HistogramResourceAttribute
type ExponentialHistogramResourceAttribute struct {
	ResourceID     string `avro:"resource_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// HistogramScopeAttribute
type ExponentialHistogramScopeAttribute struct {
	ScopeID        string `avro:"scope_id"`
	ScopeName      string `avro:"scope_name"`
	ScopeVersion   string `avro:"scope_version"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// END Exponential Histogram

// Summary

type Summary struct {
	SummaryID   string `avro:"summary_id"`
	ResourceID  string `avro:"resource_id"`
	ScopeID     string `avro:"scope_id"`
	MetricName  string `avro:"metric_name"`
	Description string `avro:"metric_description"`
	Unit        string `avro:"metric_unit"`
}

// SummaryDatapoint
type SummaryDatapoint struct {
	SummaryID     string  `avro:"summary_id"`
	ID            string  `avro:"id"`
	StartTimeUnix string  `avro:"start_time_unix"`
	TimeUnix      string  `avro:"time_unix"`
	Count         uint64  `avro:"count"`
	Sum           float64 `avro:"sum"`
	Flags         int     `avro:"flags"`
}

// SummaryDataPointAttribute
type SummaryDataPointAttribute struct {
	SummaryID      string `avro:"summary_id"`
	DatapointID    string `avro:"datapoint_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

type SummaryDatapointQuantileValues struct {
	SummaryID   string  `avro:"summary_id"`
	DatapointID string  `avro:"datapoint_id"`
	QuantileID  string  `avro:"quantile_id"`
	Quantile    float64 `avro:"quantile"`
	Value       float64 `avro:"value"`
}

// SummaryResourceAttribute
type SummaryResourceAttribute struct {
	ResourceID     string `avro:"resource_id"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// SummaryScopeAttribute
type SummaryScopeAttribute struct {
	ScopeID        string `avro:"scope_id"`
	ScopeName      string `avro:"scope_name"`
	ScopeVersion   string `avro:"scope_version"`
	Key            string `avro:"key"`
	AttributeValue `mapstructure:",squash"`
}

// END Summary

// END Metrics Handling

// persistLogRecord
//
//	@receiver kiwriter
//	@param logRecords
//	@return error
func (kiwriter *KiWriter) persistLogRecord(logRecords []kineticaLogRecord) error {

	var errs []error
	var logs []any
	var logAttribs []interface{}
	var resourceAttribs []interface{}
	var scopeAttribs []interface{}

	for _, logrecord := range logRecords {

		// For each chunk of 10K records persist eveything
		logs = append(logs, *logrecord.log)
		logAttribs = append(logAttribs, logrecord.logAttribute)
		resourceAttribs = append(resourceAttribs, logrecord.resourceAttribute)
		scopeAttribs = append(scopeAttribs, logrecord.scopeAttribute)
	}

	err := kiwriter.doChunkedInsert(context.TODO(), LogTable, logs)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), LogAttributeTable, logAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), LogResourceAttributeTable, resourceAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), LogScopeAttributeTable, scopeAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	return multierr.Combine(errs...)
}

// persistTraceRecord
//
//	@receiver kiwriter
//	@param traceRecords
//	@return error
func (kiwriter *KiWriter) persistTraceRecord(traceRecords []kineticaTraceRecord) error {
	var errs []error
	var spans []any
	var spanAttribs []interface{}
	var spanResourceAttribs []interface{}
	var spanScopeAttribs []interface{}
	var spanEventAttribs []interface{}
	var spanLinkAttribs []interface{}

	for _, tracerecord := range traceRecords {

		spans = append(spans, *tracerecord.span)
		spanAttribs = append(spanAttribs, tracerecord.spanAttribute)
		spanResourceAttribs = append(spanResourceAttribs, tracerecord.resourceAttribute)
		spanScopeAttribs = append(spanScopeAttribs, tracerecord.scopeAttribute)
		spanEventAttribs = append(spanEventAttribs, tracerecord.eventAttribute)
		spanLinkAttribs = append(spanLinkAttribs, tracerecord.linkAttribute)
	}

	err := kiwriter.doChunkedInsert(context.TODO(), TraceSpanTable, spans)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), TraceSpanAttributeTable, spanAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), TraceResourceAttributeTable, spanResourceAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), TraceScopeAttributeTable, spanScopeAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), TraceEventAttributeTable, spanEventAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	err = kiwriter.doChunkedInsert(context.TODO(), TraceLinkAttributeTable, spanLinkAttribs)
	if err != nil {
		errs = append(errs, err)
	}

	return multierr.Combine(errs...)
}

func (kiwriter *KiWriter) persistGaugeRecord(gaugeRecords []kineticaGaugeRecord) error {
	kiwriter.logger.Info("In persistGaugeRecord ...")

	var errs []error
	var gauges []any
	var resourceAttributes []any
	var scopeAttributes []any
	var datapoints []any
	var datapointAttributes []any
	var exemplars []any
	var exemplarAttributes []any

	var tableDataMap map[string][]any

	for _, gaugerecord := range gaugeRecords {

		gauges = append(gauges, *gaugerecord.gauge)

		for _, gr := range gaugerecord.resourceAttribute {
			resourceAttributes = append(resourceAttributes, gr)
		}

		for _, sa := range gaugerecord.scopeAttribute {
			scopeAttributes = append(scopeAttributes, sa)
		}

		for _, dp := range gaugerecord.datapoint {
			datapoints = append(datapoints, dp)
		}

		for _, dpattr := range gaugerecord.datapointAttribute {
			datapointAttributes = append(datapointAttributes, dpattr)
		}

		for _, ge := range gaugerecord.exemplars {
			exemplars = append(exemplars, ge)
		}

		for _, geattr := range gaugerecord.exemplarAttribute {
			exemplarAttributes = append(exemplarAttributes, geattr)
		}

	}

	tableDataMap = make(map[string][]any, 7)

	tableDataMap[GaugeTable] = gauges
	tableDataMap[GaugeResourceAttributeTable] = resourceAttributes
	tableDataMap[GaugeScopeAttributeTable] = scopeAttributes
	tableDataMap[GaugeDatapointTable] = datapoints
	tableDataMap[GaugeDatapointAttributeTable] = datapointAttributes
	tableDataMap[GaugeDatapointExemplarTable] = exemplars
	tableDataMap[GaugeDatapointExemplarAttributeTable] = exemplarAttributes

	for tableName, data := range tableDataMap {
		err := kiwriter.doChunkedInsert(context.TODO(), tableName, data)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return multierr.Combine(errs...)
}

func (kiwriter *KiWriter) persistSumRecord(sumRecords []kineticaSumRecord) error {
	kiwriter.logger.Info("In persistSumRecord ...")

	var errs []error

	var sums []any
	var resourceAttributes []any
	var scopeAttributes []any
	var datapoints []any
	var datapointAttributes []any
	var exemplars []any
	var exemplarAttributes []any

	var tableDataMap map[string][]any

	for _, sumrecord := range sumRecords {

		sums = append(sums, *sumrecord.sum)

		for _, sr := range sumrecord.sumResourceAttribute {
			resourceAttributes = append(resourceAttributes, sr)
		}

		for _, sa := range sumrecord.sumScopeAttribute {
			scopeAttributes = append(scopeAttributes, sa)
		}

		for _, dp := range sumrecord.datapoint {
			datapoints = append(datapoints, dp)
		}

		for _, dpattr := range sumrecord.datapointAttribute {
			datapointAttributes = append(datapointAttributes, dpattr)
		}

		for _, se := range sumrecord.exemplars {
			exemplars = append(exemplars, se)
		}

		for _, seattr := range sumrecord.exemplarAttribute {
			exemplarAttributes = append(exemplarAttributes, seattr)
		}

	}

	tableDataMap = make(map[string][]any, 7)

	tableDataMap[SumTable] = sums
	tableDataMap[SumResourceAttributeTable] = resourceAttributes
	tableDataMap[SumScopeAttributeTable] = scopeAttributes
	tableDataMap[SumDatapointTable] = datapoints
	tableDataMap[SumDatapointAttributeTable] = datapointAttributes
	tableDataMap[SumDatapointExemplarTable] = exemplars
	tableDataMap[SumDataPointExemplarAttributeTable] = exemplarAttributes

	for tableName, data := range tableDataMap {
		err := kiwriter.doChunkedInsert(context.TODO(), tableName, data)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return multierr.Combine(errs...)
}

func (kiwriter *KiWriter) persistHistogramRecord(histogramRecords []kineticaHistogramRecord) error {
	kiwriter.logger.Info("In persistHistogramRecord ...")

	var errs []error

	var histograms []any
	var resourceAttributes []any
	var scopeAttributes []any
	var datapoints []any
	var datapointAttributes []any
	var bucketCounts []any
	var explicitBounds []any
	var exemplars []any
	var exemplarAttributes []any

	var tableDataMap map[string][]any

	for _, histogramrecord := range histogramRecords {

		histograms = append(histograms, *histogramrecord.histogram)

		for _, ra := range histogramrecord.histogramResourceAttribute {
			resourceAttributes = append(resourceAttributes, ra)
		}

		for _, sa := range histogramrecord.histogramScopeAttribute {
			scopeAttributes = append(scopeAttributes, sa)
		}

		for _, dp := range histogramrecord.histogramDatapoint {
			datapoints = append(datapoints, dp)
		}

		for _, dpattr := range histogramrecord.histogramDatapointAtribute {
			datapointAttributes = append(datapointAttributes, dpattr)
		}

		for _, bc := range histogramrecord.histogramBucketCount {
			bucketCounts = append(bucketCounts, bc)
		}

		for _, eb := range histogramrecord.histogramExplicitBound {
			explicitBounds = append(explicitBounds, eb)
		}

		for _, ex := range histogramrecord.exemplars {
			exemplars = append(exemplars, ex)
		}

		for _, exattr := range histogramrecord.exemplarAttribute {
			exemplarAttributes = append(exemplarAttributes, exattr)
		}
	}

	tableDataMap = make(map[string][]any, 9)

	tableDataMap[HistogramTable] = histograms
	tableDataMap[HistogramResourceAttributeTable] = resourceAttributes
	tableDataMap[HistogramScopeAttributeTable] = scopeAttributes
	tableDataMap[HistogramDatapointTable] = datapoints
	tableDataMap[HistogramDatapointAttributeTable] = datapointAttributes
	tableDataMap[HistogramBucketCountsTable] = bucketCounts
	tableDataMap[HistogramExplicitBoundsTable] = explicitBounds
	tableDataMap[HistogramDatapointExemplarTable] = exemplars
	tableDataMap[HistogramDataPointExemplarAttributeTable] = exemplarAttributes

	for tableName, data := range tableDataMap {
		err := kiwriter.doChunkedInsert(context.TODO(), tableName, data)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return multierr.Combine(errs...)
}

func (kiwriter *KiWriter) persistExponentialHistogramRecord(exponentialHistogramRecords []kineticaExponentialHistogramRecord) error {
	kiwriter.logger.Info("In persistExponentialHistogramRecord ...")

	var errs []error

	var histograms []any
	var resourceAttributes []any
	var scopeAttributes []any
	var datapoints []any
	var datapointAttributes []any
	var positiveBucketCounts []any
	var negativeBucketCounts []any
	var exemplars []any
	var exemplarAttributes []any

	var tableDataMap map[string][]any

	for _, histogramrecord := range exponentialHistogramRecords {

		histograms = append(histograms, *histogramrecord.histogram)
		resourceAttributes = append(resourceAttributes, histogramrecord.histogramResourceAttribute)
		scopeAttributes = append(scopeAttributes, histogramrecord.histogramScopeAttribute)
		datapoints = append(datapoints, histogramrecord.histogramDatapoint)
		datapointAttributes = append(datapointAttributes, histogramrecord.histogramDatapointAttribute)
		positiveBucketCounts = append(positiveBucketCounts, histogramrecord.histogramBucketPositiveCount)
		negativeBucketCounts = append(negativeBucketCounts, histogramrecord.histogramBucketNegativeCount)
		exemplars = append(exemplars, histogramrecord.exemplars)
		exemplarAttributes = append(exemplarAttributes, histogramrecord.exemplarAttribute)
	}

	tableDataMap = make(map[string][]any, 9)

	tableDataMap[ExpHistogramTable] = histograms
	tableDataMap[ExpHistogramResourceAttributeTable] = resourceAttributes
	tableDataMap[ExpHistogramScopeAttributeTable] = scopeAttributes
	tableDataMap[ExpHistogramDatapointTable] = datapoints
	tableDataMap[ExpHistogramDatapointAttributeTable] = datapointAttributes
	tableDataMap[ExpHistogramPositiveBucketCountsTable] = positiveBucketCounts
	tableDataMap[ExpHistogramNegativeBucketCountsTable] = negativeBucketCounts
	tableDataMap[ExpHistogramDatapointExemplarTable] = exemplars
	tableDataMap[ExpHistogramDataPointExemplarAttributeTable] = exemplarAttributes

	for tableName, data := range tableDataMap {
		err := kiwriter.doChunkedInsert(context.TODO(), tableName, data)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return multierr.Combine(errs...)
}

func (kiwriter *KiWriter) persistSummaryRecord(summaryRecords []kineticaSummaryRecord) error {
	kiwriter.logger.Info("In persistSummaryRecord ...")

	var errs []error

	var summaries []any
	var resourceAttributes []any
	var scopeAttributes []any
	var datapoints []any
	var datapointAttributes []any
	var datapointQuantiles []any

	var tableDataMap map[string][]any

	for _, summaryrecord := range summaryRecords {

		summaries = append(summaries, *summaryrecord.summary)
		resourceAttributes = append(resourceAttributes, summaryrecord.summaryResourceAttribute)
		scopeAttributes = append(scopeAttributes, summaryrecord.summaryScopeAttribute)
		datapoints = append(datapoints, summaryrecord.summaryDatapoint)
		datapointAttributes = append(datapointAttributes, summaryrecord.summaryDatapointAttribute)
		datapointQuantiles = append(datapointQuantiles, summaryrecord.summaryDatapointQuantileValues)
	}

	tableDataMap = make(map[string][]any, 6)

	tableDataMap[SummaryTable] = summaries
	tableDataMap[SummaryResourceAttributeTable] = resourceAttributes
	tableDataMap[SummaryScopeAttributeTable] = scopeAttributes
	tableDataMap[SummaryDatapointTable] = datapoints
	tableDataMap[SummaryDatapointAttributeTable] = datapointAttributes
	tableDataMap[SummaryDatapointQuantileValueTable] = datapointQuantiles

	for tableName, data := range tableDataMap {
		err := kiwriter.doChunkedInsert(context.TODO(), tableName, data)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return multierr.Combine(errs...)

}

// doChunkedInsert - Write each chunk in a separate goroutine
//
//	@receiver kiwriter
//	@param ctx
//	@param tableName
//	@param records
//	@return error
func (kiwriter *KiWriter) doChunkedInsert(ctx context.Context, tableName string, records []any) error {

	// Build the final table name with the schema prepended
	var finalTable string
	if len(kiwriter.cfg.Schema) != 0 {
		finalTable = fmt.Sprintf("%s.%s", kiwriter.cfg.Schema, tableName)
	} else {
		finalTable = tableName
	}

	kiwriter.logger.Info("Writing to - ", zap.String("Table", finalTable), zap.Int("Recoord count", len(records)))

	recordChunks := ChunkBySize(records, ChunkSize)

	errsChan := make(chan error, len(recordChunks))

	wg := &sync.WaitGroup{}

	for _, recordChunk := range recordChunks {
		wg.Add(1)
		go func(data []any, wg *sync.WaitGroup) {
			_, err := kiwriter.Db.InsertRecordsRaw(context.TODO(), finalTable, data)
			errsChan <- err
			wg.Done()
		}(recordChunk, wg)
	}
	wg.Wait()
	close(errsChan)
	var errs error
	for err := range errsChan {
		errs = multierr.Append(errs, err)
	}
	return errs
}
