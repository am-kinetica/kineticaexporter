package kineticaotelexporter

import (
	"context"
	"encoding/hex"

	"github.com/google/uuid"
	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type kineticaLogsExporter struct {
	logger *zap.Logger

	writer *KiWriter
}

type kineticaLogRecord struct {
	log               *Log
	logAttribute      []LogAttribute
	resourceAttribute []ResourceAttribute
	scopeAttribute    []ScopeAttribute
}

// newLogsExporter
//
//	@param logger
//	@param cfg
//	@return *kineticaLogsExporter
//	@return error
func newLogsExporter(logger *zap.Logger, cfg *Config) (*kineticaLogsExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	writer := NewKiWriter(context.TODO(), *cfg, logger)
	logsExp := &kineticaLogsExporter{
		logger: logger,
		writer: writer,
	}
	return logsExp, nil
}

// pushLogsData
//
//	@receiver e
//	@param ctx
//	@param ld
//	@return error
func (e *kineticaLogsExporter) pushLogsData(ctx context.Context, logData plog.Logs) error {
	var errs []error
	var logRecords []kineticaLogRecord

	resourceLogs := logData.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		resource := rl.Resource()
		scopeLogs := rl.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			scopeLog := scopeLogs.At(j)
			logs := scopeLogs.At(j).LogRecords()
			for k := 0; k < logs.Len(); k++ {
				if kiLogRecord, err := e.createLogRecord(ctx, resource, scopeLog.Scope(), logs.At(k)); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}

					errs = append(errs, err)
				} else {
					logRecords = append(logRecords, *kiLogRecord)
				}
			}
		}
	}

	if err := e.writer.persistLogRecord(logRecords); err != nil {
		errs = append(errs, err)
	}
	return multierr.Combine(errs...)
}

// createLogRecord //
//
//	@receiver e
//	@param ctx
//	@param resource
//	@param instrumentationLibrary
//	@param logRecord
//	@return *kineticaLogRecord
//	@return error
func (e *kineticaLogsExporter) createLogRecord(ctx context.Context, resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, logRecord plog.LogRecord) (*kineticaLogRecord, error) {
	var errs []error
	ts := logRecord.Timestamp().AsTime().Unix()
	ots := logRecord.ObservedTimestamp().AsTime().Unix()

	tags := make(map[string]string)
	fields := make(map[string]interface{})

	// TODO handle logRecord.Flags()

	if traceID := logRecord.TraceID(); !traceID.IsEmpty() {
		tags[AttributeTraceID] = hex.EncodeToString(traceID[:])
		if spanID := logRecord.SpanID(); !spanID.IsEmpty() {
			tags[AttributeSpanID] = hex.EncodeToString(spanID[:])
		}
	}

	if severityNumber := logRecord.SeverityNumber(); severityNumber != plog.SeverityNumberUnspecified {
		fields[AttributeSeverityNumber] = int64(severityNumber)
	}
	if severityText := logRecord.SeverityText(); severityText != "" {
		fields[AttributeSeverityText] = severityText
	}

	if v, err := AttributeValueToKineticaFieldValue(logRecord.Body()); err != nil {
		e.logger.Debug("invalid log record body", zap.String("Error", err.Error()))
		fields[AttributeBody] = nil
	} else {
		fields[AttributeBody] = v
	}

	var logAttribute []LogAttribute
	logAttributes := make(map[string]ValueTypePair)
	droppedAttributesCount := uint64(logRecord.DroppedAttributesCount())
	logRecord.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			droppedAttributesCount++
			e.logger.Debug("log record attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err != nil {
			droppedAttributesCount++
			e.logger.Debug("invalid log record attribute value", zap.String("Error", err.Error()))
		} else {
			logAttributes[k] = v
		}
		return true
	})
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
	}

	severityNumber, ok := (fields[AttributeSeverityNumber]).(int)
	if !ok {
		severityNumber = 0
		e.logger.Warn("severity_number conversion failed, possibly not an integer; storing 0")
	}

	severityText, ok := (fields[AttributeSeverityText]).(string)
	if !ok {
		e.logger.Warn("severity_text conversion failed, possibly not a string; storing empty string")
		severityText = ""
	}

	// create log - Body, dropped_attribute_count and flags not handled now
	log := NewLog(uuid.New().String(), tags[AttributeTraceID], tags[AttributeSpanID], ts, ots, int8(severityNumber), severityText, "", 0)
	// _, err := log.insertLog()
	// errs = append(errs, err)

	// Insert log attributes
	for key := range logAttributes {
		vtPair := logAttributes[key]
		la := newLogAttributeValue(log.LogID, key, vtPair)
		logAttribute = append(logAttribute, *la)
	}

	var resourceAttribute []ResourceAttribute
	// Insert resource attributes
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
		ra := newResourceAttributeValue(log.LogID, key, vtPair)
		resourceAttribute = append(resourceAttribute, *ra)
	}

	// Insert scope attributes
	var scopeAttribute []ScopeAttribute
	scopeAttributes := make(map[string]ValueTypePair)
	scopeName := instrumentationLibrary.Name()
	scopeVersion := instrumentationLibrary.Version()
	instrumentationLibrary.Attributes().Range(func(k string, v pcommon.Value) bool {
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
		sa := newScopeAttributeValue(log.LogID, key, scopeName, scopeVersion, vtPair)
		scopeAttribute = append(scopeAttribute, *sa)
	}

	kiLogRecord := new(kineticaLogRecord)
	kiLogRecord.log = log
	copy(kiLogRecord.logAttribute, logAttribute)
	copy(kiLogRecord.resourceAttribute, resourceAttribute)
	copy(kiLogRecord.scopeAttribute, scopeAttribute)

	return kiLogRecord, multierr.Combine(errs...)

}

// newLogAttributeValue
//
//	@param logID
//	@param key
//	@param vtPair
//	@return *LogsAttribute
func newLogAttributeValue(logID string, key string, vtPair ValueTypePair) *LogAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		la := NewLogAttribute(logID, key, *av)
		return la
	}

	return nil
}

// newResourceAttributeValue
//
//	@param resourceID
//	@param key
//	@param vtPair
//	@return *LogsResourceAttribute
func newResourceAttributeValue(resourceID string, key string, vtPair ValueTypePair) *ResourceAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		la := NewResourceAttribute(resourceID, key, *av)
		return la
	}

	return nil
}

// newScopeAttributeValue newResourceAttributeValue
//
//	@param key
//	@param scopeName
//	@param scopeVersion
//	@param vtPair
//	@return *LogsScopeAttribute
func newScopeAttributeValue(scopeID string, key string, scopeName string, scopeVersion string, vtPair ValueTypePair) *ScopeAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := NewScopeAttribute(scopeID, key, scopeName, scopeVersion, *av)
		return sa
	}

	return nil
}
