package kineticaotelexporter

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type kineticaMetricsExporter struct {
	logger *zap.Logger

	writer *KiWriter
}

// Metrics handling

type kineticaGaugeRecord struct {
	gauge              *Gauge
	resourceAttribute  []GaugeResourceAttribute
	scopeAttribute     []GaugeScopeAttribute
	datapoint          []GaugeDatapoint
	datapointAttribute []GaugeDatapointAttribute
	exemplars          []GaugeDatapointExemplar
	exemplarAttribute  []GaugeDataPointExemplarAttribute
}

type kineticaSumRecord struct {
	sum                  *Sum
	sumResourceAttribute []SumResourceAttribute
	sumScopeAttribute    []SumScopeAttribute
	datapoint            []SumDatapoint
	datapointAttribute   []SumDataPointAttribute
	exemplars            []SumDatapointExemplar
	exemplarAttribute    []SumDataPointExemplarAttribute
}

type kineticaHistogramRecord struct {
	histogram                  *Histogram
	histogramResourceAttribute []HistogramResourceAttribute
	histogramScopeAttribute    []HistogramScopeAttribute
	histogramDatapoint         []HistogramDatapoint
	histogramDatapointAtribute []HistogramDataPointAttribute
	histogramBucketCount       []HistogramDatapointBucketCount
	histogramExplicitBound     []HistogramDatapointExplicitBound
	exemplars                  []HistogramDatapointExemplar
	exemplarAttribute          []HistogramDataPointExemplarAttribute
}

type kineticaExponentialHistogramRecord struct {
	histogram                    *ExponentialHistogram
	histogramResourceAttribute   []ExponentialHistogramResourceAttribute
	histogramScopeAttribute      []ExponentialHistogramScopeAttribute
	histogramDatapoint           []ExponentialHistogramDatapoint
	histogramDatapointAttribute  []ExponentialHistogramDataPointAttribute
	histogramBucketNegativeCount []ExponentialHistogramBucketNegativeCount
	histogramBucketPositiveCount []ExponentialHistogramBucketPositiveCount
	exemplars                    []ExponentialHistogramDatapointExemplar
	exemplarAttribute            []ExponentialHistogramDataPointExemplarAttribute
}

type kineticaSummaryRecord struct {
	summary                        *Summary
	summaryDatapoint               []SummaryDatapoint
	summaryDatapointAttribute      []SummaryDataPointAttribute
	summaryResourceAttribute       []SummaryResourceAttribute
	summaryScopeAttribute          []SummaryScopeAttribute
	summaryDatapointQuantileValues []SummaryDatapointQuantileValues
}

func newMetricsExporter(logger *zap.Logger, cfg *Config) (*kineticaMetricsExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	writer := NewKiWriter(context.TODO(), *cfg)
	metricsExp := &kineticaMetricsExporter{
		logger: logger,
		writer: writer,
	}
	return metricsExp, nil
}

func (e *kineticaMetricsExporter) pushMetricsData(ctx context.Context, md pmetric.Metrics) error {
	var metricType pmetric.MetricType
	var errs []error

	var gaugeRecords []kineticaGaugeRecord
	var sumRecords []kineticaSumRecord
	var histogramRecords []kineticaHistogramRecord
	var exponentialHistogramRecords []kineticaExponentialHistogramRecord
	var summaryRecords []kineticaSummaryRecord

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		metrics := md.ResourceMetrics().At(i)
		resAttr := metrics.Resource().Attributes()
		for j := 0; j < metrics.ScopeMetrics().Len(); j++ {
			metricSlice := metrics.ScopeMetrics().At(j).Metrics()
			scopeInstr := metrics.ScopeMetrics().At(j).Scope()
			scopeURL := metrics.ScopeMetrics().At(j).SchemaUrl()
			for k := 0; k < metricSlice.Len(); k++ {
				metric := metricSlice.At(k)
				metricType = metric.Type()
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					gaugeRecord, err := e.createGaugeRecord(resAttr, metrics.SchemaUrl(), scopeInstr, scopeURL, metric.Gauge(), metric.Name(), metric.Description(), metric.Unit())
					if err != nil {
						gaugeRecords = append(gaugeRecords, *gaugeRecord)
					} else {
						errs = append(errs, err)
					}
				case pmetric.MetricTypeSum:
					sumRecord, err := e.createSumRecord(resAttr, metrics.SchemaUrl(), scopeInstr, scopeURL, metric.Sum(), metric.Name(), metric.Description(), metric.Unit())
					if err != nil {
						sumRecords = append(sumRecords, *sumRecord)
					} else {
						errs = append(errs, err)
					}
				case pmetric.MetricTypeHistogram:
					histogramRecord, err := e.createHistogramRecord(resAttr, metrics.SchemaUrl(), scopeInstr, scopeURL, metric.Histogram(), metric.Name(), metric.Description(), metric.Unit())
					if err != nil {
						histogramRecords = append(histogramRecords, *histogramRecord)
					} else {
						errs = append(errs, err)
					}
				case pmetric.MetricTypeExponentialHistogram:
					exponentialHistogramRecord, err := e.createExponentialHistogramRecord(resAttr, metrics.SchemaUrl(), scopeInstr, scopeURL, metric.ExponentialHistogram(), metric.Name(), metric.Description(), metric.Unit())
					if err != nil {
						exponentialHistogramRecords = append(exponentialHistogramRecords, *exponentialHistogramRecord)
					} else {
						errs = append(errs, err)
					}
				case pmetric.MetricTypeSummary:
					summaryRecord, err := e.createSummaryRecord(resAttr, metrics.SchemaUrl(), scopeInstr, scopeURL, metric.Summary(), metric.Name(), metric.Description(), metric.Unit())
					if err != nil {
						summaryRecords = append(summaryRecords, *summaryRecord)
					} else {
						errs = append(errs, err)
					}
				default:
					return fmt.Errorf("unsupported metrics type")
				}
				if errs != nil {
					return multierr.Combine(errs...)
				}
			}
		}
	}

	switch metricType {
	case pmetric.MetricTypeGauge:
		if err := e.writer.persistGaugeRecord(gaugeRecords); err != nil {
			errs = append(errs, err)
		}
	case pmetric.MetricTypeSum:
		if err := e.writer.persistSumRecord(sumRecords); err != nil {
			errs = append(errs, err)
		}
	case pmetric.MetricTypeHistogram:
		if err := e.writer.persistHistogramRecord(histogramRecords); err != nil {
			errs = append(errs, err)
		}
	case pmetric.MetricTypeExponentialHistogram:
		if err := e.writer.persistExponentialHistogramRecord(exponentialHistogramRecords); err != nil {
			errs = append(errs, err)
		}
	case pmetric.MetricTypeSummary:
		if err := e.writer.persistSummaryRecord(summaryRecords); err != nil {
			errs = append(errs, err)
		}
	default:
		return fmt.Errorf("unsupported metrics type")

	}
	return multierr.Combine(errs...)
}

// createSummaryRecord
//
//	@receiver e
//	@param resAttr
//	@param schemaURL
//	@param scopeInstr
//	@param scopeURL
//	@param summaryRecord
//	@param name
//	@param description
//	@param unit
//	@return *kineticaSummaryRecord
//	@return error
func (e *kineticaMetricsExporter) createSummaryRecord(resAttr pcommon.Map, schemaURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, summaryRecord pmetric.Summary, name, description, unit string) (*kineticaSummaryRecord, error) {
	var errs []error

	kiSummaryRecord := new(kineticaSummaryRecord)

	summary := &Summary{
		SummaryID:   uuid.New().String(),
		ResourceID:  uuid.New().String(),
		ScopeID:     uuid.New().String(),
		MetricName:  name,
		Description: description,
		Unit:        unit,
	}

	kiSummaryRecord.summary = summary

	// Handle data points
	var datapointAttribute []SummaryDataPointAttribute
	datapointAttributes := make(map[string]ValueTypePair)

	for i := 0; i < summaryRecord.DataPoints().Len(); i++ {
		datapoint := summaryRecord.DataPoints().At(i)
		summaryDatapoint := &SummaryDatapoint{
			SummaryID:     summary.SummaryID,
			ID:            uuid.New().String(),
			StartTimeUnix: strconv.Itoa(int(datapoint.StartTimestamp().AsTime().Unix())),
			TimeUnix:      strconv.Itoa(int(datapoint.Timestamp().AsTime().Unix())),
			Count:         datapoint.Count(),
			Sum:           datapoint.Sum(),
			Flags:         int(datapoint.Flags()),
		}
		kiSummaryRecord.summaryDatapoint = append(kiSummaryRecord.summaryDatapoint, *summaryDatapoint)

		// Handle summary datapoint attribute
		datapoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				e.logger.Debug("Sum record attribute key is empty")
			} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
				datapointAttributes[k] = v
			} else {
				e.logger.Debug("invalid sum record attribute value", zap.String("Error", err.Error()))
				errs = append(errs, err)
			}
			return true
		})

		for key := range datapointAttributes {
			vtPair := datapointAttributes[key]
			sa := e.newSummaryDatapointAttributeValue(summary.SummaryID, summaryDatapoint.ID, key, vtPair)
			datapointAttribute = append(datapointAttribute, *sa)
		}
		kiSummaryRecord.summaryDatapointAttribute = append(kiSummaryRecord.summaryDatapointAttribute, datapointAttribute...)

		for k := range datapointAttributes {
			delete(datapointAttributes, k)
		}

		// Handle quantile values
		quantileValues := datapoint.QuantileValues()
		for i := 0; i < quantileValues.Len(); i++ {
			quantileValue := quantileValues.At(i)
			summaryQV := &SummaryDatapointQuantileValues{
				SummaryID:   summary.SummaryID,
				DatapointID: summaryDatapoint.ID,
				QuantileID:  uuid.New().String(),
				Quantile:    quantileValue.Quantile(),
				Value:       quantileValue.Value(),
			}
			kiSummaryRecord.summaryDatapointQuantileValues = append(kiSummaryRecord.summaryDatapointQuantileValues, *summaryQV)
		}

	}

	// Handle Resource attribute
	var resourceAttribute []SummaryResourceAttribute
	resourceAttributes := make(map[string]ValueTypePair)

	resAttr.Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Resource attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			resourceAttributes[k] = v
		} else {
			e.logger.Debug("invalid resource attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range resourceAttributes {
		vtPair := resourceAttributes[key]
		ga := e.newSummaryResourceAttributeValue(summary.ResourceID, key, vtPair)
		resourceAttribute = append(resourceAttribute, *ga)
	}

	copy(kiSummaryRecord.summaryResourceAttribute, resourceAttribute)

	// Handle Scope attribute
	var scopeAttribute []SummaryScopeAttribute
	scopeAttributes := make(map[string]ValueTypePair)
	scopeName := scopeInstr.Name()
	scopeVersion := scopeInstr.Version()

	scopeInstr.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Scope attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			scopeAttributes[k] = v
		} else {
			e.logger.Debug("invalid scope attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range scopeAttributes {
		vtPair := scopeAttributes[key]
		sa := e.newSummaryScopeAttributeValue(summary.ScopeID, key, scopeName, scopeVersion, vtPair)
		scopeAttribute = append(scopeAttribute, *sa)
	}

	copy(kiSummaryRecord.summaryScopeAttribute, scopeAttribute)

	return kiSummaryRecord, multierr.Combine(errs...)
}

// createExponentialHistogramRecord
//
//	@receiver e
//	@param resAttr
//	@param schemaURL
//	@param scopeInstr
//	@param scopeURL
//	@param exponentialHistogramRecord
//	@param name
//	@param description
//	@param unit
//	@return *kineticaExponentialHistogramRecord
//	@return error
func (e *kineticaMetricsExporter) createExponentialHistogramRecord(resAttr pcommon.Map, schemaURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, exponentialHistogramRecord pmetric.ExponentialHistogram, name, description, unit string) (*kineticaExponentialHistogramRecord, error) {
	var errs []error

	kiExpHistogramRecord := new(kineticaExponentialHistogramRecord)

	histogram := &ExponentialHistogram{
		HistogramID:            uuid.New().String(),
		ResourceID:             uuid.New().String(),
		ScopeID:                uuid.New().String(),
		MetricName:             name,
		Description:            description,
		Unit:                   unit,
		AggregationTemporality: int8(exponentialHistogramRecord.AggregationTemporality()),
	}

	kiExpHistogramRecord.histogram = histogram

	// Handle data points
	var datapointAttribute []ExponentialHistogramDataPointAttribute
	datapointAttributes := make(map[string]ValueTypePair)

	var exemplarAttribute []ExponentialHistogramDataPointExemplarAttribute
	exemplarAttributes := make(map[string]ValueTypePair)

	var datapointBucketPositiveCount []ExponentialHistogramBucketPositiveCount
	var datapointBucketNegativeCount []ExponentialHistogramBucketNegativeCount

	for i := 0; i < exponentialHistogramRecord.DataPoints().Len(); i++ {
		datapoint := exponentialHistogramRecord.DataPoints().At(i)

		expHistogramDatapoint := ExponentialHistogramDatapoint{
			HistogramID:           histogram.HistogramID,
			ID:                    uuid.New().String(),
			StartTimeUnix:         strconv.Itoa(int(datapoint.StartTimestamp().AsTime().Unix())),
			TimeUnix:              strconv.Itoa(int(datapoint.Timestamp().AsTime().Unix())),
			Count:                 datapoint.Count(),
			Sum:                   datapoint.Sum(),
			Min:                   datapoint.Min(),
			Max:                   datapoint.Max(),
			Flags:                 int(datapoint.Flags()),
			Scale:                 int(datapoint.Scale()),
			ZeroCount:             datapoint.ZeroCount(),
			BucketsPositiveOffset: int(datapoint.Positive().Offset()),
			BucketsNegativeOffset: int(datapoint.Negative().Offset()),
		}
		kiExpHistogramRecord.histogramDatapoint = append(kiExpHistogramRecord.histogramDatapoint, expHistogramDatapoint)

		// Handle histogram datapoint attribute
		datapoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				e.logger.Debug("Sum record attribute key is empty")
			} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
				datapointAttributes[k] = v
			} else {
				e.logger.Debug("invalid sum record attribute value", zap.String("Error", err.Error()))
				errs = append(errs, err)
			}
			return true
		})

		for key := range datapointAttributes {
			vtPair := datapointAttributes[key]
			sa := e.newExponentialHistogramDatapointAttributeValue(histogram.HistogramID, expHistogramDatapoint.ID, key, vtPair)
			datapointAttribute = append(datapointAttribute, *sa)
		}
		kiExpHistogramRecord.histogramDatapointAttribute = append(kiExpHistogramRecord.histogramDatapointAttribute, datapointAttribute...)

		for k := range datapointAttributes {
			delete(datapointAttributes, k)
		}

		// Handle datapoint exemplars
		exemplars := datapoint.Exemplars()

		for i := 0; i < exemplars.Len(); i++ {
			exemplar := exemplars.At(i)
			sumDatapointExemplar := ExponentialHistogramDatapointExemplar{
				HistogramID:    histogram.HistogramID,
				DatapointID:    expHistogramDatapoint.ID,
				ExemplarID:     uuid.New().String(),
				TimeUnix:       strconv.Itoa(int(exemplar.Timestamp().AsTime().Unix())),
				HistogramValue: exemplar.DoubleValue(),
				TraceID:        exemplar.TraceID().String(),
				SpanID:         exemplar.SpanID().String(),
			}
			kiExpHistogramRecord.exemplars = append(kiExpHistogramRecord.exemplars, sumDatapointExemplar)

			// Handle Exemplar attribute
			exemplar.FilteredAttributes().Range(func(k string, v pcommon.Value) bool {
				if k == "" {
					e.logger.Debug("Sum record attribute key is empty")
				} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
					exemplarAttributes[k] = v
				} else {
					e.logger.Debug("invalid sum record attribute value", zap.String("Error", err.Error()))
					errs = append(errs, err)
				}
				return true
			})

			for key := range exemplarAttributes {
				vtPair := exemplarAttributes[key]
				ea := e.newExponentialHistogramDatapointExemplarAttributeValue(expHistogramDatapoint.HistogramID, expHistogramDatapoint.ID, sumDatapointExemplar.ExemplarID, key, vtPair)
				exemplarAttribute = append(exemplarAttribute, *ea)
			}

			kiExpHistogramRecord.exemplarAttribute = append(kiExpHistogramRecord.exemplarAttribute, exemplarAttribute...)

			for k := range exemplarAttributes {
				delete(exemplarAttributes, k)
			}

		}

		// Handle positive and negative bucket counts
		for i := 0; i < datapoint.Positive().BucketCounts().Len(); i++ {
			positiveBucketCount := datapoint.Positive().BucketCounts().At(i)
			datapointBucketPositiveCount = append(datapointBucketPositiveCount, ExponentialHistogramBucketPositiveCount{
				HistogramID: expHistogramDatapoint.HistogramID,
				DatapointID: expHistogramDatapoint.ID,
				CountID:     uuid.New().String(),
				Count:       positiveBucketCount,
			})
		}
		kiExpHistogramRecord.histogramBucketPositiveCount = append(kiExpHistogramRecord.histogramBucketPositiveCount, datapointBucketPositiveCount...)

		for i := 0; i < datapoint.Negative().BucketCounts().Len(); i++ {
			negativeBucketCount := datapoint.Negative().BucketCounts().At(i)
			datapointBucketNegativeCount = append(datapointBucketNegativeCount, ExponentialHistogramBucketNegativeCount{
				HistogramID: expHistogramDatapoint.HistogramID,
				DatapointID: expHistogramDatapoint.ID,
				CountID:     uuid.New().String(),
				Count:       negativeBucketCount,
			})
		}
		kiExpHistogramRecord.histogramBucketNegativeCount = append(kiExpHistogramRecord.histogramBucketNegativeCount, datapointBucketNegativeCount...)

	}

	// Handle Resource attribute
	var resourceAttribute []ExponentialHistogramResourceAttribute
	resourceAttributes := make(map[string]ValueTypePair)

	resAttr.Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Resource attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			resourceAttributes[k] = v
		} else {
			e.logger.Debug("invalid resource attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range resourceAttributes {
		vtPair := resourceAttributes[key]
		ga := e.newExponentialHistogramResourceAttributeValue(histogram.ResourceID, key, vtPair)
		resourceAttribute = append(resourceAttribute, *ga)
	}

	copy(kiExpHistogramRecord.histogramResourceAttribute, resourceAttribute)

	// Handle Scope attribute
	var scopeAttribute []ExponentialHistogramScopeAttribute
	scopeAttributes := make(map[string]ValueTypePair)
	scopeName := scopeInstr.Name()
	scopeVersion := scopeInstr.Version()

	scopeInstr.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Scope attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			scopeAttributes[k] = v
		} else {
			e.logger.Debug("invalid scope attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range scopeAttributes {
		vtPair := scopeAttributes[key]
		sa := e.newExponentialHistogramScopeAttributeValue(histogram.ScopeID, key, scopeName, scopeVersion, vtPair)
		scopeAttribute = append(scopeAttribute, *sa)
	}

	copy(kiExpHistogramRecord.histogramScopeAttribute, scopeAttribute)

	return kiExpHistogramRecord, multierr.Combine(errs...)
}

// createHistogramRecord
//
//	@receiver e
//	@param resAttr
//	@param schemaURL
//	@param scopeInstr
//	@param scopeURL
//	@param histogramRecord
//	@param name
//	@param description
//	@param unit
//	@return *kineticaHistogramRecord
//	@return error
func (e *kineticaMetricsExporter) createHistogramRecord(resAttr pcommon.Map, schemaURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, histogramRecord pmetric.Histogram, name, description, unit string) (*kineticaHistogramRecord, error) {
	var errs []error

	kiHistogramRecord := new(kineticaHistogramRecord)

	histogram := &Histogram{
		HistogramID:            uuid.New().String(),
		ResourceID:             uuid.New().String(),
		ScopeID:                uuid.New().String(),
		MetricName:             name,
		Description:            description,
		Unit:                   unit,
		AggregationTemporality: int8(histogramRecord.AggregationTemporality()),
	}

	kiHistogramRecord.histogram = histogram

	// Handle data points
	var datapointAttribute []HistogramDataPointAttribute
	datapointAttributes := make(map[string]ValueTypePair)

	var exemplarAttribute []HistogramDataPointExemplarAttribute
	exemplarAttributes := make(map[string]ValueTypePair)

	// Handle data points
	for i := 0; i < histogramRecord.DataPoints().Len(); i++ {
		datapoint := histogramRecord.DataPoints().At(i)

		histogramDatapoint := &HistogramDatapoint{
			HistogramID:   histogram.HistogramID,
			ID:            uuid.New().String(),
			StartTimeUnix: strconv.Itoa(int(datapoint.StartTimestamp().AsTime().Unix())),
			TimeUnix:      strconv.Itoa(int(datapoint.Timestamp().AsTime().Unix())),
			Count:         datapoint.Count(),
			Sum:           datapoint.Sum(),
			Min:           datapoint.Min(),
			Max:           datapoint.Max(),
			Flags:         int(datapoint.Flags()),
		}
		kiHistogramRecord.histogramDatapoint = append(kiHistogramRecord.histogramDatapoint, *histogramDatapoint)

		// Handle histogram datapoint attribute
		datapoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				e.logger.Debug("Histogram record attribute key is empty")
			} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
				datapointAttributes[k] = v
			} else {
				e.logger.Debug("invalid histogram record attribute value", zap.String("Error", err.Error()))
				errs = append(errs, err)
			}
			return true
		})

		for key := range datapointAttributes {
			vtPair := datapointAttributes[key]
			sa := e.newHistogramDatapointAttributeValue(histogram.HistogramID, histogramDatapoint.ID, key, vtPair)
			datapointAttribute = append(datapointAttribute, *sa)
		}
		kiHistogramRecord.histogramDatapointAtribute = append(kiHistogramRecord.histogramDatapointAtribute, datapointAttribute...)

		for k := range datapointAttributes {
			delete(datapointAttributes, k)
		}

		// Handle data point exemplars
		exemplars := datapoint.Exemplars()

		for i := 0; i < exemplars.Len(); i++ {
			exemplar := exemplars.At(i)
			histogramDatapointExemplar := HistogramDatapointExemplar{
				HistogramID:    histogram.HistogramID,
				DatapointID:    histogramDatapoint.ID,
				ExemplarID:     uuid.New().String(),
				TimeUnix:       strconv.Itoa(int(exemplar.Timestamp().AsTime().Unix())),
				HistogramValue: exemplar.DoubleValue(),
				TraceID:        exemplar.TraceID().String(),
				SpanID:         exemplar.SpanID().String(),
			}
			kiHistogramRecord.exemplars = append(kiHistogramRecord.exemplars, histogramDatapointExemplar)

			// Handle Exemplar attribute
			exemplar.FilteredAttributes().Range(func(k string, v pcommon.Value) bool {
				if k == "" {
					e.logger.Debug("Histogram record attribute key is empty")
				} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
					exemplarAttributes[k] = v
				} else {
					e.logger.Debug("invalid histogram record attribute value", zap.String("Error", err.Error()))
					errs = append(errs, err)
				}
				return true
			})

			for key := range exemplarAttributes {
				vtPair := exemplarAttributes[key]
				ea := e.newHistogramDatapointExemplarAttributeValue(histogramDatapoint.HistogramID, histogramDatapoint.ID, histogramDatapointExemplar.ExemplarID, key, vtPair)
				exemplarAttribute = append(exemplarAttribute, *ea)
			}

			kiHistogramRecord.exemplarAttribute = append(kiHistogramRecord.exemplarAttribute, exemplarAttribute...)

			for k := range exemplarAttributes {
				delete(exemplarAttributes, k)
			}
		}

		histogramBucketCounts := datapoint.BucketCounts()
		for i := 0; i < histogramBucketCounts.Len(); i++ {
			bucketCount := &HistogramDatapointBucketCount{
				HistogramID: histogramDatapoint.HistogramID,
				DatapointID: histogramDatapoint.ID,
				CountID:     uuid.New().String(),
				Count:       histogramBucketCounts.At(i),
			}
			kiHistogramRecord.histogramBucketCount = append(kiHistogramRecord.histogramBucketCount, *bucketCount)
		}

		histogramExplicitBounds := datapoint.ExplicitBounds()
		for i := 0; i < histogramExplicitBounds.Len(); i++ {
			explicitBound := HistogramDatapointExplicitBound{
				HistogramID:   histogramDatapoint.HistogramID,
				DatapointID:   histogramDatapoint.ID,
				BoundID:       uuid.New().String(),
				ExplicitBound: histogramExplicitBounds.At(i),
			}
			kiHistogramRecord.histogramExplicitBound = append(kiHistogramRecord.histogramExplicitBound, explicitBound)
		}
	}

	// Handle Resource attribute
	var resourceAttribute []HistogramResourceAttribute
	resourceAttributes := make(map[string]ValueTypePair)

	resAttr.Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Resource attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			resourceAttributes[k] = v
		} else {
			e.logger.Debug("invalid resource attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range resourceAttributes {
		vtPair := resourceAttributes[key]
		ga := e.newHistogramResourceAttributeValue(histogram.ResourceID, key, vtPair)
		resourceAttribute = append(resourceAttribute, *ga)
	}

	copy(kiHistogramRecord.histogramResourceAttribute, resourceAttribute)

	// Handle Scope attribute
	var scopeAttribute []HistogramScopeAttribute
	scopeAttributes := make(map[string]ValueTypePair)
	scopeName := scopeInstr.Name()
	scopeVersion := scopeInstr.Version()

	scopeInstr.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Scope attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			scopeAttributes[k] = v
		} else {
			e.logger.Debug("invalid scope attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range scopeAttributes {
		vtPair := scopeAttributes[key]
		sa := e.newHistogramScopeAttributeValue(histogram.ScopeID, key, scopeName, scopeVersion, vtPair)
		scopeAttribute = append(scopeAttribute, *sa)
	}

	copy(kiHistogramRecord.histogramScopeAttribute, scopeAttribute)

	return kiHistogramRecord, multierr.Combine(errs...)
}

// createSumRecord
//
//	@receiver e
//	@param resAttr
//	@param schemaURL
//	@param scopeInstr
//	@param scopeURL
//	@param sumRecord
//	@param name
//	@param description
//	@param unit
//	@return *kineticaSumRecord
//	@return error
func (e *kineticaMetricsExporter) createSumRecord(resAttr pcommon.Map, schemaURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, sumRecord pmetric.Sum, name, description, unit string) (*kineticaSumRecord, error) {
	var errs []error

	kiSumRecord := new(kineticaSumRecord)
	var isMonotonic int8
	if sumRecord.IsMonotonic() {
		isMonotonic = 1
	}

	sum := &Sum{
		SumID:                  uuid.New().String(),
		ResourceID:             uuid.New().String(),
		ScopeID:                uuid.New().String(),
		MetricName:             name,
		Description:            description,
		Unit:                   unit,
		AggregationTemporality: int8(sumRecord.AggregationTemporality()),
		IsMonotonic:            isMonotonic,
	}

	kiSumRecord.sum = sum

	// Handle data points
	var sumDatapointAttribute []SumDataPointAttribute
	sumDatapointAttributes := make(map[string]ValueTypePair)

	var exemplarAttribute []SumDataPointExemplarAttribute
	exemplarAttributes := make(map[string]ValueTypePair)

	for i := 0; i < sumRecord.DataPoints().Len(); i++ {
		datapoint := sumRecord.DataPoints().At(i)

		sumDatapoint := SumDatapoint{
			SumID:         sum.SumID,
			ID:            uuid.New().String(),
			StartTimeUnix: strconv.Itoa(int(datapoint.StartTimestamp().AsTime().Unix())),
			TimeUnix:      strconv.Itoa(int(datapoint.Timestamp().AsTime().Unix())),
			SumValue:      datapoint.DoubleValue(),
			Flags:         int(datapoint.Flags()),
		}
		kiSumRecord.datapoint = append(kiSumRecord.datapoint, sumDatapoint)

		// Handle Sum attribute
		datapoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				e.logger.Debug("Sum record attribute key is empty")
			} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
				sumDatapointAttributes[k] = v
			} else {
				e.logger.Debug("invalid sum record attribute value", zap.String("Error", err.Error()))
				errs = append(errs, err)
			}
			return true
		})

		for key := range sumDatapointAttributes {
			vtPair := sumDatapointAttributes[key]
			sa := e.newSumDatapointAttributeValue(sum.SumID, sumDatapoint.ID, key, vtPair)
			sumDatapointAttribute = append(sumDatapointAttribute, *sa)
		}
		kiSumRecord.datapointAttribute = append(kiSumRecord.datapointAttribute, sumDatapointAttribute...)

		for k := range sumDatapointAttributes {
			delete(sumDatapointAttributes, k)
		}

		// Handle data point exemplars
		exemplars := datapoint.Exemplars()

		for i := 0; i < exemplars.Len(); i++ {
			exemplar := exemplars.At(i)
			sumDatapointExemplar := SumDatapointExemplar{
				SumID:       sum.SumID,
				DatapointID: sumDatapoint.ID,
				ExemplarID:  uuid.New().String(),
				TimeUnix:    strconv.Itoa(int(exemplar.Timestamp().AsTime().Unix())),
				SumValue:    exemplar.DoubleValue(),
				TraceID:     exemplar.TraceID().String(),
				SpanID:      exemplar.SpanID().String(),
			}
			kiSumRecord.exemplars = append(kiSumRecord.exemplars, sumDatapointExemplar)

			// Handle Exemplar attribute
			exemplar.FilteredAttributes().Range(func(k string, v pcommon.Value) bool {
				if k == "" {
					e.logger.Debug("Sum record attribute key is empty")
				} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
					exemplarAttributes[k] = v
				} else {
					e.logger.Debug("invalid sum record attribute value", zap.String("Error", err.Error()))
					errs = append(errs, err)
				}
				return true
			})

			for key := range exemplarAttributes {
				vtPair := exemplarAttributes[key]
				ea := e.newSumDatapointExemplarAttributeValue(sum.SumID, sumDatapoint.ID, sumDatapointExemplar.ExemplarID, key, vtPair)
				exemplarAttribute = append(exemplarAttribute, *ea)
			}

			kiSumRecord.exemplarAttribute = append(kiSumRecord.exemplarAttribute, exemplarAttribute...)

			for k := range exemplarAttributes {
				delete(exemplarAttributes, k)
			}

		}

	}

	// Handle Resource attribute
	var resourceAttribute []SumResourceAttribute
	resourceAttributes := make(map[string]ValueTypePair)

	resAttr.Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Resource attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			resourceAttributes[k] = v
		} else {
			e.logger.Debug("invalid resource attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range resourceAttributes {
		vtPair := resourceAttributes[key]
		ga := e.newSumResourceAttributeValue(sum.ResourceID, key, vtPair)
		resourceAttribute = append(resourceAttribute, *ga)
	}

	copy(kiSumRecord.sumResourceAttribute, resourceAttribute)

	// Handle Scope attribute
	var scopeAttribute []SumScopeAttribute
	scopeAttributes := make(map[string]ValueTypePair)
	scopeName := scopeInstr.Name()
	scopeVersion := scopeInstr.Version()

	scopeInstr.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Scope attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			scopeAttributes[k] = v
		} else {
			e.logger.Debug("invalid scope attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range scopeAttributes {
		vtPair := scopeAttributes[key]
		sa := e.newSumScopeAttributeValue(sum.ScopeID, key, scopeName, scopeVersion, vtPair)
		scopeAttribute = append(scopeAttribute, *sa)
	}

	copy(kiSumRecord.sumScopeAttribute, scopeAttribute)

	return kiSumRecord, multierr.Combine(errs...)
}

// createGaugeRecord
//
//	@receiver e
//	@param resAttr
//	@param schemaURL
//	@param scopeInstr
//	@param scopeURL
//	@param gaugeRecord
//	@param name
//	@param description
//	@param unit
//	@return *kineticaGaugeRecord
//	@return error
func (e *kineticaMetricsExporter) createGaugeRecord(resAttr pcommon.Map, schemaURL string, scopeInstr pcommon.InstrumentationScope, scopeURL string, gaugeRecord pmetric.Gauge, name, description, unit string) (*kineticaGaugeRecord, error) {

	var errs []error

	kiGaugeRecord := new(kineticaGaugeRecord)

	gauge := &Gauge{
		GaugeID:     uuid.New().String(),
		ResourceID:  uuid.New().String(),
		ScopeID:     uuid.New().String(),
		MetricName:  name,
		Description: description,
		Unit:        unit,
	}

	kiGaugeRecord.gauge = gauge

	// Handle data points
	var gaugeDatapointAttribute []GaugeDatapointAttribute
	gaugeDatapointAttributes := make(map[string]ValueTypePair)

	var exemplarAttribute []GaugeDataPointExemplarAttribute
	exemplarAttributes := make(map[string]ValueTypePair)

	for i := 0; i < gaugeRecord.DataPoints().Len(); i++ {
		datapoint := gaugeRecord.DataPoints().At(i)

		gaugeDatapoint := GaugeDatapoint{
			GaugeID:       gauge.GaugeID,
			ID:            uuid.New().String(),
			StartTimeUnix: strconv.Itoa(int(datapoint.StartTimestamp().AsTime().Unix())),
			TimeUnix:      strconv.Itoa(int(datapoint.Timestamp().AsTime().Unix())),
			GaugeValue:    datapoint.DoubleValue(),
			Flags:         int(datapoint.Flags()),
		}
		kiGaugeRecord.datapoint = append(kiGaugeRecord.datapoint, gaugeDatapoint)

		datapoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				e.logger.Debug("Gauge record attribute key is empty")
			} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
				gaugeDatapointAttributes[k] = v
			} else {
				e.logger.Debug("invalid gauge record attribute value", zap.String("Error", err.Error()))
				errs = append(errs, err)
			}
			return true
		})

		for key := range gaugeDatapointAttributes {
			vtPair := gaugeDatapointAttributes[key]
			ga := e.newGaugeDatapointAttributeValue(gauge.GaugeID, gaugeDatapoint.ID, key, vtPair)
			gaugeDatapointAttribute = append(gaugeDatapointAttribute, *ga)
		}

		kiGaugeRecord.datapointAttribute = append(kiGaugeRecord.datapointAttribute, gaugeDatapointAttribute...)

		for k := range gaugeDatapointAttributes {
			delete(gaugeDatapointAttributes, k)
		}

		// Handle data point exemplars
		exemplars := datapoint.Exemplars()
		for i := 0; i < exemplars.Len(); i++ {
			exemplar := exemplars.At(i)
			gaugeDatapointExemplar := GaugeDatapointExemplar{
				GaugeID:     gauge.GaugeID,
				DatapointID: gaugeDatapoint.ID,
				ExemplarID:  uuid.New().String(),
				TimeUnix:    strconv.Itoa(int(exemplar.Timestamp().AsTime().Unix())),
				GaugeValue:  exemplar.DoubleValue(),
				TraceID:     exemplar.TraceID().String(),
				SpanID:      exemplar.SpanID().String(),
			}
			kiGaugeRecord.exemplars = append(kiGaugeRecord.exemplars, gaugeDatapointExemplar)

			// Handle Exemplar attribute
			exemplar.FilteredAttributes().Range(func(k string, v pcommon.Value) bool {
				if k == "" {
					e.logger.Debug("Sum record attribute key is empty")
				} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
					exemplarAttributes[k] = v
				} else {
					e.logger.Debug("invalid sum record attribute value", zap.String("Error", err.Error()))
					errs = append(errs, err)
				}
				return true
			})

			for key := range exemplarAttributes {
				vtPair := exemplarAttributes[key]
				ea := e.newGaugeDatapointExemplarAttributeValue(gauge.GaugeID, gaugeDatapoint.ID, gaugeDatapointExemplar.ExemplarID, key, vtPair)
				exemplarAttribute = append(exemplarAttribute, *ea)
			}

			kiGaugeRecord.exemplarAttribute = append(kiGaugeRecord.exemplarAttribute, exemplarAttribute...)

			for k := range exemplarAttributes {
				delete(exemplarAttributes, k)
			}
		}
	}

	// Handle Resource attribute
	var resourceAttribute []GaugeResourceAttribute
	resourceAttributes := make(map[string]ValueTypePair)

	resAttr.Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Resource attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			resourceAttributes[k] = v
		} else {
			e.logger.Debug("invalid resource attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range resourceAttributes {
		vtPair := resourceAttributes[key]
		ga := e.newGaugeResourceAttributeValue(gauge.ResourceID, key, vtPair)
		resourceAttribute = append(resourceAttribute, *ga)
	}

	copy(kiGaugeRecord.resourceAttribute, resourceAttribute)

	// Handle Scope attribute
	var scopeAttribute []GaugeScopeAttribute
	scopeAttributes := make(map[string]ValueTypePair)
	scopeName := scopeInstr.Name()
	scopeVersion := scopeInstr.Version()

	scopeInstr.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			e.logger.Debug("Scope attribute key is empty")
		} else if v, err := AttributeValueToKineticaFieldValue(v); err == nil {
			scopeAttributes[k] = v
		} else {
			e.logger.Debug("invalid scope attribute value", zap.String("Error", err.Error()))
			errs = append(errs, err)
		}
		return true
	})

	for key := range scopeAttributes {
		vtPair := scopeAttributes[key]
		ga := e.newGaugeScopeAttributeValue(gauge.ScopeID, key, scopeName, scopeVersion, vtPair)
		scopeAttribute = append(scopeAttribute, *ga)
	}

	copy(kiGaugeRecord.scopeAttribute, scopeAttribute)

	return kiGaugeRecord, multierr.Combine(errs...)
}

// Utility functions
func (e *kineticaMetricsExporter) newGaugeResourceAttributeValue(ResourceID, key string, vtPair ValueTypePair) *GaugeResourceAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ra := &GaugeResourceAttribute{ResourceID, key, *av}
		return ra
	}

	return nil
}

func (e *kineticaMetricsExporter) newGaugeDatapointAttributeValue(GaugeID string, DatapointID string, key string, vtPair ValueTypePair) *GaugeDatapointAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ga := &GaugeDatapointAttribute{GaugeID, DatapointID, key, *av}
		return ga
	}

	return nil
}

func (e *kineticaMetricsExporter) newGaugeScopeAttributeValue(scopeID string, key string, scopeName string, scopeVersion string, vtPair ValueTypePair) *GaugeScopeAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &GaugeScopeAttribute{scopeID, key, scopeName, scopeVersion, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newSumDatapointAttributeValue(SumID string, DatapointID string, key string, vtPair ValueTypePair) *SumDataPointAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ga := &SumDataPointAttribute{SumID, DatapointID, key, *av}
		return ga
	}

	return nil
}

func (e *kineticaMetricsExporter) newSumResourceAttributeValue(ResourceID, key string, vtPair ValueTypePair) *SumResourceAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ra := &SumResourceAttribute{ResourceID, key, *av}
		return ra
	}

	return nil
}

func (e *kineticaMetricsExporter) newSumScopeAttributeValue(scopeID string, key string, scopeName string, scopeVersion string, vtPair ValueTypePair) *SumScopeAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &SumScopeAttribute{scopeID, key, scopeName, scopeVersion, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newSumDatapointExemplarAttributeValue(sumID string, sumDatapointID string, sumDatapointExemplarID string, key string, vtPair ValueTypePair) *SumDataPointExemplarAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &SumDataPointExemplarAttribute{sumID, sumDatapointID, sumDatapointExemplarID, key, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newExponentialHistogramDatapointExemplarAttributeValue(histogramID string, histogramDatapointID string, histogramDatapointExemplarID string, key string, vtPair ValueTypePair) *ExponentialHistogramDataPointExemplarAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &ExponentialHistogramDataPointExemplarAttribute{histogramID, histogramDatapointID, histogramDatapointExemplarID, key, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newExponentialHistogramDatapointAttributeValue(HistogramID string, DatapointID string, key string, vtPair ValueTypePair) *ExponentialHistogramDataPointAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ga := &ExponentialHistogramDataPointAttribute{HistogramID, DatapointID, key, *av}
		return ga
	}

	return nil
}

func (e *kineticaMetricsExporter) newHistogramDatapointExemplarAttributeValue(HistogramID string, DatapointID string, ExemplarID string, key string, vtPair ValueTypePair) *HistogramDataPointExemplarAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ga := &HistogramDataPointExemplarAttribute{HistogramID, DatapointID, ExemplarID, key, *av}
		return ga
	}

	return nil
}

func (e *kineticaMetricsExporter) newGaugeDatapointExemplarAttributeValue(gaugeID string, gaugeDatapointID string, gaugeDatapointExemplarID string, key string, vtPair ValueTypePair) *GaugeDataPointExemplarAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &GaugeDataPointExemplarAttribute{gaugeID, gaugeDatapointID, gaugeDatapointExemplarID, key, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newHistogramResourceAttributeValue(ResourceID, key string, vtPair ValueTypePair) *HistogramResourceAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ra := &HistogramResourceAttribute{ResourceID, key, *av}
		return ra
	}

	return nil
}

func (e *kineticaMetricsExporter) newHistogramScopeAttributeValue(scopeID string, key string, scopeName string, scopeVersion string, vtPair ValueTypePair) *HistogramScopeAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &HistogramScopeAttribute{scopeID, key, scopeName, scopeVersion, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newExponentialHistogramResourceAttributeValue(ResourceID, key string, vtPair ValueTypePair) *ExponentialHistogramResourceAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ra := &ExponentialHistogramResourceAttribute{ResourceID, key, *av}
		return ra
	}

	return nil
}

func (e *kineticaMetricsExporter) newExponentialHistogramScopeAttributeValue(scopeID string, key string, scopeName string, scopeVersion string, vtPair ValueTypePair) *ExponentialHistogramScopeAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &ExponentialHistogramScopeAttribute{scopeID, key, scopeName, scopeVersion, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newSummaryResourceAttributeValue(ResourceID, key string, vtPair ValueTypePair) *SummaryResourceAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ra := &SummaryResourceAttribute{ResourceID, key, *av}
		return ra
	}

	return nil
}

func (e *kineticaMetricsExporter) newSummaryScopeAttributeValue(scopeID string, key string, scopeName string, scopeVersion string, vtPair ValueTypePair) *SummaryScopeAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &SummaryScopeAttribute{scopeID, key, scopeName, scopeVersion, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newSummaryDatapointAttributeValue(summaryID string, summaryDatapointID string, key string, vtPair ValueTypePair) *SummaryDataPointAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		sa := &SummaryDataPointAttribute{summaryID, summaryDatapointID, key, *av}
		return sa
	}

	return nil
}

func (e *kineticaMetricsExporter) newHistogramDatapointAttributeValue(HistogramID string, DatapointID string, key string, vtPair ValueTypePair) *HistogramDataPointAttribute {
	var av *AttributeValue
	var err error

	av, err = getAttributeValue(vtPair)

	if err != nil {
		ga := &HistogramDataPointAttribute{HistogramID, DatapointID, key, *av}
		return ga
	}

	return nil
}
