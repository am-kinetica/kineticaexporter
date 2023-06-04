package kineticaotelexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates a factory for Kinetica exporter.
//
//	@return exporter.Factory
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		CreateDefaultConfig,
		exporter.WithLogs(createLogsExporter, stability),
		exporter.WithTraces(createTracesExporter, stability),
		exporter.WithMetrics(createMetricsExporter, stability),
	)
}

// CreateDefaultConfig - function to create a default configuration
//
//	@return component.Config
func CreateDefaultConfig() component.Config {
	return &Config{
		Host:               "https://172.17.0.2:8082/gpub-0",
		Schema:             "otel",
		Username:           "admin",
		Password:           "Kinetica1.",
		BypassSslCertCheck: true,
	}
}

// createLogsExporter creates a new exporter for logs.
//
//	@param ctx
//	@param set
//	@param cfg
//	@return exporter.Logs
//	@return error
func createLogsExporter(
	ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config,
) (exporter.Logs, error) {
	cf := cfg.(*Config)

	exporter, err := newLogsExporter(set.Logger, cf)
	if err != nil {
		return nil, fmt.Errorf("cannot configure Kinetica logs exporter: %w", err)
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exporter.pushLogsData,
	)
}

// createTracesExporter
//
//	@param ctx
//	@param set
//	@param cfg
//	@return exporter.Traces
//	@return error
func createTracesExporter(ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config) (exporter.Traces, error) {

	cf := cfg.(*Config)
	exporter, err := newTracesExporter(set.Logger, cf)
	if err != nil {
		return nil, fmt.Errorf("cannot configure Kinetica traces exporter: %w", err)
	}
	return exporterhelper.NewTracesExporter(
		ctx,
		set,
		cfg,
		exporter.pushTraceData,
	)
}

func createMetricsExporter(ctx context.Context,
	set exporter.CreateSettings,
	cfg component.Config) (exporter.Metrics, error) {

	cf := cfg.(*Config)
	exporter, err := newMetricsExporter(set.Logger, cf)
	if err != nil {
		return nil, fmt.Errorf("cannot configure Kinetica metrics exporter: %w", err)
	}
	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		exporter.pushMetricsData,
	)
}
