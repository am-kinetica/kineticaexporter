package kineticaotelexporter // import

import (
	"go.opentelemetry.io/collector/component"
)

const (
	// The value of "type" key in configuration.
	typeStr = "kinetica"
	// The stability level of the exporter.
	stability = component.StabilityLevelAlpha
)

// Config defines configuration for the Kinetica exporter.
type Config struct {
	Host               string `mapstructure:"host"`
	Schema             string `mapstructure:"schema"`
	Username           string `mapstructure:"username"`
	Password           string `mapstructure:"password"`
	BypassSslCertCheck bool   `mapstructure:"bypasssslcertcheck"`
}

// Validate the config
//
//	@receiver cfg
//	@return error
func (cfg *Config) Validate() error {
	return nil
}

var _ component.Config = (*Config)(nil)
