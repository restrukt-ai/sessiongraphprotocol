package main

import (
	"errors"
	"os"
)

type config struct {
	DatabaseURL      string
	HarnessAddr      string
	HarnessToken     string
	ManagementAddr   string
	ManagementToken  string
	TLSCert          string
	TLSKey           string
}

func loadConfig() (config, error) {
	cfg := config{
		DatabaseURL:     os.Getenv("SGPD_DATABASE_URL"),
		HarnessAddr:     envOr("SGPD_HARNESS_ADDR", ":9090"),
		HarnessToken:    os.Getenv("SGPD_HARNESS_TOKEN"),
		ManagementAddr:  envOr("SGPD_MANAGEMENT_ADDR", ":9091"),
		ManagementToken: os.Getenv("SGPD_MANAGEMENT_TOKEN"),
		TLSCert:         os.Getenv("SGPD_TLS_CERT"),
		TLSKey:          os.Getenv("SGPD_TLS_KEY"),
	}

	var errs []error
	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("SGPD_DATABASE_URL is required"))
	}
	if cfg.HarnessToken == "" {
		errs = append(errs, errors.New("SGPD_HARNESS_TOKEN is required"))
	}
	if cfg.ManagementToken == "" {
		errs = append(errs, errors.New("SGPD_MANAGEMENT_TOKEN is required"))
	}
	// TLS is optional: omit cert/key for plain HTTP (dev only).
	return cfg, errors.Join(errs...)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
