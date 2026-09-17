package main

import (
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
)

func TestBundledConfigValid(t *testing.T) {
	cfg := config.NewConfig()
	if err := cfg.LoadFromYAML("config.yaml"); err != nil {
		t.Fatalf("LoadFromYAML() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if cfg.DispatchMode != config.DispatchModeSync {
		t.Fatalf("DispatchMode = %q, want %q", cfg.DispatchMode, config.DispatchModeSync)
	}
}
