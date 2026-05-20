package main

import (
	"testing"

	"mtgcollection/internal/appdata"
)

func TestCommandBootstrapKeepsDevelopmentDataPaths(t *testing.T) {
	config := bootstrapConfig()
	if config.Mode != appdata.Development {
		t.Fatalf("bootstrap mode = %q, want development for the command smoke entrypoint", config.Mode)
	}
}
