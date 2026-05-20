package main

import (
	"testing"

	"mtgcollection/internal/appdata"
)

func TestRootBootstrapUsesPackagedAppDataPaths(t *testing.T) {
	config := bootstrapConfig()
	if config.Mode != appdata.Packaged {
		t.Fatalf("bootstrap mode = %q, want packaged for the Wails app", config.Mode)
	}
	paths := appdata.ResolvePaths(appdata.Config{Mode: config.Mode, WorkingDir: t.TempDir(), AppDataRoot: t.TempDir()})
	if got, want := paths.DatabasePath, "collection.sqlite3"; got == want {
		t.Fatalf("database path = %q, want full app-data path", got)
	}
}
