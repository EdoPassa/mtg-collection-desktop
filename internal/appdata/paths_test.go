package appdata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDevelopmentPathsUseRepoLocalDataWhenPresent(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	paths := ResolvePaths(Config{Mode: Development, WorkingDir: root})
	if paths.DatabasePath != filepath.Join(root, "data", "collection.sqlite3") {
		t.Fatalf("DatabasePath = %q, want repo-local data path", paths.DatabasePath)
	}
}

func TestPackagedPathsUseConfiguredAppDataRoot(t *testing.T) {
	root := t.TempDir()
	paths := ResolvePaths(Config{Mode: Packaged, AppDataRoot: root})
	want := filepath.Join(root, "MTG Collection", "collection.sqlite3")
	if paths.DatabasePath != want {
		t.Fatalf("DatabasePath = %q, want %q", paths.DatabasePath, want)
	}
}

func TestMigrateRepoDataCopiesDatabaseAndScryfallCache(t *testing.T) {
	source := t.TempDir()
	destination := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "data", "scryfall"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "data", "collection.sqlite3"), []byte("db"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "data", "scryfall", "oracle_cards.meta.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := MigrateRepoData(source, destination)
	if err != nil {
		t.Fatal(err)
	}
	if !result.DatabaseCopied || !result.ScryfallCopied {
		t.Fatalf("result = %#v, want database and cache copied", result)
	}
	if _, err := os.Stat(filepath.Join(destination, "collection.sqlite3")); err != nil {
		t.Fatalf("copied database missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(destination, "scryfall", "oracle_cards.meta.json")); err != nil {
		t.Fatalf("copied cache missing: %v", err)
	}
}
