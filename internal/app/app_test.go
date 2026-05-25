package app

import (
	"context"
	"path/filepath"
	"testing"

	"mtgcollection/internal/importer"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/scryfall"
	"mtgcollection/internal/storage"
)

type fakeResolver struct{}

func (fakeResolver) ResolveLine(ctx context.Context, line importer.ImportLine) (resolver.Result, error) {
	return fakeResolver{}.ResolveName(ctx, line.Name)
}

func (fakeResolver) ResolveName(_ context.Context, name string) (resolver.Result, error) {
	return resolver.Result{Card: scryfall.Card{OracleID: "oracle-" + name, Name: name, ScryfallURI: "https://example.test/" + name}, Source: "bulk"}, nil
}

func (fakeResolver) ResolveScryfallID(_ context.Context, id string) (resolver.Result, error) {
	return resolver.Result{Card: scryfall.Card{OracleID: "oracle-" + id, Name: id, ScryfallURI: "https://example.test/" + id}, Source: "bulk"}, nil
}

func TestAppMethodsExposeCollectionWorkflow(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	app := New(store, fakeResolver{}, "api-only", resolver.BulkOracleIndex{})

	preview, err := app.PreviewTextImport("2 Sol Ring")
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.Validated) != 1 {
		t.Fatalf("preview = %#v, want one validated row", preview)
	}
	if err := app.CommitImport(preview.Validated); err != nil {
		t.Fatal(err)
	}
	rows, err := app.ListCollection()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Quantity != 2 {
		t.Fatalf("collection = %#v, want qty 2", rows)
	}
	if got := app.ResolverStatus(); got != "api-only" {
		t.Fatalf("ResolverStatus() = %q, want api-only", got)
	}
}

func TestAppListCollectionReturnsEmptySliceForEmptyDatabase(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	app := New(store, fakeResolver{}, "api-only", resolver.BulkOracleIndex{})

	rows, err := app.ListCollection()
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil {
		t.Fatal("ListCollection returned nil, want empty slice for Wails JSON arrays")
	}
}

func TestAppListLentCardsReturnsEmptySliceForEmptyDatabase(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	app := New(store, fakeResolver{}, "api-only", resolver.BulkOracleIndex{})

	rows, err := app.ListLentCards(false)
	if err != nil {
		t.Fatal(err)
	}
	if rows == nil {
		t.Fatal("ListLentCards returned nil, want empty slice for Wails JSON arrays")
	}
}
