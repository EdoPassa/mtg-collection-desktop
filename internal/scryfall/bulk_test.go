package scryfall

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestIterBulkCardsIdentityReadsOracleFixture(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "scryfall", "oracle_cards.json")
	rows, err := ReadBulkIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Card.OracleID != "oracle-lightning-bolt" || rows[0].ScryfallID == "" {
		t.Fatalf("bulk rows = %#v, want two fixture rows with IDs", rows)
	}
}

func TestEnsureOracleBulkDownloadedWritesDataAndMetadata(t *testing.T) {
	bulkPayload := `[{"id":"card-id","oracle_id":"oracle-id","name":"Test Card","scryfall_uri":"https://example.test/card"}]`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bulk-data":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"data":[{"type":"oracle_cards","download_uri":"` + "http://" + r.Host + `/oracle.json","updated_at":"2026-05-20T00:00:00.000+00:00","content_type":"application/json"}]}`))
		case "/oracle.json":
			_, _ = w.Write([]byte(bulkPayload))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	paths := BulkCachePaths{RootDir: t.TempDir()}
	got, err := EnsureOracleBulkDownloaded(t.Context(), BulkOptions{Paths: paths, MetadataURL: server.URL + "/bulk-data"})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ReadBulkIdentity(got.DataPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Card.Name != "Test Card" {
		t.Fatalf("downloaded rows = %#v, want Test Card", rows)
	}
}
