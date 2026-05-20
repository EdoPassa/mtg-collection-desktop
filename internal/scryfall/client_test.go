package scryfall

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLookupNamedTriesExactBeforeFuzzy(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.RawQuery)
		if r.URL.Query().Get("exact") == "Lightning Bolt" {
			http.NotFound(w, r)
			return
		}
		writeCard(t, w, "oracle-bolt", "Lightning Bolt")
	}))
	defer server.Close()

	client := NewClient(Options{BaseURL: server.URL, MinInterval: 0, MaxAttempts: 2})
	card, err := client.LookupNamed(t.Context(), "Lightning Bolt")
	if err != nil {
		t.Fatal(err)
	}
	if card.OracleID != "oracle-bolt" {
		t.Fatalf("card = %#v, want oracle-bolt", card)
	}
	if len(seen) != 2 || seen[0] != "exact=Lightning+Bolt" || seen[1] != "fuzzy=Lightning+Bolt" {
		t.Fatalf("queries = %#v, want exact then fuzzy", seen)
	}
}

func TestClientRetries429WithRetryAfter(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "0.001")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		writeCard(t, w, "oracle-counterspell", "Counterspell")
	}))
	defer server.Close()

	client := NewClient(Options{BaseURL: server.URL, MinInterval: 0, MaxAttempts: 2})
	card, err := client.LookupScryfallID(t.Context(), "card-id")
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || card.Name != "Counterspell" {
		t.Fatalf("attempts=%d card=%#v, want retry and Counterspell", attempts, card)
	}
}

func writeCard(t *testing.T, w http.ResponseWriter, oracleID string, name string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(map[string]any{
		"object":       "card",
		"oracle_id":    oracleID,
		"name":         name,
		"scryfall_uri": "https://scryfall.test/card",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(0)
}
