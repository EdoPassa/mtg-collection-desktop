package scryfall

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientSendsRequiredScryfallHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("User-Agent") == "" || r.Header.Get("Accept") == "" {
			http.Error(w, `{"object":"error","details":"missing headers"}`, http.StatusBadRequest)
			return
		}
		writeCard(t, w, "oracle-bolt", "Lightning Bolt")
	}))
	defer server.Close()

	client := NewClient(Options{BaseURL: server.URL, UserAgent: "MTGCollectionDesktop/1.0", MinInterval: 0, MaxAttempts: 1})
	if _, err := client.LookupNamed(t.Context(), "Lightning Bolt"); err != nil {
		t.Fatal(err)
	}
}

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

func TestToCardExtractsCostColorAndImageURIs(t *testing.T) {
	payload := map[string]any{
		"oracle_id":      "oracle-bolt",
		"name":           "Lightning Bolt",
		"scryfall_uri":   "https://scryfall.test/card",
		"type_line":      "Instant",
		"mana_cost":      "{R}",
		"color_identity": []any{"R"},
		"image_uris": map[string]any{
			"small":  "https://cards.scryfall.test/small/bolt.jpg",
			"normal": "https://cards.scryfall.test/normal/bolt.jpg",
		},
	}
	card, ok, err := toCard(payload)
	if err != nil || !ok {
		t.Fatalf("toCard ok=%v err=%v", ok, err)
	}
	if card.ManaCost != "{R}" {
		t.Fatalf("mana_cost = %q, want {R}", card.ManaCost)
	}
	if len(card.ColorIdentity) != 1 || card.ColorIdentity[0] != "R" {
		t.Fatalf("color_identity = %#v, want [R]", card.ColorIdentity)
	}
	if card.ImageSmall == "" || card.ImageNormal == "" {
		t.Fatalf("image URIs missing: %#v", card)
	}
}

// Double-faced cards (DFCs) store images on each card_face instead of the top level.
// We fall back to the first face so the UI can still render a thumbnail.
func TestToCardFallsBackToCardFaceImagesForDFCs(t *testing.T) {
	payload := map[string]any{
		"oracle_id":    "oracle-delver",
		"name":         "Delver of Secrets // Insectile Aberration",
		"scryfall_uri": "https://scryfall.test/delver",
		"card_faces": []any{
			map[string]any{
				"name":      "Delver of Secrets",
				"mana_cost": "{U}",
				"image_uris": map[string]any{
					"small":  "https://cards.scryfall.test/small/delver.jpg",
					"normal": "https://cards.scryfall.test/normal/delver.jpg",
				},
			},
			map[string]any{
				"name":      "Insectile Aberration",
				"mana_cost": "",
				"image_uris": map[string]any{
					"small":  "https://cards.scryfall.test/small/aberration.jpg",
					"normal": "https://cards.scryfall.test/normal/aberration.jpg",
				},
			},
		},
	}
	card, ok, err := toCard(payload)
	if err != nil || !ok {
		t.Fatalf("toCard ok=%v err=%v", ok, err)
	}
	if card.ImageSmall == "" || card.ImageNormal == "" {
		t.Fatalf("DFC images not pulled from card_faces: %#v", card)
	}
	if card.ManaCost != "{U}" {
		t.Fatalf("DFC mana_cost = %q, want {U} from first face", card.ManaCost)
	}
}
