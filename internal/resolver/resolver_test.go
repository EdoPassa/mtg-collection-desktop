package resolver

import (
	"context"
	"path/filepath"
	"testing"

	"mtgcollection/internal/importer"
	"mtgcollection/internal/scryfall"
)

type fakeAPI struct {
	byName map[string]scryfall.Card
	byID   map[string]scryfall.Card
}

func (f fakeAPI) LookupNamed(_ context.Context, name string) (scryfall.Card, error) {
	return f.byName[name], nil
}

func (f fakeAPI) LookupScryfallID(_ context.Context, id string) (scryfall.Card, error) {
	return f.byID[id], nil
}

func TestBulkFirstResolverUsesBulkBeforeAPI(t *testing.T) {
	index, err := BuildBulkOracleIndex(filepath.Join("..", "..", "testdata", "scryfall", "oracle_cards.json"))
	if err != nil {
		t.Fatal(err)
	}
	apiCard := scryfall.Card{OracleID: "api-oracle", Name: "Lightning Bolt", ScryfallURI: "https://api.test/card"}
	resolver := NewBulkFirst(index, fakeAPI{byName: map[string]scryfall.Card{"Lightning Bolt": apiCard}})

	result, err := resolver.ResolveName(t.Context(), " lightning   bolt ")
	if err != nil {
		t.Fatal(err)
	}
	if result.Source != "bulk" || result.Card.OracleID != "oracle-lightning-bolt" {
		t.Fatalf("result = %#v, want bulk fixture card", result)
	}
}

func TestBulkFirstResolverFallsBackToAPI(t *testing.T) {
	index, err := BuildBulkOracleIndex(filepath.Join("..", "..", "testdata", "scryfall", "oracle_cards.json"))
	if err != nil {
		t.Fatal(err)
	}
	apiCard := scryfall.Card{OracleID: "oracle-api", Name: "Sol Ring", ScryfallURI: "https://api.test/sol-ring"}
	resolver := NewBulkFirst(index, fakeAPI{byName: map[string]scryfall.Card{"Sol Ring": apiCard}})

	result, err := resolver.ResolveLine(t.Context(), importer.ImportLine{Name: "Sol Ring"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Source != "api" || result.Card.OracleID != "oracle-api" {
		t.Fatalf("result = %#v, want API Sol Ring", result)
	}
}

func TestBulkOracleIndexLookupOracleID(t *testing.T) {
	index, err := BuildBulkOracleIndex(filepath.Join("..", "..", "testdata", "scryfall", "oracle_cards.json"))
	if err != nil {
		t.Fatal(err)
	}
	card, ok := index.LookupOracleID("oracle-mountain")
	if !ok || card.TypeLine != "Basic Land — Mountain" {
		t.Fatalf("LookupOracleID = %#v, ok=%v, want mountain land type", card, ok)
	}
}
