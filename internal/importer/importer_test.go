package importer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseTextAcceptsPrefixSuffixAndXQuantities(t *testing.T) {
	input := "# ignored\n4 Lightning Bolt\nLightning Bolt 1\n2x Counterspell\nBad line\n"

	lines, unresolved := ParseText(input)

	if len(lines) != 3 {
		t.Fatalf("parsed %d lines, want 3", len(lines))
	}
	assertLine(t, lines[0], "4 Lightning Bolt", 4, "Lightning Bolt", "")
	assertLine(t, lines[1], "Lightning Bolt 1", 1, "Lightning Bolt", "")
	assertLine(t, lines[2], "2x Counterspell", 2, "Counterspell", "")
	if len(unresolved) != 1 || unresolved[0] != "Bad line" {
		t.Fatalf("unresolved = %#v, want Bad line", unresolved)
	}
}

func TestParseCSVAcceptsHeaderAliasesAndOptionalScryfallID(t *testing.T) {
	data := []byte("Card Name,Quantity,Scryfall ID\nLightning Bolt,4,abc-123\nCounterspell,2,\nIsland,0,\n")

	lines, unresolved := ParseCSVBytes(data)

	if len(lines) != 2 {
		t.Fatalf("parsed %d lines, want 2", len(lines))
	}
	assertLine(t, lines[0], "{map[Card Name:Lightning Bolt Quantity:4 Scryfall ID:abc-123]}", 4, "Lightning Bolt", "abc-123")
	assertLine(t, lines[1], "{map[Card Name:Counterspell Quantity:2 Scryfall ID:]}", 2, "Counterspell", "")
	if len(unresolved) != 1 {
		t.Fatalf("unresolved = %#v, want one invalid quantity row", unresolved)
	}
}

func TestFixturesMatchContract(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "imports", "sample.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines, unresolved := ParseText(string(data))
	if len(lines) != 3 || len(unresolved) != 1 {
		t.Fatalf("fixture parsed lines=%d unresolved=%d, want 3 and 1", len(lines), len(unresolved))
	}
}

func assertLine(t *testing.T, got ImportLine, raw string, qty int, name string, scryfallID string) {
	t.Helper()
	if got.Raw != raw || got.Quantity != qty || got.Name != name || got.ScryfallID != scryfallID {
		t.Fatalf("line = %#v, want raw=%q qty=%d name=%q id=%q", got, raw, qty, name, scryfallID)
	}
}
