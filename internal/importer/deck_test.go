package importer

import (
	"testing"

	"mtgcollection/internal/cards"
)

func TestParseDeckTextSplitsMainboardAndSideboard(t *testing.T) {
	input := "4 Lightning Bolt\n\nSideboard\n2 Counterspell\n1 Mountain\n"

	lines, unresolved := ParseDeckText(input)
	if len(unresolved) != 0 {
		t.Fatalf("unresolved = %#v, want none", unresolved)
	}
	if len(lines) != 3 {
		t.Fatalf("parsed %d lines, want 3", len(lines))
	}
	if lines[0].Board != cards.BoardMain || lines[0].Name != "Lightning Bolt" || lines[0].Quantity != 4 {
		t.Fatalf("main line = %#v", lines[0])
	}
	if lines[1].Board != cards.BoardSide || lines[1].Name != "Counterspell" {
		t.Fatalf("side line 1 = %#v", lines[1])
	}
	if lines[2].Board != cards.BoardSide || lines[2].Name != "Mountain" {
		t.Fatalf("side line 2 = %#v", lines[2])
	}
}

func TestParseDeckTextIgnoresSectionHeadersInParseText(t *testing.T) {
	input := "Sideboard\n4 Lightning Bolt\n"
	lines, unresolved := ParseText(input)
	if len(lines) != 1 || lines[0].Name != "Lightning Bolt" {
		t.Fatalf("ParseText lines = %#v, want bolt only without board semantics", lines)
	}
	if len(unresolved) != 1 || unresolved[0] != "Sideboard" {
		t.Fatalf("unresolved = %#v, want Sideboard header unresolved", unresolved)
	}
}
