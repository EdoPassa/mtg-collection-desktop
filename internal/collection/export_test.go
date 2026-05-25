package collection

import (
	"strings"
	"testing"

	"mtgcollection/internal/cards"
)

func TestFormatMissingDecklistMainboardOnly(t *testing.T) {
	got := FormatMissingDecklist([]DeckCompareRow{
		{Card: cards.CardIdentity{Name: "Counterspell"}, Needed: 2, Owned: 0, Missing: 2},
		{Card: cards.CardIdentity{Name: "Lightning Bolt"}, Needed: 4, Owned: 2, Missing: 2},
	})
	want := "2 Counterspell\n2 Lightning Bolt"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestFormatMissingDecklistSkipsCompleteRows(t *testing.T) {
	got := FormatMissingDecklist([]DeckCompareRow{
		{Card: cards.CardIdentity{Name: "Lightning Bolt"}, Needed: 4, Owned: 4, Missing: 0},
		{Card: cards.CardIdentity{Name: "Shock"}, Needed: 2, Owned: 0, Missing: 2},
	})
	if got != "2 Shock" {
		t.Fatalf("got %q, want only missing row", got)
	}
}

func TestFormatMissingDecklistIncludesSideboardHeader(t *testing.T) {
	got := FormatMissingDecklist([]DeckCompareRow{
		{Board: cards.BoardMain, Card: cards.CardIdentity{Name: "Lightning Bolt"}, Missing: 1},
		{Board: cards.BoardSide, Card: cards.CardIdentity{Name: "Negate"}, Missing: 2},
	})
	if !strings.Contains(got, "Sideboard\n") {
		t.Fatalf("got %q, want Sideboard section", got)
	}
	if !strings.Contains(got, "1 Lightning Bolt") || !strings.Contains(got, "2 Negate") {
		t.Fatalf("got %q, want main and side quantities", got)
	}
}

func TestFormatMissingDecklistEmptyWhenNothingMissing(t *testing.T) {
	got := FormatMissingDecklist([]DeckCompareRow{
		{Card: cards.CardIdentity{Name: "Lightning Bolt"}, Missing: 0},
	})
	if got != "" {
		t.Fatalf("got %q, want empty string", got)
	}
}
