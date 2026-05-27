package analysis

import (
	"testing"

	"mtgcollection/internal/cards"
)

func TestBuildDeckPoolPadsToTarget(t *testing.T) {
	rows := []cards.DeckCard{
		{Card: cards.CardIdentity{OracleID: "a", Name: "Bolt", TypeLine: "Instant"}, Quantity: 4},
		{Card: cards.CardIdentity{OracleID: "b", Name: "Mountain", TypeLine: "Basic Land — Mountain"}, Quantity: 20},
	}
	pool := BuildDeckPool(rows, FormatStandard)
	if pool.DeckTotal != 24 {
		t.Fatalf("deck total: %d", pool.DeckTotal)
	}
	if pool.PopulationN != 60 {
		t.Fatalf("population: %d", pool.PopulationN)
	}
	if pool.DetectedLands != 20 {
		t.Fatalf("lands: %d", pool.DetectedLands)
	}
	if len(pool.Slots) != 60 {
		t.Fatalf("slots: %d", len(pool.Slots))
	}
}
