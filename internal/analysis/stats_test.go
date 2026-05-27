package analysis

import (
	"testing"

	"mtgcollection/internal/cards"
)

func TestNextDrawLandProb(t *testing.T) {
	rows := []cards.DeckCard{
		{Card: cards.CardIdentity{OracleID: "b", Name: "Mountain", TypeLine: "Basic Land — Mountain"}, Quantity: 20},
		{Card: cards.CardIdentity{OracleID: "a", Name: "Bolt", TypeLine: "Instant"}, Quantity: 40},
	}
	pool := BuildDeckPool(rows, FormatStandard)
	// 7 lands in hand of 20-land deck: 13 lands / 53 library
	hand := make([]SimulationCard, 7)
	for i := range hand {
		hand[i] = SimulationCard{SlotID: "x", IsLand: true}
	}
	// use real slot IDs from pool for accurate count - draw 7 lands from first 7 land slots
	hand = nil
	landSlots := 0
	for _, s := range pool.Slots {
		if s.isLand && landSlots < 7 {
			hand = append(hand, slotToSimulationCard(s))
			landSlots++
		}
	}
	rem := countRemaining(pool, hand, nil)
	got := NextDrawLandProb(rem)
	want := float64(rem.Lands) / float64(rem.Total)
	if got != want {
		t.Fatalf("got %v want %v (lands=%d total=%d)", got, want, rem.Lands, rem.Total)
	}
}
