package analysis

import (
	"testing"

	"mtgcollection/internal/cards"
)

func testDeckRows() []cards.DeckCard {
	rows := make([]cards.DeckCard, 0, 60)
	for i := 0; i < 24; i++ {
		rows = append(rows, cards.DeckCard{
			Card: cards.CardIdentity{
				OracleID: "land",
				Name:     "Island",
				TypeLine: "Basic Land — Island",
			},
			Quantity: 1,
		})
	}
	for i := 0; i < 36; i++ {
		rows = append(rows, cards.DeckCard{
			Card: cards.CardIdentity{
				OracleID: "spell",
				Name:     "Spell",
				TypeLine: "Instant",
			},
			Quantity: 1,
		})
	}
	return rows
}

func TestSimulationNewOpeningDrawsSeven(t *testing.T) {
	sim := NewSimulationWithRNG(1, FormatStandard, testDeckRows(), 42)
	if err := sim.NewOpening(); err != nil {
		t.Fatal(err)
	}
	if len(sim.Hand) != 7 {
		t.Fatalf("hand size: %d", len(sim.Hand))
	}
	if len(sim.Library) != 53 {
		t.Fatalf("library: %d", len(sim.Library))
	}
}

func TestSimulationMulliganAndBottom(t *testing.T) {
	sim := NewSimulationWithRNG(1, FormatStandard, testDeckRows(), 99)
	if err := sim.NewOpening(); err != nil {
		t.Fatal(err)
	}
	if err := sim.Mulligan(); err != nil {
		t.Fatal(err)
	}
	if sim.Phase != PhaseAwaitingBottom {
		t.Fatalf("phase: %s", sim.Phase)
	}
	if len(sim.Hand) != 7 {
		t.Fatalf("hand after mulligan: %d", len(sim.Hand))
	}
	slotID := sim.Hand[0].slotID
	if err := sim.PutOnBottom(slotID); err != nil {
		t.Fatal(err)
	}
	if len(sim.Hand) != 6 {
		t.Fatalf("hand after bottom: %d", len(sim.Hand))
	}
	if len(sim.Bottom) != 1 {
		t.Fatalf("bottom: %d", len(sim.Bottom))
	}
	if sim.Phase != PhasePlaying {
		t.Fatalf("phase: %s", sim.Phase)
	}
}

func TestSimulationDrawOne(t *testing.T) {
	sim := NewSimulationWithRNG(1, FormatStandard, testDeckRows(), 7)
	if err := sim.NewOpening(); err != nil {
		t.Fatal(err)
	}
	libBefore := len(sim.Library)
	if err := sim.DrawOne(); err != nil {
		t.Fatal(err)
	}
	if len(sim.Hand) != 8 {
		t.Fatalf("hand: %d", len(sim.Hand))
	}
	if len(sim.Library) != libBefore-1 {
		t.Fatalf("library: %d", len(sim.Library))
	}
}
