package analysis

import (
	"testing"

	"mtgcollection/internal/cards"
)

func TestBuildDeckPoolTagCounts(t *testing.T) {
	rows := []cards.DeckCard{
		{Card: cards.CardIdentity{OracleID: "ramp", Name: "Ramp", TypeLine: "Sorcery"}, Quantity: 4},
		{Card: cards.CardIdentity{OracleID: "bolt", Name: "Bolt", TypeLine: "Instant"}, Quantity: 4},
	}
	tags := map[string][]TagRef{
		"ramp": {{ID: 1, Name: "Ramp", Color: "#0f0"}},
		"bolt": {{ID: 2, Name: "Removal", Color: "#f00"}},
	}
	pool := BuildDeckPool(rows, FormatStandard, tags)
	if pool.TagCounts[1] != 4 {
		t.Fatalf("ramp copies: %d", pool.TagCounts[1])
	}
	if pool.TagCounts[2] != 4 {
		t.Fatalf("removal copies: %d", pool.TagCounts[2])
	}
	if len(pool.Slots[0].tagIDs) != 1 || pool.Slots[0].tagIDs[0] != 1 {
		t.Fatalf("first slot tags: %v", pool.Slots[0].tagIDs)
	}
}

func TestNextDrawTagProb(t *testing.T) {
	rows := []cards.DeckCard{
		{Card: cards.CardIdentity{OracleID: "ramp", Name: "Ramp", TypeLine: "Sorcery"}, Quantity: 4},
		{Card: cards.CardIdentity{OracleID: "filler", Name: "Filler", TypeLine: "Instant"}, Quantity: 56},
	}
	tags := map[string][]TagRef{
		"ramp": {{ID: 1, Name: "Ramp"}},
	}
	pool := BuildDeckPool(rows, FormatStandard, tags)
	hand := make([]SimulationCard, 7)
	for i := range hand {
		hand[i] = slotToSimulationCard(pool.Slots[i])
	}
	rem := countRemaining(pool, hand, nil)
	got := NextDrawTagProb(rem, 1)
	want := float64(rem.ByTag[1]) / float64(rem.Total)
	if got != want {
		t.Fatalf("got %v want %v (tag=%d total=%d)", got, want, rem.ByTag[1], rem.Total)
	}
}

func TestAnalyzeDeckTags(t *testing.T) {
	rows := []cards.DeckCard{
		{Card: cards.CardIdentity{OracleID: "ramp", Name: "Ramp", TypeLine: "Sorcery"}, Quantity: 4},
		{Card: cards.CardIdentity{OracleID: "filler", Name: "Filler", TypeLine: "Instant"}, Quantity: 56},
	}
	tags := map[string][]TagRef{
		"ramp": {{ID: 1, Name: "Ramp"}},
	}
	result, err := AnalyzeDeckTags(rows, tags, DeckTagAnalysisRequest{
		FormatTarget: FormatStandard,
		SampleSize:   7,
		MinTagCards:  1,
		TagFocus:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Tags) != 1 {
		t.Fatalf("tags: %d", len(result.Tags))
	}
	if result.Tags[0].CopiesInDeck != 4 {
		t.Fatalf("copies: %d", result.Tags[0].CopiesInDeck)
	}
	if result.Focus == nil || result.Focus.TagID != 1 {
		t.Fatal("expected focus on ramp tag")
	}
	if result.Tags[0].SampleProb <= 0 || result.Tags[0].SampleProb >= 1 {
		t.Fatalf("sample prob: %v", result.Tags[0].SampleProb)
	}
}

func TestAnalyzeDeckTagsEmptyWhenNoTaggedCards(t *testing.T) {
	rows := []cards.DeckCard{
		{Card: cards.CardIdentity{OracleID: "filler", Name: "Filler", TypeLine: "Instant"}, Quantity: 60},
	}
	result, err := AnalyzeDeckTags(rows, nil, DeckTagAnalysisRequest{
		FormatTarget: FormatStandard,
		SampleSize:   7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Tags == nil {
		t.Fatal("Tags should be non-nil empty slice")
	}
	if len(result.Tags) != 0 {
		t.Fatalf("tags: %d", len(result.Tags))
	}
}

func TestTagRefsForDeckFiltersOracles(t *testing.T) {
	rows := []cards.DeckCard{
		{Card: cards.CardIdentity{OracleID: "in-deck", Name: "A"}, Quantity: 1},
	}
	all := map[string][]cards.CollectionTag{
		"in-deck":  {{ID: 1, Name: "Ramp"}},
		"not-deck": {{ID: 2, Name: "Other"}},
	}
	got := TagRefsForDeck(rows, all)
	if len(got) != 1 || len(got["in-deck"]) != 1 {
		t.Fatalf("got %#v", got)
	}
	if _, ok := got["not-deck"]; ok {
		t.Fatal("unexpected not-deck entry")
	}
}
