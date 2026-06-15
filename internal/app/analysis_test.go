package app

import (
	"path/filepath"
	"testing"

	"mtgcollection/internal/analysis"
	"mtgcollection/internal/cards"
	"mtgcollection/internal/resolver"
	"mtgcollection/internal/storage"
)

func TestAppListFormatTargets(t *testing.T) {
	app := New(nil, fakeResolver{}, "api-only", resolver.BulkOracleIndex{})
	targets := app.ListFormatTargets()
	if len(targets) != 2 {
		t.Fatalf("len = %d, want 2", len(targets))
	}
	if targets[0].Size != 60 || targets[1].Size != 100 {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestAppHypergeometric(t *testing.T) {
	app := New(nil, fakeResolver{}, "api-only", resolver.BulkOracleIndex{})
	result, err := app.Hypergeometric(analysis.HypergeometricRequest{
		Population:            60,
		SuccessesInPopulation: 4,
		SampleSize:            7,
		MinSuccessesInSample:  1,
		Mode:                  "at-least",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Probability <= 0 || result.Probability >= 1 {
		t.Fatalf("probability = %v", result.Probability)
	}
	if result.ProbabilityFormatted == "" {
		t.Fatal("expected formatted probability")
	}
}

func TestAppAnalyzeDeckTags(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := t.Context()
	deckID, err := store.CreateDeck(ctx, "Tagged")
	if err != nil {
		t.Fatal(err)
	}
	ramp := cards.CardIdentity{OracleID: "ramp", Name: "Ramp", TypeLine: "Sorcery"}
	filler := cards.CardIdentity{OracleID: "filler", Name: "Filler", TypeLine: "Instant"}
	if err := store.UpsertCards(ctx, []cards.CardIdentity{ramp, filler}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDeckCardQuantity(ctx, deckID, ramp.OracleID, cards.BoardMain, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDeckCardQuantity(ctx, deckID, filler.OracleID, cards.BoardMain, 56); err != nil {
		t.Fatal(err)
	}
	if err := store.IncrementCollection(ctx, ramp.OracleID, 4); err != nil {
		t.Fatal(err)
	}
	tagID, err := store.CreateTag(ctx, "Ramp", "#00aa00")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetCardTags(ctx, ramp.OracleID, []int64{tagID}); err != nil {
		t.Fatal(err)
	}

	app := New(store, fakeResolver{}, "api-only", resolver.BulkOracleIndex{})
	result, err := app.AnalyzeDeckTags(analysis.DeckTagAnalysisRequest{
		DeckID:       deckID,
		FormatTarget: analysis.FormatStandard,
		SampleSize:   7,
		MinTagCards:  1,
		TagFocus:     tagID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Tags) != 1 || result.Tags[0].CopiesInDeck != 4 {
		t.Fatalf("tags = %#v", result.Tags)
	}
	if result.Focus == nil || result.Focus.TagID != tagID {
		t.Fatalf("focus = %#v", result.Focus)
	}
}

func TestAppDeckSimulationLifecycle(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "collection.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := t.Context()
	deckID, err := store.CreateDeck(ctx, "Test")
	if err != nil {
		t.Fatal(err)
	}
	land := cards.CardIdentity{OracleID: "land", Name: "Island", TypeLine: "Basic Land — Island"}
	spell := cards.CardIdentity{OracleID: "spell", Name: "Bolt", TypeLine: "Instant"}
	if err := store.UpsertCards(ctx, []cards.CardIdentity{land, spell}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDeckCardQuantity(ctx, deckID, land.OracleID, cards.BoardMain, 24); err != nil {
		t.Fatal(err)
	}
	if err := store.SetDeckCardQuantity(ctx, deckID, spell.OracleID, cards.BoardMain, 36); err != nil {
		t.Fatal(err)
	}

	app := New(store, fakeResolver{}, "api-only", resolver.BulkOracleIndex{})
	state, err := app.StartDeckSimulation(deckID, analysis.FormatStandard, "spell", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if state.SessionID == "" {
		t.Fatal("expected session id")
	}
	if len(state.Hand) != 7 {
		t.Fatalf("hand = %d, want 7", len(state.Hand))
	}

	state, err = app.SimMulligan(state.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != "awaiting_bottom" {
		t.Fatalf("phase = %q", state.Phase)
	}

	state, err = app.SimPutOnBottom(state.SessionID, state.Hand[0].SlotID)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Hand) != 6 {
		t.Fatalf("hand after bottom = %d", len(state.Hand))
	}

	if err := app.EndDeckSimulation(state.SessionID); err != nil {
		t.Fatal(err)
	}
	_, err = app.SimDrawCard(state.SessionID)
	if err == nil {
		t.Fatal("expected error after session ended")
	}
}
