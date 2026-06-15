package app

import (
	"context"
	"fmt"

	"mtgcollection/internal/analysis"
	"mtgcollection/internal/cards"
)

func (a *App) ensureSessions() *analysis.SessionStore {
	if a.sessions == nil {
		a.sessions = analysis.NewSessionStore()
	}
	return a.sessions
}

func (a *App) loadDeckTagRefs(ctx context.Context, rows []cards.DeckCard) (map[string][]analysis.TagRef, error) {
	all, err := a.service.GetTagsByOracleID(ctx)
	if err != nil {
		return nil, err
	}
	return analysis.TagRefsForDeck(rows, all), nil
}

// ListFormatTargets returns deck size presets for analysis UI.
func (a *App) ListFormatTargets() []analysis.FormatTarget {
	return analysis.ListFormatTargets()
}

// Hypergeometric runs the generic calculator.
func (a *App) Hypergeometric(req analysis.HypergeometricRequest) (analysis.HypergeometricResult, error) {
	return analysis.ComputeHypergeometric(req)
}

// AnalyzeDeckDraw runs deck-aware draw odds.
func (a *App) AnalyzeDeckDraw(req analysis.DeckDrawAnalysisRequest) (analysis.DeckDrawAnalysisResult, error) {
	rows, err := a.service.ListDeckCards(context.Background(), req.DeckID)
	if err != nil {
		return analysis.DeckDrawAnalysisResult{}, err
	}
	return analysis.AnalyzeDeckDraw(rows, req)
}

// AnalyzeDeckTags runs tag breakdown draw odds for a deck.
func (a *App) AnalyzeDeckTags(req analysis.DeckTagAnalysisRequest) (analysis.DeckTagAnalysisResult, error) {
	ctx := context.Background()
	rows, err := a.service.ListDeckCards(ctx, req.DeckID)
	if err != nil {
		return analysis.DeckTagAnalysisResult{}, err
	}
	tagsByOracle, err := a.loadDeckTagRefs(ctx, rows)
	if err != nil {
		return analysis.DeckTagAnalysisResult{}, err
	}
	return analysis.AnalyzeDeckTags(rows, tagsByOracle, req)
}

// StartDeckSimulation creates a session and draws an opening hand.
func (a *App) StartDeckSimulation(deckID int64, formatTarget string, oracleFocus string, tagFocus int64, minLands int) (analysis.SimulationState, error) {
	ctx := context.Background()
	rows, err := a.service.ListDeckCards(ctx, deckID)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	tagsByOracle, err := a.loadDeckTagRefs(ctx, rows)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	sim := analysis.NewSimulation(deckID, formatTarget, rows, tagsByOracle, oracleFocus, tagFocus, minLands)
	if err := sim.NewOpening(); err != nil {
		return analysis.SimulationState{}, err
	}
	id := a.ensureSessions().Create(sim)
	return sim.State(id), nil
}

// SimNewOpening reshuffles and draws a new opening hand for the session.
func (a *App) SimNewOpening(sessionID string) (analysis.SimulationState, error) {
	sim, err := a.getSimulation(sessionID)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	if err := sim.NewOpening(); err != nil {
		return analysis.SimulationState{}, err
	}
	return sim.State(sessionID), nil
}

// SimMulligan performs a mulligan (draw 7, await bottom).
func (a *App) SimMulligan(sessionID string) (analysis.SimulationState, error) {
	sim, err := a.getSimulation(sessionID)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	if err := sim.Mulligan(); err != nil {
		return analysis.SimulationState{}, err
	}
	return sim.State(sessionID), nil
}

// SimPutOnBottom moves a hand card to the bottom after a mulligan.
func (a *App) SimPutOnBottom(sessionID string, slotID string) (analysis.SimulationState, error) {
	sim, err := a.getSimulation(sessionID)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	if err := sim.PutOnBottom(slotID); err != nil {
		return analysis.SimulationState{}, err
	}
	return sim.State(sessionID), nil
}

// SimDrawCard draws one card from the library.
func (a *App) SimDrawCard(sessionID string) (analysis.SimulationState, error) {
	sim, err := a.getSimulation(sessionID)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	if err := sim.DrawOne(); err != nil {
		return analysis.SimulationState{}, err
	}
	return sim.State(sessionID), nil
}

// SimSetOracleFocus updates which card next-draw stats use.
func (a *App) SimSetOracleFocus(sessionID string, oracleID string) (analysis.SimulationState, error) {
	sim, err := a.getSimulation(sessionID)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	sim.SetOracleFocus(oracleID)
	return sim.State(sessionID), nil
}

// SimSetTagFocus updates which tag next-draw stats use.
func (a *App) SimSetTagFocus(sessionID string, tagID int64) (analysis.SimulationState, error) {
	sim, err := a.getSimulation(sessionID)
	if err != nil {
		return analysis.SimulationState{}, err
	}
	sim.SetTagFocus(tagID)
	return sim.State(sessionID), nil
}

// EndDeckSimulation removes a simulation session.
func (a *App) EndDeckSimulation(sessionID string) error {
	a.ensureSessions().Delete(sessionID)
	return nil
}

func (a *App) getSimulation(sessionID string) (*analysis.Simulation, error) {
	sim, ok := a.ensureSessions().Get(sessionID)
	if !ok {
		return nil, fmt.Errorf("simulation session not found")
	}
	return sim, nil
}
