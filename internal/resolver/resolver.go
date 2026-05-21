// Package resolver maps import lines and card names to Scryfall card identities.
// Lookups prefer the local oracle_cards bulk cache when available, falling back to the live API.
package resolver

import (
	"context"
	"strings"

	"mtgcollection/internal/cards"
	"mtgcollection/internal/importer"
	"mtgcollection/internal/scryfall"
)

type Result struct {
	Card   scryfall.Card `json:"card"`
	Source string        `json:"source"`
}

type Resolver interface {
	ResolveLine(ctx context.Context, line importer.ImportLine) (Result, error)
	ResolveName(ctx context.Context, name string) (Result, error)
	ResolveScryfallID(ctx context.Context, scryfallID string) (Result, error)
}

type API interface {
	LookupNamed(ctx context.Context, name string) (scryfall.Card, error)
	LookupScryfallID(ctx context.Context, scryfallID string) (scryfall.Card, error)
}

// BulkOracleIndex is an in-memory index built from Scryfall's oracle_cards bulk file.
// Duplicate names keep the first occurrence only (oracle_cards has one row per oracle card).
type BulkOracleIndex struct {
	byName       map[string]scryfall.Card
	byScryfallID map[string]scryfall.Card
}

// BulkFirst resolves cards locally first, then calls the Scryfall API on cache miss.
type BulkFirst struct {
	index BulkOracleIndex
	api   API
}

// APIOnly always uses the live Scryfall API (fallback when bulk data is unavailable).
type APIOnly struct {
	api API
}

func BuildBulkOracleIndex(path string) (BulkOracleIndex, error) {
	rows, err := scryfall.ReadBulkIdentity(path)
	if err != nil {
		return BulkOracleIndex{}, err
	}
	index := BulkOracleIndex{byName: map[string]scryfall.Card{}, byScryfallID: map[string]scryfall.Card{}}
	for _, row := range rows {
		nameKey := cards.NormalizeName(row.Card.Name)
		if nameKey != "" {
			if _, exists := index.byName[nameKey]; !exists {
				index.byName[nameKey] = row.Card
			}
		}
		if idKey := strings.TrimSpace(strings.ToLower(row.ScryfallID)); idKey != "" {
			if _, exists := index.byScryfallID[idKey]; !exists {
				index.byScryfallID[idKey] = row.Card
			}
		}
	}
	return index, nil
}

func NewBulkFirst(index BulkOracleIndex, api API) *BulkFirst {
	return &BulkFirst{index: index, api: api}
}

func NewAPIOnly(api API) *APIOnly {
	return &APIOnly{api: api}
}

func (r *BulkFirst) ResolveLine(ctx context.Context, line importer.ImportLine) (Result, error) {
	if strings.TrimSpace(line.ScryfallID) != "" {
		return r.ResolveScryfallID(ctx, line.ScryfallID)
	}
	return r.ResolveName(ctx, line.Name)
}

func (r *BulkFirst) ResolveName(ctx context.Context, name string) (Result, error) {
	if card, ok := r.index.LookupName(name); ok {
		return Result{Card: card, Source: "bulk"}, nil
	}
	card, err := r.api.LookupNamed(ctx, name)
	if err != nil {
		return Result{}, err
	}
	return Result{Card: card, Source: "api"}, nil
}

func (r *BulkFirst) ResolveScryfallID(ctx context.Context, scryfallID string) (Result, error) {
	if card, ok := r.index.LookupScryfallID(scryfallID); ok {
		return Result{Card: card, Source: "bulk"}, nil
	}
	card, err := r.api.LookupScryfallID(ctx, scryfallID)
	if err != nil {
		return Result{}, err
	}
	return Result{Card: card, Source: "api"}, nil
}

func (r *APIOnly) ResolveLine(ctx context.Context, line importer.ImportLine) (Result, error) {
	if strings.TrimSpace(line.ScryfallID) != "" {
		return r.ResolveScryfallID(ctx, line.ScryfallID)
	}
	return r.ResolveName(ctx, line.Name)
}

func (r *APIOnly) ResolveName(ctx context.Context, name string) (Result, error) {
	card, err := r.api.LookupNamed(ctx, name)
	if err != nil {
		return Result{}, err
	}
	return Result{Card: card, Source: "api"}, nil
}

func (r *APIOnly) ResolveScryfallID(ctx context.Context, scryfallID string) (Result, error) {
	card, err := r.api.LookupScryfallID(ctx, scryfallID)
	if err != nil {
		return Result{}, err
	}
	return Result{Card: card, Source: "api"}, nil
}

func (i BulkOracleIndex) LookupName(name string) (scryfall.Card, bool) {
	card, ok := i.byName[cards.NormalizeName(name)]
	return card, ok
}

func (i BulkOracleIndex) LookupScryfallID(scryfallID string) (scryfall.Card, bool) {
	card, ok := i.byScryfallID[strings.TrimSpace(strings.ToLower(scryfallID))]
	return card, ok
}
