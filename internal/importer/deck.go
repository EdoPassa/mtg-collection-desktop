package importer

import (
	"strings"

	"mtgcollection/internal/cards"
)

// ParseDeckText parses a pasted decklist, splitting mainboard and sideboard at section headers.
// Section headers include "Sideboard", "SB", "Main", "Mainboard", and "// Sideboard" style comments.
func ParseDeckText(text string) ([]ImportLine, []string) {
	var parsed []ImportLine
	var unresolved []string
	board := cards.BoardMain

	for _, raw := range strings.Split(text, "\n") {
		raw = strings.TrimSuffix(raw, "\r")
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if nextBoard, ok := deckSectionHeader(line); ok {
			board = nextBoard
			continue
		}

		qty, name, ok := parseTextLine(line)
		if !ok || qty <= 0 || strings.TrimSpace(name) == "" {
			unresolved = append(unresolved, raw)
			continue
		}
		parsed = append(parsed, ImportLine{
			Raw:      raw,
			Quantity: qty,
			Name:     strings.TrimSpace(name),
			Board:    board,
		})
	}

	return parsed, unresolved
}

func deckSectionHeader(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "//") {
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "//"))
	}
	trimmed = strings.TrimRight(trimmed, ":")
	key := headerCleaner.ReplaceAllString(strings.ToLower(trimmed), "")
	switch key {
	case "sideboard", "sb", "side":
		return cards.BoardSide, true
	case "main", "maindeck", "mainboard", "deck":
		return cards.BoardMain, true
	default:
		return "", false
	}
}
