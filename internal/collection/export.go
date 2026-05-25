package collection

import (
	"fmt"
	"sort"
	"strings"

	"mtgcollection/internal/cards"
)

// FormatMissingDecklist returns a plain-text decklist of cards still needed to complete
// the comparison. Lines use the same "4 Card Name" format accepted by text import.
// Sideboard cards are grouped under a "Sideboard" header when present.
func FormatMissingDecklist(rows []DeckCompareRow) string {
	var mainboard, sideboard []DeckCompareRow
	for _, row := range rows {
		if row.Missing <= 0 {
			continue
		}
		if cards.NormalizeBoard(row.Board) == cards.BoardSide {
			sideboard = append(sideboard, row)
		} else {
			mainboard = append(mainboard, row)
		}
	}
	sortMissingRows(mainboard)
	sortMissingRows(sideboard)

	var b strings.Builder
	writeMissingLines(&b, mainboard)
	if len(sideboard) > 0 {
		if b.Len() > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("Sideboard\n")
		writeMissingLines(&b, sideboard)
	}
	return strings.TrimRight(b.String(), "\n")
}

func sortMissingRows(rows []DeckCompareRow) {
	sort.Slice(rows, func(i, j int) bool {
		return cards.NormalizeName(rows[i].Card.Name) < cards.NormalizeName(rows[j].Card.Name)
	})
}

func writeMissingLines(b *strings.Builder, rows []DeckCompareRow) {
	for i, row := range rows {
		if i > 0 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(b, "%d %s", row.Missing, row.Card.Name)
	}
}
