package analysis

import (
	"fmt"

	"mtgcollection/internal/cards"
)

const (
	FormatStandard  = "standard"
	FormatCommander = "commander"
)

// FormatTargetSize returns deck size for a format target id.
func FormatTargetSize(formatTarget string) int {
	if formatTarget == FormatCommander {
		return 100
	}
	return 60
}

type poolSlot struct {
	slotID   string
	oracleID string
	card     cards.CardIdentity
	isLand   bool
	isBlank  bool
}

// DeckPool is an expanded mainboard for simulation and analysis.
type DeckPool struct {
	Slots         []poolSlot
	PopulationN   int
	DeckTotal     int
	TargetSize    int
	DetectedLands int
	OracleCounts  map[string]int
}

// BuildDeckPool expands mainboard cards and pads to population N.
func BuildDeckPool(rows []cards.DeckCard, formatTarget string) DeckPool {
	target := FormatTargetSize(formatTarget)
	mainboard := filterMainboard(rows)
	deckTotal := totalQuantity(mainboard)

	var slots []poolSlot
	oracleCounts := make(map[string]int)
	detectedLands := 0
	slotIdx := 0

	for _, row := range mainboard {
		isLand := cards.IsLandTypeLine(row.Card.TypeLine)
		for i := 0; i < row.Quantity; i++ {
			slotIdx++
			slots = append(slots, poolSlot{
				slotID:   fmt.Sprintf("s%d", slotIdx),
				oracleID: row.Card.OracleID,
				card:     row.Card,
				isLand:   isLand,
			})
			oracleCounts[row.Card.OracleID]++
			if isLand {
				detectedLands++
			}
		}
	}

	populationN := deckTotal
	if deckTotal < target {
		populationN = target
		for i := deckTotal; i < target; i++ {
			slotIdx++
			slots = append(slots, poolSlot{
				slotID:  fmt.Sprintf("blank%d", slotIdx),
				isBlank: true,
			})
		}
	} else if deckTotal > target {
		populationN = deckTotal
	}

	return DeckPool{
		Slots:         slots,
		PopulationN:   populationN,
		DeckTotal:     deckTotal,
		TargetSize:    target,
		DetectedLands: detectedLands,
		OracleCounts:  oracleCounts,
	}
}

func filterMainboard(rows []cards.DeckCard) []cards.DeckCard {
	out := make([]cards.DeckCard, 0, len(rows))
	for _, row := range rows {
		if cards.NormalizeBoard(row.Board) == cards.BoardMain {
			out = append(out, row)
		}
	}
	return out
}

func totalQuantity(rows []cards.DeckCard) int {
	sum := 0
	for _, row := range rows {
		sum += row.Quantity
	}
	return sum
}

// SizeWarning returns a user-facing warning when deck size differs from target.
func (p DeckPool) SizeWarning() string {
	if p.DeckTotal <= 0 {
		return ""
	}
	if p.DeckTotal < p.TargetSize {
		return fmt.Sprintf("Deck lists %d cards; probabilities assume a %d-card deck (unlisted slots are non-hits).", p.DeckTotal, p.TargetSize)
	}
	if p.DeckTotal > p.TargetSize {
		return fmt.Sprintf("Deck has %d cards (over %d-card target).", p.DeckTotal, p.TargetSize)
	}
	return ""
}

func slotToSimulationCard(s poolSlot) SimulationCard {
	if s.isBlank {
		return SimulationCard{SlotID: s.slotID, Name: "—", OracleID: ""}
	}
	return SimulationCard{
		SlotID:        s.slotID,
		OracleID:      s.oracleID,
		Name:          s.card.Name,
		IsLand:        s.isLand,
		TypeLine:      s.card.TypeLine,
		ImageSmall:    s.card.ImageSmall,
		ImageNormal:   s.card.ImageNormal,
		ColorIdentity: s.card.ColorIdentity,
	}
}
