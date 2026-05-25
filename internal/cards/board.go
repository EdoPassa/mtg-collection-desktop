package cards

import "strings"

const (
	BoardMain = "main"
	BoardSide = "side"
)

// NormalizeBoard maps empty or legacy values to BoardMain; only "side" is treated as sideboard.
func NormalizeBoard(board string) string {
	if strings.EqualFold(strings.TrimSpace(board), BoardSide) {
		return BoardSide
	}
	return BoardMain
}
