package cards

import "strings"

// IsLandTypeLine reports whether a Scryfall type_line denotes a land.
func IsLandTypeLine(typeLine string) bool {
	return strings.Contains(typeLine, "Land")
}
