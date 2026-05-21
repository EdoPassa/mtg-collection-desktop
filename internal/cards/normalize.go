package cards

import (
	"strings"

	"golang.org/x/text/cases"
)

// NormalizeName folds case and collapses whitespace so "Lightning Bolt"
// and "lightning  bolt" match the same card during name-based lookups.
func NormalizeName(name string) string {
	fields := strings.Fields(strings.TrimSpace(name))
	if len(fields) == 0 {
		return ""
	}
	return cases.Fold().String(strings.Join(fields, " "))
}
