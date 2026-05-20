package cards

import (
	"strings"

	"golang.org/x/text/cases"
)

func NormalizeName(name string) string {
	fields := strings.Fields(strings.TrimSpace(name))
	if len(fields) == 0 {
		return ""
	}
	return cases.Fold().String(strings.Join(fields, " "))
}
