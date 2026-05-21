// Package importer parses decklists and CSV exports into normalized import lines.
//
// Text format accepts "4 Lightning Bolt", "4x Lightning Bolt", or "Lightning Bolt x4".
// Lines starting with # and blank lines are ignored.
package importer

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type ImportLine struct {
	Raw        string `json:"raw"`
	Quantity   int    `json:"quantity"`
	Name       string `json:"name"`
	ScryfallID string `json:"scryfallId,omitempty"`
}

var (
	quantityPrefix = regexp.MustCompile(`(?i)^\s*(\d+)\s*x?\s+(.+?)\s*$`)
	quantitySuffix = regexp.MustCompile(`(?i)^\s*(.+?)\s*x?\s*(\d+)\s*$`)
	headerCleaner  = regexp.MustCompile(`[^a-z0-9]+`)
)

func ParseText(text string) ([]ImportLine, []string) {
	var parsed []ImportLine
	var unresolved []string

	for _, raw := range strings.Split(text, "\n") {
		raw = strings.TrimSuffix(raw, "\r")
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		qty, name, ok := parseTextLine(line)
		if !ok || qty <= 0 || strings.TrimSpace(name) == "" {
			unresolved = append(unresolved, raw)
			continue
		}
		parsed = append(parsed, ImportLine{Raw: raw, Quantity: qty, Name: strings.TrimSpace(name)})
	}

	return parsed, unresolved
}

func ParseCSVBytes(data []byte) ([]ImportLine, []string) {
	reader := csv.NewReader(bytes.NewReader(data))
	// Allow rows with a variable number of columns (extra fields are ignored).
	reader.FieldsPerRecord = -1
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, []string{err.Error()}
	}
	if len(rows) == 0 {
		return nil, []string{"CSV has no header row"}
	}

	headers := make(map[string]int, len(rows[0]))
	for i, h := range rows[0] {
		headers[normalizeHeader(h)] = i
	}
	nameIndex, hasName := firstHeader(headers, "name", "cardname")
	qtyIndex, hasQty := firstHeader(headers, "quantity", "qty")
	idIndex, hasID := firstHeader(headers, "scryfallid")
	if !hasName || !hasQty {
		return nil, []string{fmt.Sprintf("CSV must include columns for card name and quantity (expected one of: name/card name + quantity/qty; found: %v)", rows[0])}
	}

	var parsed []ImportLine
	var unresolved []string
	for _, row := range rows[1:] {
		name := strings.TrimSpace(cell(row, nameIndex))
		rawQty := strings.TrimSpace(cell(row, qtyIndex))
		rawID := ""
		if hasID {
			rawID = strings.TrimSpace(cell(row, idIndex))
		}
		raw := fmt.Sprintf("{map[%s:%s %s:%s", rows[0][nameIndex], name, rows[0][qtyIndex], rawQty)
		if hasID {
			raw += fmt.Sprintf(" %s:%s", rows[0][idIndex], rawID)
		}
		raw += "]}"

		qty, err := strconv.Atoi(rawQty)
		if name == "" || err != nil || qty <= 0 {
			unresolved = append(unresolved, raw)
			continue
		}
		parsed = append(parsed, ImportLine{Raw: raw, Quantity: qty, Name: name, ScryfallID: rawID})
	}

	return parsed, unresolved
}

func parseTextLine(line string) (int, string, bool) {
	if m := quantityPrefix.FindStringSubmatch(line); m != nil {
		qty, err := strconv.Atoi(m[1])
		return qty, m[2], err == nil
	}
	if m := quantitySuffix.FindStringSubmatch(line); m != nil {
		qty, err := strconv.Atoi(m[2])
		return qty, m[1], err == nil
	}
	return 0, "", false
}

func firstHeader(headers map[string]int, names ...string) (int, bool) {
	for _, name := range names {
		if i, ok := headers[name]; ok {
			return i, true
		}
	}
	return 0, false
}

// normalizeHeader strips punctuation and spaces so "Card Name" and "card_name" match.
func normalizeHeader(h string) string {
	return headerCleaner.ReplaceAllString(strings.ToLower(strings.TrimSpace(h)), "")
}

func cell(row []string, i int) string {
	if i < 0 || i >= len(row) {
		return ""
	}
	return row[i]
}
