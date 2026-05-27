package analysis

// FormatTarget describes a deck size preset for analysis and simulation.
type FormatTarget struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Size  int    `json:"size"`
}

// ListFormatTargets returns supported format presets (single source of truth for UI).
func ListFormatTargets() []FormatTarget {
	return []FormatTarget{
		{ID: FormatStandard, Label: "60-card", Size: FormatTargetSize(FormatStandard)},
		{ID: FormatCommander, Label: "100-card", Size: FormatTargetSize(FormatCommander)},
	}
}
