package cards

import "testing"

func TestIsLandTypeLine(t *testing.T) {
	tests := []struct {
		typeLine string
		want     bool
	}{
		{"Basic Land — Mountain", true},
		{"Land", true},
		{"Artifact Land", true},
		{"Legendary Land — Urza's Saga", true},
		{"Instant", false},
		{"Creature — Human Wizard", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := IsLandTypeLine(tc.typeLine); got != tc.want {
			t.Fatalf("IsLandTypeLine(%q) = %v, want %v", tc.typeLine, got, tc.want)
		}
	}
}
