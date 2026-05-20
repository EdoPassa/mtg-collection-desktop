package cards

import "testing"

func TestNormalizeNameTrimsCollapsesWhitespaceAndFoldsCase(t *testing.T) {
	got := NormalizeName("  LIGHTNING\t\n Bolt  ")
	want := "lightning bolt"
	if got != want {
		t.Fatalf("NormalizeName() = %q, want %q", got, want)
	}
}

func TestNormalizeNameReturnsEmptyForWhitespace(t *testing.T) {
	if got := NormalizeName(" \t\n "); got != "" {
		t.Fatalf("NormalizeName() = %q, want empty string", got)
	}
}
