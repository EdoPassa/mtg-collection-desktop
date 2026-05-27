package analysis

import "testing"

func TestHypergeometricPMFImpossible(t *testing.T) {
	got := PMF(60, 24, 7, 8)
	if got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestHypergeometricOpeningLandsBand(t *testing.T) {
	var atLeastTwo float64
	for k := 2; k <= 7; k++ {
		atLeastTwo += PMF(60, 24, 7, k)
	}
	if atLeastTwo <= 0.75 || atLeastTwo >= 0.88 {
		t.Fatalf("at least two lands opening: got %v, want between 0.75 and 0.88", atLeastTwo)
	}
}

func TestHypergeometricAtLeastComplement(t *testing.T) {
	atLeastOne := AtLeast(60, 4, 7, 1)
	exactlyZero := PMF(60, 4, 7, 0)
	if diff := atLeastOne - (1 - exactlyZero); diff > 1e-10 || diff < -1e-10 {
		t.Fatalf("at least one = 1 - P(0): got %v vs %v", atLeastOne, 1-exactlyZero)
	}
}

func TestFormatProbability(t *testing.T) {
	if FormatProbability(0.753) != "75.3%" {
		t.Fatalf("got %q", FormatProbability(0.753))
	}
	if FormatProbability(0.0001) != "<0.1%" {
		t.Fatalf("got %q", FormatProbability(0.0001))
	}
}
