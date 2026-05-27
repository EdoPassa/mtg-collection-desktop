package analysis

import (
	"fmt"
	"math"
)

// PMF returns P(X = k) drawing n cards from population N with K successes.
func PMF(population, successesInPopulation, sampleSize, successesInSample int) float64 {
	logP := logHypergeometricPMF(population, successesInPopulation, sampleSize, successesInSample)
	if !isFinite(logP) {
		return 0
	}
	return math.Exp(logP)
}

// AtLeast returns P(X >= minK).
func AtLeast(population, successesInPopulation, sampleSize, minK int) float64 {
	maxK := min(successesInPopulation, sampleSize)
	var total float64
	for k := minK; k <= maxK; k++ {
		total += PMF(population, successesInPopulation, sampleSize, k)
	}
	if total > 1 {
		return 1
	}
	if total < 0 {
		return 0
	}
	return total
}

func logHypergeometricPMF(N, K, n, k int) float64 {
	if N <= 0 || K < 0 || K > N || n < 0 || n > N {
		return math.Inf(-1)
	}
	if k < 0 || k > n || k > K || n-k > N-K {
		return math.Inf(-1)
	}
	return logCombination(K, k) + logCombination(N-K, n-k) - logCombination(N, n)
}

func logCombination(n, k int) float64 {
	if k < 0 || k > n {
		return math.Inf(-1)
	}
	var sum float64
	for i := 1; i <= k; i++ {
		sum += math.Log(float64(n-k+i)) - math.Log(float64(i))
	}
	return sum
}

func isFinite(v float64) bool {
	return !math.IsInf(v, 0) && !math.IsNaN(v)
}

// FormatProbability renders a probability for the UI.
func FormatProbability(value float64) string {
	if !isFinite(value) {
		return "—"
	}
	pct := value * 100
	if pct >= 99.95 {
		return "99.9%"
	}
	if pct <= 0.05 {
		return "<0.1%"
	}
	return fmt.Sprintf("%.1f%%", pct)
}
