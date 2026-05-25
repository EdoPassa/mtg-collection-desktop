export type HypergeometricInput = {
  population: number;
  successesInPopulation: number;
  sampleSize: number;
  successesInSample: number;
};

export type HypergeometricAtLeastInput = Omit<HypergeometricInput, "successesInSample"> & {
  minSuccessesInSample: number;
};

function logCombination(n: number, k: number): number {
  if (k < 0 || k > n) {
    return Number.NEGATIVE_INFINITY;
  }
  let sum = 0;
  for (let i = 1; i <= k; i++) {
    sum += Math.log(n - k + i) - Math.log(i);
  }
  return sum;
}

function logHypergeometricPMF(input: HypergeometricInput): number {
  const { population: N, successesInPopulation: K, sampleSize: n, successesInSample: k } = input;
  if (N <= 0 || K < 0 || K > N || n < 0 || n > N) {
    return Number.NEGATIVE_INFINITY;
  }
  if (k < 0 || k > n || k > K || n - k > N - K) {
    return Number.NEGATIVE_INFINITY;
  }
  return logCombination(K, k) + logCombination(N - K, n - k) - logCombination(N, n);
}

/** P(X = k) drawing n cards from a deck of N with K successes. */
export function hypergeometricPMF(input: HypergeometricInput): number {
  const logP = logHypergeometricPMF(input);
  if (!Number.isFinite(logP)) {
    return 0;
  }
  return Math.exp(logP);
}

/** P(X >= minK) */
export function hypergeometricAtLeast(input: HypergeometricAtLeastInput): number {
  const { population: N, successesInPopulation: K, sampleSize: n, minSuccessesInSample: minK } = input;
  const maxK = Math.min(K, n);
  let total = 0;
  for (let k = minK; k <= maxK; k++) {
    total += hypergeometricPMF({ population: N, successesInPopulation: K, sampleSize: n, successesInSample: k });
  }
  return Math.min(1, Math.max(0, total));
}

export function formatProbability(value: number): string {
  if (!Number.isFinite(value)) {
    return "—";
  }
  const pct = value * 100;
  if (pct >= 99.95) {
    return "99.9%";
  }
  if (pct <= 0.05) {
    return "<0.1%";
  }
  return `${pct.toFixed(1)}%`;
}

export const DRAW_PRESETS = [
  { id: "opening", label: "Opening hand", sampleSize: 7 },
  { id: "mulligan-6", label: "Mulligan to 6", sampleSize: 6 },
  { id: "mulligan-5", label: "Mulligan to 5", sampleSize: 5 },
  { id: "turn-3", label: "~Turn 3 (10 seen)", sampleSize: 10 },
  { id: "turn-5", label: "~Turn 5 (14 seen)", sampleSize: 14 }
] as const;
