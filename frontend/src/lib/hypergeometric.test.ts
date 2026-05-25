import { describe, expect, it } from "vitest";
import { hypergeometricAtLeast, hypergeometricPMF } from "./hypergeometric";

describe("hypergeometricPMF", () => {
  it("returns 0 for impossible draws", () => {
    expect(
      hypergeometricPMF({ population: 60, successesInPopulation: 24, sampleSize: 7, successesInSample: 8 })
    ).toBe(0);
  });

  it("matches a known 60-card deck land opening probability", () => {
    const atLeastTwoLands = Array.from({ length: 6 }, (_, k) => k + 2).reduce(
      (sum, k) =>
        sum +
        hypergeometricPMF({ population: 60, successesInPopulation: 24, sampleSize: 7, successesInSample: k }),
      0
    );
    expect(atLeastTwoLands).toBeGreaterThan(0.75);
    expect(atLeastTwoLands).toBeLessThan(0.88);
  });
});

describe("hypergeometricAtLeast", () => {
  it("sums tail probabilities for at least k successes", () => {
    const atLeastOne = hypergeometricAtLeast({
      population: 60,
      successesInPopulation: 4,
      sampleSize: 7,
      minSuccessesInSample: 1
    });
    const exactlyZero = hypergeometricPMF({
      population: 60,
      successesInPopulation: 4,
      sampleSize: 7,
      successesInSample: 0
    });
    expect(atLeastOne).toBeCloseTo(1 - exactlyZero, 10);
  });
});
