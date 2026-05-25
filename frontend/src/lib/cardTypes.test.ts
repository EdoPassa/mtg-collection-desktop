import { describe, expect, it } from "vitest";
import { countLandsInDeck, isLandTypeLine } from "./cardTypes";

describe("isLandTypeLine", () => {
  it("detects land type lines", () => {
    expect(isLandTypeLine("Basic Land — Forest")).toBe(true);
    expect(isLandTypeLine("Instant")).toBe(false);
  });
});

describe("countLandsInDeck", () => {
  it("sums quantities for land cards only", () => {
    const total = countLandsInDeck([
      { card: { typeLine: "Instant" }, quantity: 4 },
      { card: { typeLine: "Basic Land — Mountain" }, quantity: 20 },
      { card: { typeLine: "Basic Land — Mountain" }, quantity: 4 }
    ]);
    expect(total).toBe(24);
  });
});
