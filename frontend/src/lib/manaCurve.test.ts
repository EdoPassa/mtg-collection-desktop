import { describe, expect, it } from "vitest";
import type { DeckCard } from "../backend";
import { buildManaCurve, isLandCard, manaValue, MAX_BUCKET } from "./manaCurve";

function card(overrides: Partial<DeckCard["card"]> = {}): DeckCard["card"] {
  return {
    oracleId: overrides.oracleId ?? "oracle",
    name: overrides.name ?? "Card",
    scryfallUri: "https://example.test/card",
    ...overrides
  };
}

function deckCard(quantity: number, overrides: Partial<DeckCard["card"]> = {}): DeckCard {
  return { card: card(overrides), quantity };
}

describe("manaValue", () => {
  it("is 0 for empty or missing cost", () => {
    expect(manaValue(undefined)).toBe(0);
    expect(manaValue("")).toBe(0);
    expect(manaValue("{0}")).toBe(0);
  });

  it("sums generic and colored pips", () => {
    expect(manaValue("{2}{W}{U}")).toBe(4);
    expect(manaValue("{R}")).toBe(1);
  });

  it("treats X as 0", () => {
    expect(manaValue("{X}{R}{R}")).toBe(2);
  });

  it("uses the numeric half of hybrid mana and counts color hybrids as 1", () => {
    expect(manaValue("{2/W}{2/W}")).toBe(4);
    expect(manaValue("{W/U}{W/U}")).toBe(2);
  });

  it("counts Phyrexian pips as 1", () => {
    expect(manaValue("{W/P}")).toBe(1);
  });
});

describe("isLandCard", () => {
  it("matches type lines containing Land", () => {
    expect(isLandCard("Basic Land — Mountain")).toBe(true);
    expect(isLandCard("Legendary Land")).toBe(true);
  });
  it("does not match nonland types or missing data", () => {
    expect(isLandCard("Instant")).toBe(false);
    expect(isLandCard(undefined)).toBe(false);
    expect(isLandCard("Island Sanctuary")).toBe(false);
  });
});

describe("buildManaCurve", () => {
  it("excludes lands and reports land/nonland counts", () => {
    const data = buildManaCurve([
      deckCard(20, { typeLine: "Basic Land — Mountain" }),
      deckCard(4, { typeLine: "Instant", manaCost: "{R}", colorIdentity: ["R"] })
    ]);
    expect(data.landCount).toBe(20);
    expect(data.nonlandCount).toBe(4);
    expect(data.buckets[1].total).toBe(4);
    expect(data.buckets[1].byColor.R).toBe(4);
  });

  it("buckets by mana value and segments by color identity", () => {
    const data = buildManaCurve([
      deckCard(4, { typeLine: "Creature", manaCost: "{1}{U}", colorIdentity: ["U"] }),
      deckCard(2, { typeLine: "Creature", manaCost: "{1}{U}", colorIdentity: ["U", "B"] }),
      deckCard(3, { typeLine: "Sorcery", manaCost: "{3}{B}", colorIdentity: ["B"] })
    ]);
    expect(data.buckets[2].byColor.U).toBe(4);
    expect(data.buckets[2].byColor.M).toBe(2);
    expect(data.buckets[2].total).toBe(6);
    expect(data.buckets[4].byColor.B).toBe(3);
    expect(data.maxTotal).toBe(6);
  });

  it("groups high costs into the 7+ bucket", () => {
    const data = buildManaCurve([
      deckCard(1, { typeLine: "Creature", manaCost: "{9}{G}", colorIdentity: ["G"] })
    ]);
    expect(data.buckets[MAX_BUCKET].label).toBe("7+");
    expect(data.buckets[MAX_BUCKET].total).toBe(1);
  });

  it("counts costless nonland cards as unknown without placing them in a bucket", () => {
    const data = buildManaCurve([deckCard(2, { typeLine: "Token Creature" })]);
    expect(data.nonlandCount).toBe(2);
    expect(data.unknownCount).toBe(2);
    expect(data.buckets[0].total).toBe(0);
    expect(data.averageManaValue).toBe(0);
  });

  it("computes a quantity-weighted average mana value", () => {
    const data = buildManaCurve([
      deckCard(3, { typeLine: "Creature", manaCost: "{1}", colorIdentity: [] }),
      deckCard(1, { typeLine: "Creature", manaCost: "{5}", colorIdentity: [] })
    ]);
    expect(data.averageManaValue).toBeCloseTo((3 * 1 + 1 * 5) / 4, 5);
  });

  it("never returns maxTotal below 1", () => {
    expect(buildManaCurve([]).maxTotal).toBe(1);
  });
});
