import { describe, expect, it } from "vitest";
import { colorIdentityBucket, matchesColorFilter, parseManaCost } from "./mana";

describe("parseManaCost", () => {
  it("returns empty array for null or empty input", () => {
    expect(parseManaCost(undefined)).toEqual([]);
    expect(parseManaCost(null)).toEqual([]);
    expect(parseManaCost("")).toEqual([]);
  });

  it("parses single colored cost", () => {
    expect(parseManaCost("{R}")).toEqual([{ raw: "R", label: "R", color: "R" }]);
  });

  it("parses generic and colored mana", () => {
    const symbols = parseManaCost("{2}{W}{W}");
    expect(symbols.map((s) => s.color)).toEqual(["C", "W", "W"]);
    expect(symbols.map((s) => s.label)).toEqual(["2", "W", "W"]);
  });

  it("parses X as a variable cost", () => {
    expect(parseManaCost("{X}{R}{R}")[0]).toEqual({ raw: "X", label: "X", color: "X" });
  });

  it("keeps hybrid mana as a single pip with first-color tint", () => {
    const symbols = parseManaCost("{W/U}{1}");
    expect(symbols[0]).toEqual({ raw: "W/U", label: "W/U", color: "W" });
    expect(symbols[1].color).toBe("C");
  });

  it("falls back to colorless for unknown tokens", () => {
    expect(parseManaCost("{TAP}")[0].color).toBe("C");
  });
});

describe("colorIdentityBucket", () => {
  it("returns C for empty or missing identity", () => {
    expect(colorIdentityBucket(undefined)).toBe("C");
    expect(colorIdentityBucket([])).toBe("C");
  });
  it("returns the single color for monocolored cards", () => {
    expect(colorIdentityBucket(["G"])).toBe("G");
  });
  it("returns M for multicolored cards", () => {
    expect(colorIdentityBucket(["U", "B"])).toBe("M");
  });
});

describe("matchesColorFilter", () => {
  it("passes when no filter is active", () => {
    expect(matchesColorFilter(["R"], new Set())).toBe(true);
    expect(matchesColorFilter(undefined, new Set())).toBe(true);
  });
  it("colorless cards match only when C is allowed", () => {
    expect(matchesColorFilter([], new Set(["C"]))).toBe(true);
    expect(matchesColorFilter([], new Set(["R"]))).toBe(false);
  });
  it("colored cards match when any of their colors is allowed", () => {
    expect(matchesColorFilter(["W", "U"], new Set(["U"]))).toBe(true);
    expect(matchesColorFilter(["W"], new Set(["U"]))).toBe(false);
  });
});
