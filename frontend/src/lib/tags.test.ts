import { describe, expect, it } from "vitest";
import type { CollectionTag } from "../backend";
import { matchesTagFilter } from "./tags";

describe("matchesTagFilter", () => {
  const tags: CollectionTag[] = [
    { id: 1, name: "Trade" },
    { id: 2, name: "Foil" }
  ];

  it("allows all cards when no tags are selected", () => {
    expect(matchesTagFilter(tags, new Set())).toBe(true);
    expect(matchesTagFilter([], new Set())).toBe(true);
  });

  it("requires all selected tags (AND semantics)", () => {
    expect(matchesTagFilter(tags, new Set([1]))).toBe(true);
    expect(matchesTagFilter(tags, new Set([1, 2]))).toBe(true);
    expect(matchesTagFilter(tags, new Set([2]))).toBe(true);
    expect(matchesTagFilter(tags, new Set([1, 2, 3]))).toBe(false);
    expect(matchesTagFilter([{ id: 1, name: "Trade" }], new Set([1, 2]))).toBe(false);
  });
});
