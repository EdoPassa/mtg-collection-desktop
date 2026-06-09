import { describe, expect, it } from "vitest";
import { clampWidth, COLLECTION_COLUMN_DEFAULTS } from "./useResizableColumns";

describe("clampWidth", () => {
  it("enforces per-column minimum widths", () => {
    expect(clampWidth("name", 40)).toBe(120);
    expect(clampWidth("thumb", 40)).toBe(48);
    expect(clampWidth("quantity", 200)).toBe(200);
  });

  it("defines defaults for every column key", () => {
    expect(COLLECTION_COLUMN_DEFAULTS.name).toBeGreaterThan(0);
    expect(COLLECTION_COLUMN_DEFAULTS.tags).toBeGreaterThan(0);
  });
});
