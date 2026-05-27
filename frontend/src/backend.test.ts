import { describe, expect, it, vi } from "vitest";
import { createBackendApi } from "./backend";
import { defaultWailsOverrides } from "./test/fakeBackend";

describe("createBackendApi", () => {
  it("adapts generated Wails bindings to the app API surface", async () => {
    const bindings = defaultWailsOverrides();
    const api = createBackendApi(bindings);

    await expect(api.ResolverStatus()).resolves.toBe("api-only");
    await expect(api.ListFormatTargets()).resolves.toHaveLength(2);
    await api.BuildDeckFromCompare({ name: "Burn", replaceDeckId: 0, rows: [] });

    expect(bindings.ResolverStatus).toHaveBeenCalledOnce();
    expect(bindings.ListFormatTargets).toHaveBeenCalledOnce();
    expect(bindings.BuildDeckFromCompare).toHaveBeenCalledWith({ name: "Burn", replaceDeckId: 0, rows: [] });
  });
});
