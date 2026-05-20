import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import type { BackendApi } from "./backend";

afterEach(() => cleanup());

function fakeApi(): BackendApi {
  return {
    ResolverStatus: vi.fn().mockResolvedValue("api-only"),
    ListCollection: vi.fn().mockResolvedValue([]),
    PreviewTextImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    PreviewCSVImport: vi.fn().mockResolvedValue({ validated: [], unresolved: [] }),
    CommitImport: vi.fn().mockResolvedValue(undefined),
    CompareDeck: vi.fn().mockResolvedValue({ rows: [], unresolved: [], repairs: [], hasUnresolved: false }),
    BuildDeckFromCompare: vi.fn().mockResolvedValue(1),
    ListDecks: vi.fn().mockResolvedValue([]),
    LendCard: vi.fn().mockResolvedValue(undefined),
    ListLentCards: vi.fn().mockResolvedValue([]),
    ReturnCard: vi.fn().mockResolvedValue(undefined),
    RepairCompareMismatches: vi.fn().mockResolvedValue(undefined)
  };
}

describe("App", () => {
  it("switches between workflow tabs", async () => {
    render(<App api={fakeApi()} />);

    expect(await screen.findByRole("heading", { name: "Import Cards" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Collection" }));

    expect(await screen.findByRole("heading", { name: "Collection" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Collection" })).toHaveAttribute("aria-current", "page");
  });

  it("loads collection data from the backend", async () => {
    const api = fakeApi();
    vi.mocked(api.ListCollection).mockResolvedValue([
      {
        card: { oracleId: "oracle-bolt", name: "Lightning Bolt", scryfallUri: "https://example.test/bolt" },
        quantity: 4,
        lentQty: 1,
        available: 3,
        inDeck: true
      }
    ]);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Collection" }));

    expect(await screen.findByText("Lightning Bolt")).toBeInTheDocument();
    expect(screen.getByText("Available: 3")).toBeInTheDocument();
  });

  it("renders import preview when the backend returns null slices", async () => {
    const api = fakeApi();
    vi.mocked(api.PreviewTextImport).mockResolvedValue({ validated: null, unresolved: null } as never);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Validate" }));

    expect(await screen.findByText("Validated 0 row(s).")).toBeInTheDocument();
    expect(screen.getAllByText("None.")).toHaveLength(2);
  });

  it("validates CSV imports through the backend", async () => {
    const api = fakeApi();
    vi.mocked(api.PreviewCSVImport).mockResolvedValue({
      validated: [
        {
          line: { raw: "csv", quantity: 2, name: "Counterspell" },
          oracleId: "oracle-counterspell",
          name: "Counterspell",
          scryfallUri: "https://example.test/counterspell",
          source: "bulk"
        }
      ],
      unresolved: []
    });

    render(<App api={api} />);
    await userEvent.selectOptions(screen.getByLabelText("Import source mode"), "csv");

    const file = new File(["name,quantity\nCounterspell,2\n"], "cards.csv", { type: "text/csv" });
    const hiddenInput = document.getElementById("import-csv-file") as HTMLInputElement;
    await userEvent.upload(hiddenInput, file);
    await userEvent.click(screen.getByRole("button", { name: "Validate" }));

    expect(api.PreviewCSVImport).toHaveBeenCalled();
    expect(await screen.findByText("Validated 1 row(s).")).toBeInTheDocument();
    expect(screen.getByText("2x Counterspell (bulk)")).toBeInTheDocument();
  });

  it("renders an empty collection instead of crashing when the backend returns null", async () => {
    const api = fakeApi();
    vi.mocked(api.ListCollection).mockResolvedValue(null as never);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Collection" }));

    expect(await screen.findByText("No cards found.")).toBeInTheDocument();
  });

  it("renders an empty lending list instead of crashing when the backend returns null", async () => {
    const api = fakeApi();
    vi.mocked(api.ListLentCards).mockResolvedValue(null as never);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Lending" }));

    expect(await screen.findByText("No active lending records.")).toBeInTheDocument();
  });

  it("shows repair candidates and refreshes compare results after repairing", async () => {
    const api = fakeApi();
    vi.mocked(api.CompareDeck)
      .mockResolvedValueOnce({
        rows: [
          {
            card: { oracleId: "new-shock", name: "Shock", scryfallUri: "https://example.test/new" },
            needed: 1,
            owned: 1,
            missing: 0
          }
        ],
        unresolved: ["Oracle ID mismatch for Shock"],
        repairs: [
          {
            fromOracleId: "old-shock",
            toCard: { oracleId: "new-shock", name: "Shock", scryfallUri: "https://example.test/new" }
          }
        ],
        hasUnresolved: true
      })
      .mockResolvedValueOnce({
        rows: [
          {
            card: { oracleId: "new-shock", name: "Shock", scryfallUri: "https://example.test/new" },
            needed: 1,
            owned: 1,
            missing: 0
          }
        ],
        unresolved: [],
        repairs: [],
        hasUnresolved: false
      });

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Decks / Compare" }));
    await userEvent.click(screen.getByRole("button", { name: "Compare" }));

    expect(await screen.findByText("Repair old-shock to Shock")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Repair Mismatches" }));

    expect(api.RepairCompareMismatches).toHaveBeenCalledWith([
      {
        fromOracleId: "old-shock",
        toCard: { oracleId: "new-shock", name: "Shock", scryfallUri: "https://example.test/new" }
      }
    ]);
    expect(await screen.findByText("Compared 1 card(s).")).toBeInTheDocument();
    expect(screen.queryByText("Repair old-shock to Shock")).not.toBeInTheDocument();
    expect(api.CompareDeck).toHaveBeenCalledTimes(2);
  });
});
