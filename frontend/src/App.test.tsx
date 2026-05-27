import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { createFakeBackendTestKit } from "./test/fakeBackend";

afterEach(() => cleanup());

describe("App", () => {
  it("switches between workflow tabs", async () => {
    render(<App api={createFakeBackendTestKit().api} />);

    expect(await screen.findByRole("heading", { name: "Import Cards" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Collection" }));

    expect(await screen.findByRole("heading", { name: "Collection" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Collection" })).toHaveAttribute("aria-current", "page");
  });

  it("loads and displays saved decks", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.ListDecks).mockResolvedValue([
      { id: 1, name: "Burn" },
      { id: 2, name: "Control" }
    ]);
    vi.mocked(bindings.ListDeckCards).mockResolvedValue([
      {
        card: { oracleId: "oracle-bolt", name: "Lightning Bolt", scryfallUri: "https://example.test/bolt" },
        quantity: 4
      }
    ]);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Library" }));

    expect(await screen.findByRole("heading", { name: "Decks" })).toBeInTheDocument();
    expect(screen.getByText("Burn")).toBeInTheDocument();
    expect(screen.getByText("Control")).toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Burn" }));
    expect(await screen.findByRole("heading", { name: "Lightning Bolt" })).toBeInTheDocument();
    expect(screen.getByLabelText("Lightning Bolt")).toBeInTheDocument();
    expect(bindings.ListDeckCards).toHaveBeenCalledWith(1);
  });

  it("renames and deletes decks through the backend", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.ListDecks).mockResolvedValue([{ id: 7, name: "Burn" }]);
    vi.stubGlobal("confirm", vi.fn().mockReturnValue(true));

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Library" }));
    await userEvent.click(await screen.findByRole("button", { name: "Burn" }));

    const nameInput = screen.getByLabelText("Deck name");
    await userEvent.clear(nameInput);
    await userEvent.type(nameInput, "Mono Red");
    await userEvent.click(screen.getByRole("button", { name: "Save name" }));

    expect(bindings.RenameDeck).toHaveBeenCalledWith(7, "Mono Red");

    await userEvent.click(screen.getByRole("button", { name: "Delete deck" }));
    expect(bindings.DeleteDeck).toHaveBeenCalledWith(7);
  });

  it("adds and updates deck cards through the backend", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.ListDecks).mockResolvedValue([{ id: 3, name: "Burn" }]);
    vi.mocked(bindings.ListDeckCards).mockResolvedValue([
      {
        card: { oracleId: "oracle-bolt", name: "Lightning Bolt", scryfallUri: "https://example.test/bolt" },
        quantity: 2
      }
    ]);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Library" }));
    await userEvent.click(await screen.findByRole("button", { name: "Burn" }));
    await userEvent.click(await screen.findByRole("button", { name: "+" }));

    expect(bindings.SetDeckCardQuantity).toHaveBeenCalledWith(3, "oracle-bolt", "main", 3);

    await userEvent.type(screen.getByLabelText("Card name to add"), "Counterspell");
    await userEvent.click(screen.getByRole("button", { name: "Add card" }));

    expect(bindings.AddCardToDeckByName).toHaveBeenCalledWith(3, "Counterspell", 1);
  });

  it("loads collection data from the backend", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.ListCollection).mockResolvedValue([
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

    expect(await screen.findByText("Lightning Bolt", { selector: "strong" })).toBeInTheDocument();
    expect(screen.getByText("Available: 3")).toBeInTheDocument();
  });

  it("renders import preview when the backend returns null slices", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.PreviewTextImport).mockResolvedValue({ validated: null, unresolved: null } as never);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Validate" }));

    expect(await screen.findByText("Validated 0 row(s).")).toBeInTheDocument();
    expect(screen.getAllByText("None.")).toHaveLength(2);
  });

  it("validates CSV imports through the backend", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.PreviewCSVImport).mockResolvedValue({
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
    await userEvent.click(screen.getByLabelText("Import source mode"));
    await userEvent.click(screen.getByRole("option", { name: "CSV file" }));

    const file = new File(["name,quantity\nCounterspell,2\n"], "cards.csv", { type: "text/csv" });
    const hiddenInput = document.getElementById("import-csv-file") as HTMLInputElement;
    await userEvent.upload(hiddenInput, file);
    await userEvent.click(screen.getByRole("button", { name: "Validate" }));

    expect(bindings.PreviewCSVImport).toHaveBeenCalled();
    expect(await screen.findByText("Validated 1 row(s).")).toBeInTheDocument();
    expect(screen.getByText("2x Counterspell (bulk)")).toBeInTheDocument();
  });

  it("renders an empty collection instead of crashing when the backend returns null", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.ListCollection).mockResolvedValue(null as never);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Collection" }));

    expect(await screen.findByText("No cards found.")).toBeInTheDocument();
  });

  it("renders an empty lending list instead of crashing when the backend returns null", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.ListLentCards).mockResolvedValue(null as never);

    render(<App api={api} />);
    await userEvent.click(screen.getByRole("button", { name: "Lending" }));

    expect(await screen.findByText("No active lending records.")).toBeInTheDocument();
  });

  it("shows repair candidates and refreshes compare results after repairing", async () => {
    const { api, bindings } = createFakeBackendTestKit();
    vi.mocked(bindings.CompareDeck)
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
    await userEvent.click(screen.getByRole("button", { name: "Compare" }));
    await userEvent.click(screen.getByRole("button", { name: "Run compare" }));

    expect(await screen.findByText("Repair old-shock to Shock")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Repair Mismatches" }));

    expect(bindings.RepairCompareMismatches).toHaveBeenCalledWith([
      {
        fromOracleId: "old-shock",
        toCard: { oracleId: "new-shock", name: "Shock", scryfallUri: "https://example.test/new" }
      }
    ]);
    expect(await screen.findByText("Compared 1 card(s).")).toBeInTheDocument();
    expect(screen.queryByText("Repair old-shock to Shock")).not.toBeInTheDocument();
    expect(bindings.CompareDeck).toHaveBeenCalledTimes(2);
  });
});
