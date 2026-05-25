import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BackendApi, CollectionItem } from "../backend";
import { CollectionPanel } from "./CollectionPanel";

afterEach(() => cleanup());

function sampleRows(): CollectionItem[] {
  return [
    {
      card: {
        oracleId: "oracle-bolt",
        name: "Lightning Bolt",
        scryfallUri: "https://example.test/bolt",
        typeLine: "Instant",
        manaCost: "{R}",
        colorIdentity: ["R"],
        imageSmall: "https://cards.test/small/bolt.jpg",
        imageNormal: "https://cards.test/normal/bolt.jpg"
      },
      quantity: 4,
      lentQty: 1,
      available: 3,
      inDeck: true
    },
    {
      card: {
        oracleId: "oracle-counterspell",
        name: "Counterspell",
        scryfallUri: "https://example.test/counterspell",
        typeLine: "Instant",
        manaCost: "{U}{U}",
        colorIdentity: ["U"],
        imageSmall: "https://cards.test/small/counterspell.jpg",
        imageNormal: "https://cards.test/normal/counterspell.jpg"
      },
      quantity: 2,
      lentQty: 0,
      available: 2,
      inDeck: false
    },
    {
      card: {
        oracleId: "oracle-solring",
        name: "Sol Ring",
        scryfallUri: "https://example.test/solring",
        typeLine: "Artifact",
        manaCost: "{1}",
        colorIdentity: []
      },
      quantity: 1,
      lentQty: 0,
      available: 1,
      inDeck: true
    }
  ];
}

function panelApi(rows: CollectionItem[]): BackendApi {
  return {
    ResolverStatus: vi.fn().mockResolvedValue("bulk-first"),
    ListCollection: vi.fn().mockResolvedValue(rows),
    PreviewTextImport: vi.fn(),
    PreviewCSVImport: vi.fn(),
    CommitImport: vi.fn(),
    CompareDeck: vi.fn(),
    BuildDeckFromCompare: vi.fn(),
    ListDecks: vi.fn(),
    ListDeckCards: vi.fn(),
    DeleteDeck: vi.fn(),
    RenameDeck: vi.fn(),
    SetDeckCardQuantity: vi.fn(),
    AddCardToDeckByName: vi.fn(),
    LendCard: vi.fn(),
    ListLentCards: vi.fn(),
    ReturnCard: vi.fn(),
    RepairCompareMismatches: vi.fn(),
    FormatMissingDecklist: vi.fn()
  } as unknown as BackendApi;
}

describe("CollectionPanel", () => {
  it("renders the summary, table view, and Scryfall image thumbnails by default", async () => {
    render(<CollectionPanel api={panelApi(sampleRows())} setMessage={vi.fn()} />);

    expect(await screen.findByText("Lightning Bolt")).toBeInTheDocument();
    expect(screen.getByText("unique").closest("span")?.querySelector("strong")?.textContent).toBe("3");

    const boltImage = screen.getByAltText("Lightning Bolt") as HTMLImageElement;
    expect(boltImage).toBeInTheDocument();
    expect(boltImage.tagName).toBe("IMG");
    expect(boltImage).toHaveAttribute("src", "https://cards.test/small/bolt.jpg");
    expect(boltImage).toHaveAttribute("loading", "lazy");
  });

  it("switches to gallery view and loads the larger Scryfall image", async () => {
    render(<CollectionPanel api={panelApi(sampleRows())} setMessage={vi.fn()} />);

    await screen.findByText("Lightning Bolt");
    await userEvent.click(screen.getByRole("button", { name: "Gallery" }));

    const boltImage = screen.getByAltText("Lightning Bolt") as HTMLImageElement;
    expect(boltImage).toHaveAttribute("src", "https://cards.test/normal/bolt.jpg");
    expect(screen.getByRole("button", { name: "Gallery" })).toHaveAttribute("aria-pressed", "true");
  });

  it("filters by status chip (In a deck) and color toggle", async () => {
    render(<CollectionPanel api={panelApi(sampleRows())} setMessage={vi.fn()} />);

    await screen.findByText("Lightning Bolt", { selector: "strong" });

    await userEvent.click(screen.getByRole("button", { name: "In a deck" }));
    expect(screen.getByText("Lightning Bolt", { selector: "strong" })).toBeInTheDocument();
    expect(screen.getByText("Sol Ring", { selector: "strong" })).toBeInTheDocument();
    expect(screen.queryByText("Counterspell", { selector: "strong" })).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "All" }));
    await userEvent.click(screen.getByRole("button", { name: "Toggle color U" }));
    expect(screen.getByText("Counterspell", { selector: "strong" })).toBeInTheDocument();
    expect(screen.queryByText("Lightning Bolt", { selector: "strong" })).not.toBeInTheDocument();
    expect(screen.queryByText("Sol Ring", { selector: "strong" })).not.toBeInTheDocument();
  });

  it("renders a name placeholder when the bulk index has no image", async () => {
    const rows = sampleRows();
    render(<CollectionPanel api={panelApi(rows)} setMessage={vi.fn()} />);

    await screen.findByText("Sol Ring", { selector: "strong" });
    const placeholder = screen.getByLabelText("Sol Ring");
    expect(placeholder.tagName).toBe("SPAN");
    expect(within(placeholder).getByText("Sol Ring")).toBeInTheDocument();
  });

  it("shows the friendly empty state when filters return nothing", async () => {
    render(<CollectionPanel api={panelApi(sampleRows())} setMessage={vi.fn()} />);

    await screen.findByText("Lightning Bolt", { selector: "strong" });
    await userEvent.type(screen.getByLabelText("Search collection"), "Llanowar Elves");
    expect(await screen.findByText("No cards found.")).toBeInTheDocument();
  });
});
