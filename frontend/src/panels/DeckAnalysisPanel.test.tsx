import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createFakeBackendTestKit } from "../test/fakeBackend";
import { DeckAnalysisPanel } from "./DeckAnalysisPanel";

afterEach(() => cleanup());

describe("DeckAnalysisPanel", () => {
  it("starts simulation when a deck is selected", async () => {
    const { api, bindings } = createFakeBackendTestKit({
      ListDecks: vi.fn().mockResolvedValue([{ id: 1, name: "Burn" }]),
      ListDeckCards: vi.fn().mockResolvedValue([
        {
          card: { oracleId: "oracle-bolt", name: "Lightning Bolt", scryfallUri: "https://example.test/bolt", typeLine: "Instant" },
          quantity: 4
        },
        {
          card: {
            oracleId: "oracle-mountain",
            name: "Mountain",
            scryfallUri: "https://example.test/mountain",
            typeLine: "Basic Land — Mountain"
          },
          quantity: 20
        }
      ]),
      StartDeckSimulation: vi.fn().mockResolvedValue({
        sessionId: "sim-1",
        phase: "playing",
        hand: [
          {
            slotId: "s1",
            oracleId: "oracle-bolt",
            name: "Lightning Bolt",
            isLand: false
          }
        ],
        libraryCount: 53,
        mulliganCount: 0,
        canMulligan: true,
        canDraw: true,
        stats: {
          landsInHand: 0,
          libraryRemaining: 53,
          nextDrawLandProb: 0.4,
          nextDrawLandProbFormatted: "40.0%",
          nextDrawCardProb: 0.07,
          nextDrawCardProbFormatted: "7.0%",
          afterOneDrawLandsProb: 0.5,
          afterOneDrawLandsProbFormatted: "50.0%",
          minLandsThreshold: 2
        },
        deckId: 1,
        formatTarget: "standard"
      })
    });
    render(<DeckAnalysisPanel api={api} setMessage={vi.fn()} />);

    await userEvent.click(await screen.findByRole("button", { name: "Burn" }));

    expect(bindings.StartDeckSimulation).toHaveBeenCalled();
    expect(await screen.findByText(/P\(next card is a land\)/)).toBeInTheDocument();
  });

  it("shows draw odds on calculators tab", async () => {
    const { api, bindings } = createFakeBackendTestKit({
      ListDecks: vi.fn().mockResolvedValue([{ id: 1, name: "Burn" }]),
      ListDeckCards: vi.fn().mockResolvedValue([
        {
          card: { oracleId: "oracle-bolt", name: "Lightning Bolt", scryfallUri: "https://example.test/bolt", typeLine: "Instant" },
          quantity: 4
        }
      ]),
      AnalyzeDeckDraw: vi.fn().mockResolvedValue({
        populationN: 60,
        deckTotal: 24,
        targetSize: 60,
        detectedLands: 20,
        effectiveLandsK: 20,
        effectiveSampleSize: 7,
        cardProbability: 0.5,
        cardProbabilityFormatted: "50.0%",
        landProbability: 0.8,
        landProbabilityFormatted: "80.0%"
      })
    });
    render(<DeckAnalysisPanel api={api} setMessage={vi.fn()} />);

    await userEvent.click(await screen.findByRole("button", { name: "Burn" }));
    await userEvent.click(screen.getByRole("tab", { name: "Calculators" }));

    await waitFor(() => expect(bindings.AnalyzeDeckDraw).toHaveBeenCalled());
    const cardResult = await screen.findByText(/Chance to draw at least 1/);
    await waitFor(() => {
      expect(cardResult.querySelector("strong")?.textContent).toBe("50.0%");
    });
  });

  it("shows empty tags state on calculators tab when deck has no tagged cards", async () => {
    const { api } = createFakeBackendTestKit({
      ListDecks: vi.fn().mockResolvedValue([{ id: 1, name: "Burn" }]),
      ListDeckCards: vi.fn().mockResolvedValue([
        {
          card: { oracleId: "oracle-bolt", name: "Lightning Bolt", scryfallUri: "https://example.test/bolt", typeLine: "Instant" },
          quantity: 4
        }
      ]),
      AnalyzeDeckTags: vi.fn().mockResolvedValue({
        populationN: 60,
        deckTotal: 4,
        tags: null
      })
    });
    render(<DeckAnalysisPanel api={api} setMessage={vi.fn()} />);

    await userEvent.click(await screen.findByRole("button", { name: "Burn" }));
    await userEvent.click(screen.getByRole("tab", { name: "Calculators" }));

    expect(await screen.findByText("No tagged cards in deck")).toBeInTheDocument();
  });
});
