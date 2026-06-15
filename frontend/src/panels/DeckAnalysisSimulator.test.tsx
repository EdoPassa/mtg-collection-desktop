import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createFakeBackendTestKit } from "../test/fakeBackend";
import { DeckAnalysisSimulator } from "./DeckAnalysisSimulator";

function makeSimState(sessionId: string, oracleIdUsed = "") {
  return {
    sessionId,
    phase: "playing" as const,
    hand: [{ slotId: "s1", oracleId: "oracle-bolt", name: "Lightning Bolt", isLand: false }],
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
      oracleIdUsed,
      afterOneDrawLandsProb: 0.5,
      afterOneDrawLandsProbFormatted: "50.0%",
      minLandsThreshold: 2
    },
    deckId: 1,
    formatTarget: "standard"
  };
}

afterEach(() => cleanup());

const mainboard = [
  {
    card: {
      oracleId: "oracle-bolt",
      name: "Lightning Bolt",
      scryfallUri: "https://example.test/bolt",
      typeLine: "Instant"
    },
    quantity: 4
  }
];

describe("DeckAnalysisSimulator", () => {
  it("updates oracle focus without restarting the session when selectedOracleId arrives later", async () => {
    const setMessage = vi.fn();
    const { api, bindings } = createFakeBackendTestKit({
      StartDeckSimulation: vi.fn().mockResolvedValue(makeSimState("sim-1")),
      SimSetOracleFocus: vi.fn().mockResolvedValue(makeSimState("sim-1", "oracle-bolt"))
    });
    const { rerender } = render(
      <DeckAnalysisSimulator
        api={api}
        setMessage={setMessage}
        deckId={1}
        formatTarget="standard"
        mainboardCards={mainboard}
        selectedOracleId=""
        onOracleChange={vi.fn()}
        selectedTagId={0}
        onTagChange={vi.fn()}
      />
    );

    await waitFor(() => expect(bindings.StartDeckSimulation).toHaveBeenCalledTimes(1));
    await screen.findByText(/P\(next card is a land\)/);

    rerender(
      <DeckAnalysisSimulator
        api={api}
        setMessage={setMessage}
        deckId={1}
        formatTarget="standard"
        mainboardCards={mainboard}
        selectedOracleId="oracle-bolt"
        onOracleChange={vi.fn()}
        selectedTagId={0}
        onTagChange={vi.fn()}
      />
    );

    await waitFor(() => expect(bindings.SimSetOracleFocus).toHaveBeenCalledWith("sim-1", "oracle-bolt"));
    expect(bindings.StartDeckSimulation).toHaveBeenCalledTimes(1);
    expect(setMessage).not.toHaveBeenCalledWith("simulation session not found");
  });

  it("puts a card on bottom when awaiting bottom phase", async () => {
    const { api, bindings } = createFakeBackendTestKit({
      StartDeckSimulation: vi.fn().mockResolvedValue({
        sessionId: "sim-1",
        phase: "awaiting_bottom",
        hand: [{ slotId: "s1", oracleId: "oracle-bolt", name: "Lightning Bolt", isLand: false }],
        libraryCount: 53,
        mulliganCount: 1,
        canMulligan: false,
        canDraw: false,
        stats: {
          landsInHand: 0,
          libraryRemaining: 53,
          nextDrawLandProb: 0.4,
          nextDrawLandProbFormatted: "40.0%",
          nextDrawCardProb: 0,
          nextDrawCardProbFormatted: "0.0%",
          afterOneDrawLandsProb: 0.5,
          afterOneDrawLandsProbFormatted: "50.0%",
          minLandsThreshold: 2
        },
        deckId: 1,
        formatTarget: "standard"
      }),
      SimPutOnBottom: vi.fn().mockResolvedValue({
        sessionId: "sim-1",
        phase: "playing",
        hand: [],
        libraryCount: 53,
        mulliganCount: 1,
        canMulligan: true,
        canDraw: true,
        stats: {
          landsInHand: 0,
          libraryRemaining: 53,
          nextDrawLandProb: 0.4,
          nextDrawLandProbFormatted: "40.0%",
          nextDrawCardProb: 0,
          nextDrawCardProbFormatted: "0.0%",
          afterOneDrawLandsProb: 0.5,
          afterOneDrawLandsProbFormatted: "50.0%",
          minLandsThreshold: 2
        },
        deckId: 1,
        formatTarget: "standard"
      }),
      SimSetOracleFocus: vi.fn().mockImplementation(() =>
        Promise.resolve({
          sessionId: "sim-1",
          phase: "awaiting_bottom",
          hand: [{ slotId: "s1", oracleId: "oracle-bolt", name: "Lightning Bolt", isLand: false }],
          libraryCount: 53,
          mulliganCount: 1,
          canMulligan: false,
          canDraw: false,
          stats: {
            landsInHand: 0,
            libraryRemaining: 53,
            nextDrawLandProb: 0.4,
            nextDrawLandProbFormatted: "40.0%",
            nextDrawCardProb: 0,
            nextDrawCardProbFormatted: "0.0%",
            oracleIdUsed: "oracle-bolt",
            afterOneDrawLandsProb: 0.5,
            afterOneDrawLandsProbFormatted: "50.0%",
            minLandsThreshold: 2
          },
          deckId: 1,
          formatTarget: "standard"
        })
      )
    });
    render(
      <DeckAnalysisSimulator
        api={api}
        setMessage={vi.fn()}
        deckId={1}
        formatTarget="standard"
        mainboardCards={mainboard}
        selectedOracleId="oracle-bolt"
        onOracleChange={vi.fn()}
        selectedTagId={0}
        onTagChange={vi.fn()}
      />
    );

    await screen.findByText(/put it on the bottom/i);
    const bottomButton = await screen.findByRole("button", { name: /Put Lightning Bolt on bottom/i });
    await waitFor(() => expect(bottomButton).not.toBeDisabled());
    await userEvent.click(bottomButton);

    await waitFor(() => expect(bindings.SimPutOnBottom).toHaveBeenCalledWith("sim-1", "s1"));
  });
});
