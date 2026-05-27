import React, { useEffect, useMemo, useState } from "react";
import type { Deck, DeckCard } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { Select } from "../components/Select";
import { isMainboard } from "../lib/deckBoard";
import { DeckAnalysisCalculators } from "./DeckAnalysisCalculators";
import { DeckAnalysisSimulator } from "./DeckAnalysisSimulator";
import type { PanelProps } from "./types";

type AnalysisView = "simulator" | "calculators";

const FORMAT_TARGETS = [
  { id: "standard", label: "60-card" },
  { id: "commander", label: "100-card" }
] as const;

export function DeckAnalysisPanel({ api, setMessage }: PanelProps) {
  const [decks, setDecks] = useState<Deck[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [cards, setCards] = useState<DeckCard[]>([]);
  const [formatTarget, setFormatTarget] = useState<(typeof FORMAT_TARGETS)[number]["id"]>("standard");
  const [selectedOracleId, setSelectedOracleId] = useState("");
  const [view, setView] = useState<AnalysisView>("simulator");

  const selectedDeck = decks.find((deck) => deck.id === selectedId) ?? null;
  const mainboardCards = useMemo(() => cards.filter((row) => isMainboard(row.board)), [cards]);

  async function loadDecks() {
    try {
      const next = (await api.ListDecks()) ?? [];
      setDecks(next);
      if (selectedId !== null && !next.some((deck) => deck.id === selectedId)) {
        setSelectedId(null);
        setCards([]);
        setSelectedOracleId("");
      }
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function loadCards(deckID: number) {
    try {
      const next = (await api.ListDeckCards(deckID)) ?? [];
      setCards(next);
      const main = next.filter((row) => isMainboard(row.board));
      if (main.length > 0 && !main.some((row) => row.card.oracleId === selectedOracleId)) {
        setSelectedOracleId(main[0].card.oracleId);
      }
      if (next.length === 0) {
        setSelectedOracleId("");
      }
    } catch (error) {
      setMessage(String(error));
    }
  }

  useEffect(() => {
    void loadDecks();
  }, []);

  useEffect(() => {
    if (selectedId === null) {
      setCards([]);
      return;
    }
    void loadCards(selectedId);
  }, [selectedId]);

  return (
    <section className="panel" aria-label="Deck Analysis">
      <p className="analysis-intro">
        Simulate opening hands and estimate draw odds with hypergeometric math on the server. Only mainboard cards count;
        sideboard is ignored.
      </p>

      <div className="deck-browser analysis-layout">
        <div className="deck-browser-list">
          <div className="toolbar">
            <button type="button" className="ghost" onClick={() => void loadDecks()}>
              Refresh decks
            </button>
          </div>
          {decks.length === 0 ? (
            <EmptyState title="No decks yet" detail="Build a deck from Deck Compare, then analyze it here." />
          ) : (
            <ul className="deck-list">
              {decks.map((deck) => (
                <li key={deck.id}>
                  <button
                    type="button"
                    className={`deck-list-item${selectedId === deck.id ? " deck-list-item--active" : ""}`}
                    aria-current={selectedId === deck.id ? "true" : undefined}
                    onClick={() => setSelectedId(deck.id)}
                  >
                    <span className="deck-list-name">{deck.name}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="deck-browser-detail analysis-detail">
          {selectedDeck === null ? (
            <EmptyState title="Select a deck" detail="Pick a deck to run the simulator or calculators." />
          ) : (
            <>
              <div className="toolbar analysis-toolbar">
                <h3>{selectedDeck.name}</h3>
                <div className="field field--inline">
                  <label className="field-label" htmlFor="deck-format-target">
                    Format
                  </label>
                  <Select
                    id="deck-format-target"
                    aria-label="Deck format target"
                    value={formatTarget}
                    onChange={(event) =>
                      setFormatTarget(event.target.value as (typeof FORMAT_TARGETS)[number]["id"])
                    }
                  >
                    {FORMAT_TARGETS.map((format) => (
                      <option key={format.id} value={format.id}>
                        {format.label}
                      </option>
                    ))}
                  </Select>
                </div>
              </div>

              <div className="analysis-tabs" role="tablist" aria-label="Analysis views">
                <button
                  type="button"
                  role="tab"
                  aria-selected={view === "simulator"}
                  className={`analysis-tab${view === "simulator" ? " analysis-tab--active" : ""}`}
                  onClick={() => setView("simulator")}
                >
                  Simulator
                </button>
                <button
                  type="button"
                  role="tab"
                  aria-selected={view === "calculators"}
                  className={`analysis-tab${view === "calculators" ? " analysis-tab--active" : ""}`}
                  onClick={() => setView("calculators")}
                >
                  Calculators
                </button>
              </div>

              {view === "simulator" ? (
                <DeckAnalysisSimulator
                  api={api}
                  setMessage={setMessage}
                  deckId={selectedDeck.id}
                  formatTarget={formatTarget}
                  mainboardCards={mainboardCards}
                  selectedOracleId={selectedOracleId}
                  onOracleChange={setSelectedOracleId}
                />
              ) : (
                <DeckAnalysisCalculators
                  api={api}
                  setMessage={setMessage}
                  deck={selectedDeck}
                  cards={cards}
                  formatTarget={formatTarget}
                />
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
}
