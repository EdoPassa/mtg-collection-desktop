import React, { useEffect, useMemo, useState } from "react";
import type { Deck, DeckCard, FormatTarget } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { Select } from "../components/Select";
import { isMainboard } from "../lib/deckBoard";
import { DeckAnalysisCalculators } from "./DeckAnalysisCalculators";
import { DeckAnalysisOverview } from "./DeckAnalysisOverview";
import { DeckAnalysisSimulator } from "./DeckAnalysisSimulator";
import type { PanelProps } from "./types";

type AnalysisView = "overview" | "simulator" | "calculators";

export function DeckAnalysisPanel({ api, setMessage }: PanelProps) {
  const [decks, setDecks] = useState<Deck[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [cards, setCards] = useState<DeckCard[]>([]);
  const [formatTargets, setFormatTargets] = useState<FormatTarget[]>([]);
  const [formatTarget, setFormatTarget] = useState("");
  const [selectedOracleId, setSelectedOracleId] = useState("");
  const [selectedTagId, setSelectedTagId] = useState(0);
  const [view, setView] = useState<AnalysisView>("overview");

  const selectedFormat = formatTargets.find((format) => format.id === formatTarget) ?? null;
  const selectedDeck = decks.find((deck) => deck.id === selectedId) ?? null;
  const mainboardCards = useMemo(() => cards.filter((row) => isMainboard(row.board)), [cards]);

  async function loadFormatTargets() {
    try {
      const next = (await api.ListFormatTargets()) ?? [];
      setFormatTargets(next);
      if (next.length > 0 && !next.some((format) => format.id === formatTarget)) {
        setFormatTarget(next[0].id);
      }
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function loadDecks() {
    try {
      const next = (await api.ListDecks()) ?? [];
      setDecks(next);
      if (selectedId !== null && !next.some((deck) => deck.id === selectedId)) {
        setSelectedId(null);
        setCards([]);
        setSelectedOracleId("");
        setSelectedTagId(0);
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
        setSelectedTagId(0);
      }
    } catch (error) {
      setMessage(String(error));
    }
  }

  useEffect(() => {
    void loadFormatTargets();
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
    <section className="panel analysis-panel" aria-label="Deck Analysis">
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
          ) : formatTargets.length === 0 || selectedFormat === null ? (
            <EmptyState title="Loading formats" detail="Format presets are loaded from the server." />
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
                    onChange={(event) => setFormatTarget(event.target.value)}
                  >
                    {formatTargets.map((format) => (
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
                  aria-selected={view === "overview"}
                  className={`analysis-tab${view === "overview" ? " analysis-tab--active" : ""}`}
                  onClick={() => setView("overview")}
                >
                  Overview
                </button>
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

              <div
                className="analysis-scroll-region"
                tabIndex={0}
                aria-label={
                  view === "overview"
                    ? "Overview content"
                    : view === "simulator"
                      ? "Simulator content"
                      : "Calculator content"
                }
              >
                {view === "overview" ? (
                  <DeckAnalysisOverview deckId={selectedDeck.id} mainboardCards={mainboardCards} />
                ) : view === "simulator" ? (
                  <DeckAnalysisSimulator
                    api={api}
                    setMessage={setMessage}
                    deckId={selectedDeck.id}
                    formatTarget={formatTarget}
                    mainboardCards={mainboardCards}
                    selectedOracleId={selectedOracleId}
                    onOracleChange={setSelectedOracleId}
                    selectedTagId={selectedTagId}
                    onTagChange={setSelectedTagId}
                  />
                ) : (
                  <DeckAnalysisCalculators
                    api={api}
                    setMessage={setMessage}
                    deck={selectedDeck}
                    cards={cards}
                    formatTarget={formatTarget}
                    formatTargetSize={selectedFormat.size}
                    selectedTagId={selectedTagId}
                    onTagChange={setSelectedTagId}
                  />
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </section>
  );
}
