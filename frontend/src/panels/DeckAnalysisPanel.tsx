import React, { useEffect, useMemo, useState } from "react";
import type { Deck, DeckCard } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { Stat } from "../components/Stat";
import { countLandsInDeck } from "../lib/cardTypes";
import { isMainboard, totalQuantity } from "../lib/deckBoard";
import {
  DRAW_PRESETS,
  formatProbability,
  hypergeometricAtLeast,
  hypergeometricPMF
} from "../lib/hypergeometric";
import type { PanelProps } from "./types";

const FORMAT_TARGETS = [
  { id: "standard", label: "60-card", size: 60 },
  { id: "commander", label: "100-card", size: 100 }
] as const;

export function DeckAnalysisPanel({ api, setMessage }: PanelProps) {
  const [decks, setDecks] = useState<Deck[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [cards, setCards] = useState<DeckCard[]>([]);
  const [formatTarget, setFormatTarget] = useState<(typeof FORMAT_TARGETS)[number]["id"]>("standard");
  const [landsInDeck, setLandsInDeck] = useState("");
  const [landsManual, setLandsManual] = useState(false);
  const [selectedOracleId, setSelectedOracleId] = useState("");
  const [sampleSize, setSampleSize] = useState(7);
  const [minCardCopies, setMinCardCopies] = useState(1);
  const [minLands, setMinLands] = useState(2);
  const [genericN, setGenericN] = useState(60);
  const [genericK, setGenericK] = useState(4);
  const [genericNsample, setGenericNsample] = useState(7);
  const [genericMinK, setGenericMinK] = useState(1);
  const [genericMode, setGenericMode] = useState<"at-least" | "exactly">("at-least");

  const selectedDeck = decks.find((deck) => deck.id === selectedId) ?? null;
  const targetSize = FORMAT_TARGETS.find((f) => f.id === formatTarget)?.size ?? 60;
  const mainboardCards = useMemo(() => cards.filter((row) => isMainboard(row.board)), [cards]);
  const deckTotal = useMemo(() => totalQuantity(mainboardCards), [mainboardCards]);
  const populationN = deckTotal >= targetSize ? deckTotal : targetSize;
  const effectiveSampleSize = Math.min(sampleSize, populationN);

  const selectedCard = mainboardCards.find((row) => row.card.oracleId === selectedOracleId) ?? null;
  const detectedLands = useMemo(() => countLandsInDeck(mainboardCards), [mainboardCards]);
  const effectiveLandsK = landsInDeck !== "" ? Number(landsInDeck) : detectedLands;

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
    setLandsManual(false);
    setLandsInDeck("");
    void loadCards(selectedId);
  }, [selectedId]);

  useEffect(() => {
    if (!landsManual && detectedLands > 0) {
      setLandsInDeck(String(detectedLands));
    }
  }, [detectedLands, landsManual, selectedId]);

  useEffect(() => {
    if (deckTotal > 0) {
      setGenericN(deckTotal);
    }
  }, [deckTotal]);

  const cardProbability = useMemo(() => {
    if (!selectedCard || populationN <= 0) {
      return null;
    }
    const K = Math.min(selectedCard.quantity, populationN);
    const n = effectiveSampleSize;
    const minK = Math.min(minCardCopies, K, n);
    return hypergeometricAtLeast({
      population: populationN,
      successesInPopulation: K,
      sampleSize: n,
      minSuccessesInSample: minK
    });
  }, [selectedCard, populationN, effectiveSampleSize, minCardCopies]);

  const landProbability = useMemo(() => {
    const K = Math.min(effectiveLandsK, populationN);
    if (!Number.isFinite(K) || K < 0 || populationN <= 0) {
      return null;
    }
    const n = effectiveSampleSize;
    const minK = Math.min(minLands, K, n);
    return hypergeometricAtLeast({
      population: populationN,
      successesInPopulation: K,
      sampleSize: n,
      minSuccessesInSample: minK
    });
  }, [effectiveLandsK, populationN, effectiveSampleSize, minLands]);

  const genericProbability = useMemo(() => {
    if (genericN <= 0 || genericK < 0 || genericK > genericN || genericNsample < 0 || genericNsample > genericN) {
      return null;
    }
    if (genericMode === "exactly") {
      const k = Math.min(genericMinK, genericK, genericNsample);
      return hypergeometricPMF({
        population: genericN,
        successesInPopulation: genericK,
        sampleSize: genericNsample,
        successesInSample: k
      });
    }
    const minK = Math.min(genericMinK, genericK, genericNsample);
    return hypergeometricAtLeast({
      population: genericN,
      successesInPopulation: genericK,
      sampleSize: genericNsample,
      minSuccessesInSample: minK
    });
  }, [genericN, genericK, genericNsample, genericMinK, genericMode]);

  const sizeWarning =
    deckTotal > 0 && deckTotal < targetSize
      ? `Deck lists ${deckTotal} cards; probabilities assume a ${targetSize}-card deck (unlisted slots are non-hits).`
      : deckTotal > targetSize
        ? `Deck has ${deckTotal} cards (over ${targetSize}-card target).`
        : null;

  return (
    <section className="panel" aria-label="Deck Analysis">
      <p className="analysis-intro">
        Estimate draw odds with the hypergeometric distribution: population (deck size), successes (copies of a card or
        lands), sample (cards seen), and how many successes you need. Only mainboard cards count toward deck size and land
        detection; sideboard is ignored for now.
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
            <EmptyState title="Select a deck" detail="Pick a deck to run card and land probability checks." />
          ) : (
            <>
              <div className="toolbar analysis-toolbar">
                <h3>{selectedDeck.name}</h3>
                <label className="analysis-field">
                  Format
                  <select
                    aria-label="Deck format target"
                    value={formatTarget}
                    onChange={(event) => setFormatTarget(event.target.value as typeof formatTarget)}
                  >
                    {FORMAT_TARGETS.map((format) => (
                      <option key={format.id} value={format.id}>
                        {format.label}
                      </option>
                    ))}
                  </select>
                </label>
              </div>

              <div className="stat-row">
                <Stat label="Mainboard cards" value={deckTotal || "—"} />
                <Stat label="Target size" value={targetSize} />
                <Stat label="Lands detected" value={detectedLands > 0 ? detectedLands : "—"} />
              </div>
              {sizeWarning && <p className="analysis-warning">{sizeWarning}</p>}

              <fieldset className="analysis-block">
                <legend>Cards drawn (sample size)</legend>
                <div className="preset-row">
                  {DRAW_PRESETS.map((preset) => (
                    <button
                      key={preset.id}
                      type="button"
                      className={`preset-chip${sampleSize === preset.sampleSize ? " preset-chip--active" : ""}`}
                      onClick={() => setSampleSize(preset.sampleSize)}
                    >
                      {preset.label}
                    </button>
                  ))}
                </div>
                <label className="analysis-field">
                  Custom sample size
                  <input
                    aria-label="Sample size"
                    type="number"
                    min={1}
                    max={populationN || 250}
                    value={sampleSize}
                    onChange={(event) => setSampleSize(Math.max(1, Number(event.target.value) || 1))}
                  />
                </label>
              </fieldset>

              <article className="analysis-card">
                <h3>Draw a specific card</h3>
                <p className="analysis-hint">Successes (K) come from how many copies of that card are in the deck.</p>
                {mainboardCards.length === 0 ? (
                  <EmptyState title="No mainboard cards" detail="Add mainboard cards in Decks or build from Deck Compare." />
                ) : (
                  <>
                    <label className="analysis-field">
                      Card
                      <select
                        aria-label="Card to analyze"
                        value={selectedOracleId}
                        onChange={(event) => setSelectedOracleId(event.target.value)}
                      >
                        {mainboardCards.map((row) => (
                          <option key={row.card.oracleId} value={row.card.oracleId}>
                            {row.quantity}x {row.card.name}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label className="analysis-field">
                      At least copies
                      <input
                        aria-label="Minimum copies drawn"
                        type="number"
                        min={1}
                        max={selectedCard?.quantity ?? 4}
                        value={minCardCopies}
                        onChange={(event) => setMinCardCopies(Math.max(1, Number(event.target.value) || 1))}
                      />
                    </label>
                    <p className="analysis-result" aria-live="polite">
                      Chance to draw at least {minCardCopies} by seeing {effectiveSampleSize} cards:{" "}
                      <strong>{cardProbability === null ? "—" : formatProbability(cardProbability)}</strong>
                    </p>
                  </>
                )}
              </article>

              <article className="analysis-card">
                <h3>Land count</h3>
                <p className="analysis-hint">
                  {detectedLands > 0
                    ? "Counted from Scryfall type_line on mainboard cards (bulk cache). Override if needed."
                    : "Land auto-count needs bulk-first resolver status and cards with type_line in the oracle index."}
                </p>
                <label className="analysis-field">
                  Lands in deck (K)
                  <input
                    aria-label="Lands in deck"
                    type="number"
                    min={0}
                    max={populationN || 250}
                    placeholder={detectedLands > 0 ? String(detectedLands) : "e.g. 24"}
                    value={landsInDeck}
                    onChange={(event) => {
                      setLandsManual(true);
                      setLandsInDeck(event.target.value);
                    }}
                  />
                </label>
                {detectedLands > 0 && landsManual && (
                  <button
                    type="button"
                    className="ghost"
                    onClick={() => {
                      setLandsManual(false);
                      setLandsInDeck(String(detectedLands));
                    }}
                  >
                    Reset to detected ({detectedLands})
                  </button>
                )}
                <label className="analysis-field">
                  At least lands
                  <input
                    aria-label="Minimum lands drawn"
                    type="number"
                    min={0}
                    max={Number(landsInDeck) || 40}
                    value={minLands}
                    onChange={(event) => setMinLands(Math.max(0, Number(event.target.value) || 0))}
                  />
                </label>
                <p className="analysis-result" aria-live="polite">
                  Chance for at least {minLands} land(s) in {effectiveSampleSize} cards:{" "}
                  <strong>{landProbability === null ? "—" : formatProbability(landProbability)}</strong>
                </p>
              </article>

              <article className="analysis-card">
                <h3>Generic calculator</h3>
                <div className="analysis-grid">
                  <label className="analysis-field">
                    Population N
                    <input
                      aria-label="Population size"
                      type="number"
                      min={1}
                      value={genericN}
                      onChange={(event) => setGenericN(Math.max(1, Number(event.target.value) || 1))}
                    />
                  </label>
                  <label className="analysis-field">
                    Successes K
                    <input
                      aria-label="Successes in population"
                      type="number"
                      min={0}
                      value={genericK}
                      onChange={(event) => setGenericK(Math.max(0, Number(event.target.value) || 0))}
                    />
                  </label>
                  <label className="analysis-field">
                    Sample n
                    <input
                      aria-label="Sample size generic"
                      type="number"
                      min={0}
                      value={genericNsample}
                      onChange={(event) => setGenericNsample(Math.max(0, Number(event.target.value) || 0))}
                    />
                  </label>
                  <label className="analysis-field">
                    {genericMode === "at-least" ? "At least k" : "Exactly k"}
                    <input
                      aria-label="Successes in sample"
                      type="number"
                      min={0}
                      value={genericMinK}
                      onChange={(event) => setGenericMinK(Math.max(0, Number(event.target.value) || 0))}
                    />
                  </label>
                </div>
                <div className="preset-row">
                  <button
                    type="button"
                    className={`preset-chip${genericMode === "at-least" ? " preset-chip--active" : ""}`}
                    onClick={() => setGenericMode("at-least")}
                  >
                    At least
                  </button>
                  <button
                    type="button"
                    className={`preset-chip${genericMode === "exactly" ? " preset-chip--active" : ""}`}
                    onClick={() => setGenericMode("exactly")}
                  >
                    Exactly
                  </button>
                </div>
                <p className="analysis-result" aria-live="polite">
                  Probability:{" "}
                  <strong>{genericProbability === null ? "—" : formatProbability(genericProbability)}</strong>
                </p>
              </article>
            </>
          )}
        </div>
      </div>
    </section>
  );
}
