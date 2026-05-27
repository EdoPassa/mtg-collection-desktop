import React, { useEffect, useMemo, useState } from "react";
import type { BackendApi, Deck, DeckCard, DeckDrawAnalysisResult, HypergeometricResult } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { Select } from "../components/Select";
import { Stat } from "../components/Stat";
import { isMainboard } from "../lib/deckBoard";

const DRAW_PRESETS = [
  { id: "opening", label: "Opening hand", sampleSize: 7 },
  { id: "mulligan-6", label: "Mulligan to 6", sampleSize: 6 },
  { id: "mulligan-5", label: "Mulligan to 5", sampleSize: 5 },
  { id: "turn-3", label: "~Turn 3 (10 seen)", sampleSize: 10 },
  { id: "turn-5", label: "~Turn 5 (14 seen)", sampleSize: 14 }
] as const;

type Props = {
  api: BackendApi;
  setMessage: (message: string) => void;
  deck: Deck;
  cards: DeckCard[];
  formatTarget: string;
  formatTargetSize: number;
};

export function DeckAnalysisCalculators({
  api,
  setMessage,
  deck,
  cards,
  formatTarget,
  formatTargetSize
}: Props) {
  const [landsInDeck, setLandsInDeck] = useState("");
  const [landsManual, setLandsManual] = useState(false);
  const [selectedOracleId, setSelectedOracleId] = useState("");
  const [sampleSize, setSampleSize] = useState(7);
  const [minCardCopies, setMinCardCopies] = useState(1);
  const [minLands, setMinLands] = useState(2);
  const [genericN, setGenericN] = useState(formatTargetSize);
  const [genericK, setGenericK] = useState(4);
  const [genericNsample, setGenericNsample] = useState(7);
  const [genericMinK, setGenericMinK] = useState(1);
  const [genericMode, setGenericMode] = useState<"at-least" | "exactly">("at-least");
  const [deckResult, setDeckResult] = useState<DeckDrawAnalysisResult | null>(null);
  const [genericResult, setGenericResult] = useState<HypergeometricResult | null>(null);

  const mainboardCards = useMemo(() => cards.filter((row) => isMainboard(row.board)), [cards]);
  const deckTotal = useMemo(
    () => mainboardCards.reduce((sum, row) => sum + row.quantity, 0),
    [mainboardCards]
  );
  const selectedCard = mainboardCards.find((row) => row.card.oracleId === selectedOracleId) ?? null;

  useEffect(() => {
    if (mainboardCards.length > 0 && !mainboardCards.some((row) => row.card.oracleId === selectedOracleId)) {
      setSelectedOracleId(mainboardCards[0].card.oracleId);
    }
  }, [mainboardCards, selectedOracleId]);

  useEffect(() => {
    if (deckTotal > 0) {
      setGenericN(deckTotal);
    } else {
      setGenericN(formatTargetSize);
    }
  }, [deckTotal, formatTargetSize]);

  useEffect(() => {
    setLandsManual(false);
    setLandsInDeck("");
  }, [deck.id]);

  const landsKInput = landsInDeck !== "" ? Number(landsInDeck) : 0;

  useEffect(() => {
    if (mainboardCards.length === 0) {
      setDeckResult(null);
      return;
    }
    const timer = window.setTimeout(() => {
      void (async () => {
        try {
          const result = await api.AnalyzeDeckDraw({
            deckId: deck.id,
            formatTarget,
            sampleSize,
            oracleId: selectedOracleId,
            minCardCopies,
            minLands,
            landsInDeck: landsKInput
          });
          setDeckResult(result);
          if (!landsManual && result.detectedLands > 0) {
            setLandsInDeck(String(result.detectedLands));
          }
        } catch (error) {
          setMessage(String(error));
        }
      })();
    }, 150);
    return () => window.clearTimeout(timer);
  }, [
    api,
    deck.id,
    formatTarget,
    sampleSize,
    selectedOracleId,
    minCardCopies,
    minLands,
    landsKInput,
    landsManual,
    mainboardCards.length,
    setMessage
  ]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      void (async () => {
        try {
          const result = await api.Hypergeometric({
            population: genericN,
            successesInPopulation: genericK,
            sampleSize: genericNsample,
            minSuccessesInSample: genericMinK,
            mode: genericMode
          });
          setGenericResult(result);
        } catch (error) {
          setMessage(String(error));
        }
      })();
    }, 150);
    return () => window.clearTimeout(timer);
  }, [api, genericN, genericK, genericNsample, genericMinK, genericMode, setMessage]);

  const populationN = deckResult?.populationN ?? 0;
  const detectedLands = deckResult?.detectedLands ?? 0;

  return (
    <>
      <div className="stat-row">
        <Stat label="Mainboard cards" value={deckTotal || "—"} />
        <Stat label="Target size" value={deckResult?.targetSize ?? "—"} />
        <Stat label="Lands detected" value={detectedLands > 0 ? detectedLands : "—"} />
      </div>
      {deckResult?.sizeWarning && <p className="analysis-warning">{deckResult.sizeWarning}</p>}

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
              <Select
                aria-label="Card to analyze"
                value={selectedOracleId}
                onChange={(event) => setSelectedOracleId(event.target.value)}
              >
                {mainboardCards.map((row) => (
                  <option key={row.card.oracleId} value={row.card.oracleId}>
                    {row.quantity}x {row.card.name}
                  </option>
                ))}
              </Select>
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
              Chance to draw at least {minCardCopies} by seeing {deckResult?.effectiveSampleSize ?? sampleSize} cards:{" "}
              <strong>{deckResult?.cardProbabilityFormatted ?? "—"}</strong>
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
          Chance for at least {minLands} land(s) in {deckResult?.effectiveSampleSize ?? sampleSize} cards:{" "}
          <strong>{deckResult?.landProbabilityFormatted ?? "—"}</strong>
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
          Probability: <strong>{genericResult?.probabilityFormatted ?? "—"}</strong>
        </p>
      </article>
    </>
  );
}
