import React, { useMemo, useState } from "react";
import type { DeckCompareResult, DeckCompareRow } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { ResultList } from "../components/ResultList";
import { Stat } from "../components/Stat";
import { boardLabel, isMainboard } from "../lib/deckBoard";
import { downloadTextFile } from "../lib/downloadText";
import type { PanelProps } from "./types";

export function DeckComparePanel({ api, setMessage }: PanelProps) {
  const [text, setText] = useState("4 Lightning Bolt\n2 Counterspell");
  const [deckName, setDeckName] = useState("");
  const [result, setResult] = useState<DeckCompareResult>({ rows: [], unresolved: [], repairs: [], hasUnresolved: false });

  async function compare() {
    try {
      const next = await api.CompareDeck(text);
      setResult(next);
      setMessage(`Compared ${next.rows.length} card(s).`);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function buildDeck() {
    try {
      await api.BuildDeckFromCompare({ name: deckName, replaceDeckId: 0, rows: result.rows });
      setMessage(`Built deck ${deckName}.`);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function repair() {
    try {
      await api.RepairCompareMismatches(result.repairs);
      const next = await api.CompareDeck(text);
      setResult(next);
      setMessage(`Compared ${next.rows.length} card(s).`);
    } catch (error) {
      setMessage(String(error));
    }
  }

  const canBuild = result.rows.length > 0 && result.rows.every((row) => row.missing === 0) && !result.hasUnresolved && deckName.trim() !== "";
  const hasMissing = result.rows.some((row) => row.missing > 0);

  async function exportMissing() {
    try {
      const content = await api.FormatMissingDecklist(result.rows);
      if (!content.trim()) {
        setMessage("No missing cards to export.");
        return;
      }
      downloadTextFile("missing-cards.txt", content);
      setMessage("Exported missing cards.");
    } catch (error) {
      setMessage(String(error));
    }
  }
  const repairRows = result.repairs.map((repair) => `Repair ${repair.fromOracleId} to ${repair.toCard.name}`);
  const { mainboard, sideboard } = useMemo(() => {
    const mainboard: DeckCompareRow[] = [];
    const sideboard: DeckCompareRow[] = [];
    for (const row of result.rows) {
      if (isMainboard(row.board)) {
        mainboard.push(row);
      } else {
        sideboard.push(row);
      }
    }
    return { mainboard, sideboard };
  }, [result.rows]);

  function CompareRows({ rows }: { rows: DeckCompareRow[] }) {
    return (
      <div className="card-grid">
        {rows.map((row) => (
          <article key={`${row.board ?? "main"}-${row.card.oracleId}`} className="card-row">
            <h3>{row.card.name}</h3>
            <div className="stat-row">
              <Stat label="Needed" value={row.needed} />
              <Stat label="Owned" value={row.owned} />
              <Stat label="Missing" value={row.missing} tone={row.missing > 0 ? "danger" : "success"} />
            </div>
            <span className={`badge ${row.missing === 0 ? "badge--complete" : "badge--missing"}`}>
              {row.missing === 0 ? "Complete" : `${row.missing} short`}
            </span>
          </article>
        ))}
      </div>
    );
  }

  return (
    <section className="panel" aria-label="Deck Compare">
      <label>
        Decklist
        <textarea
          value={text}
          onChange={(event) => setText(event.target.value)}
          rows={8}
          placeholder={"One card per line…\n\nSideboard\n2 Card Name"}
        />
      </label>
      <div className="actions compare-toolbar">
        <button type="button" className="primary" onClick={compare}>
          Run compare
        </button>
        <input aria-label="New deck name" placeholder="New deck name" value={deckName} onChange={(event) => setDeckName(event.target.value)} />
        <button type="button" onClick={buildDeck} disabled={!canBuild}>
          Build Deck
        </button>
        <button type="button" className="ghost" onClick={repair} disabled={result.repairs.length === 0}>
          Repair Mismatches
        </button>
        <button type="button" className="ghost" onClick={exportMissing} disabled={!hasMissing}>
          Export missing (.txt)
        </button>
      </div>
      {result.rows.length === 0 ? (
        <EmptyState title="No comparison yet" detail="Paste a decklist and run Compare to see owned vs missing." />
      ) : (
        <>
          {mainboard.length > 0 && (
            <section className="deck-board-section" aria-label="Mainboard comparison">
              <h3 className="deck-board-heading">{boardLabel("main")}</h3>
              <CompareRows rows={mainboard} />
            </section>
          )}
          {sideboard.length > 0 && (
            <section className="deck-board-section" aria-label="Sideboard comparison">
              <h3 className="deck-board-heading">{boardLabel("side")}</h3>
              <CompareRows rows={sideboard} />
            </section>
          )}
        </>
      )}
      <ResultList title="Compare warnings" rows={result.unresolved} warn={result.unresolved.length > 0} />
      {repairRows.length > 0 && <ResultList title="Repair candidates" rows={repairRows} />}
    </section>
  );
}
