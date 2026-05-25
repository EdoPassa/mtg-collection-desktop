import React, { useState } from "react";
import type { DeckCompareResult } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { ResultList } from "../components/ResultList";
import { Stat } from "../components/Stat";
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
      await api.BuildDeckFromCompare({ Name: deckName, ReplaceDeckID: 0, Rows: result.rows });
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
  const repairRows = result.repairs.map((repair) => `Repair ${repair.fromOracleId} to ${repair.toCard.name}`);

  return (
    <section className="panel" aria-label="Deck Compare">
      <label>
        Decklist
        <textarea value={text} onChange={(event) => setText(event.target.value)} rows={8} placeholder="One card per line…" />
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
      </div>
      {result.rows.length === 0 ? (
        <EmptyState title="No comparison yet" detail="Paste a decklist and run Compare to see owned vs missing." />
      ) : (
        <div className="card-grid">
          {result.rows.map((row) => (
            <article key={row.card.oracleId} className="card-row">
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
      )}
      <ResultList title="Compare warnings" rows={result.unresolved} warn={result.unresolved.length > 0} />
      {repairRows.length > 0 && <ResultList title="Repair candidates" rows={repairRows} />}
    </section>
  );
}
