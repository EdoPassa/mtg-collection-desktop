import React, { useEffect, useMemo, useState } from "react";
import type { CollectionItem, LentCard } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { Select } from "../components/Select";
import { Stat } from "../components/Stat";
import type { PanelProps } from "./types";

export function LendingPanel({ api, setMessage }: PanelProps) {
  const [collection, setCollection] = useState<CollectionItem[]>([]);
  const [oracleId, setOracleId] = useState("");
  const [borrower, setBorrower] = useState("");
  const [rows, setRows] = useState<LentCard[]>([]);

  const lendableCards = useMemo(
    () => collection.filter((row) => row.available > 0).sort((a, b) => a.card.name.localeCompare(b.card.name)),
    [collection]
  );

  async function loadCollection() {
    try {
      setCollection((await api.ListCollection()) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function loadLoans() {
    try {
      setRows((await api.ListLentCards(false)) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

  useEffect(() => {
    void loadCollection();
    void loadLoans();
  }, []);

  async function lend() {
    try {
      await api.LendCard({
        OracleID: oracleId,
        Quantity: 1,
        BorrowerName: borrower,
        LentDate: new Date().toISOString().slice(0, 10),
        Notes: ""
      });
      setMessage("Lending record added.");
      setOracleId("");
      setBorrower("");
      await loadLoans();
      await loadCollection();
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function markReturned(id: number) {
    try {
      await api.ReturnCard(id, new Date().toISOString().slice(0, 10));
      setMessage("Card marked returned.");
      await loadLoans();
      await loadCollection();
    } catch (error) {
      setMessage(String(error));
    }
  }

  return (
    <section className="panel" aria-label="Lending">
      <div className="toolbar">
        <div className="field field--inline field--grow">
          <label className="field-label" htmlFor="lending-card">
            Card
          </label>
          <Select
            id="lending-card"
            aria-label="Card to lend"
            value={oracleId}
            onChange={(event) => setOracleId(event.target.value)}
            disabled={lendableCards.length === 0}
          >
            <option value="">{lendableCards.length === 0 ? "No available cards" : "Select a card…"}</option>
            {lendableCards.map((row) => (
              <option key={row.card.oracleId} value={row.card.oracleId}>
                {row.card.name} ({row.available} available)
              </option>
            ))}
          </Select>
        </div>
        <input aria-label="Borrower" placeholder="Borrower name" value={borrower} onChange={(event) => setBorrower(event.target.value)} />
        <button type="button" className="primary" onClick={lend} disabled={!oracleId || !borrower.trim()}>
          Add Lending Record
        </button>
      </div>
      {lendableCards.length === 0 && collection.length > 0 && (
        <p className="card-meta">All owned cards are currently lent or unavailable. Mark a loan as returned to lend again.</p>
      )}
      {collection.length === 0 && <p className="card-meta">Import cards into your collection before recording a loan.</p>}
      {rows.length === 0 ? (
        <EmptyState title="No active lending records." detail="Lent cards you track will appear here until marked returned." />
      ) : (
        <div className="card-grid">
          {rows.map((row) => (
            <article key={row.id} className="card-row">
              <h3>{row.card.name}</h3>
              <div className="stat-row">
                <Stat label="Qty" value={row.quantity} />
                <Stat label="Lent" value={row.lentDate} />
              </div>
              <p className="card-meta">Borrower: {row.borrowerName}</p>
              <button type="button" className="ghost" onClick={() => markReturned(row.id)}>
                Mark Returned
              </button>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
