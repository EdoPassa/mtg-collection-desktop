import React, { useEffect, useMemo, useState } from "react";
import type { CollectionItem } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { Stat } from "../components/Stat";
import type { PanelProps } from "./types";

export function CollectionPanel({ api, setMessage }: PanelProps) {
  const [rows, setRows] = useState<CollectionItem[]>([]);
  const [query, setQuery] = useState("");

  async function load() {
    try {
      setRows((await api.ListCollection()) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

  useEffect(() => {
    void load();
  }, []);

  const filtered = useMemo(
    () => rows.filter((row) => row.card.name.toLocaleLowerCase().includes(query.toLocaleLowerCase())),
    [rows, query]
  );

  return (
    <section className="panel" aria-label="Collection">
      <div className="toolbar">
        <input aria-label="Search collection" placeholder="Search cards…" value={query} onChange={(event) => setQuery(event.target.value)} />
        <button type="button" className="ghost" onClick={load}>
          Refresh
        </button>
      </div>
      {filtered.length === 0 ? (
        <EmptyState
          title="No cards found."
          detail={rows.length === 0 ? "Import a list to start building your collection." : "Try a different search term."}
        />
      ) : (
        <div className="card-grid">
          {filtered.map((row) => (
            <article key={row.card.oracleId} className="card-row">
              <h3>{row.card.name}</h3>
              <div className="stat-row">
                <Stat label="Owned" value={row.quantity} />
                <Stat label="Lent" value={row.lentQty} tone={row.lentQty > 0 ? "warning" : undefined} />
                <Stat label="Available" value={row.available} tone="success" />
              </div>
              <p className="sr-only">Available: {row.available}</p>
              <span className={`badge ${row.inDeck ? "badge--deck" : "badge--free"}`}>{row.inDeck ? "In deck" : "Not in deck"}</span>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}
