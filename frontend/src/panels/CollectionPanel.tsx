import React, { useEffect, useMemo, useState } from "react";
import type { CollectionItem } from "../backend";
import { CardImage } from "../components/CardImage";
import { ColorIdentityDot } from "../components/ColorIdentityDot";
import { EmptyState } from "../components/EmptyState";
import { ManaCost } from "../components/ManaCost";
import { matchesColorFilter, type ManaColor } from "../lib/mana";
import type { PanelProps } from "./types";

type StatusFilter = "all" | "in-deck" | "lent" | "free";
type ViewMode = "table" | "gallery";

const COLOR_BUTTONS: ReadonlyArray<{ id: ManaColor; label: string }> = [
  { id: "W", label: "W" },
  { id: "U", label: "U" },
  { id: "B", label: "B" },
  { id: "R", label: "R" },
  { id: "G", label: "G" },
  { id: "C", label: "C" }
];

const STATUS_BUTTONS: ReadonlyArray<{ id: StatusFilter; label: string }> = [
  { id: "all", label: "All" },
  { id: "in-deck", label: "In a deck" },
  { id: "lent", label: "Lent" },
  { id: "free", label: "Free" }
];

function rowMatchesStatus(row: CollectionItem, status: StatusFilter): boolean {
  switch (status) {
    case "in-deck":
      return row.inDeck;
    case "lent":
      return row.lentQty > 0;
    case "free":
      return !row.inDeck && row.lentQty === 0;
    default:
      return true;
  }
}

export function CollectionPanel({ api, setMessage }: PanelProps) {
  const [rows, setRows] = useState<CollectionItem[]>([]);
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState<StatusFilter>("all");
  const [activeColors, setActiveColors] = useState<ReadonlySet<ManaColor>>(new Set());
  const [view, setView] = useState<ViewMode>("table");

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

  const filtered = useMemo(() => {
    const needle = query.trim().toLocaleLowerCase();
    return rows.filter((row) => {
      if (needle && !row.card.name.toLocaleLowerCase().includes(needle)) {
        return false;
      }
      if (!rowMatchesStatus(row, status)) {
        return false;
      }
      if (!matchesColorFilter(row.card.colorIdentity, activeColors)) {
        return false;
      }
      return true;
    });
  }, [rows, query, status, activeColors]);

  const summary = useMemo(() => {
    let totalCopies = 0;
    let lentOut = 0;
    let inDecks = 0;
    for (const row of rows) {
      totalCopies += row.quantity;
      lentOut += row.lentQty;
      if (row.inDeck) {
        inDecks += 1;
      }
    }
    return { unique: rows.length, totalCopies, lentOut, inDecks };
  }, [rows]);

  function toggleColor(color: ManaColor) {
    setActiveColors((current) => {
      const next = new Set(current);
      if (next.has(color)) {
        next.delete(color);
      } else {
        next.add(color);
      }
      return next;
    });
  }

  return (
    <section className="panel collection-panel" aria-label="Collection">
      <header className="collection-summary" aria-live="polite">
        <span>
          <strong>{summary.unique}</strong> unique
        </span>
        <span aria-hidden="true">·</span>
        <span>
          <strong>{summary.totalCopies}</strong> total
        </span>
        <span aria-hidden="true">·</span>
        <span>
          <strong>{summary.inDecks}</strong> in a deck
        </span>
        <span aria-hidden="true">·</span>
        <span>
          <strong>{summary.lentOut}</strong> lent out
        </span>
      </header>

      <div className="collection-toolbar">
        <input
          aria-label="Search collection"
          className="collection-search"
          placeholder="Search cards…"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />

        <div className="chip-row" role="group" aria-label="Filter by status">
          {STATUS_BUTTONS.map((option) => (
            <button
              key={option.id}
              type="button"
              className={`chip${status === option.id ? " chip--active" : ""}`}
              aria-pressed={status === option.id}
              onClick={() => setStatus(option.id)}
            >
              {option.label}
            </button>
          ))}
        </div>

        <div className="mana-filter" role="group" aria-label="Filter by color">
          {COLOR_BUTTONS.map((color) => {
            const isActive = activeColors.has(color.id);
            return (
              <button
                key={color.id}
                type="button"
                className={`mana-pip mana-pip--${color.id.toLowerCase()} mana-pip--toggle${isActive ? " mana-pip--toggle-active" : ""}`}
                aria-pressed={isActive}
                aria-label={`Toggle color ${color.label}`}
                onClick={() => toggleColor(color.id)}
              >
                {color.label}
              </button>
            );
          })}
        </div>

        <div className="view-toggle" role="group" aria-label="View mode">
          <button
            type="button"
            className={`view-toggle-btn${view === "table" ? " view-toggle-btn--active" : ""}`}
            aria-pressed={view === "table"}
            onClick={() => setView("table")}
          >
            Table
          </button>
          <button
            type="button"
            className={`view-toggle-btn${view === "gallery" ? " view-toggle-btn--active" : ""}`}
            aria-pressed={view === "gallery"}
            onClick={() => setView("gallery")}
          >
            Gallery
          </button>
        </div>

        <button type="button" className="ghost collection-refresh" onClick={() => void load()}>
          Refresh
        </button>
      </div>

      {filtered.length === 0 ? (
        <EmptyState
          title="No cards found."
          detail={rows.length === 0 ? "Import a list to start building your collection." : "Try a different search term or filter."}
        />
      ) : view === "table" ? (
        <CollectionTable rows={filtered} />
      ) : (
        <CollectionGallery rows={filtered} />
      )}
    </section>
  );
}

function CollectionTable({ rows }: { rows: CollectionItem[] }) {
  return (
    <div className="collection-table-wrap">
      <table className="collection-table">
        <thead>
          <tr>
            <th scope="col" className="col-thumb" aria-label="Card image" />
            <th scope="col" className="col-name">
              Card
            </th>
            <th scope="col" className="col-cost">
              Cost
            </th>
            <th scope="col" className="col-type">
              Type
            </th>
            <th scope="col" className="col-num">
              Owned
            </th>
            <th scope="col" className="col-num">
              Lent
            </th>
            <th scope="col" className="col-num">
              Avail
            </th>
            <th scope="col" className="col-status">
              Status
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.card.oracleId}>
              <td className="col-thumb">
                <CardImage
                  name={row.card.name}
                  small={row.card.imageSmall}
                  normal={row.card.imageNormal}
                  colorIdentity={row.card.colorIdentity}
                  size="thumb"
                  alt={row.card.name}
                />
              </td>
              <td className="col-name">
                <div className="card-name-cell">
                  <ColorIdentityDot identity={row.card.colorIdentity} />
                  <strong>{row.card.name}</strong>
                </div>
              </td>
              <td className="col-cost">
                <ManaCost cost={row.card.manaCost} ariaLabel={`Mana cost for ${row.card.name}`} />
              </td>
              <td className="col-type">{row.card.typeLine ?? ""}</td>
              <td className="col-num">{row.quantity}</td>
              <td className={`col-num${row.lentQty > 0 ? " col-num--warn" : " col-num--zero"}`}>{row.lentQty}</td>
              <td className={`col-num${row.available > 0 ? " col-num--good" : " col-num--zero"}`}>
                {row.available}
                <span className="sr-only">Available: {row.available}</span>
              </td>
              <td className="col-status">
                <span className={`badge ${row.inDeck ? "badge--deck" : "badge--free"}`}>
                  {row.inDeck ? "In deck" : "Free"}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CollectionGallery({ rows }: { rows: CollectionItem[] }) {
  return (
    <div className="collection-gallery">
      {rows.map((row) => (
        <article key={row.card.oracleId} className="gallery-card">
          <CardImage
            name={row.card.name}
            small={row.card.imageSmall}
            normal={row.card.imageNormal}
            colorIdentity={row.card.colorIdentity}
            size="tile"
            alt={row.card.name}
          />
          <div className="gallery-card-body">
            <div className="card-name-cell">
              <ColorIdentityDot identity={row.card.colorIdentity} />
              <strong>{row.card.name}</strong>
            </div>
            <ManaCost cost={row.card.manaCost} ariaLabel={`Mana cost for ${row.card.name}`} />
            {row.card.typeLine ? <p className="gallery-card-type">{row.card.typeLine}</p> : null}
            <div className="gallery-card-quantities">
              <span>
                Owned <strong>{row.quantity}</strong>
              </span>
              <span className={row.lentQty > 0 ? "is-warn" : "is-zero"}>
                Lent <strong>{row.lentQty}</strong>
              </span>
              <span className={row.available > 0 ? "is-good" : "is-zero"}>
                Avail <strong>{row.available}</strong>
                <span className="sr-only"> for {row.card.name}: Available: {row.available}</span>
              </span>
            </div>
            <span className={`badge ${row.inDeck ? "badge--deck" : "badge--free"}`}>
              {row.inDeck ? "In deck" : "Free"}
            </span>
          </div>
        </article>
      ))}
    </div>
  );
}
