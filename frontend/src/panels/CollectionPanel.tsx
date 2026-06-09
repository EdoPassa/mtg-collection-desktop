import React, { useEffect, useMemo, useState } from "react";
import type { CollectionFolder, CollectionItem, FolderCard } from "../backend";
import { UnsortedFolderID } from "../backend";
import { CardImage } from "../components/CardImage";
import { ColorIdentityDot } from "../components/ColorIdentityDot";
import { EmptyState } from "../components/EmptyState";
import { FolderTree, scopeLabel } from "../components/FolderTree";
import { ManaCost } from "../components/ManaCost";
import { Select } from "../components/Select";
import { flattenFolderOptions, scopeFolderID, type CollectionScope } from "../lib/folders";
import { matchesColorFilter, type ManaColor } from "../lib/mana";
import type { PanelProps } from "./types";

type StatusFilter = "all" | "in-deck" | "lent" | "free";
type ViewMode = "table" | "gallery";

type DisplayRow = {
  card: CollectionItem["card"];
  quantity: number;
  lentQty: number;
  available: number;
  inDeck: boolean;
  allocatedQty?: number;
  unassignedQty?: number;
};

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

function rowMatchesStatus(row: DisplayRow, status: StatusFilter): boolean {
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

function collectionItemToRow(row: CollectionItem): DisplayRow {
  return {
    card: row.card,
    quantity: row.quantity,
    lentQty: row.lentQty,
    available: row.available,
    inDeck: row.inDeck,
    allocatedQty: row.allocatedQty,
    unassignedQty: row.unassignedQty
  };
}

function folderCardToRow(row: FolderCard): DisplayRow {
  return {
    card: row.card,
    quantity: row.quantity,
    lentQty: row.lentQty ?? 0,
    available: row.available ?? row.quantity,
    inDeck: row.inDeck ?? false
  };
}

export function CollectionPanel({ api, setMessage }: PanelProps) {
  const [scope, setScope] = useState<CollectionScope>({ kind: "all" });
  const [folders, setFolders] = useState<CollectionFolder[]>([]);
  const [rows, setRows] = useState<DisplayRow[]>([]);
  const [unsortedCount, setUnsortedCount] = useState(0);
  const [query, setQuery] = useState("");
  const [status, setStatus] = useState<StatusFilter>("all");
  const [activeColors, setActiveColors] = useState<ReadonlySet<ManaColor>>(new Set());
  const [view, setView] = useState<ViewMode>("table");
  const [movingOracleId, setMovingOracleId] = useState<string | null>(null);
  const [moveQty, setMoveQty] = useState(1);
  const [moveTargetId, setMoveTargetId] = useState<string>(String(UnsortedFolderID));

  async function loadFolders() {
    try {
      setFolders((await api.ListCollectionFolders()) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function loadUnsortedCount() {
    try {
      const unsorted = (await api.ListCollectionInFolder(UnsortedFolderID)) ?? [];
      setUnsortedCount(unsorted.length);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function loadRows(currentScope: CollectionScope = scope) {
    try {
      if (currentScope.kind === "all") {
        const items = (await api.ListCollection()) ?? [];
        setRows(items.map(collectionItemToRow));
      } else {
        const folderID = currentScope.kind === "unsorted" ? UnsortedFolderID : currentScope.id;
        const items = (await api.ListCollectionInFolder(folderID)) ?? [];
        setRows(items.map(folderCardToRow));
      }
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function refreshAll(currentScope: CollectionScope = scope) {
    await Promise.all([loadFolders(), loadUnsortedCount(), loadRows(currentScope)]);
  }

  useEffect(() => {
    void refreshAll();
  }, []);

  useEffect(() => {
    void loadRows(scope);
  }, [scope]);

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

  const folderOptions = useMemo(() => flattenFolderOptions(folders), [folders]);
  const currentFolderID = scopeFolderID(scope);

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

  async function handleCreateFolder(parentId: number | null, name: string) {
    try {
      await api.CreateCollectionFolder(parentId, name);
      setMessage(`Created folder ${name}.`);
      await refreshAll();
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function handleRenameFolder(id: number, name: string) {
    try {
      await api.RenameCollectionFolder(id, name);
      setMessage(`Renamed folder to ${name}.`);
      await refreshAll();
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function handleMoveFolder(id: number, newParentId: number | null) {
    try {
      await api.MoveCollectionFolder(id, newParentId);
      setMessage("Moved folder.");
      await refreshAll();
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function handleDeleteFolder(id: number) {
    try {
      await api.DeleteCollectionFolder(id);
      if (scope.kind === "folder" && scope.id === id) {
        setScope({ kind: "all" });
      }
      setMessage("Deleted folder. Copies returned to Unsorted.");
      await refreshAll({ kind: "all" });
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function handleMoveCopies(oracleId: string, maxQty: number) {
    const qty = Math.min(moveQty, maxQty);
    if (qty <= 0) {
      return;
    }
    const toFolderID = Number(moveTargetId);
    if (toFolderID === currentFolderID) {
      setMessage("Choose a different destination folder.");
      return;
    }
    try {
      await api.MoveCollectionCopies(oracleId, currentFolderID, toFolderID, qty);
      setMessage(`Moved ${qty} cop${qty === 1 ? "y" : "ies"}.`);
      setMovingOracleId(null);
      setMoveQty(1);
      await refreshAll();
    } catch (error) {
      setMessage(String(error));
    }
  }

  function startMove(row: DisplayRow) {
    setMovingOracleId(row.card.oracleId);
    setMoveQty(1);
    const defaultTarget = folderOptions.find((option) => option.id !== currentFolderID);
    setMoveTargetId(String(defaultTarget?.id ?? UnsortedFolderID));
  }

  return (
    <section className="panel collection-panel" aria-label="Collection">
      <div className="collection-browser">
        <div className="collection-browser-folders">
          <FolderTree
            folders={folders}
            scope={scope}
            unsortedCount={unsortedCount}
            onSelectScope={setScope}
            onCreateFolder={handleCreateFolder}
            onRenameFolder={handleRenameFolder}
            onDeleteFolder={handleDeleteFolder}
            onMoveFolder={handleMoveFolder}
          />
        </div>

        <div className="collection-browser-detail">
          <header className="collection-summary" aria-live="polite">
            <span className="collection-scope-label">{scopeLabel(scope, folders)}</span>
            <span aria-hidden="true">·</span>
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

            <button type="button" className="ghost collection-refresh" onClick={() => void refreshAll()}>
              Refresh
            </button>
          </div>

          {filtered.length === 0 ? (
            <EmptyState
              title="No cards found."
              detail={rows.length === 0 ? "Import a list to start building your collection." : "Try a different search term or filter."}
            />
          ) : view === "table" ? (
            <CollectionTable
              rows={filtered}
              showAllocation={scope.kind === "all"}
              canMove={scope.kind !== "all"}
              movingOracleId={movingOracleId}
              moveQty={moveQty}
              moveTargetId={moveTargetId}
              folderOptions={folderOptions}
              currentFolderID={currentFolderID}
              onStartMove={startMove}
              onCancelMove={() => setMovingOracleId(null)}
              onMoveQtyChange={setMoveQty}
              onMoveTargetChange={setMoveTargetId}
              onSubmitMove={(oracleId, maxQty) => void handleMoveCopies(oracleId, maxQty)}
            />
          ) : (
            <CollectionGallery
              rows={filtered}
              showAllocation={scope.kind === "all"}
              canMove={scope.kind !== "all"}
              movingOracleId={movingOracleId}
              moveQty={moveQty}
              moveTargetId={moveTargetId}
              folderOptions={folderOptions}
              currentFolderID={currentFolderID}
              onStartMove={startMove}
              onCancelMove={() => setMovingOracleId(null)}
              onMoveQtyChange={setMoveQty}
              onMoveTargetChange={setMoveTargetId}
              onSubmitMove={(oracleId, maxQty) => void handleMoveCopies(oracleId, maxQty)}
            />
          )}
        </div>
      </div>
    </section>
  );
}

type CollectionViewProps = {
  rows: DisplayRow[];
  showAllocation: boolean;
  canMove: boolean;
  movingOracleId: string | null;
  moveQty: number;
  moveTargetId: string;
  folderOptions: Array<{ id: number; label: string }>;
  currentFolderID: number;
  onStartMove: (row: DisplayRow) => void;
  onCancelMove: () => void;
  onMoveQtyChange: (qty: number) => void;
  onMoveTargetChange: (targetId: string) => void;
  onSubmitMove: (oracleId: string, maxQty: number) => void;
};

function MoveCopiesForm({
  row,
  moveQty,
  moveTargetId,
  folderOptions,
  currentFolderID,
  onCancelMove,
  onMoveQtyChange,
  onMoveTargetChange,
  onSubmitMove
}: {
  row: DisplayRow;
  moveQty: number;
  moveTargetId: string;
  folderOptions: Array<{ id: number; label: string }>;
  currentFolderID: number;
  onCancelMove: () => void;
  onMoveQtyChange: (qty: number) => void;
  onMoveTargetChange: (targetId: string) => void;
  onSubmitMove: (oracleId: string, maxQty: number) => void;
}) {
  const destinations = folderOptions.filter((option) => option.id !== currentFolderID);
  return (
    <div className="collection-move-form">
      <input
        aria-label="Copies to move"
        type="number"
        min={1}
        max={row.quantity}
        value={moveQty}
        onChange={(event) => onMoveQtyChange(Math.max(1, Math.min(row.quantity, Number(event.target.value) || 1)))}
      />
      <Select aria-label="Destination folder" value={moveTargetId} onChange={(event) => onMoveTargetChange(event.target.value)}>
        {destinations.map((option) => (
          <option key={option.id} value={option.id}>
            {option.label}
          </option>
        ))}
      </Select>
      <button type="button" className="primary" onClick={() => onSubmitMove(row.card.oracleId, row.quantity)}>
        Move
      </button>
      <button type="button" className="ghost" onClick={onCancelMove}>
        Cancel
      </button>
    </div>
  );
}

function CollectionTable({
  rows,
  showAllocation,
  canMove,
  movingOracleId,
  moveQty,
  moveTargetId,
  folderOptions,
  currentFolderID,
  onStartMove,
  onCancelMove,
  onMoveQtyChange,
  onMoveTargetChange,
  onSubmitMove
}: CollectionViewProps) {
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
              {showAllocation ? "Owned" : "Here"}
            </th>
            {showAllocation ? (
              <>
                <th scope="col" className="col-num">
                  In folders
                </th>
                <th scope="col" className="col-num">
                  Unsorted
                </th>
              </>
            ) : null}
            <th scope="col" className="col-num">
              Lent
            </th>
            <th scope="col" className="col-num">
              Avail
            </th>
            <th scope="col" className="col-status">
              Status
            </th>
            {canMove ? (
              <th scope="col" className="col-actions">
                Actions
              </th>
            ) : null}
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
              {showAllocation ? (
                <>
                  <td className="col-num">{row.allocatedQty ?? 0}</td>
                  <td className="col-num">{row.unassignedQty ?? 0}</td>
                </>
              ) : null}
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
              {canMove ? (
                <td className="col-actions">
                  {movingOracleId === row.card.oracleId ? (
                    <MoveCopiesForm
                      row={row}
                      moveQty={moveQty}
                      moveTargetId={moveTargetId}
                      folderOptions={folderOptions}
                      currentFolderID={currentFolderID}
                      onCancelMove={onCancelMove}
                      onMoveQtyChange={onMoveQtyChange}
                      onMoveTargetChange={onMoveTargetChange}
                      onSubmitMove={onSubmitMove}
                    />
                  ) : (
                    <button type="button" className="ghost" onClick={() => onStartMove(row)}>
                      Move copies…
                    </button>
                  )}
                </td>
              ) : null}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function CollectionGallery({
  rows,
  showAllocation,
  canMove,
  movingOracleId,
  moveQty,
  moveTargetId,
  folderOptions,
  currentFolderID,
  onStartMove,
  onCancelMove,
  onMoveQtyChange,
  onMoveTargetChange,
  onSubmitMove
}: CollectionViewProps) {
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
                {showAllocation ? "Owned" : "Here"} <strong>{row.quantity}</strong>
              </span>
              {showAllocation ? (
                <>
                  <span>
                    In folders <strong>{row.allocatedQty ?? 0}</strong>
                  </span>
                  <span>
                    Unsorted <strong>{row.unassignedQty ?? 0}</strong>
                  </span>
                </>
              ) : null}
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
            {canMove ? (
              movingOracleId === row.card.oracleId ? (
                <MoveCopiesForm
                  row={row}
                  moveQty={moveQty}
                  moveTargetId={moveTargetId}
                  folderOptions={folderOptions}
                  currentFolderID={currentFolderID}
                  onCancelMove={onCancelMove}
                  onMoveQtyChange={onMoveQtyChange}
                  onMoveTargetChange={onMoveTargetChange}
                  onSubmitMove={onSubmitMove}
                />
              ) : (
                <button type="button" className="ghost" onClick={() => onStartMove(row)}>
                  Move copies…
                </button>
              )
            ) : null}
          </div>
        </article>
      ))}
    </div>
  );
}
