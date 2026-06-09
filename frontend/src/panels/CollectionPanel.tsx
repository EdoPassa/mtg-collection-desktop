import React, { useEffect, useMemo, useState } from "react";
import type { CollectionFolder, CollectionItem, CollectionTag, FolderCard } from "../backend";
import { UnsortedFolderID } from "../backend";
import { CardImage } from "../components/CardImage";
import { ColorIdentityDot } from "../components/ColorIdentityDot";
import { EmptyState } from "../components/EmptyState";
import { FolderTree, scopeLabel } from "../components/FolderTree";
import { ManaCost } from "../components/ManaCost";
import { Select } from "../components/Select";
import { TagBadge, tagBadgeStyle } from "../components/TagBadge";
import { TagEditor } from "../components/TagEditor";
import { TagManagerModal } from "../components/TagManagerModal";
import { flattenFolderOptions, scopeFolderID, type CollectionScope } from "../lib/folders";
import { matchesColorFilter, type ManaColor } from "../lib/mana";
import { matchesTagFilter } from "../lib/tags";
import { useResizableColumns, type CollectionColumnKey } from "../lib/useResizableColumns";
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
  tags: CollectionTag[];
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
    unassignedQty: row.unassignedQty,
    tags: row.tags ?? []
  };
}

function folderCardToRow(row: FolderCard): DisplayRow {
  return {
    card: row.card,
    quantity: row.quantity,
    lentQty: row.lentQty ?? 0,
    available: row.available ?? row.quantity,
    inDeck: row.inDeck ?? false,
    tags: row.tags ?? []
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
  const [tags, setTags] = useState<CollectionTag[]>([]);
  const [activeTagIds, setActiveTagIds] = useState<ReadonlySet<number>>(new Set());
  const [tagManagerOpen, setTagManagerOpen] = useState(false);
  const [editingTagsRow, setEditingTagsRow] = useState<DisplayRow | null>(null);

  async function loadTags() {
    try {
      setTags((await api.ListCollectionTags()) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

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
    await Promise.all([loadFolders(), loadUnsortedCount(), loadTags(), loadRows(currentScope)]);
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
      if (!matchesTagFilter(row.tags, activeTagIds)) {
        return false;
      }
      return true;
    });
  }, [rows, query, status, activeColors, activeTagIds]);

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

  function toggleTag(tagID: number) {
    setActiveTagIds((current) => {
      const next = new Set(current);
      if (next.has(tagID)) {
        next.delete(tagID);
      } else {
        next.add(tagID);
      }
      return next;
    });
  }

  async function handleSaveCardTags(oracleID: string, tagIDs: number[]) {
    try {
      await api.SetCardTags(oracleID, tagIDs);
      setEditingTagsRow(null);
      await Promise.all([loadRows(), loadTags()]);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function handleCreateTag(name: string): Promise<number> {
    const id = await api.CreateCollectionTag(name, "");
    await loadTags();
    return id;
  }

  async function handleRenameTag(tagID: number, name: string) {
    try {
      await api.RenameCollectionTag(tagID, name);
      await Promise.all([loadTags(), loadRows()]);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function handleUpdateTagColor(tagID: number, color: string) {
    try {
      await api.UpdateCollectionTagColor(tagID, color);
      await Promise.all([loadTags(), loadRows()]);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function handleDeleteTag(tagID: number) {
    try {
      await api.DeleteCollectionTag(tagID);
      setActiveTagIds((current) => {
        if (!current.has(tagID)) {
          return current;
        }
        const next = new Set(current);
        next.delete(tagID);
        return next;
      });
      await Promise.all([loadTags(), loadRows()]);
    } catch (error) {
      setMessage(String(error));
    }
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

            <div className="chip-row tag-filter-row" role="group" aria-label="Filter by tag">
              {tags.map((tag) => {
                const isActive = activeTagIds.has(tag.id);
                const isMuted = (tag.cardCount ?? 0) === 0;
                return (
                  <button
                    key={tag.id}
                    type="button"
                    className={`chip tag-chip${isActive ? " chip--active" : ""}${isMuted ? " tag-chip--muted" : ""}`}
                    style={tag.color && isActive ? tagBadgeStyle(tag) : undefined}
                    aria-pressed={isActive}
                    onClick={() => toggleTag(tag.id)}
                  >
                    {tag.name}
                  </button>
                );
              })}
              <button type="button" className="ghost tag-manage-btn" onClick={() => setTagManagerOpen(true)}>
                Manage tags
              </button>
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
              allTags={tags}
              showAllocation={scope.kind === "all"}
              canMove={scope.kind !== "all"}
              movingOracleId={movingOracleId}
              moveQty={moveQty}
              moveTargetId={moveTargetId}
              folderOptions={folderOptions}
              currentFolderID={currentFolderID}
              editingTagsRow={editingTagsRow}
              onStartMove={startMove}
              onCancelMove={() => setMovingOracleId(null)}
              onMoveQtyChange={setMoveQty}
              onMoveTargetChange={setMoveTargetId}
              onSubmitMove={(oracleId, maxQty) => void handleMoveCopies(oracleId, maxQty)}
              onEditTags={setEditingTagsRow}
              onCloseTagEditor={() => setEditingTagsRow(null)}
              onSaveCardTags={handleSaveCardTags}
              onCreateTag={handleCreateTag}
            />
          ) : (
            <CollectionGallery
              rows={filtered}
              allTags={tags}
              showAllocation={scope.kind === "all"}
              canMove={scope.kind !== "all"}
              movingOracleId={movingOracleId}
              moveQty={moveQty}
              moveTargetId={moveTargetId}
              folderOptions={folderOptions}
              currentFolderID={currentFolderID}
              editingTagsRow={editingTagsRow}
              onStartMove={startMove}
              onCancelMove={() => setMovingOracleId(null)}
              onMoveQtyChange={setMoveQty}
              onMoveTargetChange={setMoveTargetId}
              onSubmitMove={(oracleId, maxQty) => void handleMoveCopies(oracleId, maxQty)}
              onEditTags={setEditingTagsRow}
              onCloseTagEditor={() => setEditingTagsRow(null)}
              onSaveCardTags={handleSaveCardTags}
              onCreateTag={handleCreateTag}
            />
          )}
        </div>
      </div>

      <TagManagerModal
        open={tagManagerOpen}
        tags={tags}
        onClose={() => setTagManagerOpen(false)}
        onRename={handleRenameTag}
        onUpdateColor={handleUpdateTagColor}
        onDelete={handleDeleteTag}
      />
    </section>
  );
}

type CollectionViewProps = {
  rows: DisplayRow[];
  allTags: CollectionTag[];
  showAllocation: boolean;
  canMove: boolean;
  movingOracleId: string | null;
  moveQty: number;
  moveTargetId: string;
  folderOptions: Array<{ id: number; label: string }>;
  currentFolderID: number;
  editingTagsRow: DisplayRow | null;
  onStartMove: (row: DisplayRow) => void;
  onCancelMove: () => void;
  onMoveQtyChange: (qty: number) => void;
  onMoveTargetChange: (targetId: string) => void;
  onSubmitMove: (oracleId: string, maxQty: number) => void;
  onEditTags: (row: DisplayRow) => void;
  onCloseTagEditor: () => void;
  onSaveCardTags: (oracleID: string, tagIDs: number[]) => Promise<void>;
  onCreateTag: (name: string) => Promise<number>;
};

function CardTagsCell({
  row,
  allTags,
  editingTagsRow,
  onEditTags,
  onCloseTagEditor,
  onSaveCardTags,
  onCreateTag
}: {
  row: DisplayRow;
  allTags: CollectionTag[];
  editingTagsRow: DisplayRow | null;
  onEditTags: (row: DisplayRow) => void;
  onCloseTagEditor: () => void;
  onSaveCardTags: (oracleID: string, tagIDs: number[]) => Promise<void>;
  onCreateTag: (name: string) => Promise<number>;
}) {
  const isEditing = editingTagsRow?.card.oracleId === row.card.oracleId;
  return (
    <div className="card-tags-cell">
      <div className="card-tags-list">
        {row.tags.map((tag) => (
          <TagBadge key={tag.id} tag={tag} onClick={() => onEditTags(row)} />
        ))}
        <button type="button" className="ghost card-tags-edit" onClick={() => onEditTags(row)}>
          {row.tags.length === 0 ? "Add tags" : "Edit"}
        </button>
      </div>
      {isEditing ? (
        <TagEditor
          cardName={row.card.name}
          assignedTags={row.tags}
          allTags={allTags}
          onClose={onCloseTagEditor}
          onSave={(tagIDs) => onSaveCardTags(row.card.oracleId, tagIDs)}
          onCreateTag={onCreateTag}
        />
      ) : null}
    </div>
  );
}

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

type HeaderColumn = {
  key: CollectionColumnKey;
  className: string;
  label: React.ReactNode;
  ariaLabel?: string;
};

function ResizableHeaderCell({
  column,
  width,
  onResizeStart
}: {
  column: HeaderColumn;
  width: number;
  onResizeStart: (key: CollectionColumnKey, clientX: number) => void;
}) {
  const resizeLabel = column.ariaLabel ?? (typeof column.label === "string" ? column.label : column.key);
  return (
    <th
      scope="col"
      className={column.className}
      style={{ width, minWidth: width, maxWidth: width }}
      aria-label={column.ariaLabel}
    >
      <span className="collection-th-label">{column.label}</span>
      <span
        className="col-resize-handle"
        role="separator"
        aria-orientation="vertical"
        aria-label={`Resize ${resizeLabel} column`}
        onMouseDown={(event) => {
          event.preventDefault();
          onResizeStart(column.key, event.clientX);
        }}
      />
    </th>
  );
}

function CollectionTable({
  rows,
  allTags,
  showAllocation,
  canMove,
  movingOracleId,
  moveQty,
  moveTargetId,
  folderOptions,
  currentFolderID,
  editingTagsRow,
  onStartMove,
  onCancelMove,
  onMoveQtyChange,
  onMoveTargetChange,
  onSubmitMove,
  onEditTags,
  onCloseTagEditor,
  onSaveCardTags,
  onCreateTag
}: CollectionViewProps) {
  const { widths, startResize } = useResizableColumns();

  const headerColumns = useMemo((): HeaderColumn[] => {
    const columns: HeaderColumn[] = [
      { key: "thumb", className: "col-thumb", label: null, ariaLabel: "Card image" },
      { key: "name", className: "col-name", label: "Card" },
      { key: "cost", className: "col-cost", label: "Cost" },
      { key: "type", className: "col-type", label: "Type" },
      { key: "quantity", className: "col-num", label: showAllocation ? "Owned" : "Here" }
    ];
    if (showAllocation) {
      columns.push(
        { key: "allocated", className: "col-num", label: "In folders" },
        { key: "unassigned", className: "col-num", label: "Unsorted" }
      );
    }
    columns.push(
      { key: "lent", className: "col-num", label: "Lent" },
      { key: "avail", className: "col-num", label: "Avail" },
      { key: "status", className: "col-status", label: "Status" },
      { key: "tags", className: "col-tags", label: "Tags" }
    );
    if (canMove) {
      columns.push({ key: "actions", className: "col-actions", label: "Actions" });
    }
    return columns;
  }, [showAllocation, canMove]);

  const tableMinWidth = useMemo(
    () => headerColumns.reduce((sum, column) => sum + widths[column.key], 0),
    [headerColumns, widths]
  );

  return (
    <div className="collection-table-wrap" tabIndex={0} aria-label="Collection table, scroll horizontally to see more columns">
      <table className="collection-table collection-table--resizable" style={{ minWidth: tableMinWidth }}>
        <colgroup>
          {headerColumns.map((column) => (
            <col key={column.key} style={{ width: widths[column.key] }} />
          ))}
        </colgroup>
        <thead>
          <tr>
            {headerColumns.map((column) => (
              <ResizableHeaderCell
                key={column.key}
                column={column}
                width={widths[column.key]}
                onResizeStart={startResize}
              />
            ))}
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
              <td className="col-tags">
                <CardTagsCell
                  row={row}
                  allTags={allTags}
                  editingTagsRow={editingTagsRow}
                  onEditTags={onEditTags}
                  onCloseTagEditor={onCloseTagEditor}
                  onSaveCardTags={onSaveCardTags}
                  onCreateTag={onCreateTag}
                />
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
  allTags,
  showAllocation,
  canMove,
  movingOracleId,
  moveQty,
  moveTargetId,
  folderOptions,
  currentFolderID,
  editingTagsRow,
  onStartMove,
  onCancelMove,
  onMoveQtyChange,
  onMoveTargetChange,
  onSubmitMove,
  onEditTags,
  onCloseTagEditor,
  onSaveCardTags,
  onCreateTag
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
            <CardTagsCell
              row={row}
              allTags={allTags}
              editingTagsRow={editingTagsRow}
              onEditTags={onEditTags}
              onCloseTagEditor={onCloseTagEditor}
              onSaveCardTags={onSaveCardTags}
              onCreateTag={onCreateTag}
            />
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
