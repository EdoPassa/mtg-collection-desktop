import React, { useState } from "react";
import type { CollectionTag } from "../backend";

type BulkTagModalProps = {
  mode: "add" | "remove";
  cardCount: number;
  allTags: CollectionTag[];
  onClose: () => void;
  onConfirm: (tagIDs: number[]) => Promise<void>;
  onCreateTag: (name: string) => Promise<number>;
};

export function BulkTagModal({ mode, cardCount, allTags, onClose, onConfirm, onCreateTag }: BulkTagModalProps) {
  const [selected, setSelected] = useState<Set<number>>(() => new Set());
  const [newTagName, setNewTagName] = useState("");
  const [saving, setSaving] = useState(false);

  const title =
    mode === "add"
      ? `Add tags to ${cardCount} card${cardCount === 1 ? "" : "s"}`
      : `Remove tags from ${cardCount} card${cardCount === 1 ? "" : "s"}`;

  function toggleTag(id: number) {
    setSelected((current) => {
      const next = new Set(current);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }

  async function handleCreate() {
    const name = newTagName.trim();
    if (!name) {
      return;
    }
    const id = await onCreateTag(name);
    setSelected((current) => new Set(current).add(id));
    setNewTagName("");
  }

  async function handleConfirm() {
    setSaving(true);
    try {
      await onConfirm([...selected]);
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="tag-manager-backdrop" role="presentation" onClick={onClose}>
      <div className="tag-manager bulk-tag-modal" role="dialog" aria-label={title} onClick={(event) => event.stopPropagation()}>
        <div className="tag-manager-header">
          <p className="tag-surface-title">{title}</p>
          <button type="button" className="ghost tag-manager-close" onClick={onClose} aria-label="Close">
            ×
          </button>
        </div>
        <div className="tag-editor-list">
          {allTags.length === 0 ? (
            <p className="tag-editor-empty">
              {mode === "add" ? "No tags yet. Create one below." : "No tags to remove."}
            </p>
          ) : null}
          {allTags.map((tag) => (
            <label key={tag.id} className="tag-editor-option">
              <input type="checkbox" checked={selected.has(tag.id)} onChange={() => toggleTag(tag.id)} />
              <span>{tag.name}</span>
            </label>
          ))}
        </div>
        {mode === "add" ? (
          <div className="tag-editor-create">
            <input
              aria-label="New tag name"
              placeholder="Create tag…"
              value={newTagName}
              onChange={(event) => setNewTagName(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") {
                  event.preventDefault();
                  void handleCreate();
                }
              }}
            />
            <button type="button" className="ghost" disabled={!newTagName.trim()} onClick={() => void handleCreate()}>
              Add
            </button>
          </div>
        ) : null}
        <div className="tag-editor-actions">
          <button
            type="button"
            className="primary"
            disabled={saving || selected.size === 0}
            onClick={() => void handleConfirm()}
          >
            {mode === "add" ? "Add tags" : "Remove tags"}
          </button>
          <button type="button" className="ghost" onClick={onClose}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}
