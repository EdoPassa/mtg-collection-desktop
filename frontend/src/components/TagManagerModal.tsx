import React, { useEffect, useState } from "react";
import type { CollectionTag } from "../backend";
import { TAG_COLOR_PRESETS } from "../lib/tags";
import { tagBadgeStyle } from "./TagBadge";

type TagManagerModalProps = {
  open: boolean;
  tags: CollectionTag[];
  onClose: () => void;
  onRename: (id: number, name: string) => Promise<void>;
  onUpdateColor: (id: number, color: string) => Promise<void>;
  onDelete: (id: number) => Promise<void>;
};

export function TagManagerModal({ open, tags, onClose, onRename, onUpdateColor, onDelete }: TagManagerModalProps) {
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState("");

  useEffect(() => {
    if (!open) {
      setEditingId(null);
      setEditName("");
    }
  }, [open]);

  if (!open) {
    return null;
  }

  async function submitRename(id: number) {
    const name = editName.trim();
    if (!name) {
      return;
    }
    await onRename(id, name);
    setEditingId(null);
    setEditName("");
  }

  async function handleDelete(tag: CollectionTag) {
    const message =
      (tag.cardCount ?? 0) > 0
        ? `Delete tag "${tag.name}"? It will be removed from ${tag.cardCount} card(s).`
        : `Delete tag "${tag.name}"?`;
    if (!window.confirm(message)) {
      return;
    }
    await onDelete(tag.id);
  }

  return (
    <div className="tag-manager-backdrop" role="presentation" onClick={onClose}>
      <div
        className="tag-manager"
        role="dialog"
        aria-label="Manage collection tags"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="tag-manager-header">
          <p className="tag-surface-title">Manage tags</p>
          <button type="button" className="ghost tag-manager-close" onClick={onClose} aria-label="Close">
            ×
          </button>
        </div>
        {tags.length === 0 ? (
          <p className="tag-manager-empty">No tags yet. Assign tags from a card row to create them.</p>
        ) : (
          <ul className="tag-manager-list">
            {tags.map((tag) => (
              <li key={tag.id} className="tag-manager-item">
                {editingId === tag.id ? (
                  <div className="tag-manager-edit">
                    <input
                      aria-label={`Rename ${tag.name}`}
                      value={editName}
                      onChange={(event) => setEditName(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") {
                          event.preventDefault();
                          void submitRename(tag.id);
                        }
                      }}
                    />
                    <button type="button" className="primary" onClick={() => void submitRename(tag.id)}>
                      Save
                    </button>
                    <button type="button" className="ghost" onClick={() => setEditingId(null)}>
                      Cancel
                    </button>
                  </div>
                ) : (
                  <>
                    <span className="tag-badge" style={tagBadgeStyle(tag)}>
                      {tag.name}
                    </span>
                    <span className="tag-manager-count">{(tag.cardCount ?? 0) === 1 ? "1 card" : `${tag.cardCount ?? 0} cards`}</span>
                    <div className="tag-manager-colors" role="group" aria-label={`Color for ${tag.name}`}>
                      <button
                        type="button"
                        className={`tag-color-swatch tag-color-swatch--clear${!tag.color ? " tag-color-swatch--active" : ""}`}
                        aria-label="No color"
                        onClick={() => void onUpdateColor(tag.id, "")}
                      />
                      {TAG_COLOR_PRESETS.map((color) => (
                        <button
                          key={color}
                          type="button"
                          className={`tag-color-swatch${tag.color === color ? " tag-color-swatch--active" : ""}`}
                          style={{ backgroundColor: color }}
                          aria-label={`Set color ${color}`}
                          onClick={() => void onUpdateColor(tag.id, color)}
                        />
                      ))}
                    </div>
                    <div className="tag-manager-actions">
                      <button
                        type="button"
                        className="ghost"
                        onClick={() => {
                          setEditingId(tag.id);
                          setEditName(tag.name);
                        }}
                      >
                        Rename
                      </button>
                      <button type="button" className="ghost tag-manager-delete" onClick={() => void handleDelete(tag)}>
                        Delete
                      </button>
                    </div>
                  </>
                )}
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
