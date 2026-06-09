import React, { useEffect, useState } from "react";
import type { CollectionTag } from "../backend";

type TagEditorProps = {
  cardName: string;
  assignedTags: CollectionTag[];
  allTags: CollectionTag[];
  onClose: () => void;
  onSave: (tagIDs: number[]) => Promise<void>;
  onCreateTag: (name: string) => Promise<number>;
};

export function TagEditor({ cardName, assignedTags, allTags, onClose, onSave, onCreateTag }: TagEditorProps) {
  const [selected, setSelected] = useState<Set<number>>(() => new Set(assignedTags.map((tag) => tag.id)));
  const [newTagName, setNewTagName] = useState("");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setSelected(new Set(assignedTags.map((tag) => tag.id)));
  }, [assignedTags]);

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

  async function handleSave() {
    setSaving(true);
    try {
      await onSave([...selected]);
      onClose();
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="tag-editor" role="dialog" aria-label={`Edit tags for ${cardName}`}>
      <div className="tag-editor-header">
        <p className="tag-surface-title">Tags for {cardName}</p>
        <button type="button" className="ghost tag-editor-close" onClick={onClose} aria-label="Close">
          ×
        </button>
      </div>
      <div className="tag-editor-list">
        {allTags.length === 0 ? <p className="tag-editor-empty">No tags yet. Create one below.</p> : null}
        {allTags.map((tag) => (
          <label key={tag.id} className="tag-editor-option">
            <input type="checkbox" checked={selected.has(tag.id)} onChange={() => toggleTag(tag.id)} />
            <span>{tag.name}</span>
          </label>
        ))}
      </div>
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
      <div className="tag-editor-actions">
        <button type="button" className="primary" disabled={saving} onClick={() => void handleSave()}>
          Save
        </button>
        <button type="button" className="ghost" onClick={onClose}>
          Cancel
        </button>
      </div>
    </div>
  );
}
