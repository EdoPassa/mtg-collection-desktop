import React, { useMemo, useState } from "react";
import type { CollectionFolder } from "../backend";
import { UnsortedFolderID } from "../backend";
import {
  buildFolderTree,
  folderDescendantIDs,
  type CollectionScope,
  type FolderTreeNode,
  scopesEqual
} from "../lib/folders";
import { Select } from "./Select";

type FolderTreeProps = {
  folders: CollectionFolder[];
  scope: CollectionScope;
  unsortedCount: number;
  onSelectScope: (scope: CollectionScope) => void;
  onCreateFolder: (parentId: number | null, name: string) => Promise<void>;
  onRenameFolder: (id: number, name: string) => Promise<void>;
  onDeleteFolder: (id: number) => Promise<void>;
  onMoveFolder: (id: number, newParentId: number | null) => Promise<void>;
};

export function FolderTree({
  folders,
  scope,
  unsortedCount,
  onSelectScope,
  onCreateFolder,
  onRenameFolder,
  onDeleteFolder,
  onMoveFolder
}: FolderTreeProps) {
  const tree = useMemo(() => buildFolderTree(folders), [folders]);
  const [createParentId, setCreateParentId] = useState<number | null>(null);
  const [createName, setCreateName] = useState("");
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editName, setEditName] = useState("");
  const [moveId, setMoveId] = useState<number | null>(null);
  const [moveParentId, setMoveParentId] = useState<string>("root");

  const parentOptions = useMemo(() => {
    const options: Array<{ value: string; label: string }> = [{ value: "root", label: "Top level" }];
    for (const folder of folders) {
      options.push({ value: String(folder.id), label: folder.name });
    }
    return options;
  }, [folders]);

  async function submitCreate() {
    const name = createName.trim();
    if (!name) {
      return;
    }
    await onCreateFolder(createParentId, name);
    setCreateName("");
    setCreateParentId(null);
  }

  async function submitRename(id: number) {
    const name = editName.trim();
    if (!name) {
      return;
    }
    await onRenameFolder(id, name);
    setEditingId(null);
    setEditName("");
  }

  async function submitMove(id: number) {
    const newParentId = moveParentId === "root" ? null : Number(moveParentId);
    await onMoveFolder(id, newParentId);
    setMoveId(null);
    setMoveParentId("root");
  }

  return (
    <div className="folder-tree" aria-label="Collection folders">
      <ul className="folder-tree-list">
        <li>
          <button
            type="button"
            className={`folder-tree-node${scopesEqual(scope, { kind: "all" }) ? " folder-tree-node--selected" : ""}`}
            aria-current={scopesEqual(scope, { kind: "all" }) ? "true" : undefined}
            onClick={() => onSelectScope({ kind: "all" })}
          >
            All cards
          </button>
        </li>
        <li>
          <button
            type="button"
            className={`folder-tree-node${scopesEqual(scope, { kind: "unsorted" }) ? " folder-tree-node--selected" : ""}`}
            aria-current={scopesEqual(scope, { kind: "unsorted" }) ? "true" : undefined}
            onClick={() => onSelectScope({ kind: "unsorted" })}
          >
            <span>Unsorted</span>
            {unsortedCount > 0 ? <span className="folder-tree-badge">{unsortedCount}</span> : null}
          </button>
        </li>
        {tree.map((node) => (
          <FolderTreeBranch
            key={node.id}
            node={node}
            depth={0}
            scope={scope}
            editingId={editingId}
            editName={editName}
            moveId={moveId}
            moveParentId={moveParentId}
            parentOptions={parentOptions}
            folders={folders}
            onSelectScope={onSelectScope}
            onStartRename={(id, name) => {
              setEditingId(id);
              setEditName(name);
              setMoveId(null);
            }}
            onEditNameChange={setEditName}
            onSubmitRename={submitRename}
            onCancelRename={() => {
              setEditingId(null);
              setEditName("");
            }}
            onStartMove={(id) => {
              setMoveId(id);
              setMoveParentId("root");
              setEditingId(null);
            }}
            onMoveParentChange={setMoveParentId}
            onSubmitMove={submitMove}
            onCancelMove={() => setMoveId(null)}
            onDeleteFolder={onDeleteFolder}
          />
        ))}
      </ul>

      <div className="folder-tree-create">
        <input
          aria-label="New folder name"
          placeholder="New folder…"
          value={createName}
          onChange={(event) => setCreateName(event.target.value)}
        />
        <Select
          aria-label="Parent folder"
          value={createParentId === null ? "root" : String(createParentId)}
          onChange={(event) => {
            const value = event.target.value;
            setCreateParentId(value === "root" ? null : Number(value));
          }}
        >
          <option value="root">Top level</option>
          {folders.map((folder) => (
            <option key={folder.id} value={folder.id}>
              {folder.name}
            </option>
          ))}
        </Select>
        <button type="button" className="primary" disabled={!createName.trim()} onClick={() => void submitCreate()}>
          Add folder
        </button>
      </div>
    </div>
  );
}

type FolderTreeBranchProps = {
  node: FolderTreeNode;
  depth: number;
  scope: CollectionScope;
  editingId: number | null;
  editName: string;
  moveId: number | null;
  moveParentId: string;
  parentOptions: Array<{ value: string; label: string }>;
  folders: CollectionFolder[];
  onSelectScope: (scope: CollectionScope) => void;
  onStartRename: (id: number, name: string) => void;
  onEditNameChange: (name: string) => void;
  onSubmitRename: (id: number) => Promise<void>;
  onCancelRename: () => void;
  onStartMove: (id: number) => void;
  onMoveParentChange: (value: string) => void;
  onSubmitMove: (id: number) => Promise<void>;
  onCancelMove: () => void;
  onDeleteFolder: (id: number) => Promise<void>;
};

function FolderTreeBranch({
  node,
  depth,
  scope,
  editingId,
  editName,
  moveId,
  moveParentId,
  parentOptions,
  folders,
  onSelectScope,
  onStartRename,
  onEditNameChange,
  onSubmitRename,
  onCancelRename,
  onStartMove,
  onMoveParentChange,
  onSubmitMove,
  onCancelMove,
  onDeleteFolder
}: FolderTreeBranchProps) {
  const isSelected = scope.kind === "folder" && scope.id === node.id;
  const descendants = useMemo(() => folderDescendantIDs(folders, node.id), [folders, node.id]);
  const moveTargets = parentOptions.filter(
    (option) => option.value === "root" || !descendants.has(Number(option.value))
  );

  return (
    <li>
      {editingId === node.id ? (
        <div className="folder-tree-edit" style={{ paddingLeft: `${depth * 0.75}rem` }}>
          <input
            aria-label={`Rename ${node.name}`}
            value={editName}
            onChange={(event) => onEditNameChange(event.target.value)}
          />
          <button type="button" onClick={() => void onSubmitRename(node.id)} disabled={!editName.trim()}>
            Save
          </button>
          <button type="button" className="ghost" onClick={onCancelRename}>
            Cancel
          </button>
        </div>
      ) : moveId === node.id ? (
        <div className="folder-tree-edit" style={{ paddingLeft: `${depth * 0.75}rem` }}>
          <Select aria-label="Move to parent" value={moveParentId} onChange={(event) => onMoveParentChange(event.target.value)}>
            {moveTargets.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </Select>
          <button type="button" onClick={() => void onSubmitMove(node.id)}>
            Move
          </button>
          <button type="button" className="ghost" onClick={onCancelMove}>
            Cancel
          </button>
        </div>
      ) : (
        <div className="folder-tree-row" style={{ paddingLeft: `${depth * 0.75}rem` }}>
          <button
            type="button"
            className={`folder-tree-node${isSelected ? " folder-tree-node--selected" : ""}`}
            aria-current={isSelected ? "true" : undefined}
            onClick={() => onSelectScope({ kind: "folder", id: node.id })}
          >
            {node.name}
          </button>
          <div className="folder-tree-actions">
            <button type="button" className="ghost folder-tree-action" aria-label={`Rename ${node.name}`} onClick={() => onStartRename(node.id, node.name)}>
              Rename
            </button>
            <button type="button" className="ghost folder-tree-action" aria-label={`Move ${node.name}`} onClick={() => onStartMove(node.id)}>
              Move
            </button>
            <button
              type="button"
              className="ghost folder-tree-action"
              aria-label={`Delete ${node.name}`}
              onClick={() => {
                if (window.confirm(`Delete folder "${node.name}"? Copies inside will return to Unsorted.`)) {
                  void onDeleteFolder(node.id);
                }
              }}
            >
              Delete
            </button>
          </div>
        </div>
      )}
      {node.children.length > 0 ? (
        <ul className="folder-tree-list folder-tree-list--nested">
          {node.children.map((child) => (
            <FolderTreeBranch
              key={child.id}
              node={child}
              depth={depth + 1}
              scope={scope}
              editingId={editingId}
              editName={editName}
              moveId={moveId}
              moveParentId={moveParentId}
              parentOptions={parentOptions}
              folders={folders}
              onSelectScope={onSelectScope}
              onStartRename={onStartRename}
              onEditNameChange={onEditNameChange}
              onSubmitRename={onSubmitRename}
              onCancelRename={onCancelRename}
              onStartMove={onStartMove}
              onMoveParentChange={onMoveParentChange}
              onSubmitMove={onSubmitMove}
              onCancelMove={onCancelMove}
              onDeleteFolder={onDeleteFolder}
            />
          ))}
        </ul>
      ) : null}
    </li>
  );
}

export function scopeLabel(scope: CollectionScope, folders: CollectionFolder[]): string {
  switch (scope.kind) {
    case "all":
      return "All cards";
    case "unsorted":
      return "Unsorted";
    case "folder": {
      const folder = folders.find((item) => item.id === scope.id);
      return folder?.name ?? "Folder";
    }
  }
}

export function scopeFromFolderID(folderID: number): CollectionScope {
  if (folderID === UnsortedFolderID) {
    return { kind: "unsorted" };
  }
  return { kind: "folder", id: folderID };
}
