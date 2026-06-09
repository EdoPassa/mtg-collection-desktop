import type { CollectionFolder } from "../backend";
import { UnsortedFolderID } from "../backend";

export type CollectionScope =
  | { kind: "all" }
  | { kind: "unsorted" }
  | { kind: "folder"; id: number };

export type FolderTreeNode = CollectionFolder & {
  children: FolderTreeNode[];
};

export function scopeFolderID(scope: CollectionScope): number {
  switch (scope.kind) {
    case "all":
      return -1;
    case "unsorted":
      return UnsortedFolderID;
    case "folder":
      return scope.id;
  }
}

export function scopesEqual(a: CollectionScope, b: CollectionScope): boolean {
  return scopeFolderID(a) === scopeFolderID(b) && a.kind === b.kind;
}

export function buildFolderTree(folders: CollectionFolder[]): FolderTreeNode[] {
  const byParent = new Map<number | null, FolderTreeNode[]>();
  for (const folder of folders) {
    const parentKey = folder.parentId ?? null;
    const node: FolderTreeNode = { ...folder, children: [] };
    const siblings = byParent.get(parentKey) ?? [];
    siblings.push(node);
    byParent.set(parentKey, siblings);
  }
  const attach = (parentKey: number | null): FolderTreeNode[] => {
    const nodes = byParent.get(parentKey) ?? [];
    for (const node of nodes) {
      node.children = attach(node.id);
    }
    return nodes.sort((a, b) => a.name.localeCompare(b.name));
  };
  return attach(null);
}

export type FlatFolderOption = {
  id: number;
  label: string;
};

export function flattenFolderOptions(
  folders: CollectionFolder[],
  excludeID?: number
): FlatFolderOption[] {
  const tree = buildFolderTree(folders);
  const out: FlatFolderOption[] = [{ id: UnsortedFolderID, label: "Unsorted" }];

  function walk(nodes: FolderTreeNode[], depth: number) {
    for (const node of nodes) {
      if (node.id !== excludeID) {
        out.push({ id: node.id, label: `${"  ".repeat(depth)}${node.name}` });
      }
      walk(node.children, depth + 1);
    }
  }
  walk(tree, 0);
  return out;
}

export function folderDescendantIDs(folders: CollectionFolder[], rootID: number): Set<number> {
  const childrenByParent = new Map<number | null, number[]>();
  for (const folder of folders) {
    const key = folder.parentId ?? null;
    const list = childrenByParent.get(key) ?? [];
    list.push(folder.id);
    childrenByParent.set(key, list);
  }
  const out = new Set<number>([rootID]);
  const stack = [rootID];
  while (stack.length > 0) {
    const current = stack.pop()!;
    for (const childID of childrenByParent.get(current) ?? []) {
      if (!out.has(childID)) {
        out.add(childID);
        stack.push(childID);
      }
    }
  }
  return out;
}
