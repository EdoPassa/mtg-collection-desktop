import type { CollectionTag } from "../backend";

export function matchesTagFilter(tags: CollectionTag[], active: ReadonlySet<number>): boolean {
  if (active.size === 0) {
    return true;
  }
  const ids = new Set(tags.map((tag) => tag.id));
  for (const id of active) {
    if (!ids.has(id)) {
      return false;
    }
  }
  return true;
}

export const TAG_COLOR_PRESETS = [
  "#3b82f6",
  "#22c55e",
  "#ef4444",
  "#a855f7",
  "#f97316",
  "#ec4899",
  "#14b8a6",
  "#eab308"
] as const;
