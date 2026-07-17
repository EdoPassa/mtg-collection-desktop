import { useCallback, useEffect, useRef, useState } from "react";

export type CollectionColumnKey =
  | "select"
  | "thumb"
  | "name"
  | "cost"
  | "type"
  | "quantity"
  | "allocated"
  | "unassigned"
  | "lent"
  | "avail"
  | "status"
  | "tags"
  | "actions";

export const COLLECTION_COLUMN_DEFAULTS: Record<CollectionColumnKey, number> = {
  select: 40,
  thumb: 56,
  name: 220,
  cost: 96,
  type: 180,
  quantity: 72,
  allocated: 88,
  unassigned: 88,
  lent: 72,
  avail: 72,
  status: 96,
  tags: 160,
  actions: 180
};

const COLUMN_MIN: Partial<Record<CollectionColumnKey, number>> = {
  select: 36,
  thumb: 48,
  name: 120,
  cost: 64,
  type: 100,
  quantity: 56,
  allocated: 64,
  unassigned: 64,
  lent: 56,
  avail: 56,
  status: 72,
  tags: 100,
  actions: 120
};

const STORAGE_KEY = "mtg-collection.table-column-widths";

function loadStoredWidths(): Partial<Record<CollectionColumnKey, number>> {
  if (typeof window === "undefined") {
    return {};
  }
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return {};
    }
    return JSON.parse(raw) as Partial<Record<CollectionColumnKey, number>>;
  } catch {
    return {};
  }
}

function mergeWidths(stored: Partial<Record<CollectionColumnKey, number>>): Record<CollectionColumnKey, number> {
  const merged = { ...COLLECTION_COLUMN_DEFAULTS };
  for (const key of Object.keys(COLLECTION_COLUMN_DEFAULTS) as CollectionColumnKey[]) {
    const value = stored[key];
    if (typeof value === "number" && Number.isFinite(value)) {
      merged[key] = clampWidth(key, value);
    }
  }
  return merged;
}

export function clampWidth(key: CollectionColumnKey, width: number): number {
  const min = COLUMN_MIN[key] ?? 48;
  return Math.max(min, Math.round(width));
}

export function useResizableColumns() {
  const [widths, setWidths] = useState<Record<CollectionColumnKey, number>>(() => mergeWidths(loadStoredWidths()));
  const resizeRef = useRef<{
    key: CollectionColumnKey;
    startX: number;
    startWidth: number;
  } | null>(null);

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(widths));
    } catch {
      // Ignore quota / private mode errors.
    }
  }, [widths]);

  useEffect(() => {
    function onMouseMove(event: MouseEvent) {
      const active = resizeRef.current;
      if (!active) {
        return;
      }
      const next = clampWidth(active.key, active.startWidth + (event.clientX - active.startX));
      setWidths((current) => ({ ...current, [active.key]: next }));
    }

    function onMouseUp() {
      if (!resizeRef.current) {
        return;
      }
      resizeRef.current = null;
      document.body.classList.remove("col-resize-active");
    }

    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
      document.body.classList.remove("col-resize-active");
    };
  }, []);

  const startResize = useCallback((key: CollectionColumnKey, clientX: number) => {
    resizeRef.current = { key, startX: clientX, startWidth: widths[key] };
    document.body.classList.add("col-resize-active");
  }, [widths]);

  return { widths, startResize };
}
