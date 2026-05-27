import { EventsOn } from "../../wailsjs/runtime/runtime";

/** Subscribe to a Wails runtime event; no-op when running outside the desktop shell (e.g. Vitest). */
export function subscribeWailsEvent(eventName: string, callback: (...data: unknown[]) => void): () => void {
  const runtime = typeof window !== "undefined" ? window.runtime : undefined;
  if (!runtime?.EventsOnMultiple) {
    return () => {};
  }
  return EventsOn(eventName, callback);
}
