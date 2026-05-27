import { EventsOn } from "../../wailsjs/runtime/runtime";

type WailsRuntime = {
  EventsOnMultiple?: (eventName: string, callback: (...data: unknown[]) => void, maxCallbacks: number) => () => void;
};

function wailsRuntime(): WailsRuntime | undefined {
  if (typeof window === "undefined") {
    return undefined;
  }
  return (window as Window & { runtime?: WailsRuntime }).runtime;
}

/** Subscribe to a Wails runtime event; no-op when running outside the desktop shell (e.g. Vitest). */
export function subscribeWailsEvent<T>(eventName: string, callback: (payload: T) => void): () => void {
  const runtime = wailsRuntime();
  if (!runtime?.EventsOnMultiple) {
    return () => {};
  }
  return EventsOn(eventName, (payload) => callback(payload as T));
}
