import React, { useEffect, useState } from "react";
import type { ImportPreview } from "../backend";
import { subscribeWailsEvent } from "../lib/wailsEvents";
import { ProgressBar } from "../components/ProgressBar";
import { ResultList } from "../components/ResultList";
import { Select } from "../components/Select";
import type { PanelProps } from "./types";

type ImportProgressState = {
  current: number;
  total: number;
  label: string;
};

type ImportMode = "text" | "csv";

function normalizeImportPreview(preview: ImportPreview): ImportPreview {
  return {
    validated: preview.validated ?? [],
    unresolved: preview.unresolved ?? []
  };
}

export function ImportPanel({ api, setMessage }: PanelProps) {
  const [mode, setMode] = useState<ImportMode>("text");
  const [text, setText] = useState("4 Lightning Bolt\n2 Counterspell");
  const [csvFileName, setCsvFileName] = useState("");
  const [csvBytes, setCsvBytes] = useState<number[] | null>(null);
  const [preview, setPreview] = useState<ImportPreview>({ validated: [], unresolved: [] });
  const [busy, setBusy] = useState(false);
  const [importProgress, setImportProgress] = useState<ImportProgressState | null>(null);
  const csvInputId = "import-csv-file";

  useEffect(() => {
    const unsubscribe = subscribeWailsEvent("import:progress", (payload: ImportProgressState) => {
      setImportProgress({
        current: payload?.current ?? 0,
        total: payload?.total ?? 0,
        label: payload?.label ?? ""
      });
    });
    return unsubscribe;
  }, []);

  const validated = preview.validated ?? [];
  const unresolved = preview.unresolved ?? [];

  async function onCsvSelected(file: File | undefined) {
    if (!file) {
      return;
    }
    setCsvFileName(file.name);
    setCsvBytes(Array.from(new Uint8Array(await file.arrayBuffer())));
    setMode("csv");
  }

  async function previewImport() {
    if (mode === "csv" && csvBytes === null) {
      setMessage("Choose a CSV file before validating.");
      return;
    }

    setImportProgress({ current: 0, total: 0, label: "" });
    setBusy(true);
    try {
      const next = mode === "csv" ? await api.PreviewCSVImport(csvBytes ?? []) : await api.PreviewTextImport(text);
      const normalized = normalizeImportPreview(next);
      setPreview(normalized);
      setMessage(`Validated ${normalized.validated.length} row(s).`);
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
      setImportProgress(null);
    }
  }

  async function commitImport() {
    setBusy(true);
    try {
      await api.CommitImport(validated);
      setMessage(`Imported ${validated.length} row(s).`);
      setPreview({ validated: [], unresolved: [] });
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="panel" aria-label="Import Cards" aria-busy={busy}>
      <div className="toolbar">
        <div className="field field--inline">
          <label className="field-label" htmlFor="import-source-mode">
            Source
          </label>
          <Select
            id="import-source-mode"
            aria-label="Import source mode"
            value={mode}
            onChange={(event) => setMode(event.target.value as ImportMode)}
          >
            <option value="text">Plain text</option>
            <option value="csv">CSV file</option>
          </Select>
        </div>
        {mode === "csv" && (
          <>
            <input id={csvInputId} type="file" accept=".csv,text/csv" hidden onChange={(event) => void onCsvSelected(event.target.files?.[0])} />
            <button type="button" className="ghost" onClick={() => document.getElementById(csvInputId)?.click()}>
              Choose CSV…
            </button>
            {csvFileName && <span className="card-meta">{csvFileName}</span>}
          </>
        )}
      </div>
      {mode === "text" ? (
        <label>
          Card list
          <textarea value={text} onChange={(event) => setText(event.target.value)} rows={8} placeholder="4 Lightning Bolt&#10;2x Counterspell" />
        </label>
      ) : (
        <p className="card-meta">
          CSV must include card name (<code>name</code> or <code>card name</code>) and quantity (<code>quantity</code> or <code>qty</code>
          ). Optional: <code>scryfall id</code>.
        </p>
      )}
      <div className="actions">
        <button type="button" className="primary" onClick={previewImport} disabled={busy}>
          Validate
        </button>
        <button type="button" onClick={commitImport} disabled={busy || validated.length === 0}>
          Commit Import
        </button>
      </div>
      <ProgressBar
        current={importProgress?.current ?? 0}
        total={importProgress?.total ?? 0}
        label={importProgress?.label}
        visible={busy && importProgress !== null && (importProgress.total ?? 0) > 0}
      />
      <ResultList title="Validated" rows={validated.map((row) => `${row.line?.quantity ?? 0}x ${row.name} (${row.source})`)} />
      <ResultList title="Unresolved" rows={unresolved} warn={unresolved.length > 0} />
    </section>
  );
}
