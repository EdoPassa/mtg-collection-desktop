import React, { useEffect, useMemo, useState } from "react";
import type { BackendApi, CollectionItem, DeckCompareResult, ImportPreview, LentCard } from "./backend";

const sections = [
  { id: "Import", icon: "↓", description: "Validate and add cards" },
  { id: "Collection", icon: "◆", description: "Browse owned cards" },
  { id: "Decks / Compare", icon: "⚔", description: "Compare decklists" },
  { id: "Lending", icon: "↔", description: "Track lent cards" }
] as const;

type Section = (typeof sections)[number]["id"];

const sectionTitles: Record<Section, string> = {
  Import: "Import Cards",
  Collection: "Collection",
  "Decks / Compare": "Decks / Compare",
  Lending: "Lending"
};

export function App({ api }: { api: BackendApi }) {
  const [active, setActive] = useState<Section>("Import");
  const [status, setStatus] = useState("loading");
  const [message, setMessage] = useState("");

  useEffect(() => {
    api.ResolverStatus().then(setStatus).catch((error) => setStatus(`error: ${String(error)}`));
  }, [api]);

  const messageIsError = message.toLowerCase().includes("error") || message.startsWith("TypeError");

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            MTG
          </div>
          <p className="brand-title">MTG Collection</p>
          <p>Local tracker · Scryfall</p>
        </div>

        <nav aria-label="Application sections" className="tabs">
          {sections.map((section) => (
            <button
              key={section.id}
              type="button"
              className="tab-btn"
              aria-current={active === section.id ? "page" : undefined}
              onClick={() => {
                setActive(section.id);
                setMessage("");
              }}
            >
              <span className="tab-icon" aria-hidden="true">
                {section.icon}
              </span>
              {section.id}
            </button>
          ))}
        </nav>

        <div className="sidebar-footer">
          <StatusPill status={status} />
        </div>
      </aside>

      <div className="app-main">
        <header className="app-header">
          <div>
            <h2>{sectionTitles[active]}</h2>
            <p className="subtitle">{sections.find((s) => s.id === active)?.description}</p>
          </div>
        </header>

        {message && <p className={`message${messageIsError ? " message--error" : ""}`}>{message}</p>}

        {active === "Import" && <ImportPanel api={api} setMessage={setMessage} />}
        {active === "Collection" && <CollectionPanel api={api} setMessage={setMessage} />}
        {active === "Decks / Compare" && <DeckPanel api={api} setMessage={setMessage} />}
        {active === "Lending" && <LendingPanel api={api} setMessage={setMessage} />}
      </div>
    </div>
  );
}

function StatusPill({ status }: { status: string }) {
  const variant = status.startsWith("error")
    ? "error"
    : status === "loading"
      ? "loading"
      : "ready";

  return (
    <span className={`status-pill status-pill--${variant}`} title={status}>
      Resolver: {status}
    </span>
  );
}

function Stat({ label, value, tone }: { label: string; value: number | string; tone?: "success" | "warning" | "danger" }) {
  return (
    <div className={`stat${tone ? ` stat--${tone}` : ""}`}>
      <span className="stat-label">{label}</span>
      <span className="stat-value">{value}</span>
    </div>
  );
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <p>{detail}</p>
    </div>
  );
}

type ImportMode = "text" | "csv";

function normalizeImportPreview(preview: ImportPreview): ImportPreview {
  return {
    validated: preview.validated ?? [],
    unresolved: preview.unresolved ?? []
  };
}

function ImportPanel({ api, setMessage }: { api: BackendApi; setMessage: (message: string) => void }) {
  const [mode, setMode] = useState<ImportMode>("text");
  const [text, setText] = useState("4 Lightning Bolt\n2 Counterspell");
  const [csvFileName, setCsvFileName] = useState("");
  const [csvBytes, setCsvBytes] = useState<number[] | null>(null);
  const [preview, setPreview] = useState<ImportPreview>({ validated: [], unresolved: [] });
  const [busy, setBusy] = useState(false);
  const csvInputId = "import-csv-file";

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

    setBusy(true);
    try {
      const next =
        mode === "csv" ? await api.PreviewCSVImport(csvBytes ?? []) : await api.PreviewTextImport(text);
      const normalized = normalizeImportPreview(next);
      setPreview(normalized);
      setMessage(`Validated ${normalized.validated.length} row(s).`);
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
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
    <section className="panel" aria-label="Import Cards">
      <div className="toolbar">
        <label>
          Source
          <select
            aria-label="Import source mode"
            value={mode}
            onChange={(event) => setMode(event.target.value as ImportMode)}
          >
            <option value="text">Plain text</option>
            <option value="csv">CSV file</option>
          </select>
        </label>
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
          CSV must include card name (<code>name</code> or <code>card name</code>) and quantity (<code>quantity</code> or{" "}
          <code>qty</code>). Optional: <code>scryfall id</code>.
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
      <ResultList
        title="Validated"
        rows={validated.map((row) => `${row.line?.quantity ?? 0}x ${row.name} (${row.source})`)}
      />
      <ResultList title="Unresolved" rows={unresolved} warn={unresolved.length > 0} />
    </section>
  );
}

function CollectionPanel({ api, setMessage }: { api: BackendApi; setMessage: (message: string) => void }) {
  const [rows, setRows] = useState<CollectionItem[]>([]);
  const [query, setQuery] = useState("");

  async function load() {
    try {
      setRows((await api.ListCollection()) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

  useEffect(() => {
    void load();
  }, []);

  const filtered = useMemo(
    () => rows.filter((row) => row.card.name.toLocaleLowerCase().includes(query.toLocaleLowerCase())),
    [rows, query]
  );

  return (
    <section className="panel" aria-label="Collection">
      <div className="toolbar">
        <input aria-label="Search collection" placeholder="Search cards…" value={query} onChange={(event) => setQuery(event.target.value)} />
        <button type="button" className="ghost" onClick={load}>
          Refresh
        </button>
      </div>
      {filtered.length === 0 ? (
        <EmptyState title="No cards found." detail={rows.length === 0 ? "Import a list to start building your collection." : "Try a different search term."} />
      ) : (
        <div className="card-grid">
          {filtered.map((row) => (
            <article key={row.card.oracleId} className="card-row">
              <h3>{row.card.name}</h3>
              <div className="stat-row">
                <Stat label="Owned" value={row.quantity} />
                <Stat label="Lent" value={row.lentQty} tone={row.lentQty > 0 ? "warning" : undefined} />
                <Stat label="Available" value={row.available} tone="success" />
              </div>
              <p className="sr-only">Available: {row.available}</p>
              <span className={`badge ${row.inDeck ? "badge--deck" : "badge--free"}`}>{row.inDeck ? "In deck" : "Not in deck"}</span>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function DeckPanel({ api, setMessage }: { api: BackendApi; setMessage: (message: string) => void }) {
  const [text, setText] = useState("4 Lightning Bolt\n2 Counterspell");
  const [deckName, setDeckName] = useState("");
  const [result, setResult] = useState<DeckCompareResult>({ rows: [], unresolved: [], repairs: [], hasUnresolved: false });

  async function compare() {
    try {
      const next = await api.CompareDeck(text);
      setResult(next);
      setMessage(`Compared ${next.rows.length} card(s).`);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function buildDeck() {
    try {
      await api.BuildDeckFromCompare({ Name: deckName, ReplaceDeckID: 0, Rows: result.rows });
      setMessage(`Built deck ${deckName}.`);
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function repair() {
    try {
      await api.RepairCompareMismatches(result.repairs);
      const next = await api.CompareDeck(text);
      setResult(next);
      setMessage(`Compared ${next.rows.length} card(s).`);
    } catch (error) {
      setMessage(String(error));
    }
  }

  const canBuild = result.rows.length > 0 && result.rows.every((row) => row.missing === 0) && !result.hasUnresolved && deckName.trim() !== "";
  const repairRows = result.repairs.map((repair) => `Repair ${repair.fromOracleId} to ${repair.toCard.name}`);

  return (
    <section className="panel" aria-label="Decks / Compare">
      <label>
        Decklist
        <textarea value={text} onChange={(event) => setText(event.target.value)} rows={8} placeholder="One card per line…" />
      </label>
      <div className="actions compare-toolbar">
        <button type="button" className="primary" onClick={compare}>
          Compare
        </button>
        <input aria-label="New deck name" placeholder="New deck name" value={deckName} onChange={(event) => setDeckName(event.target.value)} />
        <button type="button" onClick={buildDeck} disabled={!canBuild}>
          Build Deck
        </button>
        <button type="button" className="ghost" onClick={repair} disabled={result.repairs.length === 0}>
          Repair Mismatches
        </button>
      </div>
      {result.rows.length === 0 ? (
        <EmptyState title="No comparison yet" detail="Paste a decklist and run Compare to see owned vs missing." />
      ) : (
        <div className="card-grid">
          {result.rows.map((row) => (
            <article key={row.card.oracleId} className="card-row">
              <h3>{row.card.name}</h3>
              <div className="stat-row">
                <Stat label="Needed" value={row.needed} />
                <Stat label="Owned" value={row.owned} />
                <Stat label="Missing" value={row.missing} tone={row.missing > 0 ? "danger" : "success"} />
              </div>
              <span className={`badge ${row.missing === 0 ? "badge--complete" : "badge--missing"}`}>
                {row.missing === 0 ? "Complete" : `${row.missing} short`}
              </span>
            </article>
          ))}
        </div>
      )}
      <ResultList title="Compare warnings" rows={result.unresolved} warn={result.unresolved.length > 0} />
      {repairRows.length > 0 && <ResultList title="Repair candidates" rows={repairRows} />}
    </section>
  );
}

function LendingPanel({ api, setMessage }: { api: BackendApi; setMessage: (message: string) => void }) {
  const [oracleId, setOracleId] = useState("");
  const [borrower, setBorrower] = useState("");
  const [rows, setRows] = useState<LentCard[]>([]);

  async function load() {
    try {
      setRows((await api.ListLentCards(false)) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

  useEffect(() => {
    void load();
  }, []);

  async function lend() {
    try {
      await api.LendCard({
        OracleID: oracleId,
        Quantity: 1,
        BorrowerName: borrower,
        LentDate: new Date().toISOString().slice(0, 10),
        Notes: ""
      });
      setMessage("Lending record added.");
      setOracleId("");
      setBorrower("");
      await load();
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function markReturned(id: number) {
    try {
      await api.ReturnCard(id, new Date().toISOString().slice(0, 10));
      setMessage("Card marked returned.");
      await load();
    } catch (error) {
      setMessage(String(error));
    }
  }

  return (
    <section className="panel" aria-label="Lending">
      <div className="toolbar">
        <input aria-label="Oracle ID" placeholder="Oracle ID" value={oracleId} onChange={(event) => setOracleId(event.target.value)} />
        <input aria-label="Borrower" placeholder="Borrower name" value={borrower} onChange={(event) => setBorrower(event.target.value)} />
        <button type="button" className="primary" onClick={lend} disabled={!oracleId.trim() || !borrower.trim()}>
          Add Lending Record
        </button>
      </div>
      {rows.length === 0 ? (
        <EmptyState title="No active lending records." detail="Lent cards you track will appear here until marked returned." />
      ) : (
        <div className="card-grid">
          {rows.map((row) => (
            <article key={row.id} className="card-row">
              <h3>{row.card.name}</h3>
              <div className="stat-row">
                <Stat label="Qty" value={row.quantity} />
                <Stat label="Lent" value={row.lentDate} />
              </div>
              <p className="card-meta">Borrower: {row.borrowerName}</p>
              <button type="button" className="ghost" onClick={() => markReturned(row.id)}>
                Mark Returned
              </button>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function ResultList({ title, rows, warn }: { title: string; rows: string[]; warn?: boolean }) {
  return (
    <div className={`result-list${warn ? " result-list--warn" : ""}`}>
      <h3>{title}</h3>
      {rows.length === 0 ? (
        <p>None.</p>
      ) : (
        <ul>
          {rows.map((row, index) => (
            <li key={`${row}-${index}`}>{row}</li>
          ))}
        </ul>
      )}
    </div>
  );
}
