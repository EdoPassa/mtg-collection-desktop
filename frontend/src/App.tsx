import React, { useEffect, useMemo, useState } from "react";
import type { BackendApi, CollectionItem, DeckCompareResult, DeckCompareRow, ImportPreview, LentCard } from "./backend";


const sections = ["Import", "Collection", "Decks / Compare", "Lending"] as const;
type Section = (typeof sections)[number];

export function App({ api }: { api: BackendApi }) {
  const [active, setActive] = useState<Section>("Import");
  const [status, setStatus] = useState("loading");
  const [message, setMessage] = useState("");

  useEffect(() => {
    api.ResolverStatus().then(setStatus).catch((error) => setStatus(`error: ${String(error)}`));
  }, [api]);

  return (
    <main className="app-shell">
      <header className="app-header">
        <div>
          <h1>MTG Collection</h1>
          <p>Local collection tracking backed by Go, SQLite, and Scryfall.</p>
        </div>
        <span className="status-pill">Resolver: {status}</span>
      </header>

      <nav aria-label="Application sections" className="tabs">
        {sections.map((section) => (
          <button
            key={section}
            type="button"
            aria-current={active === section ? "page" : undefined}
            onClick={() => {
              setActive(section);
              setMessage("");
            }}
          >
            {section}
          </button>
        ))}
      </nav>

      {message && <p className="message">{message}</p>}

      {active === "Import" && <ImportPanel api={api} setMessage={setMessage} />}
      {active === "Collection" && <CollectionPanel api={api} setMessage={setMessage} />}
      {active === "Decks / Compare" && <DeckPanel api={api} setMessage={setMessage} />}
      {active === "Lending" && <LendingPanel api={api} setMessage={setMessage} />}
    </main>
  );
}

function ImportPanel({ api, setMessage }: { api: BackendApi; setMessage: (message: string) => void }) {
  const [text, setText] = useState("4 Lightning Bolt\n2 Counterspell");
  const [preview, setPreview] = useState<ImportPreview>({ validated: [], unresolved: [] });
  const [busy, setBusy] = useState(false);

  async function previewImport() {
    setBusy(true);
    try {
      const next = await api.PreviewTextImport(text);
      setPreview(next);
      setMessage(`Validated ${next.validated.length} row(s).`);
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }

  async function commitImport() {
    setBusy(true);
    try {
      await api.CommitImport(preview.validated);
      setMessage(`Imported ${preview.validated.length} row(s).`);
      setPreview({ validated: [], unresolved: [] });
    } catch (error) {
      setMessage(String(error));
    } finally {
      setBusy(false);
    }
  }

  return (
    <section className="panel">
      <h2>Import Cards</h2>
      <label>
        Card list
        <textarea value={text} onChange={(event) => setText(event.target.value)} rows={8} />
      </label>
      <div className="actions">
        <button type="button" onClick={previewImport} disabled={busy}>
          Validate
        </button>
        <button type="button" onClick={commitImport} disabled={busy || preview.validated.length === 0}>
          Commit Import
        </button>
      </div>
      <ResultList title="Validated" rows={preview.validated.map((row) => `${row.line.quantity}x ${row.name} (${row.source})`)} />
      <ResultList title="Unresolved" rows={preview.unresolved} />
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
    <section className="panel">
      <h2>Collection</h2>
      <div className="toolbar">
        <input aria-label="Search collection" placeholder="Search cards" value={query} onChange={(event) => setQuery(event.target.value)} />
        <button type="button" onClick={load}>
          Refresh
        </button>
      </div>
      <div className="card-grid">
        {filtered.map((row) => (
          <article key={row.card.oracleId} className="card-row">
            <h3>{row.card.name}</h3>
            <p>Owned: {row.quantity}</p>
            <p>Lent: {row.lentQty}</p>
            <p>Available: {row.available}</p>
            <p>{row.inDeck ? "In deck" : "Not in deck"}</p>
          </article>
        ))}
      </div>
      {filtered.length === 0 && <p>No cards found.</p>}
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
    <section className="panel">
      <h2>Decks / Compare</h2>
      <label>
        Decklist
        <textarea value={text} onChange={(event) => setText(event.target.value)} rows={8} />
      </label>
      <div className="actions">
        <button type="button" onClick={compare}>
          Compare
        </button>
        <input aria-label="New deck name" placeholder="New deck name" value={deckName} onChange={(event) => setDeckName(event.target.value)} />
        <button type="button" onClick={buildDeck} disabled={!canBuild}>
          Build Deck
        </button>
        <button type="button" onClick={repair} disabled={result.repairs.length === 0}>
          Repair Mismatches
        </button>
      </div>
      <div className="card-grid">
        {result.rows.map((row) => (
          <article key={row.card.oracleId} className="card-row">
            <h3>{row.card.name}</h3>
            <p>Needed: {row.needed}</p>
            <p>Owned: {row.owned}</p>
            <p>Missing: {row.missing}</p>
          </article>
        ))}
      </div>
      <ResultList title="Compare warnings" rows={result.unresolved} />
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
    <section className="panel">
      <h2>Lending</h2>
      <div className="toolbar">
        <input aria-label="Oracle ID" placeholder="Oracle ID" value={oracleId} onChange={(event) => setOracleId(event.target.value)} />
        <input aria-label="Borrower" placeholder="Borrower" value={borrower} onChange={(event) => setBorrower(event.target.value)} />
        <button type="button" onClick={lend} disabled={!oracleId.trim() || !borrower.trim()}>
          Add Lending Record
        </button>
      </div>
      <div className="card-grid">
        {rows.map((row) => (
          <article key={row.id} className="card-row">
            <h3>{row.card.name}</h3>
            <p>Borrower: {row.borrowerName}</p>
            <p>Quantity: {row.quantity}</p>
            <p>Lent: {row.lentDate}</p>
            <button type="button" onClick={() => markReturned(row.id)}>
              Mark Returned
            </button>
          </article>
        ))}
      </div>
      {rows.length === 0 && <p>No active lending records.</p>}
    </section>
  );
}

function ResultList({ title, rows }: { title: string; rows: string[] }) {
  return (
    <div className="result-list">
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
