import React, { useEffect, useMemo, useState } from "react";
import type { Deck, DeckCard } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { Stat } from "../components/Stat";
import { boardLabel, isMainboard, normalizeBoard, partitionDeckCards, totalQuantity } from "../lib/deckBoard";
import type { PanelProps } from "./types";

export function DecksPanel({ api, setMessage }: PanelProps) {
  const [decks, setDecks] = useState<Deck[]>([]);
  const [deckQuery, setDeckQuery] = useState("");
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [cards, setCards] = useState<DeckCard[]>([]);
  const [cardQuery, setCardQuery] = useState("");
  const [renameValue, setRenameValue] = useState("");
  const [addName, setAddName] = useState("");
  const [addQty, setAddQty] = useState(1);

  const selectedDeck = decks.find((deck) => deck.id === selectedId) ?? null;

  async function loadDecks() {
    try {
      const next = (await api.ListDecks()) ?? [];
      setDecks(next);
      if (selectedId !== null && !next.some((deck) => deck.id === selectedId)) {
        setSelectedId(null);
        setCards([]);
        setRenameValue("");
      }
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function loadCards(deckID: number) {
    try {
      setCards((await api.ListDeckCards(deckID)) ?? []);
    } catch (error) {
      setMessage(String(error));
    }
  }

  useEffect(() => {
    void loadDecks();
  }, []);

  useEffect(() => {
    if (selectedId === null) {
      setCards([]);
      return;
    }
    void loadCards(selectedId);
  }, [selectedId]);

  useEffect(() => {
    if (selectedDeck) {
      setRenameValue(selectedDeck.name);
    }
  }, [selectedDeck?.id, selectedDeck?.name]);

  const filteredDecks = useMemo(
    () => decks.filter((deck) => deck.name.toLocaleLowerCase().includes(deckQuery.toLocaleLowerCase())),
    [decks, deckQuery]
  );

  const filteredCards = useMemo(
    () => cards.filter((row) => row.card.name.toLocaleLowerCase().includes(cardQuery.toLocaleLowerCase())),
    [cards, cardQuery]
  );

  const { mainboard, sideboard } = useMemo(() => partitionDeckCards(filteredCards), [filteredCards]);
  const mainTotal = useMemo(() => totalQuantity(cards.filter((row) => isMainboard(row.board))), [cards]);
  const sideTotal = useMemo(() => totalQuantity(cards.filter((row) => !isMainboard(row.board))), [cards]);

  async function selectDeck(deck: Deck) {
    setSelectedId(deck.id);
    setCardQuery("");
  }

  async function saveRename() {
    if (selectedId === null || renameValue.trim() === "") {
      return;
    }
    try {
      await api.RenameDeck(selectedId, renameValue);
      setMessage(`Renamed deck to ${renameValue.trim()}.`);
      await loadDecks();
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function deleteDeck() {
    if (selectedId === null || selectedDeck === null) {
      return;
    }
    if (!window.confirm(`Delete deck "${selectedDeck.name}"?`)) {
      return;
    }
    try {
      await api.DeleteDeck(selectedId);
      setMessage(`Deleted deck ${selectedDeck.name}.`);
      setSelectedId(null);
      setCards([]);
      await loadDecks();
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function setQuantity(oracleID: string, board: string, qty: number) {
    if (selectedId === null) {
      return;
    }
    try {
      await api.SetDeckCardQuantity(selectedId, oracleID, normalizeBoard(board), qty);
      await loadCards(selectedId);
      setMessage(qty <= 0 ? "Removed card from deck." : "Updated card quantity.");
    } catch (error) {
      setMessage(String(error));
    }
  }

  async function addCard() {
    if (selectedId === null || addName.trim() === "" || addQty <= 0) {
      return;
    }
    try {
      await api.AddCardToDeckByName(selectedId, addName.trim(), addQty);
      setMessage(`Added ${addQty}x ${addName.trim()} to deck.`);
      setAddName("");
      setAddQty(1);
      await loadCards(selectedId);
    } catch (error) {
      setMessage(String(error));
    }
  }

  return (
    <section className="panel" aria-label="Decks">
      <div className="deck-browser">
        <div className="deck-browser-list">
          <div className="toolbar">
            <input
              aria-label="Search decks"
              placeholder="Search decks…"
              value={deckQuery}
              onChange={(event) => setDeckQuery(event.target.value)}
            />
            <button type="button" className="ghost" onClick={() => void loadDecks()}>
              Refresh
            </button>
          </div>
          {filteredDecks.length === 0 ? (
            <EmptyState
              title="No decks found."
              detail={decks.length === 0 ? "Build a deck from Deck Compare to see it here." : "Try a different search term."}
            />
          ) : (
            <ul className="deck-list">
              {filteredDecks.map((deck) => (
                <li key={deck.id}>
                  <button
                    type="button"
                    className={`deck-list-item${selectedId === deck.id ? " deck-list-item--active" : ""}`}
                    aria-current={selectedId === deck.id ? "true" : undefined}
                    onClick={() => void selectDeck(deck)}
                  >
                    <span className="deck-list-name">{deck.name}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="deck-browser-detail">
          {selectedDeck === null ? (
            <EmptyState title="Select a deck" detail="Choose a deck from the list to view and edit its cards." />
          ) : (
            <>
              <div className="toolbar deck-detail-toolbar">
                <input aria-label="Deck name" value={renameValue} onChange={(event) => setRenameValue(event.target.value)} />
                <button type="button" onClick={() => void saveRename()} disabled={renameValue.trim() === "" || renameValue.trim() === selectedDeck.name}>
                  Save name
                </button>
                <button type="button" className="ghost" onClick={() => void deleteDeck()}>
                  Delete deck
                </button>
                <span className="card-meta">
                  {mainTotal} main{sideTotal > 0 ? ` · ${sideTotal} side` : ""}
                </span>
              </div>
              <div className="toolbar">
                <input
                  aria-label="Search deck cards"
                  placeholder="Search cards…"
                  value={cardQuery}
                  onChange={(event) => setCardQuery(event.target.value)}
                />
                <input
                  aria-label="Card name to add"
                  placeholder="Add card name…"
                  value={addName}
                  onChange={(event) => setAddName(event.target.value)}
                />
                <input
                  aria-label="Quantity to add"
                  type="number"
                  min={1}
                  value={addQty}
                  onChange={(event) => setAddQty(Math.max(1, Number(event.target.value) || 1))}
                />
                <button type="button" className="primary" onClick={() => void addCard()} disabled={!addName.trim()}>
                  Add card
                </button>
              </div>
              {filteredCards.length === 0 ? (
                <EmptyState
                  title="No cards in this deck."
                  detail={cards.length === 0 ? "Add cards by name or build this deck from Deck Compare." : "Try a different search term."}
                />
              ) : (
                <>
                  {mainboard.length > 0 && (
                    <DeckBoardGrid
                      title={boardLabel("main")}
                      rows={mainboard}
                      onSetQuantity={(oracleID, board, qty) => void setQuantity(oracleID, board, qty)}
                    />
                  )}
                  {sideboard.length > 0 && (
                    <DeckBoardGrid
                      title={boardLabel("side")}
                      rows={sideboard}
                      onSetQuantity={(oracleID, board, qty) => void setQuantity(oracleID, board, qty)}
                    />
                  )}
                </>
              )}
            </>
          )}
        </div>
      </div>
    </section>
  );
}

function DeckBoardGrid({
  title,
  rows,
  onSetQuantity
}: {
  title: string;
  rows: DeckCard[];
  onSetQuantity: (oracleID: string, board: string, qty: number) => void;
}) {
  return (
    <section className="deck-board-section" aria-label={title}>
      <h3 className="deck-board-heading">{title}</h3>
      <div className="card-grid">
        {rows.map((row) => (
          <article key={`${normalizeBoard(row.board)}-${row.card.oracleId}`} className="card-row">
            <h3>{row.card.name}</h3>
            <div className="stat-row">
              <Stat label="Qty" value={row.quantity} />
            </div>
            <div className="deck-card-actions">
              <button type="button" className="ghost" onClick={() => onSetQuantity(row.card.oracleId, row.board ?? "main", row.quantity - 1)}>
                −
              </button>
              <input
                aria-label={`Quantity for ${row.card.name}`}
                type="number"
                min={0}
                defaultValue={row.quantity}
                key={`${normalizeBoard(row.board)}-${row.card.oracleId}-${row.quantity}`}
                onBlur={(event) => {
                  const qty = Number(event.target.value);
                  if (!Number.isNaN(qty) && qty !== row.quantity) {
                    onSetQuantity(row.card.oracleId, row.board ?? "main", qty);
                  }
                }}
              />
              <button type="button" className="ghost" onClick={() => onSetQuantity(row.card.oracleId, row.board ?? "main", row.quantity + 1)}>
                +
              </button>
              <button type="button" className="ghost" onClick={() => onSetQuantity(row.card.oracleId, row.board ?? "main", 0)}>
                Remove
              </button>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
