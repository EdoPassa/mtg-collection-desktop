import React, { useCallback, useEffect, useRef, useState } from "react";
import type { BackendApi, CollectionTag, DeckCard, SimulationState } from "../backend";
import { CardImage } from "../components/CardImage";
import { EmptyState } from "../components/EmptyState";
import { Select } from "../components/Select";
import { Stat } from "../components/Stat";
import { TagBadge } from "../components/TagBadge";

type Props = {
  api: BackendApi;
  setMessage: (message: string) => void;
  deckId: number;
  formatTarget: string;
  mainboardCards: DeckCard[];
  selectedOracleId: string;
  onOracleChange: (oracleId: string) => void;
  selectedTagId: number;
  onTagChange: (tagId: number) => void;
};

function tagFromStat(stat: { tagId: number; name: string; color?: string }): CollectionTag {
  return { id: stat.tagId, name: stat.name, color: stat.color };
}

export function DeckAnalysisSimulator({
  api,
  setMessage,
  deckId,
  formatTarget,
  mainboardCards,
  selectedOracleId,
  onOracleChange,
  selectedTagId,
  onTagChange
}: Props) {
  const [sim, setSim] = useState<SimulationState | null>(null);
  const [loading, setLoading] = useState(false);
  const sessionRef = useRef<string | undefined>(undefined);

  const endSession = useCallback(
    async (sessionId: string | undefined) => {
      if (!sessionId) {
        return;
      }
      try {
        await api.EndDeckSimulation(sessionId);
      } catch {
        // ignore cleanup errors
      }
    },
    [api]
  );

  const startSimulation = useCallback(async () => {
    await endSession(sessionRef.current);
    sessionRef.current = undefined;
    setLoading(true);
    try {
      const next = await api.StartDeckSimulation(deckId, formatTarget, selectedOracleId, selectedTagId, 2);
      sessionRef.current = next.sessionId;
      setSim(next);
    } catch (error) {
      setMessage(String(error));
      setSim(null);
    } finally {
      setLoading(false);
    }
  }, [api, deckId, endSession, formatTarget, selectedOracleId, selectedTagId, setMessage]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      await endSession(sessionRef.current);
      sessionRef.current = undefined;
      if (cancelled) {
        return;
      }
      setLoading(true);
      try {
        const next = await api.StartDeckSimulation(deckId, formatTarget, selectedOracleId, selectedTagId, 2);
        if (cancelled) {
          await api.EndDeckSimulation(next.sessionId);
          return;
        }
        sessionRef.current = next.sessionId;
        setSim(next);
      } catch (error) {
        if (!cancelled) {
          setMessage(String(error));
          setSim(null);
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
      void endSession(sessionRef.current);
      sessionRef.current = undefined;
    };
    // Oracle/tag focus changes are handled separately; restarting here races with SimSet*Focus.
  }, [api, deckId, endSession, formatTarget, setMessage]);

  useEffect(() => {
    const sessionId = sim?.sessionId;
    if (!sessionId || selectedOracleId === sim.stats.oracleIdUsed) {
      return;
    }
    if (sessionRef.current !== sessionId) {
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const next = await api.SimSetOracleFocus(sessionId, selectedOracleId);
        if (cancelled || sessionRef.current !== sessionId) {
          return;
        }
        setSim(next);
      } catch (error) {
        if (cancelled || sessionRef.current !== sessionId) {
          return;
        }
        const message = String(error);
        if (message.includes("simulation session not found")) {
          return;
        }
        setMessage(message);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [api, selectedOracleId, setMessage, sim?.sessionId, sim?.stats.oracleIdUsed]);

  useEffect(() => {
    const sessionId = sim?.sessionId;
    if (!sessionId || selectedTagId === (sim.stats.tagIdUsed ?? 0)) {
      return;
    }
    if (sessionRef.current !== sessionId) {
      return;
    }
    let cancelled = false;
    void (async () => {
      try {
        const next = await api.SimSetTagFocus(sessionId, selectedTagId);
        if (cancelled || sessionRef.current !== sessionId) {
          return;
        }
        setSim(next);
      } catch (error) {
        if (cancelled || sessionRef.current !== sessionId) {
          return;
        }
        const message = String(error);
        if (message.includes("simulation session not found")) {
          return;
        }
        setMessage(message);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [api, selectedTagId, setMessage, sim?.sessionId, sim?.stats.tagIdUsed]);

  async function runAction(action: () => Promise<SimulationState>) {
    setLoading(true);
    try {
      const next = await action();
      setSim(next);
    } catch (error) {
      setMessage(String(error));
    } finally {
      setLoading(false);
    }
  }

  const awaitingBottom = sim?.phase === "awaiting_bottom";
  const tagStats = sim?.stats.tags ?? [];
  const focusedTag = tagStats.find((stat) => stat.tagId === selectedTagId);

  return (
    <div className="analysis-simulator">
      <p className="analysis-hint">
        Shuffle and draw a real opening from your mainboard. Mulligan shuffles back, draws seven, then you put one card on
        the bottom. Draw odds below update based on what is in your hand and library.
      </p>

      <div className="sim-actions toolbar">
        <button type="button" className="primary" disabled={loading} onClick={() => void startSimulation()}>
          New opening
        </button>
        <button
          type="button"
          className="ghost"
          disabled={loading || !sim?.canMulligan || awaitingBottom}
          onClick={() => sim && void runAction(() => api.SimMulligan(sim.sessionId))}
        >
          Mulligan
        </button>
        <button
          type="button"
          className="ghost"
          disabled={loading || !sim?.canDraw || awaitingBottom}
          onClick={() => sim && void runAction(() => api.SimDrawCard(sim.sessionId))}
        >
          Draw card
        </button>
      </div>

      {awaitingBottom && (
        <p className="analysis-warning" role="status">
          Click a card in your hand to put it on the bottom of your library.
        </p>
      )}

      {!sim && !loading && (
        <EmptyState title="No simulation" detail="Choose New opening to shuffle and draw seven cards." />
      )}

      {sim && (
        <>
          <div className="stat-row">
            <Stat label="In hand" value={sim.hand.length} />
            <Stat label="In library" value={sim.libraryCount} />
            <Stat label="Mulligans" value={sim.mulliganCount} />
          </div>

          <div className="sim-hand" aria-label="Opening hand">
            {sim.hand.map((card) => (
              <button
                key={card.slotId}
                type="button"
                className={`sim-hand-card${awaitingBottom ? " sim-hand-card--bottom-pick" : ""}${card.isLand ? " sim-hand-card--land" : ""}`}
                disabled={!awaitingBottom || loading}
                aria-label={
                  awaitingBottom ? `Put ${card.name} on bottom` : card.name
                }
                onClick={() => {
                  if (!awaitingBottom || !sim) {
                    return;
                  }
                  void runAction(() => api.SimPutOnBottom(sim.sessionId, card.slotId));
                }}
              >
                <CardImage
                  name={card.name}
                  small={card.imageSmall}
                  normal={card.imageNormal}
                  colorIdentity={card.colorIdentity}
                  size="thumb"
                />
                <span className="sim-hand-card-name">{card.name}</span>
              </button>
            ))}
          </div>

          {mainboardCards.length > 0 && (
            <label className="analysis-field">
              Next-draw stats for card
              <Select
                aria-label="Card for next-draw probability"
                value={selectedOracleId}
                onChange={(event) => onOracleChange(event.target.value)}
              >
                {mainboardCards.map((row) => (
                  <option key={row.card.oracleId} value={row.card.oracleId}>
                    {row.quantity}x {row.card.name}
                  </option>
                ))}
              </Select>
            </label>
          )}

          {tagStats.length > 0 && (
            <label className="analysis-field">
              Next-draw stats for tag
              <Select
                aria-label="Tag for next-draw probability"
                value={selectedTagId}
                onChange={(event) => onTagChange(Number(event.target.value) || 0)}
              >
                <option value={0}>None</option>
                {tagStats.map((stat) => (
                  <option key={stat.tagId} value={stat.tagId}>
                    {stat.name} ({stat.copiesInLibrary ?? 0} in library)
                  </option>
                ))}
              </Select>
            </label>
          )}

          <article className="analysis-card" aria-live="polite">
            <h3>Next draw</h3>
            <p className="analysis-result">
              P(next card is a land): <strong>{sim.stats.nextDrawLandProbFormatted}</strong>
            </p>
            {selectedOracleId && (
              <p className="analysis-result">
                P(next card is selected card): <strong>{sim.stats.nextDrawCardProbFormatted}</strong>
              </p>
            )}
            {selectedTagId > 0 && focusedTag && (
              <p className="analysis-result">
                P(next card has tag &ldquo;{focusedTag.name}&rdquo;):{" "}
                <strong>{sim.stats.nextDrawTagProbFormatted}</strong>
              </p>
            )}
            <p className="analysis-result">
              P(at least {sim.stats.minLandsThreshold} lands after one more draw):{" "}
              <strong>{sim.stats.afterOneDrawLandsProbFormatted}</strong>
            </p>
            <p className="analysis-hint">
              Lands in hand: {sim.stats.landsInHand}. Library remaining: {sim.stats.libraryRemaining}.
            </p>
            {tagStats.length > 0 && (
              <>
                <h4 className="analysis-subheading">Tags in library</h4>
                <ul className="analysis-tag-list" aria-label="Per-tag next draw odds">
                  {tagStats.map((stat) => (
                    <li key={stat.tagId}>
                      <button
                        type="button"
                        className={`analysis-tag-row${selectedTagId === stat.tagId ? " analysis-tag-row--active" : ""}`}
                        onClick={() => onTagChange(selectedTagId === stat.tagId ? 0 : stat.tagId)}
                      >
                        <TagBadge tag={tagFromStat(stat)} />
                        <span className="analysis-tag-copies">{stat.copiesInLibrary ?? 0} in library</span>
                        <span className="analysis-tag-prob">{stat.nextDrawProbFormatted ?? "—"}</span>
                      </button>
                    </li>
                  ))}
                </ul>
              </>
            )}
          </article>
        </>
      )}
    </div>
  );
}
