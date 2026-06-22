import React, { useMemo } from "react";
import type { DeckCard } from "../backend";
import { EmptyState } from "../components/EmptyState";
import { ManaCurveChart } from "../components/ManaCurveChart";
import { Stat } from "../components/Stat";
import { buildManaCurve } from "../lib/manaCurve";

type Props = {
  deckId: number;
  mainboardCards: DeckCard[];
};

export function DeckAnalysisOverview({ deckId, mainboardCards }: Props) {
  const curve = useMemo(() => buildManaCurve(mainboardCards), [mainboardCards]);

  if (curve.nonlandCount === 0 && curve.landCount === 0) {
    return (
      <EmptyState
        title="No mainboard cards"
        detail="Add mainboard cards in Decks or build from Deck Compare to see the mana curve."
      />
    );
  }

  return (
    <div className="analysis-overview">
      <div className="stat-row">
        <Stat label="Nonland" value={curve.nonlandCount} />
        <Stat label="Lands" value={curve.landCount} />
        <Stat label="Avg mana value" value={curve.averageManaValue.toFixed(2)} />
      </div>

      <article className="analysis-card">
        <h3>Mana curve</h3>
        <p className="analysis-hint">
          Mainboard nonland cards by mana value, segmented by color identity.
          {curve.unknownCount > 0
            ? ` ${curve.unknownCount} card${curve.unknownCount === 1 ? "" : "s"} missing a cost (not shown).`
            : ""}
        </p>
        {curve.nonlandCount - curve.unknownCount === 0 ? (
          <EmptyState
            title="No costed nonland cards"
            detail="Mana costs come from the Scryfall bulk cache. Resolve cards to populate the curve."
          />
        ) : (
          <ManaCurveChart key={deckId} data={curve} />
        )}
      </article>
    </div>
  );
}
