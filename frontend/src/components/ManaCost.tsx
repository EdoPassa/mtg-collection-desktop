import React from "react";
import { parseManaCost, type ManaSymbol } from "../lib/mana";

export function ManaCost({ cost, ariaLabel }: { cost: string | undefined | null; ariaLabel?: string }) {
  const symbols = parseManaCost(cost);
  if (symbols.length === 0) {
    return <span className="mana-cost mana-cost--empty" aria-label={ariaLabel ?? "No mana cost"}>—</span>;
  }
  return (
    <span className="mana-cost" aria-label={ariaLabel ?? `Mana cost ${cost}`}>
      {symbols.map((symbol, index) => (
        <ManaPip key={`${symbol.raw}-${index}`} symbol={symbol} />
      ))}
    </span>
  );
}

export function ManaPip({ symbol }: { symbol: ManaSymbol }) {
  return (
    <span className={`mana-pip mana-pip--${symbol.color.toLowerCase()}`} aria-hidden="true">
      {symbol.label}
    </span>
  );
}
