import React, { useEffect, useState } from "react";
import { CURVE_COLORS, type CurveColor, type ManaCurveData } from "../lib/manaCurve";

const COLOR_NAMES: Record<CurveColor, string> = {
  W: "White",
  U: "Blue",
  B: "Black",
  R: "Red",
  G: "Green",
  M: "Multicolor",
  C: "Colorless"
};

function bucketTooltip(label: string, byColor: Record<CurveColor, number>, total: number): string {
  if (total === 0) {
    return `Mana value ${label}: 0 cards`;
  }
  const parts = CURVE_COLORS.filter((color) => byColor[color] > 0).map(
    (color) => `${byColor[color]} ${COLOR_NAMES[color]}`
  );
  return `Mana value ${label}: ${total} card${total === 1 ? "" : "s"} (${parts.join(", ")})`;
}

function summary(data: ManaCurveData): string {
  const populated = data.buckets.filter((bucket) => bucket.total > 0);
  if (populated.length === 0) {
    return "Mana curve: no costed nonland cards.";
  }
  const breakdown = populated.map((bucket) => `${bucket.total} at ${bucket.label}`).join(", ");
  return `Mana curve by mana value: ${breakdown}. Average mana value ${data.averageManaValue.toFixed(2)}.`;
}

/**
 * Color-segmented bar chart of a deck's mana curve. Bars grow from zero on mount
 * (staggered) unless the user prefers reduced motion.
 */
export function ManaCurveChart({ data }: { data: ManaCurveData }) {
  const [grown, setGrown] = useState(false);

  useEffect(() => {
    const frame = requestAnimationFrame(() => setGrown(true));
    return () => cancelAnimationFrame(frame);
  }, []);

  return (
    <figure className="mana-curve" role="img" aria-label={summary(data)}>
      <div className="mana-curve-plot">
        {data.buckets.map((bucket, index) => {
          const heightPct = grown ? (bucket.total / data.maxTotal) * 100 : 0;
          return (
            <div
              key={bucket.cmc}
              className="mana-curve-col"
              title={bucketTooltip(bucket.label, bucket.byColor, bucket.total)}
            >
              <span className="mana-curve-count" aria-hidden="true">
                {bucket.total > 0 ? bucket.total : ""}
              </span>
              <div className="mana-curve-track">
                <div
                  className="mana-curve-bar"
                  style={{ height: `${heightPct}%`, ["--col" as string]: index } as React.CSSProperties}
                >
                  {CURVE_COLORS.map((color) =>
                    bucket.byColor[color] > 0 ? (
                      <span
                        key={color}
                        className={`mana-seg mana-seg--${color.toLowerCase()}`}
                        style={{ height: `${(bucket.byColor[color] / bucket.total) * 100}%` }}
                      />
                    ) : null
                  )}
                </div>
              </div>
              <span className="mana-curve-axis" aria-hidden="true">
                {bucket.label}
              </span>
            </div>
          );
        })}
      </div>
    </figure>
  );
}
