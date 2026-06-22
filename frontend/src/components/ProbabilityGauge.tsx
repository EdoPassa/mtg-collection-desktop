import React, { useEffect, useState } from "react";

const RADIUS = 42;
const CIRCUMFERENCE = 2 * Math.PI * RADIUS;
/** Fraction of the full circle the dial sweeps (270°, leaving a gap at the bottom). */
const SWEEP = 0.75;
const TRACK_LENGTH = CIRCUMFERENCE * SWEEP;

type GaugeTone = "accent" | "auto";

type Props = {
  /** Probability in the 0..1 range. Missing/NaN renders an empty dial. */
  value: number | undefined | null;
  /** Pre-formatted percentage (e.g. "42.0%") shown in the center; falls back to value. */
  formatted?: string;
  /** Short caption under the value. */
  label?: string;
  /** "accent" keeps the brand cyan; "auto" ramps danger → warning → success by likelihood. */
  tone?: GaugeTone;
};

function toneColor(tone: GaugeTone, clamped: number): string {
  if (tone === "auto") {
    if (clamped < 0.34) return "var(--danger)";
    if (clamped < 0.67) return "var(--warning)";
    return "var(--success)";
  }
  return "var(--gold)";
}

/**
 * Radial probability dial. The arc grows from empty on mount and animates whenever the
 * value changes (unless the user prefers reduced motion). The center reuses the backend's
 * pre-formatted percentage string so formatting stays consistent with the rest of the UI.
 */
export function ProbabilityGauge({ value, formatted, label, tone = "accent" }: Props) {
  const hasValue = typeof value === "number" && Number.isFinite(value);
  const clamped = hasValue ? Math.max(0, Math.min(1, value as number)) : 0;
  const [grown, setGrown] = useState(false);

  useEffect(() => {
    const frame = requestAnimationFrame(() => setGrown(true));
    return () => cancelAnimationFrame(frame);
  }, []);

  const offset = grown ? TRACK_LENGTH * (1 - clamped) : TRACK_LENGTH;
  const center = formatted ?? (hasValue ? `${(clamped * 100).toFixed(1)}%` : "—");
  const ariaLabel = `${label ? `${label}: ` : ""}${center}`;

  return (
    <div className="gauge" role="img" aria-label={ariaLabel}>
      <svg
        className="gauge-svg"
        viewBox="0 0 100 100"
        aria-hidden="true"
        style={{ ["--gauge-color" as string]: toneColor(tone, clamped) } as React.CSSProperties}
      >
        <circle
          className="gauge-track"
          cx="50"
          cy="50"
          r={RADIUS}
          transform="rotate(135 50 50)"
          strokeDasharray={`${TRACK_LENGTH} ${CIRCUMFERENCE}`}
        />
        <circle
          className="gauge-arc"
          cx="50"
          cy="50"
          r={RADIUS}
          transform="rotate(135 50 50)"
          strokeDasharray={`${TRACK_LENGTH} ${CIRCUMFERENCE}`}
          strokeDashoffset={offset}
        />
      </svg>
      <div className="gauge-center" aria-hidden="true">
        <span className="gauge-value">{center}</span>
        {label ? <span className="gauge-label">{label}</span> : null}
      </div>
    </div>
  );
}
