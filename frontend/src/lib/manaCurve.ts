import type { DeckCard } from "../backend";
import { colorIdentityBucket, parseManaCost } from "./mana";

/** Colors used as stacked segments in the curve. colorIdentityBucket only ever
 * returns one of these (W/U/B/R/G for mono, M for multi, C for colorless). */
export type CurveColor = "W" | "U" | "B" | "R" | "G" | "M" | "C";

export const CURVE_COLORS: readonly CurveColor[] = ["W", "U", "B", "R", "G", "M", "C"];

/** Highest mana-value bucket; everything at or above lands in the "7+" column. */
export const MAX_BUCKET = 7;

export type ManaCurveBucket = {
  /** 0..MAX_BUCKET, where MAX_BUCKET represents "7+". */
  cmc: number;
  /** Axis label ("0".."6", "7+"). */
  label: string;
  /** Total card copies in this bucket. */
  total: number;
  /** Copies split by color identity. */
  byColor: Record<CurveColor, number>;
};

export type ManaCurveData = {
  buckets: ManaCurveBucket[];
  /** Largest bucket total, used to scale bar heights (>= 1 to avoid /0). */
  maxTotal: number;
  /** Nonland copies (including any missing a mana cost). */
  nonlandCount: number;
  /** Land copies (excluded from the curve). */
  landCount: number;
  /** Nonland copies with no mana cost — counted but not placed in a bucket. */
  unknownCount: number;
  /** Quantity-weighted average mana value over costed nonland copies. */
  averageManaValue: number;
};

/**
 * Compute the converted mana value (a.k.a. CMC) from a Scryfall mana-cost string.
 * Numeric generic mana adds its value; X adds 0; hybrid like {2/W} uses the numeric
 * half when present; every other pip (colored, Phyrexian, snow, etc.) adds 1.
 */
export function manaValue(manaCost: string | undefined | null): number {
  let total = 0;
  for (const symbol of parseManaCost(manaCost)) {
    const raw = symbol.raw;
    if (raw.includes("/")) {
      const first = raw.split("/")[0];
      total += /^\d+$/.test(first) ? Number(first) : 1;
    } else if (/^\d+$/.test(raw)) {
      total += Number(raw);
    } else if (symbol.color === "X") {
      total += 0;
    } else {
      total += 1;
    }
  }
  return total;
}

/** A card counts as a land when its type line contains the word "Land". */
export function isLandCard(typeLine: string | undefined | null): boolean {
  return typeof typeLine === "string" && /\bland\b/i.test(typeLine);
}

function emptyByColor(): Record<CurveColor, number> {
  return { W: 0, U: 0, B: 0, R: 0, G: 0, M: 0, C: 0 };
}

function bucketLabel(cmc: number): string {
  return cmc >= MAX_BUCKET ? `${MAX_BUCKET}+` : String(cmc);
}

/**
 * Build mana-curve buckets from a deck's cards. Lands are excluded; nonland cards
 * missing a cost are tallied in `unknownCount` rather than distorting bucket 0.
 */
export function buildManaCurve(cards: DeckCard[]): ManaCurveData {
  const buckets: ManaCurveBucket[] = [];
  for (let cmc = 0; cmc <= MAX_BUCKET; cmc += 1) {
    buckets.push({ cmc, label: bucketLabel(cmc), total: 0, byColor: emptyByColor() });
  }

  let nonlandCount = 0;
  let landCount = 0;
  let unknownCount = 0;
  let weightedSum = 0;
  let costedCount = 0;

  for (const row of cards) {
    const quantity = row.quantity > 0 ? row.quantity : 0;
    if (quantity === 0) {
      continue;
    }
    const card = row.card;
    if (isLandCard(card.typeLine)) {
      landCount += quantity;
      continue;
    }
    nonlandCount += quantity;

    if (!card.manaCost) {
      unknownCount += quantity;
      continue;
    }

    const mv = manaValue(card.manaCost);
    weightedSum += mv * quantity;
    costedCount += quantity;

    const index = Math.min(mv, MAX_BUCKET);
    const color = colorIdentityBucket(card.colorIdentity) as CurveColor;
    const bucket = buckets[index];
    bucket.total += quantity;
    bucket.byColor[color] += quantity;
  }

  const maxTotal = buckets.reduce((max, bucket) => Math.max(max, bucket.total), 0);
  const averageManaValue = costedCount > 0 ? weightedSum / costedCount : 0;

  return {
    buckets,
    maxTotal: Math.max(maxTotal, 1),
    nonlandCount,
    landCount,
    unknownCount,
    averageManaValue
  };
}
