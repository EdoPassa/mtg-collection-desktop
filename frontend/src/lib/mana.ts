export type ManaSymbol = {
  /** Original token text inside the braces (e.g. "R", "U", "1", "W/U", "X"). */
  raw: string;
  /** Display string rendered inside the pip. */
  label: string;
  /** Single-letter color key used for the pip background palette. */
  color: ManaColor;
};

export type ManaColor = "W" | "U" | "B" | "R" | "G" | "C" | "X" | "M";

const COLOR_KEYS: ReadonlySet<string> = new Set(["W", "U", "B", "R", "G"]);

/**
 * Parse a Scryfall-style mana cost string (e.g. "{2}{W}{U}", "{X}{R}{R}", "{W/U}{1}") into
 * a list of symbols suitable for rendering as pips. Tokens that aren't in `{...}` braces
 * are ignored; unknown tokens are kept as colorless ("C") so the UI still shows them.
 */
export function parseManaCost(cost: string | undefined | null): ManaSymbol[] {
  if (!cost) {
    return [];
  }
  const out: ManaSymbol[] = [];
  const regex = /\{([^}]+)\}/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(cost)) !== null) {
    const raw = match[1].toUpperCase();
    out.push(toSymbol(raw));
  }
  return out;
}

function toSymbol(raw: string): ManaSymbol {
  // Hybrid mana like "W/U" or Phyrexian "W/P" — show the first half color, keep full label.
  if (raw.includes("/")) {
    const [first] = raw.split("/");
    const color = COLOR_KEYS.has(first) ? (first as ManaColor) : "M";
    return { raw, label: raw, color };
  }
  if (/^\d+$/.test(raw)) {
    return { raw, label: raw, color: "C" };
  }
  if (raw === "X" || raw === "Y" || raw === "Z") {
    return { raw, label: raw, color: "X" };
  }
  if (COLOR_KEYS.has(raw)) {
    return { raw, label: raw, color: raw as ManaColor };
  }
  return { raw, label: raw, color: "C" };
}

/**
 * Reduce a Scryfall color_identity array to a single bucket for color-dot rendering.
 * Empty identity is colorless; multi-color returns "M" so the UI can render a gradient.
 */
export function colorIdentityBucket(identity: string[] | undefined | null): ManaColor {
  if (!identity || identity.length === 0) {
    return "C";
  }
  if (identity.length > 1) {
    return "M";
  }
  const first = identity[0].toUpperCase();
  return COLOR_KEYS.has(first) ? (first as ManaColor) : "C";
}

/**
 * Does the card's color identity match the active mana filter? An empty filter matches
 * everything; a filter that excludes all of W/U/B/R/G shows nothing colored.
 */
export function matchesColorFilter(identity: string[] | undefined | null, allowed: ReadonlySet<ManaColor>): boolean {
  if (allowed.size === 0) {
    return true;
  }
  if (!identity || identity.length === 0) {
    return allowed.has("C");
  }
  for (const color of identity) {
    if (allowed.has(color.toUpperCase() as ManaColor)) {
      return true;
    }
  }
  return false;
}
