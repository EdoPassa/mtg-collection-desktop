import React, { useState } from "react";
import { colorIdentityBucket } from "../lib/mana";

export type CardImageSize = "thumb" | "tile";

/**
 * Renders the Scryfall card image at the requested size, falling back to a tinted name
 * placeholder when the bulk index hasn't supplied a URL (e.g. API-only resolver mode).
 */
export function CardImage({
  name,
  small,
  normal,
  colorIdentity,
  size,
  alt
}: {
  name: string;
  small?: string;
  normal?: string;
  colorIdentity?: string[];
  size: CardImageSize;
  alt?: string;
}) {
  const initialSrc = size === "tile" ? normal || small : small || normal;
  const [src, setSrc] = useState<string | undefined>(initialSrc);
  const [failed, setFailed] = useState(false);
  const colorClass = `card-image--${colorIdentityBucket(colorIdentity).toLowerCase()}`;

  if (!src || failed) {
    return (
      <span className={`card-image card-image--placeholder card-image--${size} ${colorClass}`} aria-label={alt ?? name}>
        <span className="card-image-name">{name}</span>
      </span>
    );
  }
  return (
    <span className={`card-image card-image--${size} ${colorClass}`}>
      <img src={src} alt={alt ?? name} loading="lazy" decoding="async" onError={() => setFailed(true)} />
    </span>
  );
}
