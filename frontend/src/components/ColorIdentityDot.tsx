import React from "react";
import { colorIdentityBucket } from "../lib/mana";

export function ColorIdentityDot({ identity, label }: { identity: string[] | undefined | null; label?: string }) {
  const bucket = colorIdentityBucket(identity);
  const title = label ?? (identity && identity.length > 0 ? `Color identity: ${identity.join("")}` : "Colorless");
  return <span className={`color-dot color-dot--${bucket.toLowerCase()}`} title={title} aria-label={title} />;
}
