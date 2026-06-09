import React from "react";
import type { CollectionTag } from "../backend";

type TagBadgeProps = {
  tag: CollectionTag;
  onClick?: () => void;
};

export function tagBadgeStyle(tag: CollectionTag): React.CSSProperties | undefined {
  if (!tag.color) {
    return undefined;
  }
  return {
    backgroundColor: `${tag.color}22`,
    borderColor: tag.color,
    color: tag.color
  };
}

export function TagBadge({ tag, onClick }: TagBadgeProps) {
  const style = tagBadgeStyle(tag);
  const className = `tag-badge${onClick ? " tag-badge--clickable" : ""}`;
  if (onClick) {
    return (
      <button type="button" className={className} style={style} onClick={onClick}>
        {tag.name}
      </button>
    );
  }
  return (
    <span className={className} style={style}>
      {tag.name}
    </span>
  );
}
