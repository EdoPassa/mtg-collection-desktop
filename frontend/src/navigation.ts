export type SectionId = "import" | "collection" | "decks" | "deck-compare" | "lending";

export type SectionConfig = {
  id: SectionId;
  label: string;
  title: string;
  description: string;
  icon: string;
};

export type NavEntry =
  | { type: "item"; section: SectionConfig }
  | { type: "group"; label: string; sections: SectionConfig[] };

export const navigation: NavEntry[] = [
  {
    type: "item",
    section: {
      id: "import",
      label: "Import",
      title: "Import Cards",
      description: "Validate and add cards to your collection",
      icon: "↓"
    }
  },
  {
    type: "item",
    section: {
      id: "collection",
      label: "Collection",
      title: "Collection",
      description: "Browse owned cards and availability",
      icon: "◆"
    }
  },
  {
    type: "group",
    label: "Decks",
    sections: [
      {
        id: "decks",
        label: "Library",
        title: "Decks",
        description: "Browse and edit saved decks",
        icon: "▤"
      },
      {
        id: "deck-compare",
        label: "Compare",
        title: "Deck Compare",
        description: "Compare a decklist against your collection and build decks",
        icon: "⚔"
      }
    ]
  },
  {
    type: "item",
    section: {
      id: "lending",
      label: "Lending",
      title: "Lending",
      description: "Track cards lent to other players",
      icon: "↔"
    }
  }
];

export const defaultSectionId: SectionId = "import";

const sectionById = new Map<SectionId, SectionConfig>(
  navigation.flatMap((entry) => (entry.type === "item" ? [entry.section] : entry.sections)).map((section) => [section.id, section])
);

export function getSection(id: SectionId): SectionConfig {
  const section = sectionById.get(id);
  if (!section) {
    throw new Error(`Unknown section: ${id}`);
  }
  return section;
}
