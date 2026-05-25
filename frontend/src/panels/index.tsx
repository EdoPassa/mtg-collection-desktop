import type { ComponentType } from "react";
import type { SectionId } from "../navigation";
import { CollectionPanel } from "./CollectionPanel";
import { DeckAnalysisPanel } from "./DeckAnalysisPanel";
import { DeckComparePanel } from "./DeckComparePanel";
import { DecksPanel } from "./DecksPanel";
import { ImportPanel } from "./ImportPanel";
import { LendingPanel } from "./LendingPanel";
import type { PanelProps } from "./types";

export const sectionPanels: Record<SectionId, ComponentType<PanelProps>> = {
  import: ImportPanel,
  collection: CollectionPanel,
  decks: DecksPanel,
  "deck-compare": DeckComparePanel,
  "deck-analysis": DeckAnalysisPanel,
  lending: LendingPanel
};
