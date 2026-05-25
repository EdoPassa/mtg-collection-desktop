import { describe, expect, it } from "vitest";
import { navigation, type SectionId } from "./navigation";
import { sectionPanels } from "./panels";

describe("navigation", () => {
  it("maps every section to a panel component", () => {
    const sectionIds = navigation.flatMap((entry) => (entry.type === "item" ? [entry.section.id] : entry.sections.map((s) => s.id)));

    for (const id of sectionIds) {
      expect(sectionPanels[id as SectionId]).toBeTypeOf("function");
    }

    expect(Object.keys(sectionPanels).sort()).toEqual(sectionIds.sort());
  });
});
