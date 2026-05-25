/** True when Scryfall type_line includes Land (basic, snow, artifact lands, etc.). */
export function isLandTypeLine(typeLine: string | undefined): boolean {
  return (typeLine ?? "").includes("Land");
}

export function countLandsInDeck(cards: { card: { typeLine?: string }; quantity: number }[]): number {
  return cards.reduce((sum, row) => sum + (isLandTypeLine(row.card.typeLine) ? row.quantity : 0), 0);
}
