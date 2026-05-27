import type { DeckCard } from "../backend";

/** Display-only board labels; main/side semantics are enforced in Go (internal/cards/board.go). */

export const BOARD_MAIN = "main";
export const BOARD_SIDE = "side";

export function normalizeBoard(board?: string): string {
  return board === BOARD_SIDE ? BOARD_SIDE : BOARD_MAIN;
}

export function isMainboard(board?: string): boolean {
  return normalizeBoard(board) === BOARD_MAIN;
}

export function boardLabel(board?: string): string {
  return isMainboard(board) ? "Mainboard" : "Sideboard";
}

export function partitionDeckCards(cards: DeckCard[]): { mainboard: DeckCard[]; sideboard: DeckCard[] } {
  const mainboard: DeckCard[] = [];
  const sideboard: DeckCard[] = [];
  for (const row of cards) {
    if (isMainboard(row.board)) {
      mainboard.push(row);
    } else {
      sideboard.push(row);
    }
  }
  return { mainboard, sideboard };
}

export function totalQuantity(cards: DeckCard[]): number {
  return cards.reduce((sum, row) => sum + row.quantity, 0);
}
