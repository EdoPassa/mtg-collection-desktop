from __future__ import annotations

import unittest

from PySide6 import QtCore, QtWidgets


class ThemeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def test_stylesheet_exposes_global_design_tokens(self) -> None:
        from mtg_collection.ui.theme.theme import build_stylesheet
        from mtg_collection.ui.theme.tokens import DEFAULT_TOKENS

        stylesheet = build_stylesheet(DEFAULT_TOKENS)

        self.assertIn("QPushButton[variant=\"primary\"]", stylesheet)
        self.assertIn("QLabel[role=\"success\"]", stylesheet)
        self.assertIn("QTableWidget", stylesheet)
        self.assertIn(DEFAULT_TOKENS.colors.primary, stylesheet)

    def test_default_tokens_use_dark_color_scheme(self) -> None:
        from mtg_collection.ui.theme.tokens import DEFAULT_TOKENS

        self.assertEqual(DEFAULT_TOKENS.colors.background, "#0f172a")
        self.assertEqual(DEFAULT_TOKENS.colors.surface, "#111827")
        self.assertEqual(DEFAULT_TOKENS.colors.text, "#e5e7eb")

    def test_widget_helpers_apply_semantic_properties(self) -> None:
        from mtg_collection.ui.theme.widgets import action_button, configure_table, status_label

        button = action_button("Add to collection", variant="primary")
        label = status_label("Returned", role="success")
        table = QtWidgets.QTableWidget(0, 2)

        configure_table(table)

        self.assertEqual(button.property("variant"), "primary")
        self.assertGreaterEqual(button.minimumHeight(), 32)
        self.assertEqual(label.property("role"), "success")
        self.assertEqual(label.text(), "Returned")
        self.assertEqual(table.editTriggers(), QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.assertTrue(table.alternatingRowColors())
        self.assertEqual(table.selectionBehavior(), QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.assertEqual(
            table.horizontalHeader().sectionResizeMode(1),
            QtWidgets.QHeaderView.ResizeMode.Stretch,
        )


class TabExtractionTests(unittest.TestCase):
    def test_tab_builder_modules_are_importable(self) -> None:
        from mtg_collection.ui.tabs.collection_tab import build_collection_tab
        from mtg_collection.ui.tabs.deck_tab import build_deck_tab
        from mtg_collection.ui.tabs.import_tab import build_import_tab
        from mtg_collection.ui.tabs.lent_tab import build_lent_tab

        self.assertTrue(callable(build_collection_tab))
        self.assertTrue(callable(build_deck_tab))
        self.assertTrue(callable(build_import_tab))
        self.assertTrue(callable(build_lent_tab))


class TabBuilderAccessibilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def test_key_controls_have_accessible_names(self) -> None:
        from mtg_collection.ui.tabs.collection_tab import build_collection_tab
        from mtg_collection.ui.tabs.deck_tab import build_deck_tab
        from mtg_collection.ui.tabs.import_tab import build_import_tab
        from mtg_collection.ui.tabs.lent_tab import build_lent_tab

        class Owner:
            def _choose_csv(self) -> None:
                pass

            def _validate_import(self) -> None:
                pass

            def _commit_validated(self) -> None:
                pass

            def _filter_collection(self) -> None:
                pass

            def _apply_collection_sort_and_filter(self) -> None:
                pass

            def refresh_collection(self) -> None:
                pass

            def _repair_deck_mismatches(self) -> None:
                pass

            def _compute_deck_compare(self) -> None:
                pass

            def _apply_deck_filter(self) -> None:
                pass

            def _export_deck_compare(self) -> None:
                pass

            def _add_lent_card(self) -> None:
                pass

            def refresh_lent_cards(self) -> None:
                pass

        owner = Owner()
        tabs = [QtWidgets.QWidget() for _ in range(4)]
        build_import_tab(owner, tabs[0])
        build_collection_tab(owner, tabs[1])
        build_deck_tab(owner, tabs[2])
        build_lent_tab(owner, tabs[3])

        self.assertEqual(owner._commit_btn.accessibleName(), "Add validated cards to collection")
        self.assertEqual(owner._import_mode.accessibleName(), "Import source mode")
        self.assertEqual(owner._collection_search.accessibleName(), "Collection search")
        self.assertEqual(owner._collection_sort_col.accessibleName(), "Collection sort column")
        self.assertEqual(owner._collection_sort_order.accessibleName(), "Collection sort order")
        self.assertEqual(owner._deck_input.accessibleName(), "Target decklist input")
        self.assertEqual(owner._deck_filter.accessibleName(), "Deck comparison filter")
        self.assertEqual(owner._lent_borrower.accessibleName(), "Borrower name")
        self.assertEqual(owner._lent_quantity.accessibleName(), "Lent quantity")
        self.assertEqual(owner._lent_date.accessibleName(), "Lent date")
        self.assertEqual(owner._lent_show_returned.accessibleName(), "Show returned lent cards")


class MainWindowSmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def test_main_window_preserves_tabs_and_initial_table_actions(self) -> None:
        from mtg_collection.ui.main_window import MainWindow

        class FakeDb:
            def list_collection(self) -> list[dict[str, object]]:
                return [
                    {
                        "name": "Lightning Bolt",
                        "quantity": 4,
                        "oracle_id": "oracle-1",
                        "scryfall_uri": "https://example.test/card",
                    }
                ]

            def get_lent_summary_by_oracle_id(self) -> dict[str, tuple[int, list[object]]]:
                return {}

            def get_lent_cards(self, *, include_returned: bool = False) -> list[dict[str, object]]:
                return [
                    {
                        "id": 7,
                        "oracle_id": "oracle-1",
                        "card_name": "Lightning Bolt",
                        "quantity": 1,
                        "borrower_name": "Alex",
                        "lent_date": "2026-05-04",
                        "return_date": "",
                        "notes": "",
                    }
                ]

        window = MainWindow(FakeDb(), object())

        self.assertEqual(window._tabs.count(), 4)
        self.assertEqual([window._tabs.tabText(i) for i in range(window._tabs.count())], ["Import", "Collection", "Deck compare", "Lent cards"])
        self.assertFalse(window._commit_btn.isEnabled())
        self.assertFalse(window._deck_repair_btn.isEnabled())
        self.assertFalse(window._deck_export_btn.isEnabled())

        collection_action = window._collection_table.cellWidget(0, 4).findChild(QtWidgets.QPushButton)
        lent_action = window._lent_table.cellWidget(0, 6).findChild(QtWidgets.QPushButton)

        self.assertEqual(collection_action.accessibleName(), "Lend Lightning Bolt")
        self.assertEqual(lent_action.accessibleName(), "Mark Lightning Bolt lent to Alex on 2026-05-04 returned")


if __name__ == "__main__":
    unittest.main()
