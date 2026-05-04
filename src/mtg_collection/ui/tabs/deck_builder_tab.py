from __future__ import annotations

from typing import Any

from PySide6 import QtWidgets

from mtg_collection.ui.theme.widgets import action_button, apply_content_margins, configure_table, section_title


def build_deck_builder_tab(window: Any, tab: QtWidgets.QWidget) -> None:
    layout = QtWidgets.QVBoxLayout(tab)
    apply_content_margins(layout)

    layout.addWidget(section_title("Deck builder"))

    create_row = QtWidgets.QHBoxLayout()
    layout.addLayout(create_row)

    create_row.addWidget(QtWidgets.QLabel("New deck:"))
    window._deck_builder_name = QtWidgets.QLineEdit()
    window._deck_builder_name.setAccessibleName("New deck name")
    window._deck_builder_name.setPlaceholderText("Deck name")
    create_row.addWidget(window._deck_builder_name)

    create_btn = action_button("Create deck", variant="primary")
    create_btn.setAccessibleName("Create deck")
    create_btn.clicked.connect(window._create_deck)
    create_row.addWidget(create_btn)

    select_row = QtWidgets.QHBoxLayout()
    layout.addLayout(select_row)

    select_row.addWidget(QtWidgets.QLabel("Deck:"))
    window._deck_builder_selector = QtWidgets.QComboBox()
    window._deck_builder_selector.setAccessibleName("Saved deck selector")
    window._deck_builder_selector.currentIndexChanged.connect(lambda _: window._refresh_selected_deck_cards())
    select_row.addWidget(window._deck_builder_selector)

    add_row = QtWidgets.QHBoxLayout()
    layout.addLayout(add_row)

    add_row.addWidget(QtWidgets.QLabel("Card:"))
    window._deck_builder_card_name = QtWidgets.QLineEdit()
    window._deck_builder_card_name.setAccessibleName("Deck card name")
    window._deck_builder_card_name.setPlaceholderText("Card name")
    add_row.addWidget(window._deck_builder_card_name, 2)

    add_row.addWidget(QtWidgets.QLabel("Qty:"))
    window._deck_builder_quantity = QtWidgets.QSpinBox()
    window._deck_builder_quantity.setAccessibleName("Deck card quantity")
    window._deck_builder_quantity.setMinimum(1)
    window._deck_builder_quantity.setMaximum(999)
    window._deck_builder_quantity.setValue(1)
    add_row.addWidget(window._deck_builder_quantity)

    add_btn = action_button("Add card", variant="primary")
    add_btn.setAccessibleName("Add card to selected deck")
    add_btn.clicked.connect(window._add_card_to_selected_deck)
    add_row.addWidget(add_btn)

    window._deck_builder_cards = QtWidgets.QTableWidget(0, 2)
    window._deck_builder_cards.setHorizontalHeaderLabels(["Card", "Quantity"])
    configure_table(window._deck_builder_cards)
    layout.addWidget(window._deck_builder_cards, 3)

    window._deck_builder_status = QtWidgets.QPlainTextEdit()
    window._deck_builder_status.setReadOnly(True)
    window._deck_builder_status.setAccessibleName("Deck builder status")
    window._deck_builder_status.setPlaceholderText("Deck builder messages will appear here.")
    layout.addWidget(window._deck_builder_status, 1)
