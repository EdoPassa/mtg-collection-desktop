from __future__ import annotations

from typing import Any

from PySide6 import QtWidgets

from mtg_collection.ui.theme.widgets import action_button, apply_content_margins, configure_table, section_title


def build_deck_tab(window: Any, tab: QtWidgets.QWidget) -> None:
    layout = QtWidgets.QVBoxLayout(tab)
    apply_content_margins(layout)

    layout.addWidget(section_title("Deck compare"))

    window._deck_input = QtWidgets.QPlainTextEdit()
    window._deck_input.setAccessibleName("Target decklist input")
    window._deck_input.setPlaceholderText("Paste a target decklist (same TXT format):\n4 Lightning Bolt\n2 Opt")
    layout.addWidget(window._deck_input, 2)

    btn_row = QtWidgets.QHBoxLayout()
    layout.addLayout(btn_row)
    btn_row.addStretch(1)

    window._deck_repair_btn = action_button("Repair mismatches")
    window._deck_repair_btn.setAccessibleName("Repair deck mismatches")
    window._deck_repair_btn.setEnabled(False)
    window._deck_repair_btn.clicked.connect(window._repair_deck_mismatches)
    btn_row.addWidget(window._deck_repair_btn)

    compute_btn = action_button("Compute owned vs need", variant="primary")
    compute_btn.setAccessibleName("Compute owned versus needed cards")
    compute_btn.clicked.connect(window._compute_deck_compare)
    btn_row.addWidget(compute_btn)

    filter_export_row = QtWidgets.QHBoxLayout()
    layout.addLayout(filter_export_row)

    window._deck_filter = QtWidgets.QComboBox()
    window._deck_filter.setAccessibleName("Deck comparison filter")
    window._deck_filter.addItems(["All", "Missing cards"])
    window._deck_filter.currentTextChanged.connect(window._apply_deck_filter)
    filter_export_row.addWidget(QtWidgets.QLabel("Filter:"))
    filter_export_row.addWidget(window._deck_filter)

    filter_export_row.addStretch(1)

    window._deck_export_btn = action_button("Export to CSV...")
    window._deck_export_btn.setAccessibleName("Export deck comparison to CSV")
    window._deck_export_btn.setEnabled(False)
    window._deck_export_btn.clicked.connect(window._export_deck_compare)
    filter_export_row.addWidget(window._deck_export_btn)

    window._deck_out = QtWidgets.QTableWidget(0, 4)
    window._deck_out.setHorizontalHeaderLabels(["Card", "Needed", "Owned", "Missing"])
    configure_table(window._deck_out)
    layout.addWidget(window._deck_out, 3)

    layout.addWidget(section_title("Build deck"))

    build_row = QtWidgets.QHBoxLayout()
    layout.addLayout(build_row)

    build_row.addWidget(QtWidgets.QLabel("Mode:"))
    window._deck_build_mode = QtWidgets.QComboBox()
    window._deck_build_mode.setAccessibleName("Deck build mode")
    window._deck_build_mode.addItems(["Create new", "Replace existing"])
    window._deck_build_mode.currentTextChanged.connect(window._update_deck_build_mode)
    build_row.addWidget(window._deck_build_mode)

    build_row.addWidget(QtWidgets.QLabel("New deck:"))
    window._deck_builder_name = QtWidgets.QLineEdit()
    window._deck_builder_name.setAccessibleName("New deck name")
    window._deck_builder_name.setPlaceholderText("Deck name")
    build_row.addWidget(window._deck_builder_name, 2)

    build_row.addWidget(QtWidgets.QLabel("Existing deck:"))
    window._deck_builder_selector = QtWidgets.QComboBox()
    window._deck_builder_selector.setAccessibleName("Saved deck selector")
    window._deck_builder_selector.currentIndexChanged.connect(lambda _: window._update_deck_build_state())
    build_row.addWidget(window._deck_builder_selector, 2)

    window._deck_build_btn = action_button("Build deck from list", variant="primary")
    window._deck_build_btn.setAccessibleName("Build deck from compared list")
    window._deck_build_btn.setEnabled(False)
    window._deck_build_btn.clicked.connect(window._build_deck_from_compare)
    build_row.addWidget(window._deck_build_btn)

    window._deck_build_status = QtWidgets.QPlainTextEdit()
    window._deck_build_status.setReadOnly(True)
    window._deck_build_status.setAccessibleName("Deck build status")
    window._deck_build_status.setPlaceholderText("Build availability and results will appear here.")
    layout.addWidget(window._deck_build_status, 1)

    window._deck_unresolved = QtWidgets.QPlainTextEdit()
    window._deck_unresolved.setReadOnly(True)
    window._deck_unresolved.setPlaceholderText("Unresolved deck lines will appear here.")
    layout.addWidget(window._deck_unresolved, 1)

    window._update_deck_build_mode()
