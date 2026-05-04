from __future__ import annotations

from pathlib import Path
from typing import Any

from PySide6 import QtCore, QtWidgets

from mtg_collection.ui.theme.widgets import action_button, apply_content_margins, configure_table, section_title


def build_import_tab(window: Any, tab: QtWidgets.QWidget) -> None:
    layout = QtWidgets.QVBoxLayout(tab)
    apply_content_margins(layout)

    layout.addWidget(section_title("Import cards"))

    top_row = QtWidgets.QHBoxLayout()
    layout.addLayout(top_row)

    window._import_mode = QtWidgets.QComboBox()
    window._import_mode.setAccessibleName("Import source mode")
    window._import_mode.addItems(["TXT (paste)", "CSV (file)"])
    top_row.addWidget(QtWidgets.QLabel("Source"))
    top_row.addWidget(window._import_mode)
    top_row.addStretch(1)

    window._csv_btn = action_button("Choose CSV...")
    window._csv_btn.setAccessibleName("Choose CSV file")
    window._csv_btn.clicked.connect(window._choose_csv)
    top_row.addWidget(window._csv_btn)

    window._validate_btn = action_button("Validate", variant="secondary")
    window._validate_btn.setAccessibleName("Validate import input")
    window._validate_btn.clicked.connect(window._validate_import)
    top_row.addWidget(window._validate_btn)

    window._commit_btn = action_button("Add to collection", variant="primary")
    window._commit_btn.setAccessibleName("Add validated cards to collection")
    window._commit_btn.setEnabled(False)
    window._commit_btn.clicked.connect(window._commit_validated)
    top_row.addWidget(window._commit_btn)

    window._input = QtWidgets.QPlainTextEdit()
    window._input.setAccessibleName("Import card list input")
    window._input.setPlaceholderText("Paste lines like:\n4 Lightning Bolt\n2x Opt\nLightning Bolt x4")
    layout.addWidget(window._input, 2)

    splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
    layout.addWidget(splitter, 3)

    window._results = QtWidgets.QTableWidget(0, 5)
    window._results.setHorizontalHeaderLabels(["Qty", "Input name", "Matched name", "Oracle ID", "Scryfall"])
    configure_table(window._results)
    splitter.addWidget(window._results)

    window._unresolved = QtWidgets.QPlainTextEdit()
    window._unresolved.setReadOnly(True)
    window._unresolved.setPlaceholderText("Unresolved lines will appear here with reasons.")
    splitter.addWidget(window._unresolved)

    window._csv_path: Path | None = None
    window._validated = []
