from __future__ import annotations

from typing import Any

from PySide6 import QtCore, QtWidgets

from mtg_collection.ui.theme.widgets import action_button, apply_content_margins, configure_table, section_title


def build_lent_tab(window: Any, tab: QtWidgets.QWidget) -> None:
    layout = QtWidgets.QVBoxLayout(tab)
    apply_content_margins(layout)

    layout.addWidget(section_title("Lent cards"))

    form_row = QtWidgets.QHBoxLayout()
    layout.addLayout(form_row)

    form_row.addWidget(QtWidgets.QLabel("Card oracle_id:"))
    window._lent_oracle_id = QtWidgets.QLineEdit()
    window._lent_oracle_id.setAccessibleName("Lent card oracle ID")
    window._lent_oracle_id.setPlaceholderText("e.g., abc12345-...")
    form_row.addWidget(window._lent_oracle_id, 2)

    form_row.addWidget(QtWidgets.QLabel("Qty:"))
    window._lent_quantity = QtWidgets.QSpinBox()
    window._lent_quantity.setAccessibleName("Lent quantity")
    window._lent_quantity.setMinimum(1)
    window._lent_quantity.setMaximum(999)
    window._lent_quantity.setValue(1)
    form_row.addWidget(window._lent_quantity)

    form_row.addWidget(QtWidgets.QLabel("Borrower:"))
    window._lent_borrower = QtWidgets.QLineEdit()
    window._lent_borrower.setAccessibleName("Borrower name")
    window._lent_borrower.setPlaceholderText("Name of person")
    form_row.addWidget(window._lent_borrower, 2)

    form_row.addWidget(QtWidgets.QLabel("Date:"))
    window._lent_date = QtWidgets.QDateEdit()
    window._lent_date.setAccessibleName("Lent date")
    window._lent_date.setCalendarPopup(True)
    window._lent_date.setDate(QtCore.QDate.currentDate())
    window._lent_date.setDisplayFormat("yyyy-MM-dd")
    form_row.addWidget(window._lent_date)

    add_btn = action_button("Add lent card", variant="primary")
    add_btn.setAccessibleName("Add lent card")
    add_btn.clicked.connect(window._add_lent_card)
    form_row.addWidget(add_btn)

    notes_row = QtWidgets.QHBoxLayout()
    layout.addLayout(notes_row)
    notes_row.addWidget(QtWidgets.QLabel("Notes:"))
    window._lent_notes = QtWidgets.QLineEdit()
    window._lent_notes.setAccessibleName("Lent card notes")
    window._lent_notes.setPlaceholderText("Optional notes about this lent card")
    notes_row.addWidget(window._lent_notes)

    window._lent_table = QtWidgets.QTableWidget(0, 7)
    window._lent_table.setHorizontalHeaderLabels(["ID", "Card", "Qty", "Borrower", "Lent Date", "Returned", "Actions"])
    configure_table(window._lent_table)
    layout.addWidget(window._lent_table, 2)

    bottom_row = QtWidgets.QHBoxLayout()
    layout.addLayout(bottom_row)

    window._lent_show_returned = QtWidgets.QCheckBox("Show returned cards")
    window._lent_show_returned.setAccessibleName("Show returned lent cards")
    window._lent_show_returned.stateChanged.connect(window.refresh_lent_cards)
    bottom_row.addWidget(window._lent_show_returned)

    bottom_row.addStretch(1)

    refresh = action_button("Refresh")
    refresh.setAccessibleName("Refresh lent cards")
    refresh.clicked.connect(window.refresh_lent_cards)
    bottom_row.addWidget(refresh)

    window._lent_rows = []
