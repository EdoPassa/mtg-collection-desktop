from __future__ import annotations

from typing import Any

from PySide6 import QtWidgets

from mtg_collection.ui.theme.widgets import action_button, apply_content_margins, configure_table, section_title


def build_collection_tab(window: Any, tab: QtWidgets.QWidget) -> None:
    layout = QtWidgets.QVBoxLayout(tab)
    apply_content_margins(layout)

    layout.addWidget(section_title("Collection"))

    search_row = QtWidgets.QHBoxLayout()
    layout.addLayout(search_row)

    search_row.addWidget(QtWidgets.QLabel("Search:"))
    window._collection_search = QtWidgets.QLineEdit()
    window._collection_search.setAccessibleName("Collection search")
    window._collection_search.setPlaceholderText("Filter by card name...")
    window._collection_search.setClearButtonEnabled(True)
    window._collection_search.textChanged.connect(window._filter_collection)
    search_row.addWidget(window._collection_search)

    search_row.addWidget(QtWidgets.QLabel("Sort by:"))
    window._collection_sort_col = QtWidgets.QComboBox()
    window._collection_sort_col.setAccessibleName("Collection sort column")
    window._collection_sort_col.addItems(["Card", "Quantity"])
    search_row.addWidget(window._collection_sort_col)

    window._collection_sort_order = QtWidgets.QComboBox()
    window._collection_sort_order.setAccessibleName("Collection sort order")
    window._collection_sort_order.addItems(["Ascending", "Descending"])
    search_row.addWidget(window._collection_sort_order)

    window._collection_sort_col.currentTextChanged.connect(lambda _: window._apply_collection_sort_and_filter())
    window._collection_sort_order.currentTextChanged.connect(lambda _: window._apply_collection_sort_and_filter())

    window._collection_table = QtWidgets.QTableWidget(0, 6)
    window._collection_table.setHorizontalHeaderLabels(["Card", "Owned", "Lent", "Available", "In deck", "Actions"])
    configure_table(window._collection_table)
    window._collection_table.setSortingEnabled(True)
    window._collection_table.horizontalHeader().setSortIndicatorShown(True)
    layout.addWidget(window._collection_table)

    bottom_row = QtWidgets.QHBoxLayout()
    layout.addLayout(bottom_row)

    window._collection_count_label = QtWidgets.QLabel("")
    window._collection_count_label.setProperty("role", "muted")
    bottom_row.addWidget(window._collection_count_label)
    bottom_row.addStretch(1)

    refresh = action_button("Refresh")
    refresh.setAccessibleName("Refresh collection")
    refresh.clicked.connect(window.refresh_collection)
    bottom_row.addWidget(refresh)

    window._collection_rows = []
