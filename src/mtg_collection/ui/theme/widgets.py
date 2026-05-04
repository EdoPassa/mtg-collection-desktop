from __future__ import annotations

from PySide6 import QtCore, QtWidgets

from mtg_collection.ui.theme.tokens import DEFAULT_TOKENS


def action_button(text: str, *, variant: str = "secondary") -> QtWidgets.QPushButton:
    button = QtWidgets.QPushButton(text)
    button.setProperty("variant", variant)
    button.setMinimumHeight(32)
    button.setCursor(QtCore.Qt.CursorShape.PointingHandCursor)
    return button


def status_label(text: str, *, role: str = "muted") -> QtWidgets.QLabel:
    label = QtWidgets.QLabel(text)
    label.setProperty("role", role)
    label.setMinimumHeight(24)
    return label


def section_title(text: str) -> QtWidgets.QLabel:
    label = QtWidgets.QLabel(text)
    label.setProperty("role", "section-title")
    return label


def configure_table(table: QtWidgets.QTableWidget, *, stretch_last_section: bool = True) -> QtWidgets.QTableWidget:
    table.horizontalHeader().setStretchLastSection(stretch_last_section)
    if table.columnCount() > 0:
        table.horizontalHeader().setSectionResizeMode(table.columnCount() - 1, QtWidgets.QHeaderView.ResizeMode.Stretch)
    table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
    table.setAlternatingRowColors(True)
    table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
    table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
    table.verticalHeader().setVisible(False)
    table.verticalHeader().setDefaultSectionSize(36)
    return table


def compact_actions_layout(parent: QtWidgets.QWidget) -> QtWidgets.QHBoxLayout:
    layout = QtWidgets.QHBoxLayout(parent)
    inset = DEFAULT_TOKENS.spacing.xs
    layout.setContentsMargins(inset, inset, inset, inset)
    layout.setSpacing(DEFAULT_TOKENS.spacing.sm)
    return layout


def apply_content_margins(layout: QtWidgets.QLayout) -> None:
    spacing = DEFAULT_TOKENS.spacing
    layout.setContentsMargins(spacing.lg, spacing.lg, spacing.lg, spacing.lg)
    layout.setSpacing(spacing.md)
