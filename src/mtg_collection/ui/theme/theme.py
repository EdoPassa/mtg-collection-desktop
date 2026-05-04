from __future__ import annotations

from PySide6 import QtWidgets

from mtg_collection.ui.theme.tokens import DEFAULT_TOKENS, DesignTokens


def build_stylesheet(tokens: DesignTokens = DEFAULT_TOKENS) -> str:
    colors = tokens.colors
    spacing = tokens.spacing
    radius = tokens.radius
    type_ = tokens.typography

    return f"""
    QWidget {{
        background-color: {colors.background};
        color: {colors.text};
        font-size: {type_.body}px;
    }}

    QMainWindow,
    QDialog {{
        background-color: {colors.background};
    }}

    QTabWidget::pane,
    QFrame[role="section"] {{
        border: 1px solid {colors.border};
        border-radius: {radius.lg}px;
        background-color: {colors.surface};
    }}

    QTabBar::tab {{
        background-color: {colors.surface_muted};
        border: 1px solid {colors.border};
        border-bottom: none;
        border-top-left-radius: {radius.md}px;
        border-top-right-radius: {radius.md}px;
        padding: {spacing.sm}px {spacing.lg}px;
        margin-right: {spacing.xs}px;
    }}

    QTabBar::tab:selected {{
        background-color: {colors.surface};
        color: {colors.primary};
    }}

    QLabel[role="section-title"] {{
        color: {colors.text};
        font-size: {type_.section}px;
        font-weight: 600;
        background-color: transparent;
    }}

    QLabel[role="muted"] {{
        color: {colors.text_muted};
        background-color: transparent;
    }}

    QLabel[role="success"] {{
        color: {colors.success};
        background-color: {colors.success_surface};
        border-radius: {radius.sm}px;
        padding: {spacing.xs}px {spacing.sm}px;
        font-weight: 600;
    }}

    QLabel[role="warning"] {{
        color: {colors.warning};
        background-color: {colors.warning_surface};
        border-radius: {radius.sm}px;
        padding: {spacing.xs}px {spacing.sm}px;
        font-weight: 600;
    }}

    QLabel[role="error"] {{
        color: {colors.error};
        background-color: {colors.error_surface};
        border-radius: {radius.sm}px;
        padding: {spacing.xs}px {spacing.sm}px;
        font-weight: 600;
    }}

    QLineEdit,
    QPlainTextEdit,
    QComboBox,
    QSpinBox,
    QDateEdit {{
        background-color: {colors.surface};
        border: 1px solid {colors.border};
        border-radius: {radius.md}px;
        padding: {spacing.sm}px;
        selection-background-color: {colors.primary};
        selection-color: {colors.primary_text};
    }}

    QLineEdit:focus,
    QPlainTextEdit:focus,
    QComboBox:focus,
    QSpinBox:focus,
    QDateEdit:focus {{
        border-color: {colors.primary};
    }}

    QPushButton {{
        background-color: {colors.surface};
        border: 1px solid {colors.border};
        border-radius: {radius.md}px;
        padding: {spacing.sm}px {spacing.md}px;
    }}

    QPushButton:hover {{
        background-color: {colors.surface_muted};
    }}

    QPushButton:disabled {{
        color: {colors.text_muted};
        background-color: {colors.surface_muted};
    }}

    QPushButton[variant="primary"] {{
        background-color: {colors.primary};
        border-color: {colors.primary};
        color: {colors.primary_text};
        font-weight: 600;
    }}

    QPushButton[variant="primary"]:hover {{
        background-color: {colors.primary_hover};
        border-color: {colors.primary_hover};
    }}

    QPushButton[variant="secondary"] {{
        background-color: {colors.surface};
        color: {colors.primary};
        border-color: {colors.primary};
    }}

    QPushButton[variant="destructive"] {{
        color: {colors.error};
        border-color: {colors.error};
    }}

    QTableWidget {{
        background-color: {colors.surface};
        alternate-background-color: {colors.surface_muted};
        border: 1px solid {colors.border};
        border-radius: {radius.md}px;
        gridline-color: {colors.border};
        selection-background-color: {colors.primary};
        selection-color: {colors.primary_text};
    }}

    QHeaderView::section {{
        background-color: {colors.surface_muted};
        color: {colors.text};
        border: none;
        border-right: 1px solid {colors.border};
        border-bottom: 1px solid {colors.border};
        padding: {spacing.sm}px;
        font-weight: 600;
    }}
    """


def apply_global_theme(app: QtWidgets.QApplication, tokens: DesignTokens = DEFAULT_TOKENS) -> None:
    app.setStyle("Fusion")
    app.setStyleSheet(build_stylesheet(tokens))
