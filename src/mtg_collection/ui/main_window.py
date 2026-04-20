from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from mtg_collection.db import CardIdentity, CollectionDb
from mtg_collection.importer import ImportLine, parse_csv_bytes, parse_txt
from mtg_collection.scryfall import ScryfallClient, ScryfallError


@dataclass(frozen=True)
class ResolvedLine:
    line: ImportLine
    oracle_id: str
    canonical_name: str
    scryfall_uri: str


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, db: CollectionDb, scryfall: ScryfallClient):
        super().__init__()
        self._db = db
        self._scryfall = scryfall
        self.setWindowTitle("MTG Collection (MVP)")
        self.resize(1000, 700)

        self._tabs = QtWidgets.QTabWidget()
        self.setCentralWidget(self._tabs)

        self._import_tab = QtWidgets.QWidget()
        self._collection_tab = QtWidgets.QWidget()
        self._deck_tab = QtWidgets.QWidget()

        self._tabs.addTab(self._import_tab, "Import")
        self._tabs.addTab(self._collection_tab, "Collection")
        self._tabs.addTab(self._deck_tab, "Deck compare")

        self._build_import_tab()
        self._build_collection_tab()
        self._build_deck_tab()

        self.refresh_collection()

    def _build_import_tab(self) -> None:
        layout = QtWidgets.QVBoxLayout(self._import_tab)

        top_row = QtWidgets.QHBoxLayout()
        layout.addLayout(top_row)

        self._import_mode = QtWidgets.QComboBox()
        self._import_mode.addItems(["TXT (paste)", "CSV (file)"])
        top_row.addWidget(QtWidgets.QLabel("Source"))
        top_row.addWidget(self._import_mode)
        top_row.addStretch(1)

        self._csv_btn = QtWidgets.QPushButton("Choose CSV…")
        self._csv_btn.clicked.connect(self._choose_csv)
        top_row.addWidget(self._csv_btn)

        self._validate_btn = QtWidgets.QPushButton("Validate")
        self._validate_btn.clicked.connect(self._validate_import)
        top_row.addWidget(self._validate_btn)

        self._commit_btn = QtWidgets.QPushButton("Add to collection")
        self._commit_btn.setEnabled(False)
        self._commit_btn.clicked.connect(self._commit_validated)
        top_row.addWidget(self._commit_btn)

        self._input = QtWidgets.QPlainTextEdit()
        self._input.setPlaceholderText("Paste lines like:\n4 Lightning Bolt\n2x Opt\nLightning Bolt x4")
        layout.addWidget(self._input, 2)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        layout.addWidget(splitter, 3)

        self._results = QtWidgets.QTableWidget(0, 5)
        self._results.setHorizontalHeaderLabels(["Qty", "Input name", "Matched name", "Oracle ID", "Scryfall"])
        self._results.horizontalHeader().setStretchLastSection(True)
        self._results.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        splitter.addWidget(self._results)

        self._unresolved = QtWidgets.QPlainTextEdit()
        self._unresolved.setReadOnly(True)
        self._unresolved.setPlaceholderText("Unresolved lines will appear here with reasons.")
        splitter.addWidget(self._unresolved)

        self._csv_path: Path | None = None
        self._validated: list[ResolvedLine] = []

    def _build_collection_tab(self) -> None:
        layout = QtWidgets.QVBoxLayout(self._collection_tab)

        self._collection_table = QtWidgets.QTableWidget(0, 3)
        self._collection_table.setHorizontalHeaderLabels(["Card", "Quantity", "Scryfall"])
        self._collection_table.horizontalHeader().setStretchLastSection(True)
        self._collection_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        layout.addWidget(self._collection_table)

        refresh = QtWidgets.QPushButton("Refresh")
        refresh.clicked.connect(self.refresh_collection)
        layout.addWidget(refresh, alignment=QtCore.Qt.AlignmentFlag.AlignRight)

    def _build_deck_tab(self) -> None:
        layout = QtWidgets.QVBoxLayout(self._deck_tab)

        self._deck_input = QtWidgets.QPlainTextEdit()
        self._deck_input.setPlaceholderText("Paste a target decklist (same TXT format):\n4 Lightning Bolt\n2 Opt")
        layout.addWidget(self._deck_input, 2)

        btn = QtWidgets.QPushButton("Compute owned vs need")
        btn.clicked.connect(self._compute_deck_compare)
        layout.addWidget(btn, alignment=QtCore.Qt.AlignmentFlag.AlignRight)

        self._deck_out = QtWidgets.QTableWidget(0, 4)
        self._deck_out.setHorizontalHeaderLabels(["Card", "Needed", "Owned", "Missing"])
        self._deck_out.horizontalHeader().setStretchLastSection(True)
        self._deck_out.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        layout.addWidget(self._deck_out, 3)

        self._deck_unresolved = QtWidgets.QPlainTextEdit()
        self._deck_unresolved.setReadOnly(True)
        self._deck_unresolved.setPlaceholderText("Unresolved deck lines will appear here.")
        layout.addWidget(self._deck_unresolved, 1)

    def _choose_csv(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Choose CSV", "", "CSV Files (*.csv);;All Files (*)")
        if not path:
            return
        self._csv_path = Path(path)
        self._import_mode.setCurrentText("CSV (file)")
        self._input.setPlainText(f"Selected CSV:\n{path}")

    def _validate_import(self) -> None:
        self._commit_btn.setEnabled(False)
        self._validated = []
        self._results.setRowCount(0)
        self._unresolved.clear()

        if self._import_mode.currentText().startswith("CSV"):
            if not self._csv_path:
                self._unresolved.setPlainText("No CSV selected.")
                return
            try:
                data = self._csv_path.read_bytes()
            except OSError as e:
                self._unresolved.setPlainText(f"Failed to read CSV: {e}")
                return
            lines, unresolved = parse_csv_bytes(data)
        else:
            lines, unresolved = parse_txt(self._input.toPlainText())

        unresolved_msgs: list[str] = []
        if unresolved:
            unresolved_msgs.append("Unresolved parse rows:")
            unresolved_msgs.extend(f"- {u}" for u in unresolved)

        for line in lines:
            try:
                card = self._scryfall.lookup_named(line.name)
            except ScryfallError as e:
                unresolved_msgs.append(f"- {line.raw}  ->  {e}")
                continue

            self._validated.append(
                ResolvedLine(
                    line=line,
                    oracle_id=card.oracle_id,
                    canonical_name=card.name,
                    scryfall_uri=card.scryfall_uri,
                )
            )

        self._render_validated()
        self._unresolved.setPlainText("\n".join(unresolved_msgs).strip())
        self._commit_btn.setEnabled(len(self._validated) > 0)

    def _render_validated(self) -> None:
        self._results.setRowCount(len(self._validated))
        for r, item in enumerate(self._validated):
            self._results.setItem(r, 0, QtWidgets.QTableWidgetItem(str(item.line.qty)))
            self._results.setItem(r, 1, QtWidgets.QTableWidgetItem(item.line.name))
            self._results.setItem(r, 2, QtWidgets.QTableWidgetItem(item.canonical_name))
            self._results.setItem(r, 3, QtWidgets.QTableWidgetItem(item.oracle_id))
            self._results.setItem(r, 4, QtWidgets.QTableWidgetItem(item.scryfall_uri))

    def _commit_validated(self) -> None:
        if not self._validated:
            return

        cards = [
            CardIdentity(oracle_id=v.oracle_id, name=v.canonical_name, scryfall_uri=v.scryfall_uri)
            for v in self._validated
        ]
        self._db.upsert_cards(cards)
        for v in self._validated:
            self._db.increment_collection(v.oracle_id, v.line.qty)

        self.refresh_collection()
        QtWidgets.QMessageBox.information(self, "Imported", f"Added/updated {len(self._validated)} row(s).")

    def refresh_collection(self) -> None:
        rows = self._db.list_collection()
        self._collection_table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            self._collection_table.setItem(r, 0, QtWidgets.QTableWidgetItem(str(row["name"])))
            self._collection_table.setItem(r, 1, QtWidgets.QTableWidgetItem(str(row["quantity"])))
            self._collection_table.setItem(r, 2, QtWidgets.QTableWidgetItem(str(row["scryfall_uri"])))

    def _compute_deck_compare(self) -> None:
        self._deck_out.setRowCount(0)
        self._deck_unresolved.clear()

        lines, unresolved = parse_txt(self._deck_input.toPlainText())
        unresolved_msgs: list[str] = []
        if unresolved:
            unresolved_msgs.append("Unresolved parse rows:")
            unresolved_msgs.extend(f"- {u}" for u in unresolved)

        # Validate decklist lines against Scryfall so we can compare by oracle_id
        wanted_by_oracle: dict[str, tuple[str, int]] = {}
        resolved_cards: list[CardIdentity] = []
        for l in lines:
            try:
                card = self._scryfall.lookup_named(l.name)
            except ScryfallError as e:
                unresolved_msgs.append(f"- {l.raw}  ->  {e}")
                continue
            resolved_cards.append(CardIdentity(card.oracle_id, card.name, card.scryfall_uri))
            prev_name, prev_qty = wanted_by_oracle.get(card.oracle_id, (card.name, 0))
            wanted_by_oracle[card.oracle_id] = (prev_name, prev_qty + l.qty)

        if resolved_cards:
            self._db.upsert_cards(resolved_cards)

        owned_by_oracle = self._db.get_owned_by_oracle_id()

        oracle_ids = sorted(wanted_by_oracle.keys(), key=lambda oid: wanted_by_oracle[oid][0].casefold())
        self._deck_out.setRowCount(len(oracle_ids))
        for r, oracle_id in enumerate(oracle_ids):
            name, need = wanted_by_oracle[oracle_id]
            _, have = owned_by_oracle.get(oracle_id, (name, 0))
            missing = max(0, need - have)
            self._deck_out.setItem(r, 0, QtWidgets.QTableWidgetItem(name))
            self._deck_out.setItem(r, 1, QtWidgets.QTableWidgetItem(str(need)))
            self._deck_out.setItem(r, 2, QtWidgets.QTableWidgetItem(str(have)))
            self._deck_out.setItem(r, 3, QtWidgets.QTableWidgetItem(str(missing)))

        self._deck_unresolved.setPlainText("\n".join(unresolved_msgs).strip())


def run_app() -> None:
    app = QtWidgets.QApplication([])
    db = CollectionDb(Path("data/collection.sqlite3"))
    scryfall = ScryfallClient()

    win = MainWindow(db=db, scryfall=scryfall)
    win.show()
    try:
        app.exec()
    finally:
        db.close()

