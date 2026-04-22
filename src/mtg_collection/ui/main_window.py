from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from mtg_collection.db import CardIdentity, CollectionDb
from mtg_collection.importer import ImportLine, parse_csv_bytes, parse_txt
from mtg_collection.resolver import ApiOnlyResolver, CardResolver, ResolveResult, build_default_bulk_first_resolver, normalize_card_name
from mtg_collection.scryfall import ScryfallClient, ScryfallError


@dataclass(frozen=True)
class ResolvedLine:
    line: ImportLine
    oracle_id: str
    canonical_name: str
    scryfall_uri: str


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, db: CollectionDb, resolver: CardResolver):
        super().__init__()
        self._db = db
        self._resolver = resolver
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

        self._deck_last_mismatches: list[tuple[str, CardIdentity]] = []

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

        # --- Search bar ---
        search_row = QtWidgets.QHBoxLayout()
        layout.addLayout(search_row)

        search_row.addWidget(QtWidgets.QLabel("Search:"))
        self._collection_search = QtWidgets.QLineEdit()
        self._collection_search.setPlaceholderText("Filter by card name…")
        self._collection_search.setClearButtonEnabled(True)
        self._collection_search.textChanged.connect(self._filter_collection)
        search_row.addWidget(self._collection_search)

        # --- Sort selector ---
        search_row.addWidget(QtWidgets.QLabel("Sort by:"))
        self._collection_sort_col = QtWidgets.QComboBox()
        self._collection_sort_col.addItems(["Card", "Quantity"])
        search_row.addWidget(self._collection_sort_col)

        self._collection_sort_order = QtWidgets.QComboBox()
        self._collection_sort_order.addItems(["Ascending", "Descending"])
        search_row.addWidget(self._collection_sort_order)

        self._collection_sort_col.currentTextChanged.connect(lambda _: self._apply_collection_sort_and_filter())
        self._collection_sort_order.currentTextChanged.connect(lambda _: self._apply_collection_sort_and_filter())

        # --- Table ---
        self._collection_table = QtWidgets.QTableWidget(0, 4)
        self._collection_table.setHorizontalHeaderLabels(["Card", "Quantity", "Oracle ID", "Scryfall"])
        self._collection_table.horizontalHeader().setStretchLastSection(True)
        self._collection_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self._collection_table.setSortingEnabled(True)
        self._collection_table.horizontalHeader().setSortIndicatorShown(True)
        layout.addWidget(self._collection_table)

        # --- Bottom row ---
        bottom_row = QtWidgets.QHBoxLayout()
        layout.addLayout(bottom_row)

        self._collection_count_label = QtWidgets.QLabel("")
        bottom_row.addWidget(self._collection_count_label)
        bottom_row.addStretch(1)

        refresh = QtWidgets.QPushButton("Refresh")
        refresh.clicked.connect(self.refresh_collection)
        bottom_row.addWidget(refresh)

        # Cache of raw collection data for filtering
        self._collection_rows: list[dict] = []

    def _build_deck_tab(self) -> None:
        layout = QtWidgets.QVBoxLayout(self._deck_tab)

        self._deck_input = QtWidgets.QPlainTextEdit()
        self._deck_input.setPlaceholderText("Paste a target decklist (same TXT format):\n4 Lightning Bolt\n2 Opt")
        layout.addWidget(self._deck_input, 2)

        btn_row = QtWidgets.QHBoxLayout()
        layout.addLayout(btn_row)
        btn_row.addStretch(1)

        self._deck_repair_btn = QtWidgets.QPushButton("Repair mismatches")
        self._deck_repair_btn.setEnabled(False)
        self._deck_repair_btn.clicked.connect(self._repair_deck_mismatches)
        btn_row.addWidget(self._deck_repair_btn)

        btn = QtWidgets.QPushButton("Compute owned vs need")
        btn.clicked.connect(self._compute_deck_compare)
        btn_row.addWidget(btn)

        filter_export_row = QtWidgets.QHBoxLayout()
        layout.addLayout(filter_export_row)

        self._deck_filter = QtWidgets.QComboBox()
        self._deck_filter.addItems(["All", "Missing cards"])
        self._deck_filter.currentTextChanged.connect(self._apply_deck_filter)
        filter_export_row.addWidget(QtWidgets.QLabel("Filter:"))
        filter_export_row.addWidget(self._deck_filter)

        filter_export_row.addStretch(1)

        self._deck_export_btn = QtWidgets.QPushButton("Export to CSV…")
        self._deck_export_btn.setEnabled(False)
        self._deck_export_btn.clicked.connect(self._export_deck_compare)
        filter_export_row.addWidget(self._deck_export_btn)

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

        resolved_cache: dict[tuple[str, str], ResolveResult] = {}
        for line in lines:
            try:
                cache_key = ("id", line.scryfall_id.strip()) if line.scryfall_id else ("name", line.name.strip())
                cached = resolved_cache.get(cache_key)
                if cached is None:
                    cached = self._resolver.resolve_line(line)
                    resolved_cache[cache_key] = cached
                card = cached.card
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
        self._collection_rows = [
            {"name": str(r["name"]), "quantity": int(r["quantity"]),
             "oracle_id": str(r["oracle_id"]), "scryfall_uri": str(r["scryfall_uri"])}
            for r in rows
        ]
        self._apply_collection_sort_and_filter()

    def _filter_collection(self, _text: str | None = None) -> None:
        self._apply_collection_sort_and_filter()

    def _apply_collection_sort_and_filter(self) -> None:
        query = self._collection_search.text().strip().casefold()

        # Filter
        if query:
            filtered = [r for r in self._collection_rows if query in r["name"].casefold()]
        else:
            filtered = list(self._collection_rows)

        # Sort
        sort_col = self._collection_sort_col.currentText()
        reverse = self._collection_sort_order.currentText() == "Descending"

        if sort_col == "Quantity":
            filtered.sort(key=lambda r: r["quantity"], reverse=reverse)
        else:
            filtered.sort(key=lambda r: r["name"].casefold(), reverse=reverse)

        # Populate table (disable sorting temporarily to avoid interference)
        self._collection_table.setSortingEnabled(False)
        self._collection_table.setRowCount(len(filtered))
        for r, row in enumerate(filtered):
            self._collection_table.setItem(r, 0, QtWidgets.QTableWidgetItem(row["name"]))

            qty_item = QtWidgets.QTableWidgetItem()
            qty_item.setData(QtCore.Qt.ItemDataRole.DisplayRole, row["quantity"])
            self._collection_table.setItem(r, 1, qty_item)

            self._collection_table.setItem(r, 2, QtWidgets.QTableWidgetItem(row["oracle_id"]))
            self._collection_table.setItem(r, 3, QtWidgets.QTableWidgetItem(row["scryfall_uri"]))
        self._collection_table.setSortingEnabled(True)

        # Update status
        total = len(self._collection_rows)
        shown = len(filtered)
        if query:
            self._collection_count_label.setText(f"Showing {shown} of {total} cards")
        else:
            self._collection_count_label.setText(f"{total} cards")

    def _compute_deck_compare(self) -> None:
        self._deck_out.setRowCount(0)
        self._deck_unresolved.clear()
        self._deck_last_mismatches = []
        self._deck_repair_btn.setEnabled(False)
        self._deck_export_btn.setEnabled(False)

        lines, unresolved = parse_txt(self._deck_input.toPlainText())
        unresolved_msgs: list[str] = []
        if unresolved:
            unresolved_msgs.append("Unresolved parse rows:")
            unresolved_msgs.extend(f"- {u}" for u in unresolved)

        # Validate decklist lines against Scryfall so we can compare by oracle_id
        wanted_by_oracle: dict[str, tuple[str, int]] = {}
        resolved_cards: list[CardIdentity] = []
        resolved_cards_by_oracle: dict[str, CardIdentity] = {}
        resolved_cache: dict[str, ResolveResult] = {}
        for l in lines:
            try:
                key = l.name.strip()
                cached = resolved_cache.get(key)
                if cached is None:
                    cached = self._resolver.resolve_name(l.name)
                    resolved_cache[key] = cached
                card = cached.card
            except ScryfallError as e:
                unresolved_msgs.append(f"- {l.raw}  ->  {e}")
                continue
            ident = CardIdentity(card.oracle_id, card.name, card.scryfall_uri)
            resolved_cards.append(ident)
            resolved_cards_by_oracle[card.oracle_id] = ident
            prev_name, prev_qty = wanted_by_oracle.get(card.oracle_id, (card.name, 0))
            wanted_by_oracle[card.oracle_id] = (prev_name, prev_qty + l.qty)

        if resolved_cards:
            self._db.upsert_cards(resolved_cards)

        owned_by_oracle = self._db.get_owned_by_oracle_id()
        owned_by_norm_name = self._db.get_owned_by_normalized_name()

        oracle_ids = sorted(wanted_by_oracle.keys(), key=lambda oid: wanted_by_oracle[oid][0].casefold())
        self._deck_out.setRowCount(len(oracle_ids))
        for r, oracle_id in enumerate(oracle_ids):
            name, need = wanted_by_oracle[oracle_id]
            _, have = owned_by_oracle.get(oracle_id, (name, 0))
            if have <= 0:
                k = normalize_card_name(name)
                candidates = owned_by_norm_name.get(k, [])
                if candidates:
                    have = sum(qty for _, _, qty in candidates)
                    if len(candidates) == 1:
                        cand_oracle, cand_name, cand_qty = candidates[0]
                        if cand_oracle != oracle_id and cand_qty > 0:
                            to_card = resolved_cards_by_oracle.get(oracle_id)
                            if to_card is not None:
                                self._deck_last_mismatches.append((cand_oracle, to_card))
                            unresolved_msgs.append(
                                f"- Oracle ID mismatch for {cand_name!r}: collection has {cand_oracle}, deck resolves to {oracle_id}. Using name fallback (owned={cand_qty})."
                            )
                    else:
                        unresolved_msgs.append(
                            f"- Ambiguous name match for {name!r}: found {len(candidates)} owned entries with that name; using sum={have} but cannot auto-repair."
                        )
            missing = max(0, need - have)
            self._deck_out.setItem(r, 0, QtWidgets.QTableWidgetItem(name))
            self._deck_out.setItem(r, 1, QtWidgets.QTableWidgetItem(str(need)))
            self._deck_out.setItem(r, 2, QtWidgets.QTableWidgetItem(str(have)))
            self._deck_out.setItem(r, 3, QtWidgets.QTableWidgetItem(str(missing)))

        self._deck_unresolved.setPlainText("\n".join(unresolved_msgs).strip())
        self._deck_repair_btn.setEnabled(len(self._deck_last_mismatches) > 0)
        self._deck_export_btn.setEnabled(self._deck_out.rowCount() > 0)
        self._apply_deck_filter()

    def _repair_deck_mismatches(self) -> None:
        if not self._deck_last_mismatches:
            return

        # Dedupe repairs: (from_oracle_id, to_oracle_id)
        unique: dict[tuple[str, str], CardIdentity] = {}
        for from_oracle, to_card in self._deck_last_mismatches:
            unique[(from_oracle, to_card.oracle_id)] = to_card

        repaired = 0
        for (from_oracle, _), to_card in unique.items():
            if from_oracle == to_card.oracle_id:
                continue
            self._db.move_collection_quantity(from_oracle_id=from_oracle, to_card=to_card)
            repaired += 1

        self.refresh_collection()
        self._compute_deck_compare()
        QtWidgets.QMessageBox.information(self, "Repair complete", f"Repaired {repaired} mismatched entr(y/ies).")

    def _apply_deck_filter(self) -> None:
        mode = self._deck_filter.currentText()
        for r in range(self._deck_out.rowCount()):
            show = True
            if mode == "Missing cards":
                missing_item = self._deck_out.item(r, 3)
                if missing_item and int(missing_item.text()) <= 0:
                    show = False
            self._deck_out.setRowHidden(r, not show)

    def _export_deck_compare(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Export CSV", "", "CSV Files (*.csv);;All Files (*)"
        )
        if not path:
            return

        import csv

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                headers = [
                    self._deck_out.horizontalHeaderItem(i).text()
                    for i in range(self._deck_out.columnCount())
                ]
                writer.writerow(headers)

                for r in range(self._deck_out.rowCount()):
                    if not self._deck_out.isRowHidden(r):
                        row_data = [
                            self._deck_out.item(r, c).text()
                            for c in range(self._deck_out.columnCount())
                        ]
                        writer.writerow(row_data)

            QtWidgets.QMessageBox.information(self, "Export complete", f"Exported to {path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Export failed", f"Failed to write CSV:\n{e}")


def run_app() -> None:
    app = QtWidgets.QApplication([])
    db = CollectionDb(Path("data/collection.sqlite3"))
    api = ScryfallClient()

    progress = QtWidgets.QProgressDialog("Preparing card database…", "", 0, 0)
    progress.setWindowTitle("MTG Collection")
    progress.setCancelButton(None)
    progress.setMinimumDuration(0)
    progress.setWindowModality(QtCore.Qt.WindowModality.ApplicationModal)
    progress.show()

    win_holder: dict[str, MainWindow] = {}

    class _BootstrapWorker(QtCore.QObject):
        finished = QtCore.Signal(object)
        failed = QtCore.Signal(str)

        @QtCore.Slot()
        def run(self) -> None:
            try:
                _, _, resolver = build_default_bulk_first_resolver(api)
            except Exception as e:
                self.failed.emit(str(e))
                return
            self.finished.emit(resolver)

    thread = QtCore.QThread()
    worker = _BootstrapWorker()
    worker.moveToThread(thread)
    thread.started.connect(worker.run)

    def _start_with_resolver(resolver: CardResolver) -> None:
        progress.close()
        win = MainWindow(db=db, resolver=resolver)
        win_holder["win"] = win
        win.show()

    class _BootstrapController(QtCore.QObject):
        def __init__(self) -> None:
            super().__init__()
            self._resolver: CardResolver | None = None
            self._error: str | None = None

        @QtCore.Slot(object)
        def on_ok(self, resolver_obj: object) -> None:
            self._resolver = resolver_obj  # type: ignore[assignment]
            thread.quit()

        @QtCore.Slot(str)
        def on_failed(self, msg: str) -> None:
            self._error = msg
            thread.quit()

        @QtCore.Slot()
        def on_thread_finished(self) -> None:
            if self._resolver is not None:
                _start_with_resolver(self._resolver)
                return

            progress.close()
            QtWidgets.QMessageBox.warning(
                None,
                "Scryfall bulk data unavailable",
                "Could not prepare the local card database.\n\n"
                f"Reason: {self._error or 'Unknown error'}\n\n"
                "The app will continue using throttled online lookups.",
            )
            _start_with_resolver(ApiOnlyResolver(api))

    controller = _BootstrapController()

    # Force queued delivery so slots run on the controller's (GUI) thread.
    worker.finished.connect(controller.on_ok, QtCore.Qt.ConnectionType.QueuedConnection)
    worker.failed.connect(controller.on_failed, QtCore.Qt.ConnectionType.QueuedConnection)
    thread.finished.connect(controller.on_thread_finished, QtCore.Qt.ConnectionType.QueuedConnection)

    thread.finished.connect(worker.deleteLater)
    thread.finished.connect(thread.deleteLater)
    thread.start()
    try:
        app.exec()
    finally:
        thread.quit()
        thread.wait()
        db.close()

