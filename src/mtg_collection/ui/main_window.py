from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets

from mtg_collection.db import CardIdentity, CollectionDb
from mtg_collection.importer import ImportLine, parse_csv_bytes, parse_txt
from mtg_collection.resolver import ApiOnlyResolver, CardResolver, ResolveResult, build_default_bulk_first_resolver, normalize_card_name
from mtg_collection.scryfall import ScryfallClient, ScryfallError
from mtg_collection.ui.tabs.collection_tab import build_collection_tab
from mtg_collection.ui.tabs.deck_builder_tab import build_deck_builder_tab
from mtg_collection.ui.tabs.deck_tab import build_deck_tab
from mtg_collection.ui.tabs.import_tab import build_import_tab
from mtg_collection.ui.tabs.lent_tab import build_lent_tab
from mtg_collection.ui.theme import DEFAULT_TOKENS, apply_global_theme
from mtg_collection.ui.theme.widgets import action_button, compact_actions_layout, status_label


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
        self._deck_builder_tab = QtWidgets.QWidget()
        self._lent_tab = QtWidgets.QWidget()

        self._tabs.addTab(self._import_tab, "Import")
        self._tabs.addTab(self._collection_tab, "Collection")
        self._tabs.addTab(self._deck_tab, "Deck compare")
        self._tabs.addTab(self._deck_builder_tab, "Deck builder")
        self._tabs.addTab(self._lent_tab, "Lent cards")

        self._build_import_tab()
        self._build_collection_tab()
        self._build_deck_tab()
        self._build_deck_builder_tab()
        self._build_lent_tab()

        self._refresh_decks()
        self.refresh_collection()
        self.refresh_lent_cards()

        self._deck_last_mismatches: list[tuple[str, CardIdentity]] = []

    def _build_import_tab(self) -> None:
        build_import_tab(self, self._import_tab)

    def _build_collection_tab(self) -> None:
        build_collection_tab(self, self._collection_tab)

    def _build_deck_tab(self) -> None:
        build_deck_tab(self, self._deck_tab)

    def _build_deck_builder_tab(self) -> None:
        build_deck_builder_tab(self, self._deck_builder_tab)

    def _build_lent_tab(self) -> None:
        build_lent_tab(self, self._lent_tab)

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
        with self._db._conn:  # Single transaction for all operations
            self._db.upsert_cards(cards)
            self._db.increment_collection_batch([(v.oracle_id, v.line.qty) for v in self._validated])

        self.refresh_collection()
        QtWidgets.QMessageBox.information(self, "Imported", f"Added/updated {len(self._validated)} row(s).")

    def refresh_collection(self) -> None:
        rows = self._db.list_collection()
        lent_summary = self._db.get_lent_summary_by_oracle_id()
        self._collection_rows = [
            {
                "name": str(r["name"]),
                "quantity": int(r["quantity"]),
                "oracle_id": str(r["oracle_id"]),
                "scryfall_uri": str(r["scryfall_uri"]),
                "lent_qty": lent_summary.get(str(r["oracle_id"]), (0, []))[0],
                "in_deck": bool(r["in_deck"]) if "in_deck" in r.keys() else False,
            }
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
        elif sort_col == "Lent":
            filtered.sort(key=lambda r: r.get("lent_qty", 0), reverse=reverse)
        elif sort_col == "Available":
            filtered.sort(key=lambda r: r["quantity"] - r.get("lent_qty", 0), reverse=reverse)
        else:
            filtered.sort(key=lambda r: r["name"].casefold(), reverse=reverse)

        # Populate table (disable sorting temporarily to avoid interference)
        self._collection_table.setSortingEnabled(False)
        self._collection_table.setRowCount(len(filtered))
        for r, row in enumerate(filtered):
            self._collection_table.setItem(r, 0, QtWidgets.QTableWidgetItem(row["name"]))

            owned_item = QtWidgets.QTableWidgetItem()
            owned_item.setData(QtCore.Qt.ItemDataRole.DisplayRole, row["quantity"])
            self._collection_table.setItem(r, 1, owned_item)

            lent_qty = row.get("lent_qty", 0)
            lent_item = QtWidgets.QTableWidgetItem()
            lent_item.setData(QtCore.Qt.ItemDataRole.DisplayRole, lent_qty)
            self._collection_table.setItem(r, 2, lent_item)

            available = row["quantity"] - lent_qty
            avail_item = QtWidgets.QTableWidgetItem()
            avail_item.setData(QtCore.Qt.ItemDataRole.DisplayRole, available)
            self._collection_table.setItem(r, 3, avail_item)

            in_deck_item = QtWidgets.QTableWidgetItem("Yes" if row.get("in_deck", False) else "No")
            self._collection_table.setItem(r, 4, in_deck_item)

            # Actions column with Lent button
            actions_widget = QtWidgets.QWidget()
            actions_layout = compact_actions_layout(actions_widget)

            lent_btn = action_button("Lent")
            lent_btn.setAccessibleName(f"Lend {row['name']}")
            lent_btn.clicked.connect(lambda checked, oid=row["oracle_id"], name=row["name"]: self._quick_lent_dialog(oid, name))
            actions_layout.addWidget(lent_btn)

            actions_layout.addStretch(1)
            self._collection_table.setCellWidget(r, 5, actions_widget)
        self._collection_table.setSortingEnabled(True)

        # Update status
        total = sum(r["quantity"] for r in self._collection_rows)
        shown = sum(r["quantity"] for r in filtered)
        if query:
            self._collection_count_label.setText(f"Showing {shown} of {total} cards")
        else:
            self._collection_count_label.setText(f"{total} cards")

    def _create_deck(self) -> None:
        name = self._deck_builder_name.text().strip()
        if not name:
            QtWidgets.QMessageBox.warning(self, "Validation error", "Please enter a deck name.")
            return

        try:
            deck_id = self._db.create_deck(name)
        except ValueError as e:
            QtWidgets.QMessageBox.warning(self, "Validation error", str(e))
            return

        self._deck_builder_name.clear()
        self._refresh_decks(select_deck_id=deck_id)
        self._deck_builder_status.setPlainText(f"Created deck: {name}")

    def _refresh_decks(self, *, select_deck_id: int | None = None) -> None:
        decks = self._db.list_decks()
        selector = self._deck_builder_selector
        selector.blockSignals(True)
        selector.clear()
        selected_index = -1
        for index, row in enumerate(decks):
            deck_id = int(row["id"])
            selector.addItem(str(row["name"]), deck_id)
            if select_deck_id is not None and deck_id == select_deck_id:
                selected_index = index
        if selected_index >= 0:
            selector.setCurrentIndex(selected_index)
        selector.blockSignals(False)
        self._refresh_selected_deck_cards()

    def _refresh_selected_deck_cards(self) -> None:
        deck_id = self._deck_builder_selector.currentData()
        self._deck_builder_cards.setRowCount(0)
        if deck_id is None:
            self._deck_builder_status.setPlainText("Create a deck to start adding cards.")
            return

        rows = self._db.list_deck_cards(int(deck_id))
        self._deck_builder_cards.setRowCount(len(rows))
        for r, row in enumerate(rows):
            self._deck_builder_cards.setItem(r, 0, QtWidgets.QTableWidgetItem(str(row["name"])))
            qty_item = QtWidgets.QTableWidgetItem()
            qty_item.setData(QtCore.Qt.ItemDataRole.DisplayRole, int(row["quantity"]))
            self._deck_builder_cards.setItem(r, 1, qty_item)

    def _add_card_to_selected_deck(self) -> None:
        deck_id = self._deck_builder_selector.currentData()
        if deck_id is None:
            QtWidgets.QMessageBox.warning(self, "Validation error", "Create or select a deck first.")
            return

        card_name = self._deck_builder_card_name.text().strip()
        if not card_name:
            QtWidgets.QMessageBox.warning(self, "Validation error", "Please enter a card name.")
            return

        try:
            resolved = self._resolver.resolve_name(card_name)
            card = resolved.card
            identity = CardIdentity(
                oracle_id=card.oracle_id,
                name=card.name,
                scryfall_uri=card.scryfall_uri,
            )
            quantity = self._deck_builder_quantity.value()
            self._db.add_card_to_deck(int(deck_id), identity, quantity)
        except ScryfallError as e:
            self._deck_builder_status.setPlainText(f"Could not resolve {card_name!r}: {e}")
            return
        except ValueError as e:
            QtWidgets.QMessageBox.warning(self, "Validation error", str(e))
            return

        self._deck_builder_card_name.clear()
        self._deck_builder_quantity.setValue(1)
        self._refresh_selected_deck_cards()
        self.refresh_collection()
        self._deck_builder_status.setPlainText(f"Added {quantity}x {identity.name}.")

    def _quick_lent_dialog(self, oracle_id: str, card_name: str) -> None:
        """Open a simple dialog to quickly mark a card as lent from the collection view."""
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle(f"Lend: {card_name}")
        layout = QtWidgets.QVBoxLayout(dialog)

        form = QtWidgets.QFormLayout()

        oracle_display = QtWidgets.QLineEdit(oracle_id)
        oracle_display.setReadOnly(True)
        form.addRow("Oracle ID:", oracle_display)

        qty_spin = QtWidgets.QSpinBox()
        qty_spin.setMinimum(1)
        qty_spin.setMaximum(999)
        qty_spin.setValue(1)
        form.addRow("Quantity:", qty_spin)

        borrower_input = QtWidgets.QLineEdit()
        borrower_input.setPlaceholderText("Who are you lending to?")
        form.addRow("Borrower:", borrower_input)

        date_edit = QtWidgets.QDateEdit()
        date_edit.setCalendarPopup(True)
        date_edit.setDate(QtCore.QDate.currentDate())
        date_edit.setDisplayFormat("yyyy-MM-dd")
        form.addRow("Date:", date_edit)

        notes_input = QtWidgets.QLineEdit()
        notes_input.setPlaceholderText("Optional notes")
        form.addRow("Notes:", notes_input)

        layout.addLayout(form)

        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            quantity = qty_spin.value()
            borrower_name = borrower_input.text().strip()
            if not borrower_name:
                QtWidgets.QMessageBox.warning(self, "Validation error", "Please enter a borrower name.")
                return
            lent_date = date_edit.date().toString("yyyy-MM-dd")
            notes = notes_input.text().strip()

            try:
                self._db.lend_card(
                    oracle_id=oracle_id,
                    quantity=quantity,
                    borrower_name=borrower_name,
                    lent_date=lent_date,
                    notes=notes,
                )
                self.refresh_collection()
                self.refresh_lent_cards()
                QtWidgets.QMessageBox.information(self, "Success", f"Lent {quantity}x {card_name} to {borrower_name}.")
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"Failed to lend card:\n{e}")

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
                    self._deck_out.horizontalHeaderItem(i).text() # type: ignore
                    for i in range(self._deck_out.columnCount())
                ]
                writer.writerow(headers)

                for r in range(self._deck_out.rowCount()):
                    if not self._deck_out.isRowHidden(r):
                        row_data = [
                            self._deck_out.item(r, c).text() # type: ignore
                            for c in range(self._deck_out.columnCount())
                        ]
                        writer.writerow(row_data)

            QtWidgets.QMessageBox.information(self, "Export complete", f"Exported to {path}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Export failed", f"Failed to write CSV:\n{e}")

    def _add_lent_card(self) -> None:
        oracle_id = self._lent_oracle_id.text().strip()
        quantity = self._lent_quantity.value()
        borrower_name = self._lent_borrower.text().strip()
        lent_date = self._lent_date.date().toString("yyyy-MM-dd")
        notes = self._lent_notes.text().strip()

        if not oracle_id:
            QtWidgets.QMessageBox.warning(self, "Validation error", "Please enter an oracle_id.")
            return
        if not borrower_name:
            QtWidgets.QMessageBox.warning(self, "Validation error", "Please enter a borrower name.")
            return

        try:
            self._db.lend_card(
                oracle_id=oracle_id,
                quantity=quantity,
                borrower_name=borrower_name,
                lent_date=lent_date,
                notes=notes,
            )
            self._lent_oracle_id.clear()
            self._lent_borrower.clear()
            self._lent_notes.clear()
            self._lent_quantity.setValue(1)
            self.refresh_lent_cards()
            QtWidgets.QMessageBox.information(self, "Success", f"Lent {quantity} card(s) to {borrower_name}.")
        except ValueError as e:
            QtWidgets.QMessageBox.warning(self, "Validation error", str(e))
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to add lent card:\n{e}")

    def refresh_lent_cards(self) -> None:
        include_returned = self._lent_show_returned.isChecked()
        rows = self._db.get_lent_cards(include_returned=include_returned)
        self._lent_rows = [
            {
                "id": int(r["id"]),
                "oracle_id": str(r["oracle_id"]),
                "card_name": str(r["card_name"]),
                "quantity": int(r["quantity"]),
                "borrower_name": str(r["borrower_name"]),
                "lent_date": str(r["lent_date"]),
                "return_date": str(r["return_date"]) if r["return_date"] else "",
                "notes": str(r["notes"]) if r["notes"] else "",
            }
            for r in rows
        ]
        self._populate_lent_table()

    def _populate_lent_table(self) -> None:
        self._lent_table.setRowCount(len(self._lent_rows))
        for r, row in enumerate(self._lent_rows):
            self._lent_table.setItem(r, 0, QtWidgets.QTableWidgetItem(str(row["id"])))
            self._lent_table.setItem(r, 1, QtWidgets.QTableWidgetItem(row["card_name"]))

            qty_item = QtWidgets.QTableWidgetItem(str(row["quantity"]))
            qty_item.setData(QtCore.Qt.ItemDataRole.DisplayRole, row["quantity"])
            self._lent_table.setItem(r, 2, qty_item)

            self._lent_table.setItem(r, 3, QtWidgets.QTableWidgetItem(row["borrower_name"]))
            self._lent_table.setItem(r, 4, QtWidgets.QTableWidgetItem(row["lent_date"]))

            returned_text = row["return_date"] if row["return_date"] else "Not returned"
            returned_item = QtWidgets.QTableWidgetItem(returned_text)
            if row["return_date"]:
                returned_item.setBackground(QtGui.QColor(DEFAULT_TOKENS.colors.success_surface))
            self._lent_table.setItem(r, 5, returned_item)

            # Actions column with Return button
            actions_widget = QtWidgets.QWidget()
            actions_layout = compact_actions_layout(actions_widget)

            if not row["return_date"]:
                return_btn = action_button("Mark returned")
                return_btn.setAccessibleName(
                    f"Mark {row['card_name']} lent to {row['borrower_name']} on {row['lent_date']} returned"
                )
                return_btn.clicked.connect(lambda checked, rid=row["id"]: self._mark_card_returned(rid))
                actions_layout.addWidget(return_btn)
            else:
                returned_label = status_label("Returned", role="success")
                returned_label.setAccessibleName(
                    f"{row['card_name']} lent to {row['borrower_name']} on {row['lent_date']} returned"
                )
                actions_layout.addWidget(returned_label)

            actions_layout.addStretch(1)
            self._lent_table.setCellWidget(r, 6, actions_widget)

    def _mark_card_returned(self, lent_id: int) -> None:
        return_date = QtCore.QDate.currentDate().toString("yyyy-MM-dd")
        reply = QtWidgets.QMessageBox.question(
            self,
            "Confirm Return",
            f"Mark this card as returned on {return_date}?",
            QtWidgets.QMessageBox.StandardButton.Yes | QtWidgets.QMessageBox.StandardButton.No,
            QtWidgets.QMessageBox.StandardButton.No,
        )
        if reply == QtWidgets.QMessageBox.StandardButton.Yes:
            try:
                self._db.return_card(lent_id=lent_id, return_date=return_date)
                self.refresh_lent_cards()
            except Exception as e:
                QtWidgets.QMessageBox.critical(self, "Error", f"Failed to mark as returned:\n{e}")


def run_app() -> None:
    app = QtWidgets.QApplication([])
    apply_global_theme(app)
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
    thread.start()
    try:
        app.exec()
    finally:
        thread.quit()
        thread.wait()
        db.close()

