# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import db
import psycopg
from PySide6 import QtCore, QtWidgets


DB_PATH = Path(__file__).resolve().parent / "fraccionadora.db"
EDIT_PASSWORD = "ALFAOMEGA"


class BancosRepo:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = Path(db_path)
        self.cn = db.connect("fraccionadora")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cur = self.cn.cursor()
        db.run_ddl(self.cn, 
            """
            CREATE TABLE IF NOT EXISTS banks(
                id BIGSERIAL PRIMARY KEY,
                bank_id TEXT NOT NULL UNIQUE,
                banco_nombre TEXT NOT NULL,
                nro_cuenta TEXT NOT NULL,
                resumen TEXT,
                ts_registro TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_banks_nombre_cuenta
                ON banks(banco_nombre, nro_cuenta);

            CREATE TABLE IF NOT EXISTS bank_checkbooks(
                id BIGSERIAL PRIMARY KEY,
                chequera_id TEXT NOT NULL UNIQUE,
                bank_id TEXT NOT NULL,
                formato_chequera TEXT NOT NULL DEFAULT 'Formulario',
                tipo_cheque TEXT NOT NULL DEFAULT 'Vista',
                serie TEXT NOT NULL DEFAULT '',
                fecha_recibimiento TEXT NOT NULL DEFAULT '',
                nro_inicio INTEGER NOT NULL,
                nro_fin INTEGER NOT NULL,
                recibido_por TEXT NOT NULL,
                resumen TEXT,
                ts_registro TEXT NOT NULL,
                FOREIGN KEY(bank_id) REFERENCES banks(bank_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_checkbooks_unique
                ON bank_checkbooks(bank_id, formato_chequera, tipo_cheque, serie, nro_inicio, nro_fin);
            """
        )
        self._migrate_checkbooks_add_fecha_recibimiento()
        self._migrate_checkbooks_add_serie()
        self._migrate_checkbooks_add_format_type()
        self._migrate_legacy_checkbooks()
        self.cn.commit()

    def _table_columns(self, table_name: str) -> set[str]:
        cur = self.cn.cursor()
        rows = [{"name": r[0]} for r in db.table_columns(self.cn, {table_name})]
        return {str(row["name"]) for row in rows}

    def _migrate_legacy_checkbooks(self) -> None:
        cols = self._table_columns("bank_checkbooks")
        if not cols:
            return
        if "bank_id" in cols and "formato_chequera" in cols and "tipo_cheque" in cols and "serie" in cols:
            return
        legacy_required = {
            "chequera_id",
            "banco_nombre",
            "nro_cuenta",
            "formulario_tipo",
            "nro_inicio",
            "nro_fin",
            "recibido_por",
            "resumen",
            "ts_registro",
        }
        if not legacy_required.issubset(cols):
            return

        cur = self.cn.cursor()
        legacy_rows = cur.execute(
            """
            SELECT chequera_id, banco_nombre, nro_cuenta, formulario_tipo,
                   nro_inicio, nro_fin, recibido_por, resumen, ts_registro
            FROM bank_checkbooks;
            """
        ).fetchall()

        cur.execute("ALTER TABLE bank_checkbooks RENAME TO bank_checkbooks_legacy;")
        db.run_ddl(self.cn, 
            """
            CREATE TABLE bank_checkbooks(
                id BIGSERIAL PRIMARY KEY,
                chequera_id TEXT NOT NULL UNIQUE,
                bank_id TEXT NOT NULL,
                formato_chequera TEXT NOT NULL DEFAULT 'Formulario',
                tipo_cheque TEXT NOT NULL DEFAULT 'Vista',
                serie TEXT NOT NULL DEFAULT '',
                nro_inicio INTEGER NOT NULL,
                nro_fin INTEGER NOT NULL,
                recibido_por TEXT NOT NULL,
                resumen TEXT,
                ts_registro TEXT NOT NULL,
                FOREIGN KEY(bank_id) REFERENCES banks(bank_id)
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_checkbooks_unique
                ON bank_checkbooks(bank_id, formato_chequera, tipo_cheque, serie, nro_inicio, nro_fin);
            """
        )

        for row in legacy_rows:
            banco_nombre = str(row["banco_nombre"] or "").strip()
            nro_cuenta = str(row["nro_cuenta"] or "").strip()
            bank_id = self.build_bank_id(banco_nombre, nro_cuenta)
            cur.execute(
                """
                INSERT INTO banks(bank_id, banco_nombre, nro_cuenta, resumen, ts_registro)
                VALUES(%s, %s, %s, %s, %s)
                ON CONFLICT(bank_id) DO NOTHING;
                """,
                (
                    bank_id,
                    banco_nombre,
                    nro_cuenta,
                    "",
                    str(row["ts_registro"] or datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ),
            )
            legacy_form = str(row["formulario_tipo"] or "").strip().lower()
            formato_chequera = "Talonario" if legacy_form == "talonario" else "Formulario"
            tipo_cheque = "Diferido" if legacy_form == "diferido" else "Vista"
            cur.execute(
                """
                INSERT INTO bank_checkbooks(
                    chequera_id, bank_id, formato_chequera, tipo_cheque, serie,
                    nro_inicio, nro_fin, recibido_por, resumen, ts_registro
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    str(row["chequera_id"] or "").strip()
                    or self.build_chequera_id(
                        bank_id,
                        formato_chequera,
                        tipo_cheque,
                        "",
                        int(row["nro_inicio"] or 0),
                        int(row["nro_fin"] or 0),
                    ),
                    bank_id,
                    formato_chequera,
                    tipo_cheque,
                    "",
                    int(row["nro_inicio"] or 0),
                    int(row["nro_fin"] or 0),
                    str(row["recibido_por"] or "").strip(),
                    str(row["resumen"] or "").strip(),
                    str(row["ts_registro"] or datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                ),
            )

        cur.execute("DROP TABLE bank_checkbooks_legacy;")

    def _migrate_checkbooks_add_format_type(self) -> None:
        cols = self._table_columns("bank_checkbooks")
        if not cols or "bank_id" not in cols:
            return
        if "formato_chequera" in cols and "tipo_cheque" in cols:
            return
        if "formulario_tipo" not in cols:
            return

        cur = self.cn.cursor()
        rows = cur.execute(
            """
            SELECT id, formulario_tipo
            FROM bank_checkbooks;
            """
        ).fetchall()
        if "formato_chequera" not in cols:
            cur.execute("ALTER TABLE bank_checkbooks ADD COLUMN formato_chequera TEXT NOT NULL DEFAULT 'Formulario';")
        if "tipo_cheque" not in cols:
            cur.execute("ALTER TABLE bank_checkbooks ADD COLUMN tipo_cheque TEXT NOT NULL DEFAULT 'Vista';")
        for row in rows:
            legacy_form = str(row["formulario_tipo"] or "").strip().lower()
            formato_chequera = "Talonario" if legacy_form == "talonario" else "Formulario"
            tipo_cheque = "Diferido" if legacy_form == "diferido" else "Vista"
            cur.execute(
                """
                UPDATE bank_checkbooks
                SET formato_chequera = %s, tipo_cheque = %s
                WHERE id = %s;
                """,
                (formato_chequera, tipo_cheque, int(row["id"])),
            )
        cur.execute("DROP INDEX IF EXISTS idx_bank_checkbooks_unique;")
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_checkbooks_unique
                ON bank_checkbooks(bank_id, formato_chequera, tipo_cheque, serie, nro_inicio, nro_fin);
            """
        )

    def _migrate_checkbooks_add_serie(self) -> None:
        cols = self._table_columns("bank_checkbooks")
        if not cols:
            return
        if "serie" in cols:
            return
        cur = self.cn.cursor()
        cur.execute("ALTER TABLE bank_checkbooks ADD COLUMN serie TEXT NOT NULL DEFAULT '';")
        cur.execute("DROP INDEX IF EXISTS idx_bank_checkbooks_unique;")
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_checkbooks_unique
                ON bank_checkbooks(bank_id, formato_chequera, tipo_cheque, serie, nro_inicio, nro_fin);
            """
        )

    def _migrate_checkbooks_add_fecha_recibimiento(self) -> None:
        cols = self._table_columns("bank_checkbooks")
        if not cols:
            return
        if "fecha_recibimiento" in cols:
            return
        cur = self.cn.cursor()
        cur.execute("ALTER TABLE bank_checkbooks ADD COLUMN fecha_recibimiento TEXT NOT NULL DEFAULT '';")

    def _legacy_formulario_tipo(self, formato_chequera: str, tipo_cheque: str) -> str:
        tipo = str(tipo_cheque or "").strip().lower()
        formato = str(formato_chequera or "").strip().lower()
        if tipo == "diferido":
            return "diferido"
        if formato == "talonario":
            return "talonario"
        return "formulario"

    @staticmethod
    def _norm_token(value: str) -> str:
        text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        text = re.sub(r"[^a-z0-9]+", "-", text)
        return re.sub(r"-+", "-", text).strip("-")

    def build_bank_id(self, banco_nombre: str, nro_cuenta: str) -> str:
        banco = self._norm_token(banco_nombre) or "sin-banco"
        cuenta = self._norm_token(nro_cuenta) or "sin-cuenta"
        return f"banco-{banco}-{cuenta}"

    def build_chequera_id(
        self,
        bank_id: str,
        formato_chequera: str,
        tipo_cheque: str,
        serie: str,
        nro_inicio: int,
        nro_fin: int,
    ) -> str:
        formato = self._norm_token(formato_chequera) or "sin-formato"
        tipo = self._norm_token(tipo_cheque) or "sin-tipo"
        serie_token = self._norm_token(serie) or "sin-serie"
        return f"{bank_id}-chequera-{formato}-{tipo}-{serie_token}-{int(nro_inicio)}-{int(nro_fin)}"

    def add_bank(self, banco_nombre: str, nro_cuenta: str, resumen: str) -> None:
        cur = self.cn.cursor()
        cur.execute(
            """
            INSERT INTO banks(bank_id, banco_nombre, nro_cuenta, resumen, ts_registro)
            VALUES(%s, %s, %s, %s, %s);
            """,
            (
                self.build_bank_id(banco_nombre, nro_cuenta),
                str(banco_nombre).strip(),
                str(nro_cuenta).strip(),
                str(resumen or "").strip(),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        self.cn.commit()

    def list_banks(self) -> list:
        cur = self.cn.cursor()
        return cur.execute(
            """
            SELECT bank_id, banco_nombre, nro_cuenta, resumen, ts_registro
            FROM banks
            ORDER BY banco_nombre ASC, nro_cuenta ASC, id ASC;
            """
        ).fetchall()

    def has_overlap(
        self,
        bank_id: str,
        formato_chequera: str,
        tipo_cheque: str,
        serie: str,
        nro_inicio: int,
        nro_fin: int,
        exclude_chequera_id: str = "",
    ) -> bool:
        cur = self.cn.cursor()
        sql = """
            SELECT 1
            FROM bank_checkbooks
            WHERE bank_id = %s
              AND formato_chequera = %s
              AND tipo_cheque = %s
              AND serie = %s
              AND NOT (nro_fin < %s OR nro_inicio > %s)
        """
        params: list[object] = [
            str(bank_id).strip(),
            str(formato_chequera).strip(),
            str(tipo_cheque).strip(),
            str(serie).strip(),
            int(nro_inicio),
            int(nro_fin),
        ]
        if str(exclude_chequera_id or "").strip():
            sql += " AND chequera_id <> %s"
            params.append(str(exclude_chequera_id).strip())
        sql += " LIMIT 1;"
        row = cur.execute(sql, tuple(params)).fetchone()
        return row is not None

    def add_checkbook(
        self,
        bank_id: str,
        formato_chequera: str,
        tipo_cheque: str,
        serie: str,
        fecha_recibimiento: str,
        nro_inicio: int,
        nro_fin: int,
        recibido_por: str,
        resumen: str,
    ) -> None:
        cur = self.cn.cursor()
        cols = self._table_columns("bank_checkbooks")
        values = (
            self.build_chequera_id(bank_id, formato_chequera, tipo_cheque, serie, nro_inicio, nro_fin),
            str(bank_id).strip(),
            str(formato_chequera).strip(),
            str(tipo_cheque).strip(),
            str(serie).strip(),
            str(fecha_recibimiento).strip(),
            int(nro_inicio),
            int(nro_fin),
            str(recibido_por).strip(),
            str(resumen or "").strip(),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        if "formulario_tipo" in cols:
            cur.execute(
                """
                INSERT INTO bank_checkbooks(
                    chequera_id, bank_id, formulario_tipo, formato_chequera, tipo_cheque, serie, fecha_recibimiento,
                    nro_inicio, nro_fin, recibido_por, resumen, ts_registro
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    values[0],
                    values[1],
                    self._legacy_formulario_tipo(formato_chequera, tipo_cheque),
                    values[2],
                    values[3],
                    values[4],
                    values[5],
                    values[6],
                    values[7],
                    values[8],
                    values[9],
                    values[10],
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO bank_checkbooks(
                    chequera_id, bank_id, formato_chequera, tipo_cheque, serie, fecha_recibimiento,
                    nro_inicio, nro_fin, recibido_por, resumen, ts_registro
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                values,
            )
        self.cn.commit()

    def update_checkbook(
        self,
        original_chequera_id: str,
        bank_id: str,
        formato_chequera: str,
        tipo_cheque: str,
        serie: str,
        fecha_recibimiento: str,
        nro_inicio: int,
        nro_fin: int,
        recibido_por: str,
        resumen: str,
    ) -> None:
        cur = self.cn.cursor()
        cols = self._table_columns("bank_checkbooks")
        new_chequera_id = self.build_chequera_id(bank_id, formato_chequera, tipo_cheque, serie, nro_inicio, nro_fin)
        if "formulario_tipo" in cols:
            cur.execute(
                """
                UPDATE bank_checkbooks
                SET chequera_id = %s, bank_id = %s, formulario_tipo = %s, formato_chequera = %s, tipo_cheque = %s,
                    serie = %s, fecha_recibimiento = %s, nro_inicio = %s, nro_fin = %s, recibido_por = %s, resumen = %s
                WHERE chequera_id = %s;
                """,
                (
                    new_chequera_id,
                    str(bank_id).strip(),
                    self._legacy_formulario_tipo(formato_chequera, tipo_cheque),
                    str(formato_chequera).strip(),
                    str(tipo_cheque).strip(),
                    str(serie).strip(),
                    str(fecha_recibimiento).strip(),
                    int(nro_inicio),
                    int(nro_fin),
                    str(recibido_por).strip(),
                    str(resumen or "").strip(),
                    str(original_chequera_id).strip(),
                ),
            )
        else:
            cur.execute(
                """
                UPDATE bank_checkbooks
                SET chequera_id = %s, bank_id = %s, formato_chequera = %s, tipo_cheque = %s,
                    serie = %s, fecha_recibimiento = %s, nro_inicio = %s, nro_fin = %s, recibido_por = %s, resumen = %s
                WHERE chequera_id = %s;
                """,
                (
                    new_chequera_id,
                    str(bank_id).strip(),
                    str(formato_chequera).strip(),
                    str(tipo_cheque).strip(),
                    str(serie).strip(),
                    str(fecha_recibimiento).strip(),
                    int(nro_inicio),
                    int(nro_fin),
                    str(recibido_por).strip(),
                    str(resumen or "").strip(),
                    str(original_chequera_id).strip(),
                ),
            )
        self.cn.commit()

    def list_checkbooks(self) -> list:
        cur = self.cn.cursor()
        return cur.execute(
            """
            SELECT c.chequera_id, c.bank_id, b.banco_nombre, b.nro_cuenta,
                   c.formato_chequera, c.tipo_cheque, c.serie, c.fecha_recibimiento, c.nro_inicio, c.nro_fin,
                   c.recibido_por, c.resumen, c.ts_registro
            FROM bank_checkbooks c
            JOIN banks b ON b.bank_id = c.bank_id
            ORDER BY c.ts_registro DESC, c.id DESC;
            """
        ).fetchall()

    def close(self) -> None:
        try:
            self.cn.close()
        except Exception:
            pass


class BancosWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.repo = BancosRepo()
        self.bank_lookup: dict[str, dict[str, str]] = {}

        self.setWindowTitle("Bancos y chequeras")
        self.resize(1180, 760)
        self._apply_light_theme()

        self.status_label = QtWidgets.QLabel("Listo")
        self.statusBar().addWidget(self.status_label)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        top = QtWidgets.QHBoxLayout()
        title = QtWidgets.QLabel("Bancos y chequeras recibidas")
        font = title.font()
        font.setPointSize(font.pointSize() + 2)
        font.setBold(True)
        title.setFont(font)
        top.addWidget(title)
        top.addStretch(1)
        btn_refresh = QtWidgets.QPushButton("Refrescar")
        btn_refresh.clicked.connect(self._refresh_all)
        top.addWidget(btn_refresh)
        layout.addLayout(top)

        self.main_splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        self.main_splitter.setChildrenCollapsible(False)
        self.main_splitter.setHandleWidth(14)
        self.main_splitter.setOpaqueResize(False)
        layout.addWidget(self.main_splitter, 1)

        banks_panel = QtWidgets.QWidget()
        banks_layout = QtWidgets.QVBoxLayout(banks_panel)
        banks_layout.setContentsMargins(0, 0, 0, 0)
        banks_layout.setSpacing(8)

        grp_bank = QtWidgets.QGroupBox("1. Crear banco")
        form_bank = QtWidgets.QFormLayout(grp_bank)
        form_bank.setLabelAlignment(QtCore.Qt.AlignRight)

        self.ent_banco = QtWidgets.QLineEdit()
        self.ent_banco.setPlaceholderText("Nombre del banco")
        form_bank.addRow("Banco:", self.ent_banco)

        self.ent_cuenta = QtWidgets.QLineEdit()
        self.ent_cuenta.setPlaceholderText("Numero de cuenta")
        form_bank.addRow("Nro de cuenta:", self.ent_cuenta)

        self.ent_bank_resumen = QtWidgets.QPlainTextEdit()
        self.ent_bank_resumen.setPlaceholderText("Resumen del banco o de la cuenta")
        self.ent_bank_resumen.setFixedHeight(80)
        form_bank.addRow("Resumen:", self.ent_bank_resumen)

        self.ent_bank_id = QtWidgets.QLineEdit()
        self.ent_bank_id.setReadOnly(True)
        form_bank.addRow("ID banco:", self.ent_bank_id)

        bank_btns = QtWidgets.QHBoxLayout()
        bank_btns.addStretch(1)
        btn_bank_clear = QtWidgets.QPushButton("Limpiar")
        btn_bank_save = QtWidgets.QPushButton("Guardar banco")
        bank_btns.addWidget(btn_bank_clear)
        bank_btns.addWidget(btn_bank_save)
        form_bank.addRow("", bank_btns)

        grp_bank.setVisible(False)
        banks_layout.addWidget(grp_bank)

        self.tbl_banks = QtWidgets.QTableWidget(0, 5)
        self.tbl_banks.setHorizontalHeaderLabels(
            ["ID banco", "Banco", "Nro cuenta", "Resumen", "Registro"]
        )
        self.tbl_banks.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_banks.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.tbl_banks.setAlternatingRowColors(True)
        self.tbl_banks.verticalHeader().setVisible(False)
        self.tbl_banks.horizontalHeader().setStretchLastSection(True)
        banks_layout.addWidget(self.tbl_banks, 1)

        bank_actions = QtWidgets.QHBoxLayout()
        self.btn_add_bank = QtWidgets.QPushButton("Agregar banco")
        self.btn_open_checkbooks = QtWidgets.QPushButton("Chequeras")
        self.btn_open_checkbooks.setEnabled(False)
        bank_actions.addStretch(1)
        bank_actions.addWidget(self.btn_add_bank)
        bank_actions.addWidget(self.btn_open_checkbooks)
        banks_layout.addLayout(bank_actions)

        self.main_splitter.addWidget(banks_panel)

        self.checkbooks_panel = QtWidgets.QWidget()
        checkbooks_layout = QtWidgets.QVBoxLayout(self.checkbooks_panel)
        checkbooks_layout.setContentsMargins(0, 0, 0, 0)
        checkbooks_layout.setSpacing(8)

        self.lbl_selected_bank = QtWidgets.QLabel("Seleccione un banco para ver sus chequeras.")
        checkbooks_layout.addWidget(self.lbl_selected_bank)

        self.tbl_checkbooks = QtWidgets.QTableWidget(0, 13)
        self.tbl_checkbooks.setHorizontalHeaderLabels(
            [
                "ID chequera",
                "ID banco",
                "Banco",
                "Nro cuenta",
                "Formato",
                "Tipo",
                "Serie",
                "Fecha recibimiento",
                "Nro inicio",
                "Nro fin",
                "Recibido por",
                "Resumen",
                "Registro",
            ]
        )
        self.tbl_checkbooks.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl_checkbooks.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.tbl_checkbooks.setAlternatingRowColors(True)
        self.tbl_checkbooks.verticalHeader().setVisible(False)
        self.tbl_checkbooks.setWordWrap(False)
        self.tbl_checkbooks.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.tbl_checkbooks.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        self.tbl_checkbooks.horizontalHeader().setStretchLastSection(True)
        self.tbl_checkbooks.verticalHeader().setDefaultSectionSize(26)

        checkbooks_layout.addWidget(self.tbl_checkbooks, 1)

        self.checkbook_actions = QtWidgets.QHBoxLayout()
        self.btn_add_checkbook = QtWidgets.QPushButton("Agregar")
        self.btn_edit_checkbook = QtWidgets.QPushButton("Editar")
        self.btn_add_checkbook.setEnabled(False)
        self.btn_edit_checkbook.setEnabled(False)
        self.checkbook_actions.addStretch(1)
        self.checkbook_actions.addWidget(self.btn_add_checkbook)
        self.checkbook_actions.addWidget(self.btn_edit_checkbook)
        checkbooks_layout.addLayout(self.checkbook_actions)

        self.main_splitter.addWidget(self.checkbooks_panel)
        self.main_splitter.setSizes([420, 760])
        self.checkbooks_panel.setVisible(False)

        self.ent_banco.textChanged.connect(self._sync_bank_id)
        self.ent_cuenta.textChanged.connect(self._sync_bank_id)
        btn_bank_save.clicked.connect(self._save_bank)
        btn_bank_clear.clicked.connect(self._clear_bank_form)
        self.btn_add_bank.clicked.connect(self._open_add_bank_dialog)
        self.btn_open_checkbooks.clicked.connect(self._open_checkbooks_for_selected_bank)
        self.btn_add_checkbook.clicked.connect(self._open_add_checkbook_dialog)
        self.btn_edit_checkbook.clicked.connect(self._edit_checkbook)
        self.tbl_banks.itemSelectionChanged.connect(self._on_bank_selection_changed)
        self.tbl_checkbooks.itemSelectionChanged.connect(self._on_checkbook_selection_changed)

        self._refresh_all()

    def _apply_light_theme(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow, QWidget {
                background: #f6f8fb;
                color: #17324d;
            }
            QGroupBox {
                background: #ffffff;
                border: 1px solid #d6e2f0;
                border-radius: 10px;
                margin-top: 14px;
                padding-top: 10px;
                font-weight: 600;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 6px;
                color: #1d4f7a;
                background: #f6f8fb;
            }
            QLabel {
                color: #24415c;
            }
            QLineEdit, QPlainTextEdit, QComboBox, QDateEdit, QSpinBox, QTableWidget {
                background: #ffffff;
                border: 1px solid #cddceb;
                border-radius: 8px;
                padding: 5px 7px;
                selection-background-color: #cfe5ff;
                selection-color: #17324d;
            }
            QLineEdit:focus, QPlainTextEdit:focus, QComboBox:focus, QDateEdit:focus, QSpinBox:focus {
                border: 1px solid #6aa9e9;
            }
            QTableWidget {
                gridline-color: #e3edf7;
                alternate-background-color: #f8fbff;
            }
            QHeaderView::section {
                background: #eaf3fb;
                color: #1d4f7a;
                border: none;
                border-right: 1px solid #d6e2f0;
                border-bottom: 1px solid #d6e2f0;
                padding: 6px;
                font-weight: 600;
            }
            QPushButton {
                background: #dcebfa;
                color: #1c4567;
                border: 1px solid #bdd4ea;
                border-radius: 8px;
                padding: 7px 12px;
            }
            QPushButton:hover {
                background: #cfe5fb;
            }
            QPushButton:pressed {
                background: #bddcf8;
            }
            QPushButton:disabled {
                background: #edf2f7;
                color: #8aa0b5;
                border-color: #dde6ef;
            }
            QSplitter::handle {
                background: #dbe7f3;
                margin: 2px;
                border-radius: 4px;
            }
            QScrollArea {
                border: none;
                background: transparent;
            }
            QStatusBar {
                background: #eef4fa;
                color: #35536f;
            }
            """
        )

    def _set_item(
        self,
        table: QtWidgets.QTableWidget,
        row: int,
        col: int,
        text: str,
        align: QtCore.Qt.AlignmentFlag = QtCore.Qt.AlignLeft,
    ) -> None:
        item = QtWidgets.QTableWidgetItem(text)
        item.setTextAlignment(align | QtCore.Qt.AlignVCenter)
        table.setItem(row, col, item)

    def _sync_bank_id(self) -> None:
        self.ent_bank_id.setText(
            self.repo.build_bank_id(self.ent_banco.text(), self.ent_cuenta.text())
        )

    def _current_bank_id(self) -> str:
        row = self.tbl_banks.currentRow()
        if row < 0:
            return ""
        item = self.tbl_banks.item(row, 0)
        return str(item.text() if item else "").strip()

    def _clear_bank_form(self) -> None:
        self.ent_banco.clear()
        self.ent_cuenta.clear()
        self.ent_bank_resumen.clear()
        self._sync_bank_id()
        self.ent_banco.setFocus()

    def _load_banks(self) -> None:
        rows = self.repo.list_banks()
        self.bank_lookup = {}
        self.tbl_banks.setRowCount(len(rows))
        for i, row in enumerate(rows):
            bank_id = str(row["bank_id"] or "")
            bank_name = str(row["banco_nombre"] or "-")
            account = str(row["nro_cuenta"] or "-")
            summary = str(row["resumen"] or "-")
            self.bank_lookup[bank_id] = {
                "banco_nombre": bank_name,
                "nro_cuenta": account,
            }
            self._set_item(self.tbl_banks, i, 0, bank_id)
            self._set_item(self.tbl_banks, i, 1, bank_name)
            self._set_item(self.tbl_banks, i, 2, account, align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_banks, i, 3, summary)
            self._set_item(self.tbl_banks, i, 4, str(row["ts_registro"] or "-"), align=QtCore.Qt.AlignCenter)
        self.tbl_banks.resizeColumnsToContents()

    def _load_checkbooks(self) -> None:
        bank_id_filter = self._current_bank_id()
        rows = self.repo.list_checkbooks()
        if bank_id_filter:
            rows = [row for row in rows if str(row["bank_id"] or "").strip() == bank_id_filter]
        self.tbl_checkbooks.setRowCount(len(rows))
        for i, row in enumerate(rows):
            self._set_item(self.tbl_checkbooks, i, 0, str(row["chequera_id"] or "-"))
            self._set_item(self.tbl_checkbooks, i, 1, str(row["bank_id"] or "-"))
            self._set_item(self.tbl_checkbooks, i, 2, str(row["banco_nombre"] or "-"))
            self._set_item(self.tbl_checkbooks, i, 3, str(row["nro_cuenta"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_checkbooks, i, 4, str(row["formato_chequera"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_checkbooks, i, 5, str(row["tipo_cheque"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_checkbooks, i, 6, str(row["serie"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_checkbooks, i, 7, str(row["fecha_recibimiento"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_checkbooks, i, 8, str(row["nro_inicio"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_checkbooks, i, 9, str(row["nro_fin"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(self.tbl_checkbooks, i, 10, str(row["recibido_por"] or "-"))
            self._set_item(self.tbl_checkbooks, i, 11, str(row["resumen"] or "-"))
            self._set_item(self.tbl_checkbooks, i, 12, str(row["ts_registro"] or "-"), align=QtCore.Qt.AlignCenter)
        self.tbl_checkbooks.resizeColumnsToContents()

    def _refresh_all(self) -> None:
        self._load_banks()
        if self.checkbooks_panel.isVisible() and self._current_bank_id():
            self._load_checkbooks()
            self.status_label.setText(
                f"Bancos: {self.tbl_banks.rowCount()} | Chequeras: {self.tbl_checkbooks.rowCount()}"
            )
        else:
            self.tbl_checkbooks.setRowCount(0)
            self.status_label.setText(f"Bancos: {self.tbl_banks.rowCount()}")

    def _on_bank_selection_changed(self) -> None:
        row = self.tbl_banks.currentRow()
        if row < 0:
            self.btn_open_checkbooks.setEnabled(False)
            return
        item = self.tbl_banks.item(row, 0)
        bank_id = str(item.text() if item else "").strip()
        if not bank_id:
            self.btn_open_checkbooks.setEnabled(False)
            return
        self.btn_open_checkbooks.setEnabled(True)

    def _on_checkbook_selection_changed(self) -> None:
        self.btn_edit_checkbook.setEnabled(self.tbl_checkbooks.currentRow() >= 0)

    def _open_add_bank_dialog(self) -> None:
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Agregar banco")
        dlg.resize(460, 280)
        lay = QtWidgets.QVBoxLayout(dlg)
        form = QtWidgets.QFormLayout()
        form.setLabelAlignment(QtCore.Qt.AlignRight)
        lay.addLayout(form)

        ent_banco = QtWidgets.QLineEdit()
        ent_cuenta = QtWidgets.QLineEdit()
        ent_resumen = QtWidgets.QPlainTextEdit()
        ent_resumen.setFixedHeight(90)
        ent_bank_id = QtWidgets.QLineEdit()
        ent_bank_id.setReadOnly(True)

        def _sync_bank_id_text() -> None:
            ent_bank_id.setText(self.repo.build_bank_id(ent_banco.text(), ent_cuenta.text()))

        ent_banco.textChanged.connect(_sync_bank_id_text)
        ent_cuenta.textChanged.connect(_sync_bank_id_text)
        _sync_bank_id_text()

        form.addRow("Banco:", ent_banco)
        form.addRow("Nro de cuenta:", ent_cuenta)
        form.addRow("Resumen:", ent_resumen)
        form.addRow("ID banco:", ent_bank_id)

        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)
        btn_cancel = QtWidgets.QPushButton("Cancelar")
        btn_save = QtWidgets.QPushButton("Guardar banco")
        btns.addWidget(btn_cancel)
        btns.addWidget(btn_save)
        lay.addLayout(btns)
        btn_cancel.clicked.connect(dlg.reject)

        def _save() -> None:
            banco = ent_banco.text().strip()
            cuenta = ent_cuenta.text().strip()
            resumen = ent_resumen.toPlainText().strip()
            if not banco:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe ingresar el nombre del banco.")
                return
            if not cuenta:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe ingresar el numero de cuenta.")
                return
            try:
                self.repo.add_bank(banco, cuenta, resumen)
            except psycopg.IntegrityError:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Ese banco con esa cuenta ya existe.")
                return
            except Exception as exc:
                QtWidgets.QMessageBox.critical(dlg, "Bancos", f"No se pudo guardar el banco:\n{exc}")
                return
            dlg.accept()
            bank_id = self.repo.build_bank_id(banco, cuenta)
            self._load_banks()
            for row_idx in range(self.tbl_banks.rowCount()):
                item = self.tbl_banks.item(row_idx, 0)
                if item and item.text().strip() == bank_id:
                    self.tbl_banks.selectRow(row_idx)
                    break
            self.status_label.setText(f"Banco registrado: {bank_id}")

        btn_save.clicked.connect(_save)
        dlg.exec()

    def _open_checkbooks_for_selected_bank(self) -> None:
        bank_id = self._current_bank_id()
        if not bank_id:
            QtWidgets.QMessageBox.information(self, "Chequeras", "Seleccione primero un banco.")
            return
        bank_data = self.bank_lookup.get(bank_id, {})
        bank_name = str(bank_data.get("banco_nombre") or bank_id)
        account = str(bank_data.get("nro_cuenta") or "-")
        self.lbl_selected_bank.setText(f"Banco seleccionado: {bank_name} | Cuenta {account}")
        self.checkbooks_panel.setVisible(True)
        self.btn_add_checkbook.setEnabled(True)
        self.btn_edit_checkbook.setEnabled(False)
        self._load_checkbooks()

    def _open_add_checkbook_dialog(self) -> None:
        bank_id = self._current_bank_id()
        if not bank_id:
            QtWidgets.QMessageBox.information(self, "Chequeras", "Seleccione primero un banco.")
            return
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Agregar chequera")
        dlg.resize(540, 430)
        lay = QtWidgets.QVBoxLayout(dlg)
        form = QtWidgets.QFormLayout()
        form.setLabelAlignment(QtCore.Qt.AlignRight)
        lay.addLayout(form)

        bank_data = self.bank_lookup.get(bank_id, {})
        bank_label = f"{bank_data.get('banco_nombre', bank_id)} | Cuenta {bank_data.get('nro_cuenta', '-')}"
        lbl_bank = QtWidgets.QLabel(bank_label)
        form.addRow("Banco:", lbl_bank)

        cb_formato = QtWidgets.QComboBox()
        cb_formato.addItems(["Formulario", "Talonario"])
        form.addRow("Formato:", cb_formato)

        cb_tipo = QtWidgets.QComboBox()
        cb_tipo.addItems(["Vista", "Diferido"])
        form.addRow("Tipo:", cb_tipo)

        ent_serie = QtWidgets.QLineEdit()
        form.addRow("Serie:", ent_serie)

        dt_fecha = QtWidgets.QDateEdit()
        dt_fecha.setCalendarPopup(True)
        dt_fecha.setDate(QtCore.QDate.currentDate())
        form.addRow("Fecha recibimiento:", dt_fecha)

        sp_inicio = QtWidgets.QSpinBox()
        sp_inicio.setRange(0, 999999999)
        form.addRow("Nro inicio:", sp_inicio)

        sp_fin = QtWidgets.QSpinBox()
        sp_fin.setRange(0, 999999999)
        form.addRow("Nro fin:", sp_fin)

        ent_recibido = QtWidgets.QLineEdit()
        form.addRow("Recibido por:", ent_recibido)

        ent_resumen = QtWidgets.QPlainTextEdit()
        ent_resumen.setFixedHeight(90)
        form.addRow("Resumen:", ent_resumen)

        ent_chequera_id = QtWidgets.QLineEdit()
        ent_chequera_id.setReadOnly(True)
        form.addRow("ID chequera:", ent_chequera_id)

        def _sync_chequera_id() -> None:
            ent_chequera_id.setText(
                self.repo.build_chequera_id(
                    bank_id,
                    cb_formato.currentText(),
                    cb_tipo.currentText(),
                    ent_serie.text().strip(),
                    int(sp_inicio.value()),
                    int(sp_fin.value()),
                )
            )

        cb_formato.currentTextChanged.connect(_sync_chequera_id)
        cb_tipo.currentTextChanged.connect(_sync_chequera_id)
        ent_serie.textChanged.connect(_sync_chequera_id)
        sp_inicio.valueChanged.connect(_sync_chequera_id)
        sp_fin.valueChanged.connect(_sync_chequera_id)
        _sync_chequera_id()

        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)
        btn_cancel = QtWidgets.QPushButton("Cancelar")
        btn_save = QtWidgets.QPushButton("Guardar")
        btns.addWidget(btn_cancel)
        btns.addWidget(btn_save)
        lay.addLayout(btns)
        btn_cancel.clicked.connect(dlg.reject)

        def _save() -> None:
            formato = cb_formato.currentText().strip()
            tipo = cb_tipo.currentText().strip()
            serie = ent_serie.text().strip()
            fecha_recibimiento = dt_fecha.date().toString("yyyy-MM-dd")
            nro_inicio = int(sp_inicio.value())
            nro_fin = int(sp_fin.value())
            recibido_por = ent_recibido.text().strip()
            resumen = ent_resumen.toPlainText().strip()
            if not serie:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe ingresar la serie.")
                return
            if nro_fin < nro_inicio:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "El numero fin no puede ser menor al numero inicio.")
                return
            if not recibido_por:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe indicar quien recibio la chequera.")
                return
            if self.repo.has_overlap(bank_id, formato, tipo, serie, nro_inicio, nro_fin):
                QtWidgets.QMessageBox.warning(
                    dlg,
                    "Validacion",
                    "El rango se superpone con otro ya cargado para ese banco, formato, tipo y serie.",
                )
                return
            try:
                self.repo.add_checkbook(
                    bank_id=bank_id,
                    formato_chequera=formato,
                    tipo_cheque=tipo,
                    serie=serie,
                    fecha_recibimiento=fecha_recibimiento,
                    nro_inicio=nro_inicio,
                    nro_fin=nro_fin,
                    recibido_por=recibido_por,
                    resumen=resumen,
                )
            except psycopg.IntegrityError as exc:
                err = str(exc).strip()
                if "UNIQUE" in err.upper():
                    QtWidgets.QMessageBox.warning(dlg, "Validacion", "Esa chequera ya existe.")
                else:
                    QtWidgets.QMessageBox.warning(dlg, "Validacion", f"No se pudo guardar la chequera:\n{err}")
                return
            except Exception as exc:
                QtWidgets.QMessageBox.critical(dlg, "Chequeras", f"No se pudo guardar la chequera:\n{exc}")
                return
            dlg.accept()
            self._load_checkbooks()
            self.status_label.setText(f"Chequera registrada para {bank_id}: [{nro_inicio}-{nro_fin}]")

        btn_save.clicked.connect(_save)
        dlg.exec()

    def _selected_checkbook_row(self) -> dict | None:
        row = self.tbl_checkbooks.currentRow()
        if row < 0:
            return None
        def _txt(col: int) -> str:
            item = self.tbl_checkbooks.item(row, col)
            return str(item.text() if item else "").strip()
        return {
            "chequera_id": _txt(0),
            "bank_id": _txt(1),
            "formato_chequera": _txt(4),
            "tipo_cheque": _txt(5),
            "serie": _txt(6),
            "fecha_recibimiento": _txt(7),
            "nro_inicio": _txt(8),
            "nro_fin": _txt(9),
            "recibido_por": _txt(10),
            "resumen": _txt(11),
        }

    def _save_bank(self) -> None:
        banco = self.ent_banco.text().strip()
        cuenta = self.ent_cuenta.text().strip()
        resumen = self.ent_bank_resumen.toPlainText().strip()

        if not banco:
            QtWidgets.QMessageBox.warning(self, "Validacion", "Debe ingresar el nombre del banco.")
            return
        if not cuenta:
            QtWidgets.QMessageBox.warning(self, "Validacion", "Debe ingresar el numero de cuenta.")
            return

        try:
            self.repo.add_bank(banco, cuenta, resumen)
        except psycopg.IntegrityError:
            QtWidgets.QMessageBox.warning(self, "Validacion", "Ese banco con esa cuenta ya existe.")
            return
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Bancos", f"No se pudo guardar el banco:\n{exc}")
            return

        bank_id = self.repo.build_bank_id(banco, cuenta)
        self._load_banks()
        for row_idx in range(self.tbl_banks.rowCount()):
            item = self.tbl_banks.item(row_idx, 0)
            if item and item.text().strip() == bank_id:
                self.tbl_banks.selectRow(row_idx)
                break
        self.status_label.setText(f"Banco registrado: {bank_id}")
        self._clear_bank_form()

    def _edit_checkbook(self) -> None:
        selected = self._selected_checkbook_row()
        if not selected:
            QtWidgets.QMessageBox.information(self, "Editar chequera", "Seleccione primero una chequera.")
            return
        password, ok = QtWidgets.QInputDialog.getText(
            self,
            "Editar chequera",
            "Ingrese la contraseña:",
            QtWidgets.QLineEdit.Password,
        )
        if not ok:
            return
        if str(password).strip() != EDIT_PASSWORD:
            QtWidgets.QMessageBox.warning(self, "Editar chequera", "Contraseña incorrecta.")
            return

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Editar chequera")
        dlg.resize(520, 420)
        lay = QtWidgets.QVBoxLayout(dlg)
        form = QtWidgets.QFormLayout()
        form.setLabelAlignment(QtCore.Qt.AlignRight)
        lay.addLayout(form)

        cb_banco = QtWidgets.QComboBox()
        for bank_id, meta in self.bank_lookup.items():
            cb_banco.addItem(f"{meta.get('banco_nombre', bank_id)} | Cuenta {meta.get('nro_cuenta', '-')}", bank_id)
        idx_bank = cb_banco.findData(selected["bank_id"])
        if idx_bank >= 0:
            cb_banco.setCurrentIndex(idx_bank)
        form.addRow("Banco:", cb_banco)

        cb_formato = QtWidgets.QComboBox()
        cb_formato.addItems(["Formulario", "Talonario"])
        cb_formato.setCurrentText(selected["formato_chequera"] or "Formulario")
        form.addRow("Formato:", cb_formato)

        cb_tipo = QtWidgets.QComboBox()
        cb_tipo.addItems(["Vista", "Diferido"])
        cb_tipo.setCurrentText(selected["tipo_cheque"] or "Vista")
        form.addRow("Tipo:", cb_tipo)

        ent_serie = QtWidgets.QLineEdit(selected["serie"])
        form.addRow("Serie:", ent_serie)

        dt_fecha = QtWidgets.QDateEdit()
        dt_fecha.setCalendarPopup(True)
        fecha = QtCore.QDate.fromString(selected["fecha_recibimiento"], "yyyy-MM-dd")
        dt_fecha.setDate(fecha if fecha.isValid() else QtCore.QDate.currentDate())
        form.addRow("Fecha recibimiento:", dt_fecha)

        sp_inicio = QtWidgets.QSpinBox()
        sp_inicio.setRange(0, 999999999)
        sp_inicio.setValue(int(selected["nro_inicio"] or 0))
        form.addRow("Nro inicio:", sp_inicio)

        sp_fin = QtWidgets.QSpinBox()
        sp_fin.setRange(0, 999999999)
        sp_fin.setValue(int(selected["nro_fin"] or 0))
        form.addRow("Nro fin:", sp_fin)

        ent_recibido = QtWidgets.QLineEdit(selected["recibido_por"])
        form.addRow("Recibido por:", ent_recibido)

        ent_resumen = QtWidgets.QPlainTextEdit()
        ent_resumen.setPlainText(selected["resumen"])
        ent_resumen.setFixedHeight(90)
        form.addRow("Resumen:", ent_resumen)

        btns = QtWidgets.QHBoxLayout()
        btns.addStretch(1)
        btn_cancel = QtWidgets.QPushButton("Cancelar")
        btn_save = QtWidgets.QPushButton("Guardar cambios")
        btns.addWidget(btn_cancel)
        btns.addWidget(btn_save)
        lay.addLayout(btns)
        btn_cancel.clicked.connect(dlg.reject)

        def _save_edit() -> None:
            bank_id = str(cb_banco.currentData() or "").strip()
            formato = cb_formato.currentText().strip()
            tipo = cb_tipo.currentText().strip()
            serie = ent_serie.text().strip()
            fecha_recibimiento = dt_fecha.date().toString("yyyy-MM-dd")
            nro_inicio = int(sp_inicio.value())
            nro_fin = int(sp_fin.value())
            recibido_por = ent_recibido.text().strip()
            resumen = ent_resumen.toPlainText().strip()

            if not bank_id:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe seleccionar un banco.")
                return
            if not serie:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe ingresar la serie.")
                return
            if nro_fin < nro_inicio:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "El numero fin no puede ser menor al numero inicio.")
                return
            if not recibido_por:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", "Debe indicar quien recibio la chequera.")
                return
            if self.repo.has_overlap(bank_id, formato, tipo, serie, nro_inicio, nro_fin, exclude_chequera_id=selected["chequera_id"]):
                QtWidgets.QMessageBox.warning(
                    dlg,
                    "Validacion",
                    "El rango se superpone con otro ya cargado para ese banco, formato, tipo y serie.",
                )
                return
            try:
                self.repo.update_checkbook(
                    original_chequera_id=selected["chequera_id"],
                    bank_id=bank_id,
                    formato_chequera=formato,
                    tipo_cheque=tipo,
                    serie=serie,
                    fecha_recibimiento=fecha_recibimiento,
                    nro_inicio=nro_inicio,
                    nro_fin=nro_fin,
                    recibido_por=recibido_por,
                    resumen=resumen,
                )
            except psycopg.IntegrityError as exc:
                QtWidgets.QMessageBox.warning(dlg, "Validacion", f"No se pudo guardar la edicion:\n{exc}")
                return
            except Exception as exc:
                QtWidgets.QMessageBox.critical(dlg, "Editar chequera", f"No se pudo guardar:\n{exc}")
                return
            dlg.accept()
            self._refresh_all()
            self.status_label.setText(f"Chequera editada: {selected['chequera_id']}")

        btn_save.clicked.connect(_save_edit)
        dlg.exec()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        try:
            self.repo.close()
        finally:
            super().closeEvent(event)


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    win = BancosWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
