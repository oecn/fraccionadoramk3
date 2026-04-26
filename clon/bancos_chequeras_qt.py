# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
import psycopg
from PySide6 import QtCore, QtWidgets

DB_PATH = ROOT_DIR / "GCMK8" / "fraccionadora.db"


class BancosRepo:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = Path(db_path)
        self.cn = db.connect("fraccionadora")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        cur = self.cn.cursor()
        db.run_ddl(self.cn, 
            """
            CREATE TABLE IF NOT EXISTS bank_checkbooks(
                id BIGSERIAL PRIMARY KEY,
                chequera_id TEXT NOT NULL,
                banco_nombre TEXT NOT NULL,
                nro_cuenta TEXT NOT NULL,
                formulario_tipo TEXT NOT NULL,
                nro_inicio INTEGER NOT NULL,
                nro_fin INTEGER NOT NULL,
                recibido_por TEXT NOT NULL,
                resumen TEXT,
                ts_registro TEXT NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_bank_checkbooks_unique
                ON bank_checkbooks(banco_nombre, nro_cuenta, formulario_tipo, nro_inicio, nro_fin);
            """
        )
        self.cn.commit()

    @staticmethod
    def _norm_token(value: str) -> str:
        text = unicodedata.normalize("NFKD", str(value or "").strip().lower())
        text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
        text = re.sub(r"[^a-z0-9]+", "-", text)
        return re.sub(r"-+", "-", text).strip("-")

    def build_chequera_id(self, banco_nombre: str, nro_cuenta: str) -> str:
        banco = self._norm_token(banco_nombre) or "sin-banco"
        cuenta = self._norm_token(nro_cuenta) or "sin-cuenta"
        return f"banco-{banco}-{cuenta}-chequera"

    def has_overlap(
        self,
        banco_nombre: str,
        nro_cuenta: str,
        formulario_tipo: str,
        nro_inicio: int,
        nro_fin: int,
    ) -> bool:
        cur = self.cn.cursor()
        row = cur.execute(
            """
            SELECT 1
            FROM bank_checkbooks
            WHERE banco_nombre = %s
              AND nro_cuenta = %s
              AND formulario_tipo = %s
              AND NOT (nro_fin < %s OR nro_inicio > %s)
            LIMIT 1;
            """,
            (
                str(banco_nombre).strip(),
                str(nro_cuenta).strip(),
                str(formulario_tipo).strip(),
                int(nro_inicio),
                int(nro_fin),
            ),
        ).fetchone()
        return row is not None

    def add_checkbook(
        self,
        banco_nombre: str,
        nro_cuenta: str,
        formulario_tipo: str,
        nro_inicio: int,
        nro_fin: int,
        recibido_por: str,
        resumen: str,
    ) -> None:
        cur = self.cn.cursor()
        cur.execute(
            """
            INSERT INTO bank_checkbooks(
                chequera_id, banco_nombre, nro_cuenta, formulario_tipo,
                nro_inicio, nro_fin, recibido_por, resumen, ts_registro
            ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                self.build_chequera_id(banco_nombre, nro_cuenta),
                str(banco_nombre).strip(),
                str(nro_cuenta).strip(),
                str(formulario_tipo).strip(),
                int(nro_inicio),
                int(nro_fin),
                str(recibido_por).strip(),
                str(resumen or "").strip(),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        self.cn.commit()

    def list_checkbooks(self) -> list:
        cur = self.cn.cursor()
        return cur.execute(
            """
            SELECT chequera_id, banco_nombre, nro_cuenta, formulario_tipo,
                   nro_inicio, nro_fin, recibido_por, resumen, ts_registro
            FROM bank_checkbooks
            ORDER BY ts_registro DESC, id DESC;
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
        self.setWindowTitle("Bancos y chequeras")
        self.resize(1040, 680)

        self.status_label = QtWidgets.QLabel("Listo")
        self.statusBar().addWidget(self.status_label)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        layout = QtWidgets.QVBoxLayout(central)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)

        top = QtWidgets.QHBoxLayout()
        title = QtWidgets.QLabel("Carga de rangos de chequera recibida")
        font = title.font()
        font.setPointSize(font.pointSize() + 2)
        font.setBold(True)
        title.setFont(font)
        top.addWidget(title)
        top.addStretch(1)
        self.btn_refresh = QtWidgets.QPushButton("Refrescar")
        self.btn_refresh.clicked.connect(self._load_table)
        top.addWidget(self.btn_refresh)
        layout.addLayout(top)

        self.form = QtWidgets.QGroupBox("Formulario")
        form_layout = QtWidgets.QFormLayout(self.form)
        form_layout.setLabelAlignment(QtCore.Qt.AlignRight)

        self.ent_banco = QtWidgets.QLineEdit()
        self.ent_banco.setPlaceholderText("Nombre del banco")
        form_layout.addRow("Banco:", self.ent_banco)

        self.ent_cuenta = QtWidgets.QLineEdit()
        self.ent_cuenta.setPlaceholderText("Numero de cuenta")
        form_layout.addRow("Nro de cuenta:", self.ent_cuenta)

        self.cb_formulario = QtWidgets.QComboBox()
        self.cb_formulario.addItems(["Talonario", "Diferido"])
        form_layout.addRow("Formulario:", self.cb_formulario)

        self.sp_inicio = QtWidgets.QSpinBox()
        self.sp_inicio.setRange(0, 999999999)
        form_layout.addRow("Nro inicio:", self.sp_inicio)

        self.sp_fin = QtWidgets.QSpinBox()
        self.sp_fin.setRange(0, 999999999)
        form_layout.addRow("Nro fin:", self.sp_fin)

        self.ent_recibido = QtWidgets.QLineEdit()
        self.ent_recibido.setPlaceholderText("Responsable que recibe")
        form_layout.addRow("Recibido por:", self.ent_recibido)

        self.ent_resumen = QtWidgets.QPlainTextEdit()
        self.ent_resumen.setPlaceholderText("Resumen u observacion")
        self.ent_resumen.setFixedHeight(90)
        form_layout.addRow("Resumen:", self.ent_resumen)

        self.lbl_chequera_id = QtWidgets.QLineEdit()
        self.lbl_chequera_id.setReadOnly(True)
        form_layout.addRow("ID chequera:", self.lbl_chequera_id)

        btn_row = QtWidgets.QHBoxLayout()
        btn_row.addStretch(1)
        self.btn_guardar = QtWidgets.QPushButton("Guardar rango")
        self.btn_limpiar = QtWidgets.QPushButton("Limpiar")
        btn_row.addWidget(self.btn_limpiar)
        btn_row.addWidget(self.btn_guardar)
        form_layout.addRow("", btn_row)

        layout.addWidget(self.form)

        self.tbl = QtWidgets.QTableWidget(0, 9)
        self.tbl.setHorizontalHeaderLabels(
            [
                "ID chequera",
                "Banco",
                "Nro cuenta",
                "Formulario",
                "Nro inicio",
                "Nro fin",
                "Recibido por",
                "Resumen",
                "Registro",
            ]
        )
        self.tbl.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.tbl.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.tbl.setAlternatingRowColors(True)
        self.tbl.verticalHeader().setVisible(False)
        self.tbl.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.tbl, 1)

        self.ent_banco.textChanged.connect(self._sync_chequera_id)
        self.ent_cuenta.textChanged.connect(self._sync_chequera_id)
        self.btn_guardar.clicked.connect(self._save)
        self.btn_limpiar.clicked.connect(self._clear_form)

        self._sync_chequera_id()
        self._load_table()

    def _set_item(
        self,
        row: int,
        col: int,
        text: str,
        align: QtCore.Qt.AlignmentFlag = QtCore.Qt.AlignLeft,
    ) -> None:
        item = QtWidgets.QTableWidgetItem(text)
        item.setTextAlignment(align | QtCore.Qt.AlignVCenter)
        self.tbl.setItem(row, col, item)

    def _sync_chequera_id(self) -> None:
        self.lbl_chequera_id.setText(
            self.repo.build_chequera_id(self.ent_banco.text(), self.ent_cuenta.text())
        )

    def _clear_form(self) -> None:
        self.ent_banco.clear()
        self.ent_cuenta.clear()
        self.cb_formulario.setCurrentIndex(0)
        self.sp_inicio.setValue(0)
        self.sp_fin.setValue(0)
        self.ent_recibido.clear()
        self.ent_resumen.clear()
        self._sync_chequera_id()
        self.ent_banco.setFocus()

    def _load_table(self) -> None:
        rows = self.repo.list_checkbooks()
        self.tbl.setRowCount(len(rows))
        for i, row in enumerate(rows):
            self._set_item(i, 0, str(row["chequera_id"] or "-"))
            self._set_item(i, 1, str(row["banco_nombre"] or "-"))
            self._set_item(i, 2, str(row["nro_cuenta"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(i, 3, str(row["formulario_tipo"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(i, 4, str(row["nro_inicio"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(i, 5, str(row["nro_fin"] or "-"), align=QtCore.Qt.AlignCenter)
            self._set_item(i, 6, str(row["recibido_por"] or "-"))
            self._set_item(i, 7, str(row["resumen"] or "-"))
            self._set_item(i, 8, str(row["ts_registro"] or "-"), align=QtCore.Qt.AlignCenter)
        self.tbl.resizeColumnsToContents()
        self.status_label.setText(f"Registros cargados: {len(rows)}")

    def _save(self) -> None:
        banco = self.ent_banco.text().strip()
        cuenta = self.ent_cuenta.text().strip()
        formulario = self.cb_formulario.currentText().strip()
        nro_inicio = int(self.sp_inicio.value())
        nro_fin = int(self.sp_fin.value())
        recibido_por = self.ent_recibido.text().strip()
        resumen = self.ent_resumen.toPlainText().strip()

        if not banco:
            QtWidgets.QMessageBox.warning(self, "Validacion", "Debe ingresar el nombre del banco.")
            return
        if not cuenta:
            QtWidgets.QMessageBox.warning(self, "Validacion", "Debe ingresar el numero de cuenta.")
            return
        if nro_fin < nro_inicio:
            QtWidgets.QMessageBox.warning(self, "Validacion", "El numero fin no puede ser menor al numero inicio.")
            return
        if not recibido_por:
            QtWidgets.QMessageBox.warning(self, "Validacion", "Debe indicar quien recibio la chequera.")
            return
        if self.repo.has_overlap(banco, cuenta, formulario, nro_inicio, nro_fin):
            QtWidgets.QMessageBox.warning(
                self,
                "Validacion",
                "El rango se superpone con otro ya cargado para ese banco, cuenta y formulario.",
            )
            return

        try:
            self.repo.add_checkbook(
                banco_nombre=banco,
                nro_cuenta=cuenta,
                formulario_tipo=formulario,
                nro_inicio=nro_inicio,
                nro_fin=nro_fin,
                recibido_por=recibido_por,
                resumen=resumen,
            )
        except psycopg.IntegrityError:
            QtWidgets.QMessageBox.warning(self, "Validacion", "Ese rango ya existe.")
            return
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Bancos y chequeras", f"No se pudo guardar:\n{exc}")
            return

        self._load_table()
        self.status_label.setText(
            f"Chequera registrada: {self.lbl_chequera_id.text()} [{nro_inicio}-{nro_fin}]"
        )
        self._clear_form()

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
