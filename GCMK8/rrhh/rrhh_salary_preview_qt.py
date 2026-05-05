from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

from PySide6 import QtCore, QtWidgets

ROOT_DIR = Path(__file__).resolve().parents[2]
PARSER_DIR = ROOT_DIR / "parseo IPS" / "backend"
if str(PARSER_DIR) not in sys.path:
    sys.path.insert(0, str(PARSER_DIR))

from salary_txt_parser import parse_salary_advance_txt, to_json_dict  # type: ignore


class RRHHTxtPreviewDialog(QtWidgets.QDialog):
    def __init__(self, parsed: dict, parent=None):
        super().__init__(parent)
        self.parsed = parsed
        self.setWindowTitle("Previsualización RRHH - Adelantos/Salarios")
        self.resize(960, 640)

        layout = QtWidgets.QVBoxLayout(self)

        form = QtWidgets.QFormLayout()
        self.lbl_file = QtWidgets.QLabel(parsed.get("file_path") or "-")
        self.lbl_file.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
        self.ent_fecha = QtWidgets.QLineEdit(parsed.get("suggested_date") or date.today().strftime("%Y-%m-%d"))
        self.lbl_total = QtWidgets.QLabel(self._fmt_gs(parsed.get("total_amount") or 0.0))
        self.lbl_rows = QtWidgets.QLabel(str(parsed.get("row_count") or 0))
        self.lbl_conceptos = QtWidgets.QLabel(", ".join(parsed.get("conceptos") or []))
        form.addRow("Archivo:", self.lbl_file)
        form.addRow("Fecha a registrar:", self.ent_fecha)
        form.addRow("Total:", self.lbl_total)
        form.addRow("Cantidad de filas:", self.lbl_rows)
        form.addRow("Conceptos:", self.lbl_conceptos or QtWidgets.QLabel("-"))
        layout.addLayout(form)

        self.table = QtWidgets.QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(
            ["Funcionario", "CI", "Cuenta destino", "Concepto", "Monto (Gs)", "Confirmado", "Estado"]
        )
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        rows = parsed.get("rows") or []
        self.table.setRowCount(len(rows))
        unresolved = 0
        for row_idx, row in enumerate(rows):
            funcionario = row.get("funcionario") or "-"
            estado = "OK" if row.get("employee_id") else "CI no vinculado"
            if not row.get("employee_id"):
                unresolved += 1
            values = [
                funcionario,
                row.get("documento") or "",
                row.get("cuenta_destino") or "",
                row.get("concepto") or "",
                self._fmt_gs(row.get("monto") or 0.0),
                "Sí" if row.get("confirmado") else "No",
                estado,
            ]
            for col_idx, value in enumerate(values):
                item = QtWidgets.QTableWidgetItem(value)
                if col_idx == 6 and not row.get("employee_id"):
                    item.setForeground(QtCore.Qt.GlobalColor.red)
                self.table.setItem(row_idx, col_idx, item)
        layout.addWidget(self.table, 1)

        self.lbl_warn = QtWidgets.QLabel(
            "Todos los CI están vinculados a funcionarios." if unresolved == 0
            else f"Hay {unresolved} fila(s) con CI no vinculado. No se permitirá insertar."
        )
        if unresolved:
            self.lbl_warn.setStyleSheet("color: #b91c1c;")
        layout.addWidget(self.lbl_warn)

        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        buttons.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setText("Insertar movimientos")
        buttons.button(QtWidgets.QDialogButtonBox.StandardButton.Cancel).setText("Cancelar")
        buttons.accepted.connect(self._accept_if_valid)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _fmt_gs(self, value) -> str:
        try:
            return f"{float(value):,.0f}".replace(",", ".")
        except Exception:
            return "-"

    def _accept_if_valid(self):
        fecha = self.ent_fecha.text().strip()
        qdate = QtCore.QDate.fromString(fecha, "yyyy-MM-dd")
        if not fecha or not qdate.isValid():
            QtWidgets.QMessageBox.warning(self, "RRHH", "La fecha debe tener formato AAAA-MM-DD.")
            return
        unresolved = [row for row in (self.parsed.get("rows") or []) if not row.get("employee_id")]
        if unresolved:
            QtWidgets.QMessageBox.warning(
                self,
                "RRHH",
                "Hay filas con CI no vinculado a RRHH. Corrige eso antes de insertar.",
            )
            return
        self.accept()

    def payload(self) -> dict:
        return {
            "file_path": self.parsed.get("file_path"),
            "fecha": self.ent_fecha.text().strip(),
            "rows": self.parsed.get("rows") or [],
            "total_amount": self.parsed.get("total_amount") or 0.0,
            "row_count": self.parsed.get("row_count") or 0,
        }


def _pick_txt_from_args_or_dialog() -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1]
    default_dir = Path(r"C:\Users\osval\Desktop\GRANOS")
    txt_path, _ = QtWidgets.QFileDialog.getOpenFileName(
        None,
        "Seleccionar archivo de adelanto/salario",
        str(default_dir if default_dir.exists() else Path.home()),
        "Archivos TXT (*.txt);;Archivos CSV (*.csv);;Todos los archivos (*.*)",
    )
    return txt_path


def main() -> int:
    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication(sys.argv)
    txt_path = _pick_txt_from_args_or_dialog()
    if not txt_path:
        return 0

    try:
        parsed = to_json_dict(parse_salary_advance_txt(Path(txt_path)))
    except Exception as exc:
        QtWidgets.QMessageBox.critical(None, "RRHH", f"No se pudo parsear el TXT:\n{exc}")
        return 1

    dlg = RRHHTxtPreviewDialog(parsed)
    if dlg.exec() != QtWidgets.QDialog.DialogCode.Accepted:
        return 0

    print(json.dumps(dlg.payload(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
