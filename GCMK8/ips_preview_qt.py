from __future__ import annotations

import json
import sys
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from ips_pdf_parser import IPSExtractData, parse_ips_extract


class IPSPreviewDialog(QtWidgets.QDialog):
    def __init__(self, data: IPSExtractData, parent=None):
        super().__init__(parent)
        self.data = data
        self.setWindowTitle("Previsualización extracto IPS")
        self.resize(780, 620)

        layout = QtWidgets.QVBoxLayout(self)

        form = QtWidgets.QFormLayout()
        form.setLabelAlignment(QtCore.Qt.AlignmentFlag.AlignRight)

        self.lbl_pdf = QtWidgets.QLabel(data.pdf_path)
        self.lbl_pdf.setTextInteractionFlags(QtCore.Qt.TextInteractionFlag.TextSelectableByMouse)
        self.lbl_extracto = QtWidgets.QLabel(data.extracto_nro or "-")
        self.lbl_periodo = QtWidgets.QLabel(data.periodo_display or data.periodo or "-")
        self.lbl_patronal = QtWidgets.QLabel(data.patronal or "-")
        self.lbl_ruc = QtWidgets.QLabel(data.ruc or "-")
        self.lbl_token = QtWidgets.QLabel(data.token or "-")

        self.ent_fecha = QtWidgets.QLineEdit(data.fecha_gasto or "")
        self.ent_fecha.setPlaceholderText("AAAA-MM-DD")

        self.ent_monto = QtWidgets.QLineEdit("" if data.total_a_pagar is None else str(int(data.total_a_pagar)))
        self.ent_tipo = QtWidgets.QLineEdit("IPS")
        self.ent_factura = QtWidgets.QLineEdit(data.extracto_nro or "")
        self.ent_desc = QtWidgets.QLineEdit(data.descripcion_gasto)

        form.addRow("Archivo:", self.lbl_pdf)
        form.addRow("Nro. extracto:", self.lbl_extracto)
        form.addRow("Período:", self.lbl_periodo)
        form.addRow("Patronal:", self.lbl_patronal)
        form.addRow("RUC:", self.lbl_ruc)
        form.addRow("Token:", self.lbl_token)
        form.addRow("Fecha gasto:", self.ent_fecha)
        form.addRow("Tipo:", self.ent_tipo)
        form.addRow("Monto (Gs):", self.ent_monto)
        form.addRow("Referencia:", self.ent_factura)
        form.addRow("Descripción:", self.ent_desc)
        layout.addLayout(form)

        box_totals = QtWidgets.QGroupBox("Totales detectados")
        totals_layout = QtWidgets.QFormLayout(box_totals)
        totals_layout.addRow("Total aporte:", QtWidgets.QLabel(self._fmt_gs(data.total_aporte)))
        totals_layout.addRow("Total mora:", QtWidgets.QLabel(self._fmt_gs(data.total_mora)))
        totals_layout.addRow("Total multa:", QtWidgets.QLabel(self._fmt_gs(data.total_multa)))
        totals_layout.addRow("Total a pagar:", QtWidgets.QLabel(self._fmt_gs(data.total_a_pagar)))
        layout.addWidget(box_totals)

        self.txt_preview = QtWidgets.QPlainTextEdit()
        self.txt_preview.setReadOnly(True)
        self.txt_preview.setPlainText(data.raw_text[:12000])
        layout.addWidget(self.txt_preview, 1)

        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.StandardButton.Ok | QtWidgets.QDialogButtonBox.StandardButton.Cancel
        )
        buttons.button(QtWidgets.QDialogButtonBox.StandardButton.Ok).setText("Insertar gasto")
        buttons.button(QtWidgets.QDialogButtonBox.StandardButton.Cancel).setText("Cancelar")
        buttons.accepted.connect(self._accept_if_valid)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def _fmt_gs(self, value):
        if value is None:
            return "-"
        return f"{float(value):,.0f}".replace(",", ".")

    def _accept_if_valid(self):
        fecha = self.ent_fecha.text().strip()
        monto_txt = self.ent_monto.text().strip().replace(".", "").replace(",", ".")
        tipo = self.ent_tipo.text().strip()

        if not fecha:
            QtWidgets.QMessageBox.warning(self, "IPS", "Debes completar la fecha del gasto.")
            return
        try:
            QtCore.QDate.fromString(fecha, "yyyy-MM-dd")
            if not QtCore.QDate.fromString(fecha, "yyyy-MM-dd").isValid():
                raise ValueError
        except Exception:
            QtWidgets.QMessageBox.warning(self, "IPS", "La fecha debe tener formato AAAA-MM-DD.")
            return

        try:
            monto = float(monto_txt)
            if monto <= 0:
                raise ValueError
        except Exception:
            QtWidgets.QMessageBox.warning(self, "IPS", "El monto debe ser un número positivo.")
            return

        if not tipo:
            QtWidgets.QMessageBox.warning(self, "IPS", "Debes completar el tipo.")
            return

        self.accept()

    def payload(self) -> dict:
        return {
            "fecha": self.ent_fecha.text().strip(),
            "tipo": self.ent_tipo.text().strip() or "IPS",
            "desc": self.ent_desc.text().strip(),
            "monto": float(self.ent_monto.text().strip().replace(".", "").replace(",", ".")),
            "nro_factura": self.ent_factura.text().strip(),
            "forma_pago": "Homebanking",
            "referencia_pago": "",
            "periodo": self.data.periodo,
            "periodo_display": self.data.periodo_display,
            "pdf_path": self.data.pdf_path,
        }


def _pick_pdf_from_args_or_dialog() -> str:
    if len(sys.argv) > 1 and sys.argv[1].strip():
        return sys.argv[1]
    downloads_dir = Path.home() / "Downloads"
    pdf_path, _ = QtWidgets.QFileDialog.getOpenFileName(
        None,
        "Seleccionar extracto IPS",
        str(downloads_dir if downloads_dir.exists() else Path.home()),
        "Archivos PDF (*.pdf);;Todos los archivos (*.*)",
    )
    return pdf_path


def main() -> int:
    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication(sys.argv)
    pdf_path = _pick_pdf_from_args_or_dialog()
    if not pdf_path:
        return 0

    try:
        data = parse_ips_extract(Path(pdf_path))
    except Exception as exc:
        QtWidgets.QMessageBox.critical(None, "IPS", f"No se pudo parsear el PDF:\n{exc}")
        return 1

    if data.total_a_pagar is None:
        QtWidgets.QMessageBox.critical(None, "IPS", "No se encontró el monto 'Total a pagar' en el documento.")
        return 1

    if not data.fecha_gasto:
        QtWidgets.QMessageBox.critical(None, "IPS", "No se pudo identificar el período del extracto IPS.")
        return 1

    dlg = IPSPreviewDialog(data)
    if dlg.exec() != QtWidgets.QDialog.DialogCode.Accepted:
        return 0

    print(json.dumps(dlg.payload(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
