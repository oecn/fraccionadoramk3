import sys
from datetime import datetime
import random
from pathlib import Path
from typing import Dict, List, Any
from PySide6.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QMessageBox
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1O7ulQlKKhCkZQmqRGV7DzMgnu7EOhNm-a6PiB4hA_t4"
RANGE = "'VENTA'!A2"

# Orden de columnas a enviar al sheet
FIELDS = [
    "mes",
    "cliente",
    "factura",
    "fecha",
    "remision",
    "estado",
    "cobranza",
    "recibo",
    "extra1",
    "total",
    "iva_total",
    "extra2",
]


def get_service():
    creds_path = Path(__file__).resolve().parent / "granoscentral-32325a2941d5.json"
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


def _build_row(data: Dict[str, Any]) -> List[Any]:
    return [data.get(field, "") for field in FIELDS]


def append_factura(data: Dict[str, Any] | None = None):
    """
    Envía una fila al sheet. Si no se pasa data, usa una fila de demo.
    data debe tener las claves de FIELDS.
    """
    if data is None:
        data = _demo_row()

    service = get_service()
    body = {"values": [_build_row(data)]}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def _demo_row() -> Dict[str, Any]:
    ahora = datetime.now()
    meses = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]
    numero = f"F001-{random.randint(1, 9999):04d}"
    total = round(random.uniform(100000, 500000), 2)
    iva_total = round(total * 0.1, 2)
    extra2 = round(total - 0.3 * iva_total, 2)
    return {
        "mes": meses[ahora.month - 1],
        "cliente": random.choice(["LUQUE", "AREGUA", "ITAUGUA"]),
        "factura": numero,
        "fecha": ahora.strftime("%d/%m/%Y"),
        "remision": "Listo",
        "estado": "Entregado",
        "cobranza": "Sin OP",
        "recibo": "",
        "extra1": "",
        "total": total,
        "iva_total": iva_total,
        "extra2": extra2,
    }


class Window(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Enviar factura a Google Sheets")
        btn = QPushButton("Enviar fila demo")
        btn.clicked.connect(self.handle_send)
        layout = QVBoxLayout(self)
        layout.addWidget(btn)

    def handle_send(self):
        try:
            append_factura()
            QMessageBox.information(
                self,
                "OK",
                "Se envió la fila de demo (mes, cliente, factura, fecha, remisión/estado/cobranza, total, IVA total).",
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo enviar:\n{e}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = Window()
    win.show()
    sys.exit(app.exec())
