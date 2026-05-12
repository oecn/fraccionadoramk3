from __future__ import annotations

from pathlib import Path
from typing import Any


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1O7ulQlKKhCkZQmqRGV7DzMgnu7EOhNm-a6PiB4hA_t4"
RANGE = "'VENTA'!A2"
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


def append_factura(data: dict[str, Any]) -> None:
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except Exception as exc:
        raise RuntimeError("Faltan dependencias Google. Instale google-api-python-client y google-auth.") from exc

    creds_path = _repo_root() / "sheets" / "granoscentral-32325a2941d5.json"
    if not creds_path.exists():
        raise FileNotFoundError(f"No se encontro credencial Google: {creds_path}")

    creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    body = {"values": [[data.get(field, "") for field in FIELDS]]}
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]
