# -*- coding: utf-8 -*-
import sys
import subprocess
import tempfile
from datetime import datetime, timedelta
from html import escape
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import db
from PySide6 import QtWidgets, QtGui, QtCore

from reporte_mensual import (
    build_report,
    save_report_for_month,
    _resolve_db,
    OUT_DIR_DEFAULT,
)
from inicio_dashboard_qt import InicioDashboardWindow   


class ReporteMensualWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Reporte Mensual | Generador TXT")
        self.resize(1220, 820)
        self.setMinimumSize(980, 680)
        self._last_report_text = ""
        self._last_month = ""
        self._dark_mode = self._detect_system_dark_mode()
        self._auto_generated_count = 0
        self._inicio_win: InicioDashboardWindow | None = None

        self._tune_palette()

        central = QtWidgets.QWidget()
        central.setObjectName("root")
        self.setCentralWidget(central)

        root = QtWidgets.QVBoxLayout(central)
        root.setContentsMargins(16, 16, 16, 12)
        root.setSpacing(12)

        header = QtWidgets.QFrame()
        header.setObjectName("headerCard")
        header_l = QtWidgets.QHBoxLayout(header)
        header_l.setContentsMargins(16, 14, 16, 14)
        header_l.setSpacing(12)
        root.addWidget(header, 0)

        title_box = QtWidgets.QVBoxLayout()
        title_box.setSpacing(2)
        lbl_title = QtWidgets.QLabel("Reporte mensual en TXT")
        lbl_title.setObjectName("titleLabel")
        lbl_sub = QtWidgets.QLabel("Genera, previsualiza y guarda reportes contables por mes.")
        lbl_sub.setObjectName("subTitleLabel")
        title_box.addWidget(lbl_title)
        title_box.addWidget(lbl_sub)
        header_l.addLayout(title_box, 1)

        self.btn_dark = QtWidgets.QPushButton("Modo oscuro")
        self.btn_dark.setCheckable(True)
        self.btn_dark.setProperty("variant", "ghost")
        self.btn_dark.setMinimumHeight(34)
        self.btn_reports_menu = QtWidgets.QToolButton()
        self.btn_reports_menu.setObjectName("reportsBurger")
        self.btn_reports_menu.setText("☰ Reportes")
        self.btn_reports_menu.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        self.btn_reports_menu.setToolButtonStyle(QtCore.Qt.ToolButtonTextOnly)
        menu_reports = QtWidgets.QMenu(self.btn_reports_menu)
        self.act_reporte_mensual = menu_reports.addAction("Reporte mensual")
        self.act_reporte_ventas = menu_reports.addAction("Reporte de ventas")
        self.btn_reports_menu.setMenu(menu_reports)
        header_l.addWidget(self.btn_reports_menu, 0, QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
        header_l.addWidget(self.btn_dark, 0, QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)

        split = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        split.setChildrenCollapsible(False)
        split.setHandleWidth(8)
        root.addWidget(split, 1)

        left = QtWidgets.QFrame()
        left.setObjectName("sidePanel")
        left_l = QtWidgets.QVBoxLayout(left)
        left_l.setContentsMargins(12, 12, 12, 12)
        left_l.setSpacing(10)
        split.addWidget(left)

        card_params = QtWidgets.QFrame()
        card_params.setObjectName("panelCard")
        params_l = QtWidgets.QVBoxLayout(card_params)
        params_l.setContentsMargins(12, 12, 12, 12)
        params_l.setSpacing(8)
        left_l.addWidget(card_params)

        lbl_params = QtWidgets.QLabel("Parametros")
        lbl_params.setObjectName("sectionTitle")
        params_l.addWidget(lbl_params)

        form = QtWidgets.QFormLayout()
        form.setFieldGrowthPolicy(QtWidgets.QFormLayout.ExpandingFieldsGrow)
        form.setLabelAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        form.setFormAlignment(QtCore.Qt.AlignTop)
        form.setHorizontalSpacing(8)
        form.setVerticalSpacing(8)
        params_l.addLayout(form)

        self.cb_month = QtWidgets.QComboBox()
        self.cb_month.setEditable(True)
        self.cb_month.setMinimumWidth(170)
        form.addRow("Mes (YYYY-MM)", self.cb_month)

        self.ent_empresa = QtWidgets.QLineEdit("Fraccionadora")
        form.addRow("Empresa", self.ent_empresa)

        db_row = QtWidgets.QHBoxLayout()
        db_row.setSpacing(6)
        self.ent_db = QtWidgets.QLineEdit(str(_resolve_db(None)))
        self.ent_db.setPlaceholderText("Ruta de la base de datos")
        self.btn_db = QtWidgets.QPushButton("Examinar")
        self.btn_db.setProperty("variant", "ghost")
        self.btn_db.setMinimumHeight(30)
        db_row.addWidget(self.ent_db, 1)
        db_row.addWidget(self.btn_db, 0)
        form.addRow("Base de datos", db_row)

        self.lbl_output = QtWidgets.QLabel(str(OUT_DIR_DEFAULT))
        self.lbl_output.setObjectName("pathHint")
        self.lbl_output.setWordWrap(True)
        form.addRow("Salida por defecto", self.lbl_output)

        card_actions = QtWidgets.QFrame()
        card_actions.setObjectName("panelCard")
        actions_l = QtWidgets.QVBoxLayout(card_actions)
        actions_l.setContentsMargins(12, 12, 12, 12)
        actions_l.setSpacing(8)
        left_l.addWidget(card_actions)

        lbl_actions = QtWidgets.QLabel("Acciones")
        lbl_actions.setObjectName("sectionTitle")
        actions_l.addWidget(lbl_actions)

        self.btn_generar = QtWidgets.QPushButton("Generar vista previa")
        self.btn_generar.setProperty("variant", "primary")
        self.btn_generar.setMinimumHeight(36)

        self.btn_guardar = QtWidgets.QPushButton("Guardar TXT")
        self.btn_guardar.setEnabled(False)

        self.btn_copiar = QtWidgets.QPushButton("Copiar texto")
        self.btn_copiar.setEnabled(False)

        self.btn_imprimir = QtWidgets.QPushButton("Imprimir")
        self.btn_imprimir.setEnabled(False)

        self.btn_auto_view = QtWidgets.QPushButton("Ver reportes automaticos")
        self.btn_auto_view.setProperty("variant", "ghost")

        self.btn_auto_run = QtWidgets.QPushButton("Generar meses cerrados")
        self.btn_auto_run.setProperty("variant", "ghost")

        self.btn_inicio = QtWidgets.QPushButton("Abrir inicio")
        self.btn_inicio.setProperty("variant", "ghost")

        actions_l.addWidget(self.btn_generar)
        actions_l.addWidget(self.btn_guardar)
        actions_l.addWidget(self.btn_copiar)
        actions_l.addWidget(self.btn_imprimir)
        actions_l.addWidget(self.btn_auto_view)
        actions_l.addWidget(self.btn_auto_run)
        actions_l.addWidget(self.btn_inicio)

        card_info = QtWidgets.QFrame()
        card_info.setObjectName("panelCard")
        info_l = QtWidgets.QVBoxLayout(card_info)
        info_l.setContentsMargins(12, 12, 12, 12)
        info_l.setSpacing(6)
        left_l.addWidget(card_info)

        lbl_info = QtWidgets.QLabel("Estado")
        lbl_info.setObjectName("sectionTitle")
        info_l.addWidget(lbl_info)

        self.lbl_side_summary = QtWidgets.QLabel("Sin vista previa generada.")
        self.lbl_side_summary.setObjectName("summaryText")
        self.lbl_side_summary.setWordWrap(True)
        info_l.addWidget(self.lbl_side_summary)

        left_l.addStretch(1)

        right = QtWidgets.QFrame()
        right.setObjectName("previewCard")
        right_l = QtWidgets.QVBoxLayout(right)
        right_l.setContentsMargins(12, 12, 12, 12)
        right_l.setSpacing(8)
        split.addWidget(right)
        split.setSizes([350, 830])

        top_preview = QtWidgets.QHBoxLayout()
        top_preview.setSpacing(8)
        right_l.addLayout(top_preview)

        lbl_preview = QtWidgets.QLabel("Vista previa")
        lbl_preview.setObjectName("sectionTitle")
        top_preview.addWidget(lbl_preview)
        top_preview.addStretch(1)

        self.badge_month = QtWidgets.QLabel("Mes: -")
        self.badge_month.setProperty("badge", "true")
        self.badge_chars = QtWidgets.QLabel("Caracteres: 0")
        self.badge_chars.setProperty("badge", "true")
        self.badge_auto = QtWidgets.QLabel("Auto: 0")
        self.badge_auto.setProperty("badge", "true")
        top_preview.addWidget(self.badge_month)
        top_preview.addWidget(self.badge_chars)
        top_preview.addWidget(self.badge_auto)

        self.preview = QtWidgets.QPlainTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setPlaceholderText("Pulsa 'Generar vista previa' para mostrar el reporte.")
        self.preview.setFont(self._make_typewriter_font(11))
        right_l.addWidget(self.preview, 1)

        self.status_info = QtWidgets.QLabel("Listo")
        self.status_info.setObjectName("statusInfo")
        self.statusBar().addPermanentWidget(self.status_info, 1)
        self.statusBar().showMessage("Listo")

        self._load_month_options()

        self.btn_db.clicked.connect(self._pick_db)
        self.btn_dark.toggled.connect(self._toggle_dark_mode)
        self.btn_generar.clicked.connect(self._generate_preview)
        self.btn_guardar.clicked.connect(self._save_txt)
        self.btn_copiar.clicked.connect(self._copy_text)
        self.btn_imprimir.clicked.connect(self._print_preview)
        self.btn_auto_view.clicked.connect(self._open_auto_reports_window)
        self.btn_auto_run.clicked.connect(self._run_auto_generation)
        self.btn_inicio.clicked.connect(self._open_inicio_dashboard)
        self.act_reporte_mensual.triggered.connect(self._focus_reporte_mensual)
        self.act_reporte_ventas.triggered.connect(self._open_reporte_ventas)

        self.btn_dark.blockSignals(True)
        self.btn_dark.setChecked(self._dark_mode)
        self.btn_dark.setText("Modo claro" if self._dark_mode else "Modo oscuro")
        self.btn_dark.blockSignals(False)

        self.cb_month.currentTextChanged.connect(self._update_summary_badges)
        self.ent_empresa.textChanged.connect(self._update_summary_badges)

        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+G"), self, activated=self._generate_preview)
        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+S"), self, activated=self._save_txt)
        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+Shift+C"), self, activated=self._copy_text)
        QtGui.QShortcut(QtGui.QKeySequence("Ctrl+P"), self, activated=self._print_preview)

        self._auto_generate_closed_month_reports()
        self._update_summary_badges()
        self._apply_theme()

    def _set_status(self, message: str):
        self.status_info.setText(message)
        self.statusBar().showMessage(message, 6000)

    def _make_typewriter_font(self, point_size: int = 11) -> QtGui.QFont:
        # Preferencia por estilo maquina de escribir con fallback automatico.
        font = QtGui.QFont("Courier New")
        font.setStyleHint(QtGui.QFont.TypeWriter)
        font.setPointSize(point_size)
        return font

    def _update_summary_badges(self):
        month_txt = (self.cb_month.currentText() or "-").strip() or "-"
        char_count = len(self._last_report_text or "")
        self.badge_month.setText(f"Mes: {month_txt}")
        self.badge_chars.setText(f"Caracteres: {char_count:,}".replace(",", "."))
        self.badge_auto.setText(f"Auto: {self._auto_generated_count}")
        if self._last_report_text:
            self.lbl_side_summary.setText(
                f"Vista previa lista para {self._last_month}. "
                f"{char_count:,} caracteres.".replace(",", ".")
            )
        else:
            self.lbl_side_summary.setText("Selecciona mes y empresa, luego genera la vista previa.")

    def _detect_system_dark_mode(self) -> bool:
        app = QtWidgets.QApplication.instance()
        if not app:
            return False
        pal = app.palette()
        win_color = pal.color(QtGui.QPalette.Window)
        return win_color.lightness() < 128

    def _tune_palette(self):
        app = QtWidgets.QApplication.instance()
        if not app:
            return
        pal = app.palette()
        pal.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#d9e5ff"))
        pal.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#12326b"))
        app.setPalette(pal)

    def _load_month_options(self):
        self.cb_month.clear()
        seen = set()
        now_ym = datetime.now().strftime("%Y-%m")
        self.cb_month.addItem(now_ym)
        seen.add(now_ym)

        db_txt = self.ent_db.text().strip()
        if not db_txt:
            self._update_summary_badges()
            return

        try:
            cn = db.connect("fraccionadora")
            cur = cn.cursor()
            cur.execute(
                """
                SELECT DISTINCT ym FROM (
                    SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM sales_invoices
                    UNION
                    SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM bag_sales
                )
                WHERE ym IS NOT NULL AND ym <> ''
                ORDER BY ym DESC;
                """
            )
            for (ym,) in cur.fetchall():
                if ym not in seen:
                    self.cb_month.addItem(str(ym))
                    seen.add(str(ym))
            cn.close()
        except Exception:
            pass

        self.cb_month.setCurrentText(now_ym)
        self._update_summary_badges()

    def _list_available_months_from_db(self, db_txt: str) -> list[str]:
        months: list[str] = []
        try:
            cn = db.connect("fraccionadora")
            cur = cn.cursor()
            cur.execute(
                """
                SELECT DISTINCT ym FROM (
                    SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM sales_invoices
                    UNION
                    SELECT TO_CHAR((ts)::timestamp, 'YYYY-MM') AS ym FROM bag_sales
                )
                WHERE ym IS NOT NULL AND ym <> ''
                ORDER BY ym ASC;
                """
            )
            months = [str(r[0]) for r in cur.fetchall() if r and r[0]]
            cn.close()
        except Exception:
            return []
        return months

    def _last_closed_month(self) -> str:
        today = datetime.now().date()
        first_current = today.replace(day=1)
        last_day_prev = first_current - timedelta(days=1)
        return last_day_prev.strftime("%Y-%m")

    def _auto_generate_closed_month_reports(self) -> int:
        db_txt = (self.ent_db.text() or "").strip()
        empresa = (self.ent_empresa.text() or "").strip() or "Fraccionadora"
        if not db_txt:
            return 0

        try:
            db_path = _resolve_db(db_txt)
        except Exception:
            return 0

        months = self._list_available_months_from_db(str(db_path))
        if not months:
            return 0

        last_closed = self._last_closed_month()
        out_dir = OUT_DIR_DEFAULT
        out_dir.mkdir(parents=True, exist_ok=True)

        created = 0
        for ym in months:
            if ym > last_closed:
                continue
            out_path = out_dir / f"reporte_mensual_{ym}.txt"
            if out_path.exists():
                continue
            try:
                save_report_for_month(db_path=db_path, ym=ym, empresa=empresa, out_dir=out_dir)
                created += 1
            except Exception:
                continue

        self._auto_generated_count = created
        if created:
            self._set_status(f"Generados {created} reportes automaticos de meses cerrados.")
        self._update_summary_badges()
        return created

    def _pick_db(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Seleccionar fraccionadora.db",
            self.ent_db.text().strip() or str(Path.cwd()),
            "SQLite DB (*.db *.sqlite3);;Todos (*.*)",
        )
        if not path:
            return

        self.ent_db.setText(path)
        self._load_month_options()
        self._auto_generate_closed_month_reports()
        self._set_status("Base de datos actualizada.")

    def _generate_preview(self):
        ym = (self.cb_month.currentText() or "").strip()
        empresa = (self.ent_empresa.text() or "").strip() or "Fraccionadora"
        db_path_txt = (self.ent_db.text() or "").strip()

        if len(ym) != 7 or ym[4] != "-":
            QtWidgets.QMessageBox.warning(self, "Mes invalido", "Use formato YYYY-MM.")
            return

        try:
            db_path = _resolve_db(db_path_txt)
            txt = build_report(db_path=db_path, ym=ym, empresa=empresa)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Error", str(exc))
            self._set_status("No se pudo generar el reporte.")
            return

        self._last_report_text = txt
        self._last_month = ym
        self.preview.setPlainText(txt)
        self.btn_guardar.setEnabled(True)
        self.btn_copiar.setEnabled(True)
        self.btn_imprimir.setEnabled(True)
        self._update_summary_badges()
        self._set_status(f"Vista previa generada para {ym}.")

    def _save_txt(self):
        if not self._last_report_text:
            return

        out_dir = OUT_DIR_DEFAULT
        out_dir.mkdir(parents=True, exist_ok=True)
        default_name = f"reporte_mensual_{self._last_month or datetime.now().strftime('%Y-%m')}.txt"
        target, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Guardar reporte TXT",
            str(out_dir / default_name),
            "TXT (*.txt)",
        )
        if not target:
            self._set_status("Guardado cancelado.")
            return

        try:
            Path(target).write_text(self._last_report_text, encoding="utf-8")
            self._set_status(f"Reporte guardado: {target}")
            QtWidgets.QMessageBox.information(self, "Guardado", f"Archivo guardado:\n{target}")
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Error", str(exc))
            self._set_status("No se pudo guardar el archivo.")

    def _copy_text(self):
        if not self._last_report_text:
            return

        QtWidgets.QApplication.clipboard().setText(self._last_report_text)
        self._set_status("Reporte copiado al portapapeles.")

    def _print_preview(self):
        if not self._last_report_text:
            QtWidgets.QMessageBox.information(
                self,
                "Imprimir",
                "Genera primero la vista previa del reporte para poder imprimir.",
            )
            return

        month_label = self._last_month or datetime.now().strftime("%Y-%m")
        title = f"Reporte mensual {month_label}"
        report_html = escape(self._last_report_text).replace("\n", "<br>\n")
        html_doc = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>{escape(title)}</title>
  <style>
    @page {{ size: A4; margin: 18mm; }}
    body {{
      background: #f3f4f6;
      color: #111827;
      font-family: Consolas, "Courier New", monospace;
      margin: 0;
      padding: 24px;
    }}
    .sheet {{
      background: #ffffff;
      border: 1px solid #d1d5db;
      box-shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
      margin: 0 auto;
      max-width: 900px;
      padding: 28px 32px;
    }}
    h1 {{
      font-family: "Segoe UI", sans-serif;
      font-size: 20px;
      margin: 0 0 8px 0;
    }}
    .meta {{
      color: #4b5563;
      font-family: "Segoe UI", sans-serif;
      font-size: 13px;
      margin-bottom: 20px;
    }}
    .report {{
      font-size: 13px;
      line-height: 1.55;
      white-space: normal;
      word-break: break-word;
    }}
    @media print {{
      body {{ background: #ffffff; padding: 0; }}
      .sheet {{
        border: none;
        box-shadow: none;
        max-width: none;
        padding: 0;
      }}
    }}
  </style>
</head>
<body>
  <div class="sheet">
    <h1>{escape(title)}</h1>
    <div class="meta">Empresa: {escape((self.ent_empresa.text() or "").strip() or "Fraccionadora")} | Mes: {escape(month_label)}</div>
    <div class="report">{report_html}</div>
  </div>
</body>
</html>
"""
        try:
            tmp_dir = Path(tempfile.gettempdir()) / "gcpdfmk10_print_preview"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            html_path = tmp_dir / f"reporte_mensual_{month_label}.html"
            html_path.write_text(html_doc, encoding="utf-8")
            QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(html_path)))
            self._set_status(f"Vista previa HTML abierta en el navegador: {html_path}")
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Imprimir", f"No se pudo abrir la vista previa HTML:\n{exc}")
            self._set_status("No se pudo abrir la vista previa HTML.")

    def _run_auto_generation(self):
        created = self._auto_generate_closed_month_reports()
        if created:
            QtWidgets.QMessageBox.information(
                self,
                "Reportes automaticos",
                f"Se generaron {created} reportes de meses cerrados.",
            )
        else:
            QtWidgets.QMessageBox.information(
                self,
                "Reportes automaticos",
                "No hay reportes nuevos para generar (o ya estaban creados).",
            )
        self._update_summary_badges()

    def _month_human(self, ym: str) -> str:
        months = [
            "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
            "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
        ]
        try:
            y, m = ym.split("-")
            mi = int(m) - 1
            if 0 <= mi < 12:
                return f"{months[mi]} {y}"
        except Exception:
            pass
        return ym

    def _open_auto_reports_window(self):
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Reportes automaticos generados")
        dlg.resize(1040, 680)
        lay = QtWidgets.QVBoxLayout(dlg)
        lay.setContentsMargins(12, 12, 12, 12)
        lay.setSpacing(10)

        hdr = QtWidgets.QLabel("Reportes mensuales guardados")
        hdr.setObjectName("sectionTitle")
        lay.addWidget(hdr)

        split = QtWidgets.QSplitter(QtCore.Qt.Horizontal)
        split.setChildrenCollapsible(False)
        split.setHandleWidth(8)
        lay.addWidget(split, 1)

        left = QtWidgets.QFrame()
        left_lay = QtWidgets.QVBoxLayout(left)
        left_lay.setContentsMargins(0, 0, 0, 0)
        left_lay.setSpacing(8)
        split.addWidget(left)

        lbl = QtWidgets.QLabel("Meses disponibles")
        left_lay.addWidget(lbl)

        listw = QtWidgets.QListWidget()
        listw.setMinimumWidth(250)
        left_lay.addWidget(listw, 1)

        btn_open_dir = QtWidgets.QPushButton("Abrir carpeta de reportes")
        btn_open_dir.setProperty("variant", "ghost")
        left_lay.addWidget(btn_open_dir)

        right = QtWidgets.QFrame()
        right_lay = QtWidgets.QVBoxLayout(right)
        right_lay.setContentsMargins(0, 0, 0, 0)
        right_lay.setSpacing(8)
        split.addWidget(right)
        split.setSizes([280, 760])

        preview = QtWidgets.QPlainTextEdit()
        preview.setReadOnly(True)
        preview.setFont(self.preview.font())
        right_lay.addWidget(preview, 1)

        files = sorted(OUT_DIR_DEFAULT.glob("reporte_mensual_*.txt"), key=lambda p: p.name, reverse=True)
        for p in files:
            ym = p.stem.replace("reporte_mensual_", "")
            item = QtWidgets.QListWidgetItem(self._month_human(ym))
            item.setData(QtCore.Qt.UserRole, str(p))
            listw.addItem(item)

        def _load_selected():
            it = listw.currentItem()
            if not it:
                preview.setPlainText("")
                return
            p = Path(it.data(QtCore.Qt.UserRole))
            try:
                preview.setPlainText(p.read_text(encoding="utf-8"))
            except Exception as exc:
                preview.setPlainText(f"No se pudo abrir {p}\n\n{exc}")

        def _open_dir():
            OUT_DIR_DEFAULT.mkdir(parents=True, exist_ok=True)
            QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(OUT_DIR_DEFAULT)))

        listw.currentItemChanged.connect(lambda *_: _load_selected())
        btn_open_dir.clicked.connect(_open_dir)

        if listw.count():
            listw.setCurrentRow(0)
            _load_selected()
        else:
            preview.setPlainText("No hay reportes automaticos generados todavia.")

        if self._dark_mode:
            preview.setStyleSheet(
                """
                QPlainTextEdit {
                    background: #020617;
                    border: 1px solid #2b3a55;
                    border-radius: 8px;
                    color: #e2e8f0;
                    font-family: Consolas, 'Courier New', monospace;
                    font-size: 12px;
                }
                """
            )
        else:
            preview.setStyleSheet(
                """
                QPlainTextEdit {
                    background: #f8fafc;
                    border: 1px solid #cbd5e1;
                    border-radius: 8px;
                    color: #1f2937;
                    font-family: Consolas, 'Courier New', monospace;
                    font-size: 12px;
                }
                """
            )

        dlg.exec()

    def _toggle_dark_mode(self, enabled: bool):
        self._dark_mode = bool(enabled)
        self.btn_dark.setText("Modo claro" if self._dark_mode else "Modo oscuro")
        self._apply_theme()
        if self._inicio_win is not None:
            self._inicio_win._dark_mode = self._dark_mode
            self._inicio_win._apply_theme()

    def _open_inicio_dashboard(self):
        if self._inicio_win is None:
            self._inicio_win = InicioDashboardWindow(dark_mode=self._dark_mode)
        self._inicio_win.show()
        self._inicio_win.raise_()
        self._inicio_win.activateWindow()
        self._inicio_win.refresh_data()

    def _focus_reporte_mensual(self):
        self.raise_()
        self.activateWindow()
        self._set_status("Ya estas en Reporte mensual.")

    def _open_reporte_ventas(self):
        script_path = Path(__file__).resolve().parent / "reportes_ventas_qt.py"
        if not script_path.exists():
            QtWidgets.QMessageBox.critical(
                self,
                "Reporte de ventas",
                f"No se encontro el script:\n{script_path}",
            )
            return
        try:
            subprocess.Popen([sys.executable, str(script_path)], cwd=str(script_path.parent))
            self._set_status("Abriendo reporte de ventas...")
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Reporte de ventas", f"No se pudo abrir: {exc}")

    def _apply_theme(self):
        if self._dark_mode:
            app = QtWidgets.QApplication.instance()
            if app:
                pal = app.palette()
                pal.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#1e3a8a"))
                pal.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#e5e7eb"))
                app.setPalette(pal)

            self.setStyleSheet(
                """
                QWidget#root { background: #090f1a; color: #e2e8f0; }
                QFrame#headerCard {
                    background: qlineargradient(
                        x1: 0, y1: 0, x2: 1, y2: 0,
                        stop: 0 #0f172a,
                        stop: 1 #111827
                    );
                    border: 1px solid #334155;
                    border-radius: 14px;
                }
                QFrame#sidePanel, QFrame#previewCard, QFrame#panelCard {
                    background: #0f172a;
                    border: 1px solid #334155;
                    border-radius: 12px;
                }
                QLabel#titleLabel { font-size: 21px; font-weight: 700; color: #f8fafc; }
                QLabel#subTitleLabel { font-size: 12px; color: #94a3b8; }
                QLabel#sectionTitle { font-size: 13px; font-weight: 700; color: #cbd5e1; }
                QLabel#summaryText, QLabel#pathHint { color: #9fb0c8; }
                QLabel#statusInfo { color: #93c5fd; font-weight: 600; }
                QLineEdit, QComboBox {
                    background: #111c31;
                    color: #f8fafc;
                    border: 1px solid #3b4d74;
                    border-radius: 8px;
                    padding: 6px 8px;
                }
                QComboBox QAbstractItemView {
                    background: #0f172a;
                    color: #e5e7eb;
                    selection-background-color: #1e3a8a;
                    selection-color: #e5e7eb;
                }
                QPushButton {
                    background: #1e293b;
                    color: #e2e8f0;
                    border: 1px solid #334155;
                    border-radius: 8px;
                    padding: 6px 10px;
                    font-weight: 600;
                }
                QPushButton:hover { background: #334155; }
                QPushButton[variant="primary"] {
                    background: #0ea5e9;
                    color: #06283d;
                    border: none;
                }
                QPushButton[variant="primary"]:hover { background: #38bdf8; }
                QPushButton[variant="ghost"] { background: #0f172a; }
                QPushButton:checked { background: #0284c7; color: #ecfeff; }
                QPushButton:disabled {
                    background: #475569;
                    color: #cbd5e1;
                    border-color: #475569;
                }
                QToolButton#reportsBurger {
                    background: #0f172a;
                    color: #e2e8f0;
                    border: 1px solid #334155;
                    border-radius: 8px;
                    padding: 6px 10px;
                    font-weight: 600;
                }
                QToolButton#reportsBurger:hover { background: #1e293b; }
                QMenu {
                    background: #0f172a;
                    color: #e5e7eb;
                    border: 1px solid #334155;
                }
                QMenu::item { padding: 6px 14px; }
                QMenu::item:selected { background: #1e3a8a; }
                QLabel[badge="true"] {
                    background: #082f49;
                    border: 1px solid #0c4a6e;
                    border-radius: 12px;
                    color: #bae6fd;
                    padding: 4px 10px;
                    font-weight: 600;
                }
                """
            )
            self.preview.setStyleSheet(
                """
                QPlainTextEdit {
                    background: #020617;
                    border: 1px solid #2b3a55;
                    border-radius: 8px;
                    color: #e2e8f0;
                    font-family: Consolas, 'Courier New', monospace;
                    font-size: 12px;
                }
                """
            )
            self.statusBar().setStyleSheet("QStatusBar { color: #93c5fd; }")
        else:
            app = QtWidgets.QApplication.instance()
            if app:
                pal = app.palette()
                pal.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#d9e5ff"))
                pal.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#12326b"))
                app.setPalette(pal)

            self.setStyleSheet(
                """
                QWidget#root { background: #f4f7fb; color: #1f2937; }
                QFrame#headerCard {
                    background: qlineargradient(
                        x1: 0, y1: 0, x2: 1, y2: 0,
                        stop: 0 #eff6ff,
                        stop: 1 #f8fafc
                    );
                    border: 1px solid #bfdbfe;
                    border-radius: 14px;
                }
                QFrame#sidePanel, QFrame#previewCard, QFrame#panelCard {
                    background: #ffffff;
                    border: 1px solid #d1d9e6;
                    border-radius: 12px;
                }
                QLabel#titleLabel { font-size: 21px; font-weight: 700; color: #0f172a; }
                QLabel#subTitleLabel { font-size: 12px; color: #475569; }
                QLabel#sectionTitle { font-size: 13px; font-weight: 700; color: #1e3a8a; }
                QLabel#summaryText, QLabel#pathHint { color: #334155; }
                QLabel#statusInfo { color: #0f172a; font-weight: 600; }
                QLineEdit, QComboBox {
                    background: #ffffff;
                    color: #0f172a;
                    border: 1px solid #bfccdf;
                    border-radius: 8px;
                    padding: 6px 8px;
                }
                QComboBox QAbstractItemView {
                    background: #ffffff;
                    color: #0f172a;
                    selection-background-color: #dbeafe;
                    selection-color: #1e3a8a;
                }
                QPushButton {
                    background: #e2e8f0;
                    color: #0f172a;
                    border: 1px solid #cbd5e1;
                    border-radius: 8px;
                    padding: 6px 10px;
                    font-weight: 600;
                }
                QPushButton:hover { background: #d5deea; }
                QPushButton[variant="primary"] {
                    background: #0369a1;
                    color: #f8fafc;
                    border: none;
                }
                QPushButton[variant="primary"]:hover { background: #0284c7; }
                QPushButton[variant="ghost"] { background: #f8fafc; }
                QPushButton:checked { background: #0ea5e9; color: #082f49; }
                QPushButton:disabled {
                    background: #e2e8f0;
                    color: #94a3b8;
                    border-color: #d6dde8;
                }
                QToolButton#reportsBurger {
                    background: #f8fafc;
                    color: #0f172a;
                    border: 1px solid #cbd5e1;
                    border-radius: 8px;
                    padding: 6px 10px;
                    font-weight: 600;
                }
                QToolButton#reportsBurger:hover { background: #dbe6f4; }
                QMenu {
                    background: #ffffff;
                    color: #0f172a;
                    border: 1px solid #cbd5e1;
                }
                QMenu::item { padding: 6px 14px; }
                QMenu::item:selected { background: #dbeafe; }
                QLabel[badge="true"] {
                    background: #e0f2fe;
                    border: 1px solid #bae6fd;
                    border-radius: 12px;
                    color: #0c4a6e;
                    padding: 4px 10px;
                    font-weight: 600;
                }
                """
            )
            self.preview.setStyleSheet(
                """
                QPlainTextEdit {
                    background: #f8fafc;
                    border: 1px solid #cbd5e1;
                    border-radius: 8px;
                    color: #1f2937;
                    font-family: Consolas, 'Courier New', monospace;
                    font-size: 12px;
                }
                """
            )
            self.statusBar().setStyleSheet("QStatusBar { color: #334155; }")


def main():
    app = QtWidgets.QApplication(sys.argv)
    win = ReporteMensualWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
