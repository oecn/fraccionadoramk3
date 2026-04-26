CREATE SCHEMA IF NOT EXISTS fraccionadora;

CREATE OR REPLACE VIEW fraccionadora.bi_ventas_detalle AS
SELECT
    si.id AS invoice_id,
    sii.id AS invoice_item_id,
    si.ts AS fecha_hora,
    si.ts::date AS fecha,
    EXTRACT(YEAR FROM si.ts)::int AS anio,
    EXTRACT(MONTH FROM si.ts)::int AS mes_numero,
    TO_CHAR(si.ts, 'YYYY-MM') AS periodo,
    TO_CHAR(si.ts, 'TMMonth') AS mes_nombre,
    si.invoice_no,
    COALESCE(NULLIF(BTRIM(si.customer), ''), 'Sin cliente') AS cliente,
    p.id AS product_id,
    p.name AS producto,
    sii.gramaje,
    sii.cantidad,
    sii.price_gs,
    sii.iva,
    sii.line_total,
    sii.line_base,
    sii.line_iva,
    si.gravada5_gs,
    si.iva5_gs,
    si.gravada10_gs,
    si.iva10_gs,
    si.total_gs AS total_factura,
    (sii.cantidad * sii.gramaje) / 1000.0 AS kg_vendidos
FROM fraccionadora.sales_invoice_items sii
JOIN fraccionadora.sales_invoices si
  ON si.id = sii.invoice_id
JOIN fraccionadora.products p
  ON p.id = sii.product_id;

CREATE OR REPLACE VIEW fraccionadora.bi_ventas_mensuales AS
SELECT
    anio,
    mes_numero,
    MIN(periodo) AS periodo,
    MIN(mes_nombre) AS mes_nombre,
    COUNT(DISTINCT invoice_id) AS cantidad_facturas,
    COUNT(DISTINCT cliente) AS cantidad_clientes,
    SUM(cantidad) AS unidades_vendidas,
    SUM(kg_vendidos) AS kg_vendidos,
    SUM(line_total) AS venta_total_gs,
    SUM(line_base) AS base_imponible_gs,
    SUM(line_iva) AS iva_total_gs,
    AVG(total_factura) AS ticket_promedio_gs
FROM fraccionadora.bi_ventas_detalle
GROUP BY anio, mes_numero;

CREATE OR REPLACE VIEW fraccionadora.bi_top_productos AS
SELECT
    anio,
    product_id,
    producto,
    gramaje,
    SUM(cantidad) AS unidades_vendidas,
    SUM(kg_vendidos) AS kg_vendidos,
    SUM(line_total) AS venta_total_gs,
    COUNT(DISTINCT invoice_id) AS cantidad_facturas,
    DENSE_RANK() OVER (
        PARTITION BY anio
        ORDER BY SUM(line_total) DESC, SUM(cantidad) DESC, producto, gramaje
    ) AS ranking_anual
FROM fraccionadora.bi_ventas_detalle
GROUP BY anio, product_id, producto, gramaje;
