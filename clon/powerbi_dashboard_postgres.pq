let
    Server = "192.168.10.13",
    Database = "GRANOS",
    Source = PostgreSQL.Database(Server, Database, [CreateNavigationProperties = false]),

    PagosFlags = Value.NativeQuery(
        Source,
        "
        SELECT
            lot_id,
            paid,
            updated_ts
        FROM fraccionadora.dashboard_payment_flags
        ORDER BY lot_id;
        "
    ),

    PagosRegistrados = Value.NativeQuery(
        Source,
        "
        SELECT
            d.id,
            d.payment_group_id,
            d.lot_id,
            d.proveedor,
            d.factura,
            d.monto_gs,
            d.fecha_pago,
            d.medio,
            d.referencia,
            d.nro_deposito,
            d.nro_recibo_dinero,
            d.observacion,
            d.facturas_grupo_json,
            d.total_grupo_gs,
            d.ts_registro,
            d.ts_modificacion,
            rl.ts AS lote_ts,
            rl.lote,
            rl.costo_total_gs AS lote_costo_total_gs,
            rl.kg_inicial,
            rl.kg_saldo
        FROM fraccionadora.dashboard_payment_details d
        LEFT JOIN fraccionadora.raw_lots rl
            ON rl.id = d.lot_id
        ORDER BY d.fecha_pago DESC NULLS LAST, d.ts_registro DESC NULLS LAST, d.id DESC;
        "
    ),

    PagosPendientes = Value.NativeQuery(
        Source,
        "
        SELECT
            rl.id AS lot_id,
            rl.ts,
            rl.lote,
            rl.factura,
            rl.proveedor,
            rl.kg_inicial,
            rl.kg_saldo,
            rl.costo_total_gs,
            rl.costo_kg_gs,
            COALESCE(pf.paid, 0) AS paid
        FROM fraccionadora.raw_lots rl
        LEFT JOIN fraccionadora.dashboard_payment_flags pf
            ON pf.lot_id = rl.id
        WHERE COALESCE(pf.paid, 0) = 0
        ORDER BY rl.ts DESC NULLS LAST, rl.id DESC;
        "
    ),

    CobrosFlags = Value.NativeQuery(
        Source,
        "
        SELECT
            status_key,
            invoice_id,
            invoice_ts,
            invoice_no,
            collected,
            updated_ts
        FROM fraccionadora.dashboard_collection_flags
        ORDER BY invoice_id, status_key;
        "
    ),

    CobrosRegistrados = Value.NativeQuery(
        Source,
        "
        SELECT
            d.id,
            d.invoice_id,
            d.invoice_ts,
            d.invoice_no,
            d.cliente,
            d.monto_total_gs,
            d.monto_total_ret_gs,
            d.fecha_cobro,
            d.medio,
            d.nro_cheque,
            d.nro_deposito,
            d.referencia,
            d.observacion,
            d.ts_registro,
            d.ts_modificacion,
            si.customer,
            si.total_gs AS sales_invoice_total_gs,
            si.iva5_gs,
            si.iva10_gs
        FROM fraccionadora.dashboard_collection_details d
        LEFT JOIN fraccionadora.sales_invoices si
            ON si.id = d.invoice_id
        ORDER BY d.fecha_cobro DESC NULLS LAST, d.ts_registro DESC NULLS LAST, d.id DESC;
        "
    ),

    CobrosPendientes = Value.NativeQuery(
        Source,
        "
        WITH flags AS (
            SELECT
                invoice_id,
                MAX(CASE WHEN collected <> 0 THEN 1 ELSE 0 END) AS collected
            FROM fraccionadora.dashboard_collection_flags
            GROUP BY invoice_id
        )
        SELECT
            si.id AS invoice_id,
            si.ts,
            si.invoice_no,
            si.customer,
            si.gravada5_gs,
            si.iva5_gs,
            si.gravada10_gs,
            si.iva10_gs,
            si.total_gs,
            (COALESCE(si.total_gs, 0) - 0.3 * (COALESCE(si.iva5_gs, 0) + COALESCE(si.iva10_gs, 0))) AS total_con_retencion_gs,
            COALESCE(f.collected, 0) AS collected
        FROM fraccionadora.sales_invoices si
        LEFT JOIN flags f
            ON f.invoice_id = si.id
        WHERE COALESCE(f.collected, 0) = 0
        ORDER BY si.ts DESC NULLS LAST, si.id DESC;
        "
    ),

    Navigation = #table(
        {"Name", "Data", "ItemKind", "ItemName", "IsLeaf"},
        {
            {"PagosFlags", PagosFlags, "Table", "Table", true},
            {"PagosRegistrados", PagosRegistrados, "Table", "Table", true},
            {"PagosPendientes", PagosPendientes, "Table", "Table", true},
            {"CobrosFlags", CobrosFlags, "Table", "Table", true},
            {"CobrosRegistrados", CobrosRegistrados, "Table", "Table", true},
            {"CobrosPendientes", CobrosPendientes, "Table", "Table", true}
        }
    )
in
    Navigation
