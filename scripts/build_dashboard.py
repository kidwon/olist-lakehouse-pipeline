"""Generate resources/olist.lvdash.json (the AI/BI dashboard deployed by the bundle).

The .lvdash.json format is verbose and hard to edit by hand, so the dashboard is defined
here and the JSON is regenerated:  python3 scripts/build_dashboard.py
"""

from __future__ import annotations

import json
from pathlib import Path

C = "workspace"
OUT = Path(__file__).resolve().parent.parent / "resources" / "olist.lvdash.json"

DATASETS = {
    "kpi_ops": f"""
        SELECT COUNT(DISTINCT run_id) AS runs,
               COUNT(DISTINCT CASE WHEN NOT passed THEN run_id END) AS blocked_runs,
               (SELECT COUNT(*) FROM {C}.olist_ops.quarantine) AS quarantined
        FROM {C}.olist_ops.dq_gate_log""",
    "dq_ratio": f"""
        SELECT batch_date, table_name AS series, SUM(failed_rows) / SUM(total_rows) AS bad_ratio
        FROM {C}.olist_ops.dq_metrics
        WHERE rule = '__all__'
        GROUP BY ALL
        UNION ALL
        SELECT DISTINCT batch_date, 'threshold (5%)' AS series, 0.05 AS bad_ratio
        FROM {C}.olist_ops.dq_metrics""",
    "dq_errors": f"""
        SELECT batch_date, rule, SUM(failed_rows) AS rows
        FROM {C}.olist_ops.dq_metrics
        WHERE severity = 'error' AND rule <> '__all__' AND failed_rows > 0 AND batch_date > DATE'2018-05-31'
        GROUP BY ALL""",
    "dq_handled": f"""
        SELECT batch_date, rule, SUM(failed_rows) AS rows
        FROM {C}.olist_ops.dq_metrics
        WHERE severity IN ('info', 'warn') AND failed_rows > 0 AND batch_date > DATE'2018-05-31'
        GROUP BY ALL""",
    "dq_gate": f"""
        SELECT evaluated_at, run_id, table_name, bad_rows, total_rows,
               ROUND(bad_ratio * 100, 2) AS bad_pct, passed
        FROM {C}.olist_ops.dq_gate_log
        WHERE total_rows > 0
        ORDER BY evaluated_at DESC, table_name""",
    "dq_quarantine": f"""
        SELECT batch_date, table_name, array_join(failed_rules, ', ') AS rules, record
        FROM {C}.olist_ops.quarantine
        ORDER BY quarantined_at DESC""",
    "ops_incremental": f"""
        SELECT measured_at, replace(target, '{C}.olist_gold.', '') AS fact, mode,
               processed_orders, total_orders,
               processed_orders / total_orders AS processed_share
        FROM {C}.olist_ops.gold_incremental_stats""",
    "kpi_biz": f"""
        SELECT ROUND(SUM(CASE WHEN NOT is_cancelled THEN price END) / 1e6, 2) AS gmv_millions,
               COUNT(DISTINCT order_id) AS orders,
               AVG(CASE WHEN is_delivered THEN CAST(is_on_time AS INT) END) AS on_time_rate
        FROM {C}.olist_gold.fact_order_item""",
    "biz_monthly": f"""
        SELECT to_date(d.year_month || '-01') AS month,
               SUM(CASE WHEN NOT f.is_cancelled THEN f.price END) AS gmv,
               AVG(CASE WHEN f.is_delivered THEN CAST(f.is_on_time AS INT) END) AS on_time_rate
        FROM {C}.olist_gold.fact_order_item f
        JOIN {C}.olist_gold.dim_date d ON d.date_key = f.order_date_key
        WHERE d.date >= DATE'2017-01-01'
        GROUP BY ALL""",
    "biz_state": f"""
        SELECT s.seller_state,
               SUM(f.price) AS gmv,
               AVG(CASE WHEN f.is_delivered THEN CAST(f.is_on_time AS INT) END) AS on_time_rate
        FROM {C}.olist_gold.fact_order_item f
        JOIN {C}.olist_gold.dim_seller s ON s.seller_sk = f.seller_sk
        WHERE NOT f.is_cancelled
        GROUP BY ALL""",
    "biz_fulfillment": f"""
        SELECT to_date(date_format(purchase_ts, 'yyyy-MM-01')) AS month, stage, ROUND(AVG(hours), 1) AS avg_hours
        FROM {C}.olist_gold.fact_order_fulfillment
        LATERAL VIEW STACK(3,
            '1 purchase → approved', hours_to_approve,
            '2 approved → carrier', hours_to_ship,
            '3 carrier → customer', hours_in_transit) s AS stage, hours
        WHERE purchase_ts >= TIMESTAMP'2017-01-01' AND hours IS NOT NULL
        GROUP BY ALL""",
    "biz_backlog": f"""
        SELECT snapshot_date, order_status, SUM(open_orders) AS open_orders
        FROM {C}.olist_gold.fact_daily_order_backlog
        WHERE snapshot_date >= DATE'2017-01-01'
        GROUP BY ALL""",
    "biz_lateness_score": f"""
        SELECT CASE
                 WHEN hours_late <= -168 THEN '1. 7+ days early'
                 WHEN hours_late <= 0    THEN '2. 0-7 days early'
                 WHEN hours_late <= 72   THEN '3. 1-3 days late'
                 WHEN hours_late <= 168  THEN '4. 4-7 days late'
                 ELSE '5. 8+ days late'
               END AS lateness,
               COUNT(*) AS orders,
               ROUND(AVG(review_score), 2) AS avg_review_score
        FROM {C}.olist_gold.fact_order_fulfillment
        WHERE delivered_ts IS NOT NULL AND review_score IS NOT NULL
        GROUP BY ALL""",
    "biz_top": f"""
        SELECT seller_id, seller_state,
               ROUND(SUM(gmv), 0) AS gmv, SUM(orders) AS orders,
               ROUND(SUM(on_time_orders) / NULLIF(SUM(delivered_orders), 0), 3) AS on_time_rate,
               ROUND(AVG(avg_review_score), 2) AS avg_review_score
        FROM {C}.olist_gold.mart_seller_delivery_performance
        GROUP BY ALL
        ORDER BY gmv DESC
        LIMIT 15""",
}

PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}


def query(dataset: str, *fields: str) -> list:
    return [{
        "name": "main_query",
        "query": {
            "datasetName": dataset,
            "fields": [{"name": f, "expression": f"`{f}`"} for f in fields],
            "disaggregated": True,
        },
    }]


def text(name: str, *lines: str) -> dict:
    # Each entry is a raw markdown line; without "\n" they run together into one paragraph.
    return {"name": name, "multilineTextboxSpec": {"lines": [f"{line}\n\n" for line in lines]}}


def counter(name: str, dataset: str, field: str, title: str, fmt: dict | None = None) -> dict:
    value = {"fieldName": field, "displayName": title}
    if fmt:
        value["format"] = fmt
    return {
        "name": name,
        "queries": query(dataset, field),
        "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": value},
                 "frame": {"showTitle": True, "title": title}},
    }


def chart(name: str, kind: str, dataset: str, title: str, x: tuple, y: tuple, color: tuple | None = None) -> dict:
    """x/y/color = (field, display name, scale type[, format])."""
    fields = [x[0], y[0]] + ([color[0]] if color else [])

    def enc(t: tuple) -> dict:
        e = {"fieldName": t[0], "displayName": t[1], "scale": {"type": t[2]}}
        if len(t) > 3 and t[3]:
            e["format"] = t[3]
        if len(t) > 4:
            e["scale"]["sort"] = {"by": t[4]}
        return e

    encodings = {"x": enc(x), "y": enc(y)}
    if color:
        encodings["color"] = enc(color)
    return {
        "name": name,
        "queries": query(dataset, *fields),
        "spec": {"version": 3, "widgetType": kind, "encodings": encodings,
                 "frame": {"showTitle": True, "title": title}},
    }


def table(name: str, dataset: str, title: str, columns: list[tuple[str, str]]) -> dict:
    return {
        "name": name,
        "queries": query(dataset, *[c for c, _ in columns]),
        "spec": {
            "version": 2,
            "widgetType": "table",
            "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in columns]},
            "frame": {"showTitle": True, "title": title},
        },
    }


def at(widget: dict, x: int, y: int, w: int, h: int) -> dict:
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


quality_page = [
    at(text("dq_title",
            "## データ品質 / Data quality",
            "Every rule hit is counted per batch in `olist_ops.dq_metrics`. A run whose bad-row ratio "
            "exceeds 5% is stopped by `dq_gate` before gold is published."), 0, 0, 6, 2),
    at(counter("kpi_runs", "kpi_ops", "runs", "パイプライン実行回数 / Runs"), 0, 2, 2, 3),
    at(counter("kpi_blocked", "kpi_ops", "blocked_runs", "ゲートで停止 / Blocked runs"), 2, 2, 2, 3),
    at(counter("kpi_quarantined", "kpi_ops", "quarantined", "隔離レコード / Quarantined rows"), 4, 2, 2, 3),
    at(chart("dq_ratio_line", "line", "dq_ratio", "不正率の推移 / Bad-row ratio per batch (threshold 5%)",
             ("batch_date", "バッチ日", "temporal"), ("bad_ratio", "不正率", "quantitative", PCT),
             ("series", "テーブル", "categorical")), 0, 5, 6, 6),
    at(chart("dq_errors_bar", "bar", "dq_errors", "隔離したルール違反 / Quarantined, by rule (error)",
             ("batch_date", "バッチ日", "temporal"), ("rows", "件数", "quantitative"),
             ("rule", "ルール", "categorical")), 0, 11, 3, 6),
    at(chart("dq_handled_bar", "bar", "dq_handled", "重複排除・未知フィールド退避 / Duplicates removed, fields rescued (info/warn)",
             ("batch_date", "バッチ日", "temporal"), ("rows", "件数", "quantitative"),
             ("rule", "ルール", "categorical")), 3, 11, 3, 6),
    at(table("dq_gate_table", "dq_gate", "ゲート判定 / Gate decisions",
             [("evaluated_at", "評価日時"), ("run_id", "run_id"), ("table_name", "テーブル"), ("bad_rows", "不正行"),
              ("total_rows", "全行"), ("bad_pct", "不正率 %"), ("passed", "合格")]), 0, 17, 6, 7),
    at(table("dq_quarantine_table", "dq_quarantine", "隔離レコード / Quarantined records",
             [("batch_date", "バッチ日"), ("table_name", "テーブル"), ("rules", "違反ルール"), ("record", "レコード")]),
       0, 24, 6, 7),
    at(chart("ops_incremental_line", "line", "ops_incremental",
             "増分処理：各実行で再計算した注文の割合 / Share of orders rebuilt per run (Change Data Feed)",
             ("measured_at", "実行日時", "temporal"), ("processed_share", "再計算した割合", "quantitative", PCT),
             ("fact", "ファクト", "categorical")), 0, 31, 6, 6),
]

business_page = [
    at(text("biz_title",
            "## 業務指標 / Business",
            "Built from `olist_gold`. Seller location is taken at order time from the SCD2 `dim_seller`, "
            "so a seller who moved keeps past sales in the old state. "
            "2018-06 holds only the 10 replayed days, so its month is partial, and its stage times are "
            "biased low: only orders that finished quickly have a completed stage yet (right-censoring)."), 0, 0, 6, 2),
    at(counter("kpi_gmv", "kpi_biz", "gmv_millions", "GMV（百万 BRL / BRL millions）",
               {"type": "number-plain", "abbreviation": "none", "decimalPlaces": {"type": "exact", "places": 2}}), 0, 2, 2, 3),
    at(counter("kpi_orders", "kpi_biz", "orders", "注文数 / Orders",
               {"type": "number-plain", "abbreviation": "none", "decimalPlaces": {"type": "exact", "places": 0}}), 2, 2, 2, 3),
    at(counter("kpi_on_time", "kpi_biz", "on_time_rate", "定時配達率 / On-time rate", PCT), 4, 2, 2, 3),
    at(chart("biz_gmv_month", "bar", "biz_monthly", "月次 GMV / Monthly GMV (BRL)",
             ("month", "月", "temporal"), ("gmv", "GMV", "quantitative")), 0, 5, 3, 6),
    at(chart("biz_on_time_month", "line", "biz_monthly", "月次 定時配達率 / Monthly on-time rate",
             ("month", "月", "temporal"), ("on_time_rate", "定時配達率", "quantitative", PCT)), 3, 5, 3, 6),
    at(chart("biz_state_bar", "bar", "biz_state", "注文時点のセラー州別 GMV / GMV by seller state at order time",
             ("seller_state", "州", "categorical", None, "y-reversed"), ("gmv", "GMV", "quantitative")), 0, 11, 3, 7),
    at(chart("biz_fulfillment_line", "line", "biz_fulfillment",
             "履約の各段階の平均時間（時間）/ Average hours per fulfillment stage (fact_order_fulfillment)",
             ("month", "月", "temporal"), ("avg_hours", "平均時間", "quantitative"),
             ("stage", "段階", "categorical")), 0, 18, 6, 6),
    at(chart("biz_backlog_line", "line", "biz_backlog",
             "日次の未完了注文（ステータス別）/ Daily open orders by status (fact_daily_order_backlog)",
             ("snapshot_date", "日付", "temporal"), ("open_orders", "未完了の注文", "quantitative"),
             ("order_status", "ステータス", "categorical")), 0, 24, 6, 6),
    at(chart("biz_lateness_score_bar", "bar", "biz_lateness_score",
             "配達の遅れとレビュー評価 / Review score by delivery lateness (fact_order_fulfillment)",
             ("lateness", "配達予定日との差", "categorical"), ("avg_review_score", "平均評価（★）", "quantitative")), 0, 30, 6, 6),
    at(table("biz_top_table", "biz_top", "GMV 上位セラー / Top sellers",
             [("seller_id", "セラー"), ("seller_state", "州"), ("gmv", "GMV"), ("orders", "注文"),
              ("on_time_rate", "定時率"), ("avg_review_score", "評価")]), 3, 11, 3, 7),
]

dashboard = {
    "datasets": [
        {"name": name, "displayName": name, "queryLines": [line.strip() + "\n" for line in sql.strip().splitlines()]}
        for name, sql in DATASETS.items()
    ],
    "pages": [
        {"name": "quality", "displayName": "データ品質 / Data quality", "pageType": "PAGE_TYPE_CANVAS", "layout": quality_page},
        {"name": "business", "displayName": "業務指標 / Business", "pageType": "PAGE_TYPE_CANVAS", "layout": business_page},
    ],
}

if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n")
    print(f"wrote {OUT}")
