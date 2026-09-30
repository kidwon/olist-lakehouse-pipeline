-- Queries for the AI/BI dashboard (Databricks SQL). Catalog: workspace.
-- Page 1: データ品質 (Data quality)   Page 2: 業務指標 (Business)

-- [DQ-1] 不正率の推移 / bad-row ratio per batch and table (line chart, threshold 5%)
SELECT batch_date, table_name,
       SUM(failed_rows) / SUM(total_rows) AS bad_ratio
FROM workspace.olist_ops.dq_metrics
WHERE rule = '__all__'
GROUP BY ALL
ORDER BY batch_date;

-- [DQ-2] ルール別の件数 / hits per rule (stacked bar)
SELECT batch_date, table_name, rule, severity, SUM(failed_rows) AS rows
FROM workspace.olist_ops.dq_metrics
WHERE rule <> '__all__' AND failed_rows > 0
GROUP BY ALL
ORDER BY batch_date;

-- [DQ-3] ゲート判定の履歴 / gate decisions (table)
SELECT evaluated_at, run_id, table_name, bad_rows, total_rows,
       ROUND(bad_ratio * 100, 2) AS bad_pct, passed
FROM workspace.olist_ops.dq_gate_log
ORDER BY evaluated_at DESC;

-- [DQ-4] 隔離レコードの内訳 / latest quarantined records (table)
SELECT batch_date, table_name, array_join(failed_rules, ', ') AS rules, record, source_file
FROM workspace.olist_ops.quarantine
ORDER BY quarantined_at DESC
LIMIT 200;

-- [BIZ-1] 月次 GMV と定時配達率 / monthly GMV and on-time rate (combo chart)
SELECT d.year_month,
       SUM(CASE WHEN NOT f.is_cancelled THEN f.price END) AS gmv,
       AVG(CASE WHEN f.is_delivered THEN CAST(f.is_on_time AS INT) END) AS on_time_rate
FROM workspace.olist_gold.fact_order_item f
JOIN workspace.olist_gold.dim_date d ON d.date_key = f.order_date_key
GROUP BY ALL
ORDER BY d.year_month;

-- [BIZ-2] 注文時点の州別パフォーマンス（SCD2）/ performance by seller state at order time
SELECT s.seller_state,
       COUNT(DISTINCT f.order_id) AS orders,
       SUM(f.price) AS gmv,
       AVG(CASE WHEN f.is_delivered THEN CAST(f.is_on_time AS INT) END) AS on_time_rate
FROM workspace.olist_gold.fact_order_item f
JOIN workspace.olist_gold.dim_seller s ON s.seller_sk = f.seller_sk
WHERE NOT f.is_cancelled
GROUP BY ALL
ORDER BY gmv DESC;

-- [BIZ-3] セラー上位 / top sellers by GMV with delivery and review score
SELECT seller_id, seller_state,
       SUM(gmv) AS gmv, SUM(orders) AS orders,
       SUM(on_time_orders) / NULLIF(SUM(delivered_orders), 0) AS on_time_rate,
       AVG(avg_review_score) AS avg_review_score
FROM workspace.olist_gold.mart_seller_delivery_performance
GROUP BY ALL
ORDER BY gmv DESC
LIMIT 20;
