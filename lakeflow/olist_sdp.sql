-- Lakeflow Spark Declarative Pipelines implementation of the full spec (ADR-0013).
-- It reads the landing files the imperative pipeline reads and writes to its own schema
-- (olist_sdp). Both implementations are complete and reconciled table by table every day.
-- The seller SCD2 lives in sellers_scd2.py (historical snapshots in order need the Python API).
--
-- The daily job refreshes this pipeline twice: first bronze, silver, quarantine, metrics and the
-- gate; then, only if the gate passed, gold (ADR-0005, ADR-0013).

-- =============================================================================================
-- Bronze: each file once, the raw line kept, parsed against the contract (ADR-0001, ADR-0006).
-- JSON lines are read as text and parsed like the imperative bronze, so `_rescued_data` and
-- `_malformed` mean exactly the same thing in both implementations.
-- =============================================================================================
CREATE OR REFRESH STREAMING TABLE bronze_order_items AS
SELECT p.r.*,
       CASE WHEN size(p.extra) > 0 THEN to_json(p.extra) END AS _rescued_data,
       p.m IS NULL AS _malformed,
       p.value AS _raw,
       p._source_file,
       to_date(regexp_extract(p._source_file, 'batch_date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS _batch_date,
       current_timestamp() AS _ingested_at
FROM (
  SELECT value, _source_file, m,
         from_json(value, 'order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, order_purchase_ts TIMESTAMP, shipping_limit_ts TIMESTAMP, price DECIMAL(12,2), freight_value DECIMAL(12,2)') AS r,
         map_filter(m, (k, v) -> NOT array_contains(array('order_id', 'order_item_id', 'product_id', 'seller_id', 'order_purchase_ts', 'shipping_limit_ts', 'price', 'freight_value'), k)) AS extra
  FROM (
    SELECT value, _metadata.file_path AS _source_file, from_json(value, 'MAP<STRING, STRING>') AS m
    FROM STREAM read_files('${olist.landing}/order_items', format => 'text')
  )
) p;

CREATE OR REFRESH STREAMING TABLE bronze_orders_cdc AS
SELECT p.r.*,
       CASE WHEN size(p.extra) > 0 THEN to_json(p.extra) END AS _rescued_data,
       p.m IS NULL AS _malformed,
       p.value AS _raw,
       p._source_file,
       to_date(regexp_extract(p._source_file, 'batch_date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS _batch_date,
       current_timestamp() AS _ingested_at
FROM (
  SELECT value, _source_file, m,
         from_json(value, 'order_id STRING, customer_id STRING, order_status STRING, order_purchase_ts TIMESTAMP, order_approved_ts TIMESTAMP, order_delivered_carrier_ts TIMESTAMP, order_delivered_customer_ts TIMESTAMP, order_estimated_delivery_ts TIMESTAMP, change_seq INT, change_ts TIMESTAMP') AS r,
         map_filter(m, (k, v) -> NOT array_contains(array('order_id', 'customer_id', 'order_status', 'order_purchase_ts', 'order_approved_ts', 'order_delivered_carrier_ts', 'order_delivered_customer_ts', 'order_estimated_delivery_ts', 'change_seq', 'change_ts'), k)) AS extra
  FROM (
    SELECT value, _metadata.file_path AS _source_file, from_json(value, 'MAP<STRING, STRING>') AS m
    FROM STREAM read_files('${olist.landing}/orders_cdc', format => 'text')
  )
) p;

CREATE OR REFRESH STREAMING TABLE bronze_customers AS
SELECT p.r.*,
       CASE WHEN size(p.extra) > 0 THEN to_json(p.extra) END AS _rescued_data,
       p.m IS NULL AS _malformed,
       p.value AS _raw,
       p._source_file,
       to_date(regexp_extract(p._source_file, 'batch_date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS _batch_date,
       current_timestamp() AS _ingested_at
FROM (
  SELECT value, _source_file, m,
         from_json(value, 'customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, customer_city STRING, customer_state STRING') AS r,
         map_filter(m, (k, v) -> NOT array_contains(array('customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'), k)) AS extra
  FROM (
    SELECT value, _metadata.file_path AS _source_file, from_json(value, 'MAP<STRING, STRING>') AS m
    FROM STREAM read_files('${olist.landing}/customers', format => 'text')
  )
) p;

CREATE OR REFRESH STREAMING TABLE bronze_reviews AS
SELECT p.r.*,
       CASE WHEN size(p.extra) > 0 THEN to_json(p.extra) END AS _rescued_data,
       p.m IS NULL AS _malformed,
       p.value AS _raw,
       p._source_file,
       to_date(regexp_extract(p._source_file, 'batch_date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS _batch_date,
       current_timestamp() AS _ingested_at
FROM (
  SELECT value, _source_file, m,
         from_json(value, 'review_id STRING, order_id STRING, review_score INT, review_creation_ts TIMESTAMP, review_answer_ts TIMESTAMP') AS r,
         map_filter(m, (k, v) -> NOT array_contains(array('review_id', 'order_id', 'review_score', 'review_creation_ts', 'review_answer_ts'), k)) AS extra
  FROM (
    SELECT value, _metadata.file_path AS _source_file, from_json(value, 'MAP<STRING, STRING>') AS m
    FROM STREAM read_files('${olist.landing}/reviews', format => 'text')
  )
) p;

-- Snapshot feeds arrive as parquet with a fixed schema: nothing to rescue, nothing malformed.
CREATE OR REFRESH STREAMING TABLE bronze_sellers AS
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state,
       CAST(NULL AS STRING) AS _rescued_data, false AS _malformed, CAST(NULL AS STRING) AS _raw,
       _metadata.file_path AS _source_file,
       to_date(regexp_extract(_metadata.file_path, 'batch_date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS _batch_date,
       current_timestamp() AS _ingested_at
FROM STREAM read_files('${olist.landing}/sellers', format => 'parquet',
  schema => 'seller_id STRING, seller_zip_code_prefix STRING, seller_city STRING, seller_state STRING');

CREATE OR REFRESH STREAMING TABLE bronze_products AS
SELECT product_id, product_category_name, product_category_name_english, product_weight_g, product_photos_qty,
       CAST(NULL AS STRING) AS _rescued_data, false AS _malformed, CAST(NULL AS STRING) AS _raw,
       _metadata.file_path AS _source_file,
       to_date(regexp_extract(_metadata.file_path, 'batch_date=(\\d{4}-\\d{2}-\\d{2})', 1)) AS _batch_date,
       current_timestamp() AS _ingested_at
FROM STREAM read_files('${olist.landing}/products', format => 'parquet',
  schema => 'product_id STRING, product_category_name STRING, product_category_name_english STRING, product_weight_g INT, product_photos_qty INT');

-- =============================================================================================
-- Rules (ADR-0005): each checked_* table keeps every row and lists the rules it breaks in
-- `_failed_rules` (same names and order as quality.py; a NULL condition counts as a pass).
-- Rules are written once, here; the expectations name them so the event log counts each rule,
-- and keep the rows (no action): quarantine and silver split them below.
-- =============================================================================================
CREATE OR REFRESH STREAMING TABLE checked_products (
  CONSTRAINT missing_key EXPECT (NOT array_contains(_failed_rules, 'missing_key'))
) AS
SELECT *, filter(array(
         CASE WHEN product_id IS NULL THEN 'missing_key' END
       ), r -> r IS NOT NULL) AS _failed_rules
FROM STREAM bronze_products;

CREATE OR REFRESH STREAMING TABLE checked_order_items (
  CONSTRAINT malformed_record   EXPECT (NOT array_contains(_failed_rules, 'malformed_record')),
  CONSTRAINT missing_key        EXPECT (NOT array_contains(_failed_rules, 'missing_key')),
  CONSTRAINT price_not_positive EXPECT (NOT array_contains(_failed_rules, 'price_not_positive')),
  CONSTRAINT freight_negative   EXPECT (NOT array_contains(_failed_rules, 'freight_negative')),
  CONSTRAINT missing_product_id EXPECT (NOT array_contains(_failed_rules, 'missing_product_id')),
  CONSTRAINT unknown_product_id EXPECT (NOT array_contains(_failed_rules, 'unknown_product_id')),
  CONSTRAINT too_late           EXPECT (NOT array_contains(_failed_rules, 'too_late'))
) AS
SELECT *, filter(array(
         CASE WHEN _malformed THEN 'malformed_record' END,
         CASE WHEN order_id IS NULL OR order_item_id IS NULL THEN 'missing_key' END,
         CASE WHEN price IS NULL OR price <= 0 THEN 'price_not_positive' END,
         CASE WHEN freight_value < 0 THEN 'freight_negative' END,
         CASE WHEN product_id IS NULL THEN 'missing_product_id' END,
         CASE WHEN product_id IS NOT NULL AND NOT _product_known THEN 'unknown_product_id' END,
         -- The backfill carries years of history by design, so lateness only applies afterwards.
         CASE WHEN _batch_date > DATE'${olist.backfill_date}'
               AND datediff(_batch_date, to_date(order_purchase_ts)) > ${olist.late_tolerance_days} THEN 'too_late' END
       ), r -> r IS NOT NULL) AS _failed_rules
FROM (
  SELECT i.*, coalesce(p._product_known, false) AS _product_known
  FROM STREAM bronze_order_items i
  LEFT JOIN (SELECT product_id, true AS _product_known FROM silver_products) p USING (product_id)
);

CREATE OR REFRESH STREAMING TABLE checked_orders_cdc (
  CONSTRAINT malformed_record          EXPECT (NOT array_contains(_failed_rules, 'malformed_record')),
  CONSTRAINT missing_key               EXPECT (NOT array_contains(_failed_rules, 'missing_key')),
  CONSTRAINT unknown_status            EXPECT (NOT array_contains(_failed_rules, 'unknown_status')),
  CONSTRAINT delivered_before_purchase EXPECT (NOT array_contains(_failed_rules, 'delivered_before_purchase')),
  CONSTRAINT change_before_purchase    EXPECT (NOT array_contains(_failed_rules, 'change_before_purchase'))
) AS
SELECT *, filter(array(
         CASE WHEN _malformed THEN 'malformed_record' END,
         CASE WHEN order_id IS NULL OR change_seq IS NULL OR change_ts IS NULL THEN 'missing_key' END,
         CASE WHEN NOT order_status IN ('created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable') THEN 'unknown_status' END,
         CASE WHEN order_delivered_customer_ts < order_purchase_ts THEN 'delivered_before_purchase' END,
         CASE WHEN change_ts < order_purchase_ts THEN 'change_before_purchase' END
       ), r -> r IS NOT NULL) AS _failed_rules
FROM STREAM bronze_orders_cdc;

CREATE OR REFRESH STREAMING TABLE checked_customers (
  CONSTRAINT malformed_record EXPECT (NOT array_contains(_failed_rules, 'malformed_record')),
  CONSTRAINT missing_key      EXPECT (NOT array_contains(_failed_rules, 'missing_key'))
) AS
SELECT *, filter(array(
         CASE WHEN _malformed THEN 'malformed_record' END,
         CASE WHEN customer_id IS NULL OR customer_unique_id IS NULL THEN 'missing_key' END
       ), r -> r IS NOT NULL) AS _failed_rules
FROM STREAM bronze_customers;

CREATE OR REFRESH STREAMING TABLE checked_reviews (
  CONSTRAINT malformed_record   EXPECT (NOT array_contains(_failed_rules, 'malformed_record')),
  CONSTRAINT missing_key        EXPECT (NOT array_contains(_failed_rules, 'missing_key')),
  CONSTRAINT score_out_of_range EXPECT (NOT array_contains(_failed_rules, 'score_out_of_range'))
) AS
SELECT *, filter(array(
         CASE WHEN _malformed THEN 'malformed_record' END,
         CASE WHEN review_id IS NULL OR order_id IS NULL THEN 'missing_key' END,
         CASE WHEN NOT review_score BETWEEN 1 AND 5 THEN 'score_out_of_range' END
       ), r -> r IS NOT NULL) AS _failed_rules
FROM STREAM bronze_reviews;

CREATE OR REFRESH STREAMING TABLE checked_sellers (
  CONSTRAINT missing_key EXPECT (NOT array_contains(_failed_rules, 'missing_key'))
) AS
SELECT *, filter(array(
         CASE WHEN seller_id IS NULL THEN 'missing_key' END
       ), r -> r IS NOT NULL) AS _failed_rules
FROM STREAM bronze_sellers;

-- =============================================================================================
-- Quarantine: one table, the same columns as the imperative ops.quarantine; one append flow per feed.
-- =============================================================================================
CREATE OR REFRESH STREAMING TABLE quarantine;

CREATE FLOW quarantine_order_items AS INSERT INTO quarantine BY NAME
SELECT 'order_items' AS table_name, _batch_date AS batch_date, _failed_rules AS failed_rules,
       to_json(struct(order_id, order_item_id, product_id, seller_id, order_purchase_ts, shipping_limit_ts, price, freight_value)) AS record,
       _raw AS raw, _source_file AS source_file, current_timestamp() AS quarantined_at
FROM STREAM checked_order_items WHERE size(_failed_rules) > 0;

CREATE FLOW quarantine_orders_cdc AS INSERT INTO quarantine BY NAME
SELECT 'orders_cdc' AS table_name, _batch_date AS batch_date, _failed_rules AS failed_rules,
       to_json(struct(order_id, customer_id, order_status, order_purchase_ts, order_approved_ts, order_delivered_carrier_ts, order_delivered_customer_ts, order_estimated_delivery_ts, change_seq, change_ts)) AS record,
       _raw AS raw, _source_file AS source_file, current_timestamp() AS quarantined_at
FROM STREAM checked_orders_cdc WHERE size(_failed_rules) > 0;

CREATE FLOW quarantine_customers AS INSERT INTO quarantine BY NAME
SELECT 'customers' AS table_name, _batch_date AS batch_date, _failed_rules AS failed_rules,
       to_json(struct(customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)) AS record,
       _raw AS raw, _source_file AS source_file, current_timestamp() AS quarantined_at
FROM STREAM checked_customers WHERE size(_failed_rules) > 0;

CREATE FLOW quarantine_reviews AS INSERT INTO quarantine BY NAME
SELECT 'reviews' AS table_name, _batch_date AS batch_date, _failed_rules AS failed_rules,
       to_json(struct(review_id, order_id, review_score, review_creation_ts, review_answer_ts)) AS record,
       _raw AS raw, _source_file AS source_file, current_timestamp() AS quarantined_at
FROM STREAM checked_reviews WHERE size(_failed_rules) > 0;

CREATE FLOW quarantine_sellers AS INSERT INTO quarantine BY NAME
SELECT 'sellers' AS table_name, _batch_date AS batch_date, _failed_rules AS failed_rules,
       to_json(struct(seller_id, seller_zip_code_prefix, seller_city, seller_state)) AS record,
       _raw AS raw, _source_file AS source_file, current_timestamp() AS quarantined_at
FROM STREAM checked_sellers WHERE size(_failed_rules) > 0;

CREATE FLOW quarantine_products AS INSERT INTO quarantine BY NAME
SELECT 'products' AS table_name, _batch_date AS batch_date, _failed_rules AS failed_rules,
       to_json(struct(product_id, product_category_name, product_category_name_english, product_weight_g, product_photos_qty)) AS record,
       _raw AS raw, _source_file AS source_file, current_timestamp() AS quarantined_at
FROM STREAM checked_products WHERE size(_failed_rules) > 0;

-- =============================================================================================
-- Silver: the rows that passed every rule, collapsed on the business key by AUTO CDC.
-- =============================================================================================
-- Products: the latest snapshot wins.
CREATE TEMPORARY VIEW valid_products AS SELECT * FROM STREAM checked_products WHERE size(_failed_rules) = 0;
CREATE OR REFRESH STREAMING TABLE silver_products;
CREATE FLOW products_upsert AS AUTO CDC INTO silver_products
FROM STREAM valid_products
KEYS (product_id)
SEQUENCE BY _batch_date
COLUMNS product_id, product_category_name, product_category_name_english, product_weight_g, product_photos_qty,
        _batch_date, _source_file, _ingested_at
STORED AS SCD TYPE 1;

-- Order items are append-only: a key we already hold is a re-delivery, so the FIRST delivery wins
-- (ADR-0001). AUTO CDC keeps the highest sequence, so the sequence is the negated delivery date.
-- Not the ingestion time: a full refresh ingests every day at once, with one ingestion time.
-- Rows are dated by when the sale happened, never by when the file arrived (ADR-0004).
CREATE TEMPORARY VIEW valid_order_items AS
SELECT *,
       to_date(order_purchase_ts) AS event_date,
       CASE WHEN _batch_date > DATE'${olist.backfill_date}'
            THEN datediff(_batch_date, to_date(order_purchase_ts)) ELSE 0 END AS arrival_lag_days,
       -unix_date(_batch_date) AS _first_wins
FROM STREAM checked_order_items WHERE size(_failed_rules) = 0;
CREATE OR REFRESH STREAMING TABLE silver_order_items;
CREATE FLOW order_items_insert_new AS AUTO CDC INTO silver_order_items
FROM STREAM valid_order_items
KEYS (order_id, order_item_id)
SEQUENCE BY _first_wins
COLUMNS order_id, order_item_id, product_id, seller_id, order_purchase_ts, shipping_limit_ts, price, freight_value,
        event_date, arrival_lag_days, _batch_date, _source_file, _ingested_at
STORED AS SCD TYPE 1;

-- Orders: forward-only CDC by change_seq is a single AUTO CDC flow (ADR-0002).
CREATE TEMPORARY VIEW valid_orders_cdc AS SELECT * FROM STREAM checked_orders_cdc WHERE size(_failed_rules) = 0;
CREATE OR REFRESH STREAMING TABLE silver_orders;
CREATE FLOW orders_cdc AS AUTO CDC INTO silver_orders
FROM STREAM valid_orders_cdc
KEYS (order_id)
SEQUENCE BY change_seq
COLUMNS order_id, customer_id, order_status, order_purchase_ts, order_approved_ts, order_delivered_carrier_ts,
        order_delivered_customer_ts, order_estimated_delivery_ts, change_seq, change_ts,
        _batch_date, _source_file, _ingested_at
STORED AS SCD TYPE 1;

-- Every validated change, kept once (ADR-0009): the path that led to the current state.
CREATE TEMPORARY VIEW valid_order_changes AS
SELECT *, -unix_date(_batch_date) AS _first_wins FROM STREAM checked_orders_cdc WHERE size(_failed_rules) = 0;
CREATE OR REFRESH STREAMING TABLE silver_order_changes;
CREATE FLOW order_changes_log AS AUTO CDC INTO silver_order_changes
FROM STREAM valid_order_changes
KEYS (order_id, change_seq)
SEQUENCE BY _first_wins
COLUMNS order_id, customer_id, order_status, order_purchase_ts, order_approved_ts, order_delivered_carrier_ts,
        order_delivered_customer_ts, order_estimated_delivery_ts, change_seq, change_ts,
        _batch_date, _source_file, _ingested_at
STORED AS SCD TYPE 1;

CREATE TEMPORARY VIEW valid_customers AS SELECT * FROM STREAM checked_customers WHERE size(_failed_rules) = 0;
CREATE OR REFRESH STREAMING TABLE silver_customers;
CREATE FLOW customers_upsert AS AUTO CDC INTO silver_customers
FROM STREAM valid_customers
KEYS (customer_id)
SEQUENCE BY STRUCT(_batch_date, _ingested_at)
COLUMNS customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state,
        _batch_date, _source_file, _ingested_at
STORED AS SCD TYPE 1;

-- review_id alone is not unique in Olist. A later delivery wins; within one, the latest answer
-- (a struct sorts NULL first, so an answered review beats an unanswered one).
CREATE TEMPORARY VIEW valid_reviews AS SELECT * FROM STREAM checked_reviews WHERE size(_failed_rules) = 0;
CREATE OR REFRESH STREAMING TABLE silver_reviews;
CREATE FLOW reviews_upsert AS AUTO CDC INTO silver_reviews
FROM STREAM valid_reviews
KEYS (review_id, order_id)
SEQUENCE BY STRUCT(_batch_date, review_answer_ts, _ingested_at)
COLUMNS review_id, order_id, review_score, review_creation_ts, review_answer_ts,
        _batch_date, _source_file, _ingested_at
STORED AS SCD TYPE 1;

-- =============================================================================================
-- Metrics and gate (ADR-0005)
-- =============================================================================================
-- One row per (table, batch, rule), like ops.dq_metrics: __all__ and rescued_data always, a rule
-- only when it fired, plus the de-duplication counts of order items (info).
CREATE OR REFRESH MATERIALIZED VIEW dq_metrics AS
WITH checked AS (
  SELECT 'order_items' AS table_name, _batch_date, _failed_rules, _rescued_data FROM checked_order_items
  UNION ALL SELECT 'orders_cdc', _batch_date, _failed_rules, _rescued_data FROM checked_orders_cdc
  UNION ALL SELECT 'customers', _batch_date, _failed_rules, _rescued_data FROM checked_customers
  UNION ALL SELECT 'reviews', _batch_date, _failed_rules, _rescued_data FROM checked_reviews
  UNION ALL SELECT 'sellers', _batch_date, _failed_rules, _rescued_data FROM checked_sellers
  UNION ALL SELECT 'products', _batch_date, _failed_rules, _rescued_data FROM checked_products
),
totals AS (
  SELECT table_name, _batch_date AS batch_date, count(*) AS total_rows,
         count_if(size(_failed_rules) > 0) AS bad_rows, count_if(_rescued_data IS NOT NULL) AS rescued_rows
  FROM checked GROUP BY ALL
),
per_rule AS (
  SELECT table_name, _batch_date AS batch_date, rule, count(*) AS failed_rows
  FROM checked LATERAL VIEW explode(_failed_rules) r AS rule GROUP BY ALL
),
item_keys AS (  -- passing order items, one row per key and batch
  SELECT _batch_date, order_id, order_item_id, count(*) AS copies
  FROM checked_order_items WHERE size(_failed_rules) = 0 GROUP BY ALL
),
first_seen AS (
  SELECT order_id, order_item_id, min(_batch_date) AS first_batch FROM item_keys GROUP BY ALL
),
duplicates AS (  -- already loaded = the key passed in an earlier batch
  SELECT k._batch_date AS batch_date,
         sum(k.copies) AS total_rows,
         sum(k.copies - 1) AS in_batch,
         count_if(k._batch_date > f.first_batch) AS already_loaded
  FROM item_keys k JOIN first_seen f USING (order_id, order_item_id)
  GROUP BY k._batch_date
)
SELECT table_name, batch_date, '__all__' AS rule, 'error' AS severity, bad_rows AS failed_rows, total_rows FROM totals
UNION ALL
SELECT table_name, batch_date, 'rescued_data', 'warn', rescued_rows, total_rows FROM totals
UNION ALL
SELECT p.table_name, p.batch_date, p.rule, 'error', p.failed_rows, t.total_rows
FROM per_rule p JOIN totals t USING (table_name, batch_date)
UNION ALL
SELECT 'order_items', batch_date, 'duplicate_in_batch', 'info', in_batch, total_rows FROM duplicates
UNION ALL
SELECT 'order_items', batch_date, 'duplicate_already_loaded', 'info', already_loaded, total_rows FROM duplicates;

-- The gate looks at the latest batch only: one bad batch from the past must not fail every later
-- update. Failing it fails this refresh; silver is already written (as in the imperative gate),
-- and the job never starts the gold refresh, so gold keeps the last trusted numbers.
CREATE OR REFRESH MATERIALIZED VIEW dq_gate (
  CONSTRAINT bad_ratio_within_threshold EXPECT (bad_ratio <= ${olist.dq_max_bad_ratio}) ON VIOLATION FAIL UPDATE
) AS
SELECT table_name, batch_date, failed_rows AS bad_rows, total_rows, try_divide(failed_rows, total_rows) AS bad_ratio
FROM dq_metrics
WHERE rule = '__all__' AND batch_date = (SELECT max(batch_date) FROM dq_metrics);

-- =============================================================================================
-- Gold: dimensions, the three Kimball fact types and the mart, as materialized views. The
-- platform decides per refresh whether it can update them incrementally (ADR-0013).
-- =============================================================================================
-- One row per day, plus NOT_YET (-1) for milestones that have not happened (ADR-0009).
CREATE OR REFRESH MATERIALIZED VIEW dim_date AS
SELECT CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
       date,
       date_format(date, 'yyyy-MM-dd') AS date_label,
       year(date) AS year,
       month(date) AS month,
       date_format(date, 'yyyy-MM') AS year_month,
       dayofweek(date) AS day_of_week,
       dayofweek(date) IN (1, 7) AS is_weekend
FROM (SELECT explode(sequence(DATE'2016-01-01', DATE'2019-12-31', INTERVAL 1 DAY)) AS date)
UNION ALL
SELECT -1, CAST(NULL AS DATE), 'not yet happened', CAST(NULL AS INT), CAST(NULL AS INT), 'not yet happened',
       CAST(NULL AS INT), CAST(NULL AS BOOLEAN);

-- One row per person (customer_unique_id), attributes from the most recent order (ADR-0007).
CREATE OR REFRESH MATERIALIZED VIEW dim_customer AS
WITH c AS (
  SELECT c.customer_id, c.customer_unique_id, c.customer_zip_code_prefix, c.customer_city, c.customer_state,
         o.order_purchase_ts
  FROM silver_customers c LEFT JOIN silver_orders o USING (customer_id)
),
latest AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY customer_unique_id ORDER BY order_purchase_ts DESC NULLS LAST) AS rn
    FROM c
  ) WHERE rn = 1
),
stats AS (
  SELECT customer_unique_id, count(DISTINCT customer_id) AS order_count, min(order_purchase_ts) AS first_order_ts
  FROM c GROUP BY customer_unique_id
)
SELECT customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, order_count, first_order_ts
FROM latest JOIN stats USING (customer_unique_id);

CREATE OR REFRESH MATERIALIZED VIEW dim_product AS
SELECT product_id, product_category_name, product_category_name_english, product_weight_g, product_photos_qty
FROM silver_products;

-- The SCD2 history in the shape of the imperative dim_seller (ADR-0003). AUTO CDC starts a
-- seller's first version at the first snapshot; the spec starts it at 1900-01-01, so orders
-- placed before we started observing still find the seller (ADR-0013).
CREATE OR REFRESH MATERIALIZED VIEW dim_seller AS
WITH h AS (
  SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state,
         CASE WHEN __START_AT = min(__START_AT) OVER (PARTITION BY seller_id) THEN DATE'1900-01-01'
              ELSE to_date(CAST(__START_AT AS STRING), 'yyyyMMdd') END AS valid_from,
         to_date(CAST(__END_AT AS STRING), 'yyyyMMdd') AS valid_to
  FROM silver_seller_history
)
SELECT xxhash64(seller_id, valid_from) AS seller_sk,
       seller_id, seller_zip_code_prefix, seller_city, seller_state, valid_from, valid_to,
       valid_to IS NULL AS is_current
FROM h;

-- Transaction fact: one row per order line, the seller version valid on the order date.
CREATE OR REFRESH MATERIALIZED VIEW fact_order_item AS
WITH f AS (
  SELECT i.*, o.customer_id, o.order_status, o.order_delivered_customer_ts, o.order_estimated_delivery_ts,
         c.customer_unique_id, to_date(i.order_purchase_ts) AS order_date
  FROM silver_order_items i
  LEFT JOIN silver_orders o USING (order_id)
  LEFT JOIN silver_customers c ON c.customer_id = o.customer_id
)
SELECT f.order_id,
       f.order_item_id,
       CAST(date_format(f.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
       f.customer_unique_id,
       f.product_id,
       f.seller_id,
       s.seller_sk,
       coalesce(f.order_status, 'unknown') AS order_status,
       f.order_purchase_ts,
       f.order_delivered_customer_ts,
       f.order_estimated_delivery_ts,
       f.price,
       f.freight_value,
       f.order_status IN ('canceled', 'unavailable') AS is_cancelled,
       f.order_delivered_customer_ts IS NOT NULL AS is_delivered,
       CASE WHEN f.order_delivered_customer_ts IS NOT NULL
            THEN to_date(f.order_delivered_customer_ts) <= to_date(f.order_estimated_delivery_ts) END AS is_on_time,
       f.arrival_lag_days
FROM f
LEFT JOIN dim_seller s
  ON s.seller_id = f.seller_id
 AND f.order_date >= s.valid_from
 AND (s.valid_to IS NULL OR f.order_date < s.valid_to);

-- Accumulating snapshot: one row per order, milestones from its latest change (ADR-0009).
CREATE OR REFRESH MATERIALIZED VIEW fact_order_fulfillment AS
WITH latest AS (
  SELECT order_id, customer_id, order_status AS current_status,
         order_purchase_ts AS purchase_ts, order_approved_ts AS approved_ts,
         order_delivered_carrier_ts AS shipped_ts, order_delivered_customer_ts AS delivered_ts,
         order_estimated_delivery_ts AS estimated_ts, change_seq AS last_change_seq
  FROM (SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY change_seq DESC) AS rn FROM silver_order_changes)
  WHERE rn = 1
),
ended AS (  -- the first cancelled/unavailable change ends an unfulfilled order
  SELECT order_id, change_ts AS ended_ts, order_status AS end_reason
  FROM (SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY change_ts, change_seq) AS rn
        FROM silver_order_changes WHERE order_status IN ('canceled', 'unavailable'))
  WHERE rn = 1
),
lines AS (SELECT order_id, count(*) AS item_count, sum(price) AS order_value FROM silver_order_items GROUP BY order_id),
scores AS (  -- an order can have several reviews: their mean
  SELECT order_id, round(avg(review_score), 2) AS review_score, count(*) AS review_count
  FROM silver_reviews GROUP BY order_id
),
f AS (
  SELECT l.*, e.ended_ts, e.end_reason, li.item_count, li.order_value, c.customer_unique_id,
         s.review_score, s.review_count,
         round((unix_timestamp(approved_ts) - unix_timestamp(purchase_ts)) / 3600, 2) AS h_approve,
         round((unix_timestamp(shipped_ts) - unix_timestamp(approved_ts)) / 3600, 2) AS h_ship,
         round((unix_timestamp(delivered_ts) - unix_timestamp(shipped_ts)) / 3600, 2) AS h_transit,
         round((unix_timestamp(delivered_ts) - unix_timestamp(purchase_ts)) / 3600, 2) AS h_total,
         -- The estimate is a date: delivering any time that day is on time.
         CAST(date_add(to_date(estimated_ts), 1) AS TIMESTAMP) AS estimated_day_end
  FROM latest l
  LEFT JOIN ended e USING (order_id)
  LEFT JOIN lines li USING (order_id)
  LEFT JOIN silver_customers c ON c.customer_id = l.customer_id
  LEFT JOIN scores s USING (order_id)
)
SELECT order_id,
       customer_unique_id,
       current_status,
       end_reason,
       coalesce(CAST(date_format(purchase_ts, 'yyyyMMdd') AS INT), -1) AS purchase_date_key,
       coalesce(CAST(date_format(approved_ts, 'yyyyMMdd') AS INT), -1) AS approved_date_key,
       coalesce(CAST(date_format(shipped_ts, 'yyyyMMdd') AS INT), -1) AS shipped_date_key,
       coalesce(CAST(date_format(delivered_ts, 'yyyyMMdd') AS INT), -1) AS delivered_date_key,
       coalesce(CAST(date_format(estimated_ts, 'yyyyMMdd') AS INT), -1) AS estimated_date_key,
       purchase_ts, approved_ts, shipped_ts, delivered_ts, estimated_ts, ended_ts,
       CASE WHEN h_approve >= 0 THEN h_approve END AS hours_to_approve,
       CASE WHEN h_ship >= 0 THEN h_ship END AS hours_to_ship,
       CASE WHEN h_transit >= 0 THEN h_transit END AS hours_in_transit,
       CASE WHEN h_total >= 0 THEN h_total END AS hours_total,
       CASE WHEN delivered_ts IS NOT NULL
            THEN round((unix_timestamp(delivered_ts) - unix_timestamp(estimated_day_end)) / 3600, 2) END AS hours_late,
       CASE WHEN delivered_ts IS NOT NULL THEN to_date(delivered_ts) <= to_date(estimated_ts) END AS is_on_time,
       coalesce(h_approve < 0, false) OR coalesce(h_ship < 0, false)
         OR coalesce(h_transit < 0, false) OR coalesce(h_total < 0, false) AS has_inconsistent_milestones,
       CAST(coalesce(item_count, 0) AS INT) AS item_count,
       order_value,
       review_score,
       CAST(coalesce(review_count, 0) AS INT) AS review_count,
       last_change_seq
FROM f;

-- Periodic snapshot: open orders per day x status x customer state, by event time (ADR-0010).
-- An order's status on day D is that of its highest change_seq dated on or before D.
CREATE OR REFRESH MATERIALIZED VIEW fact_daily_order_backlog AS
WITH end_date AS (SELECT max(to_date(change_ts)) AS d FROM silver_order_changes),
per_day AS (  -- the last change of each order on each day
  SELECT order_id, customer_id, change_date, change_seq, purchase_date, estimated_date
  FROM (
    SELECT order_id, customer_id, to_date(change_ts) AS change_date, change_seq,
           to_date(order_purchase_ts) AS purchase_date, to_date(order_estimated_delivery_ts) AS estimated_date,
           row_number() OVER (PARTITION BY order_id, to_date(change_ts) ORDER BY change_seq DESC) AS rn
    FROM silver_order_changes
  ) WHERE rn = 1
),
segments AS (  -- from each change date on, the state of the highest sequence seen so far
  SELECT p.*,
         max(change_seq) OVER (PARTITION BY order_id ORDER BY change_date
                               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS state_seq,
         coalesce(date_sub(lead(change_date) OVER (PARTITION BY order_id ORDER BY change_date), 1),
                  (SELECT d FROM end_date)) AS segment_end
  FROM per_day p
),
open_segments AS (
  SELECT s.*, ch.order_status
  FROM segments s
  JOIN (SELECT order_id, change_seq AS state_seq, order_status FROM silver_order_changes) ch USING (order_id, state_seq)
  WHERE ch.order_status IN ('created', 'approved', 'invoiced', 'processing', 'shipped')
),
daily AS (  -- a change dated before the purchase never makes an order count before it existed
  SELECT order_id, customer_id, order_status, purchase_date, estimated_date,
         explode(sequence(greatest(change_date, purchase_date), segment_end)) AS snapshot_date
  FROM open_segments
  WHERE greatest(change_date, purchase_date) <= segment_end
),
value AS (SELECT order_id, sum(price) AS order_value FROM silver_order_items GROUP BY order_id)
SELECT coalesce(CAST(date_format(d.snapshot_date, 'yyyyMMdd') AS INT), -1) AS snapshot_date_key,
       d.snapshot_date,
       d.order_status,
       coalesce(c.customer_state, 'unknown') AS customer_state,
       count(*) AS open_orders,
       coalesce(sum(v.order_value), 0) AS open_order_value,
       percentile_approx(datediff(d.snapshot_date, d.purchase_date), 0.5) AS median_age_days,
       count_if(datediff(d.snapshot_date, d.purchase_date) > 30) AS orders_open_over_30_days,
       count_if(d.snapshot_date > d.estimated_date) AS overdue_orders
FROM daily d
LEFT JOIN value v USING (order_id)
LEFT JOIN silver_customers c ON c.customer_id = d.customer_id
GROUP BY d.snapshot_date, d.order_status, coalesce(c.customer_state, 'unknown');

-- Seller x month: GMV, on-time delivery rate and review score.
CREATE OR REFRESH MATERIALIZED VIEW mart_seller_delivery_performance AS
WITH order_score AS (SELECT order_id, avg(review_score) AS order_score FROM silver_reviews GROUP BY order_id),
f AS (
  SELECT fi.*, date_format(fi.order_purchase_ts, 'yyyy-MM') AS year_month, os.order_score,
         NOT coalesce(fi.is_cancelled, false) AS sold
  FROM fact_order_item fi LEFT JOIN order_score os USING (order_id)
),
orders_per_seller AS (
  SELECT seller_id, year_month, order_id,
         max(CASE WHEN sold THEN 1 ELSE 0 END) AS sold,
         max(CAST(is_delivered AS INT)) AS delivered,
         max(CAST(is_on_time AS INT)) AS on_time,
         first(order_score, true) AS order_score
  FROM f GROUP BY seller_id, year_month, order_id
),
per_order AS (
  SELECT seller_id, year_month, sum(sold) AS orders, sum(delivered) AS delivered_orders,
         sum(on_time) AS on_time_orders, round(avg(order_score), 2) AS avg_review_score
  FROM orders_per_seller GROUP BY seller_id, year_month
),
money AS (
  SELECT seller_id, year_month, sum(price) AS gmv, sum(freight_value) AS freight, count(*) AS items
  FROM f WHERE sold GROUP BY seller_id, year_month
),
current_seller AS (SELECT seller_id, seller_city, seller_state FROM dim_seller WHERE is_current)
SELECT p.seller_id, cs.seller_city, cs.seller_state, p.year_month, p.orders, m.items, m.gmv, m.freight,
       p.delivered_orders, p.on_time_orders,
       round(try_divide(p.on_time_orders, p.delivered_orders), 4) AS on_time_rate,
       p.avg_review_score
FROM per_order p
LEFT JOIN money m USING (seller_id, year_month)
LEFT JOIN current_seller cs USING (seller_id);
