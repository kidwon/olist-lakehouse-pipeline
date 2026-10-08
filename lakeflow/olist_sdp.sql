-- Lakeflow Spark Declarative Pipelines version of the same spec (ADR-0012).
-- Reads the landing files the imperative pipeline reads, writes to its own schema (olist_sdp),
-- so the two can be reconciled. The seller SCD2 lives in sellers_scd2.py (historical snapshots
-- in order need the Python API).

-- ---------------------------------------------------------------------------------------------
-- Bronze: each file once, against the contract, unknown fields rescued (ADR-0001, ADR-0006)
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_order_items AS
SELECT *,
       _metadata.file_path AS _source_file,
       to_date(regexp_extract(_metadata.file_path, 'batch_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1)) AS _batch_date
FROM STREAM read_files(
  '${olist.landing}/order_items',
  format => 'json',
  schema => 'order_id STRING, order_item_id INT, product_id STRING, seller_id STRING, order_purchase_ts TIMESTAMP, shipping_limit_ts TIMESTAMP, price DECIMAL(12,2), freight_value DECIMAL(12,2)',
  rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE bronze_orders_cdc AS
SELECT *,
       _metadata.file_path AS _source_file,
       to_date(regexp_extract(_metadata.file_path, 'batch_date=([0-9]{4}-[0-9]{2}-[0-9]{2})', 1)) AS _batch_date
FROM STREAM read_files(
  '${olist.landing}/orders_cdc',
  format => 'json',
  schema => 'order_id STRING, customer_id STRING, order_status STRING, order_purchase_ts TIMESTAMP, order_approved_ts TIMESTAMP, order_delivered_carrier_ts TIMESTAMP, order_delivered_customer_ts TIMESTAMP, order_estimated_delivery_ts TIMESTAMP, change_seq INT, change_ts TIMESTAMP',
  rescuedDataColumn => '_rescued_data'
);

CREATE OR REFRESH STREAMING TABLE bronze_customers AS
SELECT *, _metadata.file_modification_time AS _ingested_at
FROM STREAM read_files(
  '${olist.landing}/customers',
  format => 'json',
  schema => 'customer_id STRING, customer_unique_id STRING, customer_zip_code_prefix STRING, customer_city STRING, customer_state STRING',
  rescuedDataColumn => '_rescued_data'
);

-- Products arrive once, as a full snapshot: a materialized view of the latest one is enough.
CREATE OR REFRESH MATERIALIZED VIEW silver_products AS
SELECT DISTINCT product_id, product_category_name, product_category_name_english
FROM read_files('${olist.landing}/products', format => 'parquet');

-- ---------------------------------------------------------------------------------------------
-- Silver order items: expectations drop bad rows; duplicates collapse on the business key
-- (ADR-0001, ADR-0004, ADR-0005)
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_order_items_valid (
  CONSTRAINT missing_key        EXPECT (order_id IS NOT NULL AND order_item_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT price_not_positive EXPECT (price > 0)                                         ON VIOLATION DROP ROW,
  CONSTRAINT freight_negative   EXPECT (freight_value IS NULL OR freight_value >= 0)       ON VIOLATION DROP ROW,
  CONSTRAINT missing_product_id EXPECT (product_id IS NOT NULL)                            ON VIOLATION DROP ROW,
  CONSTRAINT unknown_product_id EXPECT (product_id IS NULL OR _product_known)              ON VIOLATION DROP ROW,
  CONSTRAINT too_late           EXPECT (_batch_date <= DATE'${olist.backfill_date}'
                                        OR datediff(_batch_date, to_date(order_purchase_ts)) <= 3) ON VIOLATION DROP ROW
) AS
SELECT i.*, p.product_id IS NOT NULL AS _product_known
FROM STREAM bronze_order_items i
LEFT JOIN silver_products p USING (product_id);

-- Expectations only count dropped rows. To keep "nothing disappears silently", the same rules,
-- inverted, write the bad rows and their reasons to a quarantine table.
CREATE OR REFRESH STREAMING TABLE quarantine_order_items AS
SELECT *
FROM (
  SELECT i.*,
         filter(array(
           CASE WHEN order_id IS NULL OR order_item_id IS NULL THEN 'missing_key' END,
           CASE WHEN NOT coalesce(price > 0, false) THEN 'price_not_positive' END,
           CASE WHEN freight_value < 0 THEN 'freight_negative' END,
           CASE WHEN i.product_id IS NULL THEN 'missing_product_id' END,
           CASE WHEN i.product_id IS NOT NULL AND p.product_id IS NULL THEN 'unknown_product_id' END,
           CASE WHEN _batch_date > DATE'${olist.backfill_date}'
                 AND datediff(_batch_date, to_date(order_purchase_ts)) > 3 THEN 'too_late' END
         ), r -> r IS NOT NULL) AS failed_rules
  FROM STREAM bronze_order_items i
  LEFT JOIN silver_products p USING (product_id)
)
WHERE size(failed_rules) > 0;

-- AUTO CDC keyed on the business key keeps one row per order line: re-deliveries collapse.
CREATE OR REFRESH STREAMING TABLE silver_order_items;
CREATE FLOW order_items_dedup AS AUTO CDC INTO silver_order_items
FROM STREAM silver_order_items_valid
KEYS (order_id, order_item_id)
SEQUENCE BY _batch_date
COLUMNS * EXCEPT (_product_known, _rescued_data)
STORED AS SCD TYPE 1;

-- ---------------------------------------------------------------------------------------------
-- Silver orders: forward-only CDC by change_seq is a single AUTO CDC flow (ADR-0002)
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_orders_valid (
  CONSTRAINT missing_key               EXPECT (order_id IS NOT NULL AND change_seq IS NOT NULL AND change_ts IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT unknown_status            EXPECT (order_status IN ('created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled', 'unavailable')) ON VIOLATION DROP ROW,
  CONSTRAINT delivered_before_purchase EXPECT (order_delivered_customer_ts IS NULL OR order_delivered_customer_ts >= order_purchase_ts) ON VIOLATION DROP ROW,
  CONSTRAINT change_before_purchase    EXPECT (change_ts >= order_purchase_ts) ON VIOLATION DROP ROW
) AS SELECT * FROM STREAM bronze_orders_cdc;

CREATE OR REFRESH STREAMING TABLE silver_orders;
CREATE FLOW orders_cdc AS AUTO CDC INTO silver_orders
FROM STREAM silver_orders_valid
KEYS (order_id)
SEQUENCE BY change_seq
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE silver_customers;
CREATE FLOW customers_upsert AS AUTO CDC INTO silver_customers
FROM STREAM bronze_customers
KEYS (customer_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_rescued_data)
STORED AS SCD TYPE 1;

-- ---------------------------------------------------------------------------------------------
-- Quality gate (ADR-0005), emulated: an expectation on an aggregate fails the whole update when
-- a batch has more than 5% bad order items. Unlike the imperative gate, it stops every table in
-- the update (not only gold), and it re-evaluates every batch ever received on each update.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW dq_gate (
  CONSTRAINT bad_ratio_within_threshold EXPECT (bad_ratio <= 0.05) ON VIOLATION FAIL UPDATE
) AS
SELECT b._batch_date,
       count(*) AS total_rows,
       count(q._source_file) AS bad_rows,
       count(q._source_file) / count(*) AS bad_ratio
FROM bronze_order_items b
LEFT JOIN (SELECT DISTINCT _source_file, order_id, order_item_id FROM quarantine_order_items) q
  USING (_source_file, order_id, order_item_id)
GROUP BY b._batch_date;

-- ---------------------------------------------------------------------------------------------
-- Gold: the transaction fact, joined point-in-time to the seller version valid on the order date
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW fact_order_item AS
SELECT i.order_id,
       i.order_item_id,
       cast(date_format(i.order_purchase_ts, 'yyyyMMdd') AS INT) AS order_date_key,
       c.customer_unique_id,
       i.product_id,
       i.seller_id,
       s.__START_AT AS seller_version,
       coalesce(o.order_status, 'unknown') AS order_status,
       i.order_purchase_ts,
       i.price,
       i.freight_value,
       o.order_status IN ('canceled', 'unavailable') AS is_cancelled
FROM silver_order_items i
LEFT JOIN silver_orders o USING (order_id)
LEFT JOIN silver_customers c ON c.customer_id = o.customer_id
LEFT JOIN silver_seller_history s
  ON s.seller_id = i.seller_id
 AND cast(date_format(i.order_purchase_ts, 'yyyyMMdd') AS INT) >= s.__START_AT
 AND (s.__END_AT IS NULL OR cast(date_format(i.order_purchase_ts, 'yyyyMMdd') AS INT) < s.__END_AT);
