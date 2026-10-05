# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Bronze: data contracts and `_rescued_data` / 数据契约与 `_rescued_data` / データ契約と `_rescued_data`
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Bronze has two jobs. It ingests every file **exactly once** (Auto Loader with a checkpoint and `trigger(availableNow=True)`), and it parses each line against an **explicit contract** instead of inferring a schema. The contract is in `src/olist_pipeline/contracts.py`; the parsing is `bronze.parse_json_lines`. Design decision: ADR-0006.
# MAGIC
# MAGIC Why not let Spark infer the schema? Inference lets the producer change your tables: a new field becomes a new column, a type flip breaks downstream code, and nobody decided either.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Bronze 有两项任务。一是保证每个文件**只摄取一次**（Auto Loader 配合 checkpoint 和 `trigger(availableNow=True)`）；二是按**明确的契约**解析每一行，而不是让 Spark 推断 schema。契约定义在 `src/olist_pipeline/contracts.py`，解析逻辑是 `bronze.parse_json_lines`。设计决策见 ADR-0006。
# MAGIC
# MAGIC 为什么不让 Spark 自动推断 schema？因为推断会让上游改变你的表：新字段变成新列，类型一变下游代码就坏，而这些都不是谁有意决定的。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Bronze の役割は2つです。各ファイルを**一度だけ**取り込むこと（チェックポイントと `trigger(availableNow=True)` 付きの Auto Loader）、そして各行をスキーマ推論ではなく**明示的な契約**に沿ってパースすることです。契約は `src/olist_pipeline/contracts.py` に、パース処理は `bronze.parse_json_lines` にあります。設計判断は ADR-0006 を参照してください。
# MAGIC
# MAGIC なぜ Spark にスキーマを推論させないのでしょうか。推論に任せると、送信元がテーブルを変えられてしまいます。新しいフィールドは新しい列になり、型が変われば下流のコードが壊れ、しかも誰もそれを決めていません。

# COMMAND ----------

from pyspark.sql import functions as F

from walkthrough_setup import setup
from olist_pipeline import bronze
from olist_pipeline.contracts import FEEDS, ORDER_ITEMS

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The contracts / 契约 / 契約
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Six feeds, three kinds: `append` (new rows only), `cdc` (one row per change) and `snapshot` (the full list every time). The kind decides how silver merges the feed, and the keys decide what counts as "the same row".
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 6 个数据源分三类：`append`（只有新增行）、`cdc`（每次变更一行）和 `snapshot`（每次都是完整列表）。类型决定 Silver 怎么 MERGE 这个数据源，键决定什么算"同一行"。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 6つのフィードは3種類に分かれます。`append`（新しい行のみ）、`cdc`（変更ごとに1行）、`snapshot`（毎回全件）です。種類によって Silver での MERGE の仕方が決まり、キーによって何を「同じ行」とみなすかが決まります。

# COMMAND ----------

for f in FEEDS.values():
    print(f"{f.name:<12} {f.fmt:<8} {f.kind:<9} keys={list(f.keys)}  fields={[x.name for x in f.schema.fields]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parsing lines against the contract / 按契约解析每一行 / 契約に沿って各行をパースする
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Bronze reads JSON files as **text** and parses each line with `from_json(contract)`. Four lines, four outcomes:
# MAGIC - a normal line parses into typed columns;
# MAGIC - a line with a field the contract does not know keeps that field in `_rescued_data`, so nothing is lost and the table schema does not change;
# MAGIC - a line with a wrong type (`price: "abc"`) parses with that column as NULL; silver's rule `price_not_positive` will quarantine it;
# MAGIC - a line that is not JSON at all gets `_malformed = true`; silver's rule `malformed_record` will quarantine it.
# MAGIC
# MAGIC The raw line is always kept in `_raw`, so a future contract version can backfill from bronze without re-reading files.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Bronze 把 JSON 文件当作**文本**读取，再用 `from_json(契约)` 解析每一行。四行数据，四种结果：
# MAGIC - 正常的行被解析成带类型的列；
# MAGIC - 带有契约之外字段的行，会把那个字段保存在 `_rescued_data` 里，既不丢数据，表结构也不变；
# MAGIC - 类型不对的行（`price: "abc"`）能解析，但那一列是 NULL；Silver 的 `price_not_positive` 规则会把它隔离；
# MAGIC - 根本不是 JSON 的行会被标记 `_malformed = true`；Silver 的 `malformed_record` 规则会把它隔离。
# MAGIC
# MAGIC 原始行始终保存在 `_raw` 里，将来修订契约时可以直接从 Bronze 回填，不用重新读文件。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Bronze は JSON ファイルを**テキスト**として読み、各行を `from_json(契約)` でパースします。4行のデータで、4通りの結果になります。
# MAGIC - 通常の行は、型付きの列にパースされます。
# MAGIC - 契約にないフィールドを含む行は、そのフィールドを `_rescued_data` に残します。データは失われず、テーブルのスキーマも変わりません。
# MAGIC - 型が違う行（`price: "abc"`）はパースされますが、その列は NULL になります。Silver の `price_not_positive` ルールで隔離されます。
# MAGIC - JSON ですらない行には `_malformed = true` が付きます。Silver の `malformed_record` ルールで隔離されます。
# MAGIC
# MAGIC 元の行は常に `_raw` に残るため、将来契約を改訂したときに、ファイルを読み直さず Bronze からバックフィルできます。

# COMMAND ----------

lines = spark.createDataFrame(
    [
        ('{"order_id":"o1","order_item_id":1,"product_id":"p1","price":129.9,"freight_value":12.5,"order_purchase_ts":"2018-06-06T10:00:00"}',),
        ('{"order_id":"o2","order_item_id":1,"product_id":"p2","price":59.0,"freight_value":8.0,"discount_amount":2.95}',),
        ('{"order_id":"o3","order_item_id":1,"product_id":"p3","price":"abc","freight_value":5.0}',),
        ("this is not json",),
    ],
    "value string",
)
parsed = bronze.parse_json_lines(lines, ORDER_ITEMS)
show(parsed.select("order_id", "order_item_id", "price", "order_purchase_ts", "_rescued_data", "_malformed"))
parsed.select("order_id", "price").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Where `_batch_date` comes from / `_batch_date` 从哪来 / `_batch_date` はどこから来るか
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Files land in `landing/<feed>/batch_date=YYYY-MM-DD/`. Bronze takes the batch date from the **file path** (`_metadata.file_path`), not from the data, because the delivery day is a fact about the file. Silver uses it to measure lateness: batch date minus the day the sale happened.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 文件落在 `landing/<feed>/batch_date=YYYY-MM-DD/` 下。Bronze 从**文件路径**（`_metadata.file_path`）里取批次日期，而不是从数据里取，因为投递日期是文件本身的属性。Silver 用它来衡量迟到程度：批次日期减去销售发生的日期。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC ファイルは `landing/<feed>/batch_date=YYYY-MM-DD/` に置かれます。Bronze はバッチ日を、データではなく**ファイルパス**（`_metadata.file_path`）から取得します。配信日はファイル自体の属性だからです。Silver はこれを使って遅延を測ります（バッチ日 − 販売日）。

# COMMAND ----------

paths = spark.createDataFrame(
    [("/Volumes/workspace/olist_raw/files/landing/order_items/batch_date=2018-06-06/part-00000.json",)], "path string"
)
show(paths.select("path", F.to_date(F.regexp_extract("path", bronze.BATCH_DATE_RE, 1)).alias("_batch_date")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Add `"seller_rating": 4.5` to the first line. Where does it end up?
# MAGIC 2. Parse the same lines with `spark.read.json(lines.rdd.map(lambda r: r.value))` (schema inference) and compare the schema: `price` becomes a string for every row because of one bad line. That is the failure a contract prevents.
# MAGIC 3. In `contracts.py`, `price` is `DecimalType(12, 2)`, not double. Why is that the right choice for money?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 在第一行里加上 `"seller_rating": 4.5`。它最后去了哪里？
# MAGIC 2. 用 `spark.read.json(lines.rdd.map(lambda r: r.value))`（schema 推断）解析同样的数据，对比 schema：因为一行坏数据，所有行的 `price` 都变成了字符串。这正是契约要防止的问题。
# MAGIC 3. 在 `contracts.py` 里，`price` 是 `DecimalType(12, 2)` 而不是 double。为什么金额用它才是对的？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. 1行目に `"seller_rating": 4.5` を追加してください。どこに入るでしょうか。
# MAGIC 2. 同じ行を `spark.read.json(lines.rdd.map(lambda r: r.value))`（スキーマ推論）でパースし、スキーマを比べてください。不正な1行のせいで、すべての行の `price` が文字列になります。これが契約で防いでいる問題です。
# MAGIC 3. `contracts.py` では `price` が double ではなく `DecimalType(12, 2)` です。金額にこれが適している理由は何でしょうか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Bronze parses every feed against an explicit contract instead of inferring the schema, so the producer can't change my tables. Unknown fields go to `_rescued_data` and are counted as a warning, and the raw line is kept. When the items feed started sending `discount_amount` mid-replay, nothing broke, nothing was lost, and the dashboard showed exactly how many rows carried it. Adopting the field is then a deliberate contract change."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "Bronze 按明确的契约解析每个数据源，而不是推断 schema，所以上游没法改变我的表。未知字段放进 `_rescued_data` 并计为警告，原始行也会保留。回放进行到一半时，明细数据开始带上 `discount_amount`，管道没坏、数据没丢，Dashboard 上能准确看到有多少行带了这个字段。要不要采用这个字段，是一次有意的契约变更。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「Bronze ではスキーマを推論せず、明示的な契約に沿って各フィードをパースしているため、送信元がテーブルを変えることはできません。未知のフィールドは `_rescued_data` に退避して警告として計上し、元の行も残します。リプレイの途中で明細フィードが `discount_amount` を送り始めたときも、何も壊れず、何も失われず、それを含む行数がダッシュボードで正確に分かりました。そのフィールドを採用するかどうかは、意図的な契約変更として決めます。」
