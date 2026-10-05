# Databricks notebook source
# MAGIC %md
# MAGIC # 05 Data quality: rules, quarantine, metrics and the gate / 规则、隔离、指标与闸门 / ルール、隔離、メトリクス、ゲート
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Bad rows will arrive. This notebook answers three questions with the real code in `quality.py` and `gate.py`: **where does a bad row go** (quarantine, with its reasons), **who finds out** (per-batch metrics that a dashboard reads), and **when does the pipeline stop** (the gate, before gold). Design decisions: ADR-0004 (lateness) and ADR-0005 (quality).
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 坏数据一定会来。这个 notebook 用 `quality.py` 和 `gate.py` 里的真实代码回答三个问题：**坏行去哪里**（带着原因进入隔离区）、**谁会发现**（Dashboard 读取的每批次指标），以及**管道什么时候停下**（Gold 之前的闸门）。设计决策见 ADR-0004（迟到）和 ADR-0005（数据质量）。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 不正な行は必ず届きます。このノートブックでは、`quality.py` と `gate.py` の本物のコードで3つの問いに答えます。**不正な行はどこへ行くのか**（理由付きで隔離）、**誰が気付くのか**（ダッシュボードが読むバッチごとのメトリクス）、**パイプラインはいつ止まるのか**（Gold の手前のゲート）です。設計判断は ADR-0004（遅延）と ADR-0005（データ品質）を参照してください。

# COMMAND ----------

import datetime as dt

from pyspark.sql import functions as F

from walkthrough_setup import fresh_table, setup
from olist_pipeline import gate
from olist_pipeline import quality as dq
from olist_pipeline.config import Config

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))

catalog, schema_name = SCHEMA.split(".") if "." in SCHEMA else (None, SCHEMA)
for t in ["dq_metrics", "quarantine", "dq_gate_log"]:
    fresh_table(spark, SCHEMA, t)


def cfg_for(run_id):
    # Point every layer at the scratch schema so nothing touches the real pipeline tables.
    return Config(base_path="/tmp/unused", catalog=catalog, run_id=run_id, backfill_date=dt.date(2018, 5, 31),
                  schemas={k: schema_name for k in ["bronze", "silver", "gold", "ops"]})


ITEM_COLS = "order_id string, order_item_id int, product_id string, price double, freight_value double, " \
            "order_purchase_ts timestamp, _batch_date date, _malformed boolean, _product_known boolean, _raw string, _source_file string"


def items_batch(batch_date, bad_rows):
    """20 clean order items sold on `batch_date`, then `bad_rows` of them broken."""
    sold = dt.datetime.combine(batch_date, dt.time(12))
    rows = [(f"o{i}", 1, "p1", 100.0, 10.0, sold, batch_date, False, True, "{...}", f"batch_date={batch_date}/part-0.json") for i in range(20)]
    for i, kind in enumerate(bad_rows):
        r = list(rows[i])
        if kind == "negative_price":
            r[3] = -100.0
        elif kind == "unknown_product":
            r[2], r[8] = "p999", False
        elif kind == "missing_product":
            r[2] = None
        elif kind == "five_days_late":
            r[5] = sold - dt.timedelta(days=5)
        rows[i] = tuple(r)
    return spark.createDataFrame(rows, ITEM_COLS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Every bad row carries every rule it breaks / 每个坏行都带着它违反的所有规则 / 不正な行は違反したすべてのルールを持つ
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC A rule is a name plus a condition that is TRUE when the row is bad (`dq.item_rules`). `dq.evaluate` adds `_failed_rules`, the list of every rule the row breaks, not just the first one. NULL conditions count as a pass, so a missing product is reported once, as `missing_product_id`, not also as `unknown_product_id`.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 一条规则就是一个名字，加上一个"行有问题时为 TRUE"的条件（`dq.item_rules`）。`dq.evaluate` 会加上 `_failed_rules`，列出这行违反的所有规则，而不只是第一条。条件为 NULL 视为通过，所以缺失的商品只报告一次 `missing_product_id`，不会同时报 `unknown_product_id`。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC ルールとは、名前と「行が不正なときに TRUE になる条件」の組です（`dq.item_rules`）。`dq.evaluate` は `_failed_rules` を追加し、最初の1つだけでなく、その行が違反したすべてのルールを列挙します。条件が NULL のときは合格とみなすので、商品の欠落は `missing_product_id` として1回だけ報告され、`unknown_product_id` としては重ねて報告されません。

# COMMAND ----------

rules = dq.item_rules(cfg_for("demo"))
print("rules:", [r.name for r in rules])
batch = items_batch(dt.date(2018, 6, 6), ["negative_price", "unknown_product", "missing_product", "five_days_late"])
show(dq.evaluate(batch, rules).select("order_id", "product_id", "price", F.to_date("order_purchase_ts").alias("sold_on"), "_batch_date", "_failed_rules").orderBy(F.size("_failed_rules").desc(), "order_id").limit(6))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quarantine and metrics / 隔离与指标 / 隔離とメトリクス
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `dq.record` is what silver calls inside every micro-batch. It writes the bad rows to `quarantine` (with the reasons, the parsed record, the raw line and the source file), writes one metric row per rule to `dq_metrics`, and returns only the good rows. `__all__` counts rows that broke **at least one** rule, so a row that broke two rules is still one bad row.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `dq.record` 就是 Silver 在每个微批次里调用的函数。它把坏行写入 `quarantine`（带着原因、解析后的记录、原始行和来源文件），按规则把指标写入 `dq_metrics`，然后只返回好的行。`__all__` 统计的是违反了**至少一条**规则的行数，所以一行同时违反两条规则，仍然只算一个坏行。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `dq.record` は Silver が各マイクロバッチの中で呼び出す関数です。不正な行を `quarantine` に書き込み（理由、パース済みのレコード、元の行、ソースファイル付き）、ルールごとに1行のメトリクスを `dq_metrics` に書き込み、正しい行だけを返します。`__all__` は**少なくとも1つ**のルールに違反した行数なので、2つのルールに違反した行も不正な行1行として数えます。

# COMMAND ----------

cfg_ok = cfg_for("run-06-06")
contract_cols = ["order_id", "order_item_id", "product_id", "price", "freight_value", "order_purchase_ts"]
good = dq.record(spark, cfg_ok, dq.evaluate(batch, rules), rules, "order_items", contract_cols)
print(f"rows in: {batch.count()}, rows passed on to the MERGE: {good.count()}")
show(spark.table(f"{SCHEMA}.quarantine").select("table_name", "batch_date", "failed_rules", "record"))
show(spark.table(f"{SCHEMA}.dq_metrics").where("failed_rows > 0").select("run_id", "rule", "severity", "failed_rows", "total_rows"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The gate / 闸门 / ゲート
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC After silver, `gate.run` reads the metrics of **this run only**, computes bad rows ÷ total rows per table, and raises `DataQualityGateError` when the ratio is above 5%. The job task fails, so gold and the mart are not published. It does **not** roll back silver: good rows are correct on their own, and bad rows are already isolated. The next clean run publishes gold from silver.
# MAGIC
# MAGIC Two runs below: the one above (4 bad of 20 = 20%), and a cleaner one with exactly 1 bad row of 20 = 5%, right on the threshold.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Silver 之后，`gate.run` 只读取**本次运行**的指标，按表计算"坏行数 ÷ 总行数"，比例超过 5% 就抛出 `DataQualityGateError`。作业任务因此失败，Gold 和 mart 都不会发布。它**不会**回滚 Silver：好行本身是正确的，坏行已经被隔离。下一次正常运行时，会基于 Silver 发布 Gold。
# MAGIC
# MAGIC 下面有两次运行：上面那次（20 行里 4 行坏 = 20%），以及一次更干净的运行，20 行里正好 1 行坏 = 5%，刚好卡在阈值上。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Silver の後、`gate.run` は**今回の実行分だけ**のメトリクスを読み、テーブルごとに「不正行数 ÷ 全行数」を計算し、5% を超えると `DataQualityGateError` を送出します。ジョブタスクは失敗し、Gold とマートは公開されません。Silver はロールバック**しません**。正しい行はそれ自体で正しく、不正な行は既に隔離されているからです。次の正常な実行で、Silver から Gold が公開されます。
# MAGIC
# MAGIC 以下に2回の実行を示します。上の実行（20行中4行が不正 = 20%）と、20行中ちょうど1行が不正 = 5% で閾値ちょうどの、よりきれいな実行です。

# COMMAND ----------

cfg_clean = cfg_for("run-06-07")
clean_batch = items_batch(dt.date(2018, 6, 7), ["negative_price"])
dq.record(spark, cfg_clean, dq.evaluate(clean_batch, rules), rules, "order_items", contract_cols)

for run_cfg in [cfg_ok, cfg_clean]:
    try:
        gate.run(spark, run_cfg)
        print(f"{run_cfg.run_id}: gate passed -> gold is published")
    except gate.DataQualityGateError as e:
        print(f"{run_cfg.run_id}: GATE FAILED -> {e}")

show(spark.table(f"{SCHEMA}.dq_gate_log").select("run_id", "table_name", "bad_rows", "total_rows", "bad_ratio", "threshold", "passed"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Make a row 3 days late instead of 5 (`timedelta(days=3)`). Is it quarantined? Look at `too_late` in `quality.py`: the tolerance is `> 3` days.
# MAGIC 2. Set the backfill date of `cfg_for` to 2018-06-06 and rerun section 1. The 5-day-late row passes. Why should the backfill batch be exempt from lateness?
# MAGIC 3. Change `dq_max_bad_ratio` in `Config` to 0.25 and rerun section 3. The 20% run now passes. Who should own this number in a real company, and how would you choose it?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 把迟到 5 天改成迟到 3 天（`timedelta(days=3)`）。这行会被隔离吗？看一下 `quality.py` 里的 `too_late`：容忍期是 `> 3` 天。
# MAGIC 2. 把 `cfg_for` 的回填日期改成 2018-06-06，重跑第 1 节。迟到 5 天的行通过了。为什么回填批次应该不受迟到规则约束？
# MAGIC 3. 把 `Config` 里的 `dq_max_bad_ratio` 改成 0.25，重跑第 3 节。20% 的那次运行现在通过了。在真实的公司里，这个数应该由谁来定，又该怎么定？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. 5日遅れの代わりに3日遅れにしてください（`timedelta(days=3)`）。隔離されるでしょうか。`quality.py` の `too_late` を見てください。許容は `> 3` 日です。
# MAGIC 2. `cfg_for` のバックフィル日を 2018-06-06 にして1節を再実行してください。5日遅れの行が合格します。バックフィルバッチを遅延ルールの対象外にすべき理由は何でしょうか。
# MAGIC 3. `Config` の `dq_max_bad_ratio` を 0.25 にして3節を再実行してください。20% の実行が合格するようになります。実際の会社では、この数値は誰が決めるべきで、どう選ぶべきでしょうか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Every rule is named, and each bad row goes to a quarantine table with all the rules it broke, the raw line and the source file, so nothing is dropped silently. Every batch writes per-rule counts to a metrics table that the dashboard reads. Before gold, a gate checks this run's bad-row ratio: above 5% the task fails and gold is not published. It doesn't roll back silver, because the good rows are correct and the bad ones are isolated. The 5% is a configurable default; in a regulated setting it would come from a validated acceptable range for that data."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "每条规则都有名字，每个坏行都会带着它违反的所有规则、原始行和来源文件进入隔离表，所以不会有数据被悄悄丢掉。每个批次都把各规则的命中数写进指标表，Dashboard 读取的就是这张表。在 Gold 之前，闸门检查本次运行的坏行比例：超过 5% 任务就失败，Gold 不发布。它不回滚 Silver，因为好行是正确的，坏行已经被隔离。5% 是一个可配置的默认值；在受监管的行业里，它应该来自这类数据经过验证的可接受范围。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「すべてのルールには名前があり、不正な行は違反したすべてのルール、元の行、ソースファイルとともに隔離テーブルに入るため、黙って捨てられるデータはありません。各バッチはルールごとの件数をメトリクステーブルに書き込み、ダッシュボードはそれを読んでいます。Gold の手前で、ゲートが今回の実行の不正率を確認し、5% を超えるとタスクを失敗させて Gold を公開しません。Silver はロールバックしません。正しい行は正しく、不正な行は隔離済みだからです。5% は設定可能なデフォルト値で、規制のある業界では、そのデータについて検証された許容範囲から決めることになります。」
