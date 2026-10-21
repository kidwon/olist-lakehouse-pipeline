# Databricks notebook source
# MAGIC %md
# MAGIC # 10 Two implementations, reconciled every day / 两套实现，每天对账 / 2つの実装を毎日突き合わせる
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The whole spec exists twice: hand-written Structured Streaming + MERGE (`src/olist_pipeline/`) and Lakeflow Spark Declarative Pipelines (`lakeflow/olist_sdp.sql`, `lakeflow/sellers_scd2.py`). Both read the same landing files; the declarative one writes to the schema `olist_sdp`. The daily job runs them side by side and finishes with `olist reconcile`, which compares 16 tables row by row and fails the job on any difference. The declarative pipeline cannot run on a laptop, so this check is its test; the imperative version, tested locally, is the reference. Decision and findings: ADR-0013.
# MAGIC
# MAGIC Locally, this notebook runs the comparison logic on a few hand-written rows; on Databricks it also reconciles the real tables.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 整套规格实现了两遍：手写的 Structured Streaming + MERGE（`src/olist_pipeline/`），以及 Lakeflow 声明式管道（`lakeflow/olist_sdp.sql`、`lakeflow/sellers_scd2.py`）。两者读取同一批投递文件，声明式版本写到 `olist_sdp` 库。每日作业让两者并行运行，最后执行 `olist reconcile`：逐行比较 16 张表，只要有差异就让作业失败。声明式管道不能在本地运行，所以这个对账就是它的测试；在本地测试过的命令式版本是"标准答案"。决策和发现见 ADR-0013。
# MAGIC
# MAGIC 在本地，这个 notebook 用几行手写数据演示比较逻辑；在 Databricks 上还会对真实的表做对账。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 仕様全体を2通りで実装しています。手書きの Structured Streaming + MERGE（`src/olist_pipeline/`）と、Lakeflow Spark Declarative Pipelines（`lakeflow/olist_sdp.sql`、`lakeflow/sellers_scd2.py`）です。どちらも同じランディングファイルを読み、宣言型はスキーマ `olist_sdp` に書き込みます。日次ジョブは両者を並行して実行し、最後に `olist reconcile` で 16 テーブルを行単位で比較して、差分が1行でもあればジョブを失敗させます。宣言型パイプラインはローカルで動かせないため、この突き合わせがそのテストです。ローカルでテスト済みの命令型が「正解」の役割を担います。判断と発見は ADR-0013 を参照してください。
# MAGIC
# MAGIC ローカルでは手書きの数行で比較ロジックを動かし、Databricks では実際のテーブルも突き合わせます。

# COMMAND ----------

from walkthrough_setup import ON_DATABRICKS, setup

from olist_pipeline import reconcile
from olist_pipeline.config import Config

spark, _ = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))
SDP = "workspace.olist_sdp"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. How a difference is found / 怎么发现差异 / 差分の見つけ方
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `reconcile.difference` returns the rows found only on each side (`EXCEPT ALL`). A changed value shows up on both sides; a duplicated row counts too, because double counting is exactly what a business table must never do. Ops tables (quarantine, metrics) are compared as sets: the imperative version appends them again when it reprocesses a batch.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `reconcile.difference` 返回只在某一边出现的行（`EXCEPT ALL`）。一个值变了，两边都会各多出一行；重复的行也算差异，因为重复计算正是业务表绝不能出现的问题。运维表（隔离表、指标表）按集合比较：命令式版本重新处理某个批次时会再追加一遍。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `reconcile.difference` は、片方にしかない行を返します（`EXCEPT ALL`）。値が変わると両側に1行ずつ現れ、重複行も差分として数えます。二重計上こそ業務テーブルで起きてはならないことだからです。運用テーブル（隔離・メトリクス）は集合として比較します。命令型はバッチを再処理すると、もう一度追記するためです。

# COMMAND ----------

S = "order_id string, price double"
reference = spark.createDataFrame([("o1", 10.0), ("o2", 5.0)], S)
changed = spark.createDataFrame([("o1", 10.0), ("o2", 6.0)], S)
doubled = spark.createDataFrame([("o1", 10.0), ("o2", 5.0), ("o2", 5.0)], S)
print("identical      ", reconcile.difference(reference, reference))
print("changed value  ", reconcile.difference(reference, changed))
print("duplicated row ", reconcile.difference(reference, doubled))
print("same, as a set ", reconcile.difference(reference, doubled, as_set=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The real tables / 真实的表 / 実際のテーブル
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Silver, every dimension, the three fact types, the mart, quarantine and the rule metrics: 0 rows on either side means identical. This is what the job's `reconcile` task runs.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC Silver、所有维度表、三种事实表、汇总表、隔离表和规则指标：两边都是 0 行就表示完全一致。作业里的 `reconcile` 任务跑的就是这个。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC Silver、すべてのディメンション、3種類のファクト、マート、隔離、ルールのメトリクスです。両側とも 0 行なら一致です。ジョブの `reconcile` タスクが実行しているのはこれです。

# COMMAND ----------

if ON_DATABRICKS:
    show(reconcile.evaluate(spark, Config(base_path="", catalog="workspace")).drop("run_id", "checked_at"))
else:
    print("The declarative pipeline runs only on Databricks: open this notebook in the workspace to reconcile.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Rules written once, counted by the platform / 规则只写一次，由平台计数 / ルールは一度だけ書き、数えるのはプラットフォーム
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Each `checked_*` table lists the rules a row breaks in `_failed_rules`; the expectations only name them, so the event log counts every rule without the rules being written twice. Quarantine and silver then split the rows. The counts below come from the event log of the update that read every file; they match `dq_metrics`.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 每张 `checked_*` 表都把一行违反的规则列在 `_failed_rules` 里；expectation 只是按名字引用这些规则，所以事件日志能按规则计数，而规则本身不用写两遍。之后隔离表和 silver 再把行分开。下面的数字来自读取了全部文件的那次更新的事件日志，和 `dq_metrics` 一致。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 各 `checked_*` テーブルは、行が違反したルールを `_failed_rules` に列挙します。エクスペクテーションはその名前を参照するだけなので、ルールを二重に書かずに、イベントログがルールごとに件数を記録します。その後、隔離と Silver で行を振り分けます。下の件数は、すべてのファイルを読んだ更新のイベントログから取ったもので、`dq_metrics` と一致します。

# COMMAND ----------

if ON_DATABRICKS:
    show(
        spark.sql(f"""
        WITH dq AS (
          SELECT origin.update_id AS update_id, e.name AS rule, e.failed_records AS failed, e.passed_records AS passed
          FROM {SDP}.event_log
          LATERAL VIEW explode(from_json(details:flow_progress.data_quality.expectations,
            'ARRAY<STRUCT<name STRING, dataset STRING, passed_records BIGINT, failed_records BIGINT>>')) t AS e
          WHERE event_type = 'flow_progress' AND origin.flow_name = '{SDP}.checked_order_items'
        ),
        biggest AS (SELECT update_id FROM dq GROUP BY update_id ORDER BY sum(failed + passed) DESC LIMIT 1),
        event_log AS (SELECT rule, sum(failed) AS event_log_failed FROM dq JOIN biggest USING (update_id) GROUP BY rule),
        metrics AS (
          SELECT rule, sum(failed_rows) AS dq_metrics_failed FROM {SDP}.dq_metrics
          WHERE table_name = 'order_items' GROUP BY rule
        )
        SELECT rule, event_log_failed, coalesce(dq_metrics_failed, 0) AS dq_metrics_failed
        FROM event_log LEFT JOIN metrics USING (rule) ORDER BY rule""")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The gaps ADR-0012 found, closed / ADR-0012 发现的缺口已补上 / ADR-0012 の課題の解消
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC * **First seller version from 1900:** `dim_seller` reshapes the `AUTO CDC FROM SNAPSHOT` history; its keys are the imperative `seller_sk`, so no order line is left without a seller.
# MAGIC * **A gate that blocks only gold:** the job refreshes the pipeline twice. `dq_gate` (latest batch only) is in the first refresh and fails it above 5%; silver is kept, and the gold refresh never starts.
# MAGIC * **No local tests:** the daily reconciliation above.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC * **卖家第一个版本从 1900 年开始：** `dim_seller` 把 `AUTO CDC FROM SNAPSHOT` 的历史整理成规格要求的形状，键就是命令式的 `seller_sk`，所以没有订单行找不到卖家。
# MAGIC * **只挡 Gold 的闸门：** 作业把管道分两次刷新。`dq_gate`（只看最新批次）在第一次刷新里，超过 5% 就让这次刷新失败；silver 照常保留，Gold 的刷新不会开始。
# MAGIC * **不能在本地测试：** 就是上面的每日对账。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC * **セラーの最初のバージョンを 1900 年から：** `dim_seller` が `AUTO CDC FROM SNAPSHOT` の履歴を仕様の形に整えます。キーは命令型の `seller_sk` と同じなので、セラーのない注文明細はありません。
# MAGIC * **Gold だけを止めるゲート：** ジョブはパイプラインを2回に分けて更新します。`dq_gate`（最新バッチのみ）は1回目の更新に含まれ、5% を超えるとその更新を失敗させます。Silver は残り、Gold の更新は始まりません。
# MAGIC * **ローカルでテストできない：** 上の毎日の突き合わせで補います。

# COMMAND ----------

if ON_DATABRICKS:
    show(
        spark.sql(f"""
        SELECT count(*) AS fact_rows, count_if(seller_sk IS NULL) AS without_seller_version FROM {SDP}.fact_order_item""")
    )
    show(spark.table(f"{SDP}.dq_gate"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Incremental, decided by the platform / 增量由平台决定 / 増分はプラットフォームが判断する
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The imperative gold reads Change Data Feed and keeps its own watermarks (ADR-0011). Here every gold table is a materialized view, and the event log records how the platform refreshed each one: a full refresh recomputes everything, and an update with nothing new upstream is a no-op.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 命令式的 Gold 读取 Change Data Feed，自己保存水位（ADR-0011）。这里每张 Gold 表都是物化视图，事件日志记录了平台是怎么刷新每一张的：全量刷新就整张重算，上游没有新数据的更新就直接跳过（NO_OP）。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 命令型の Gold は Change Data Feed を読み、ウォーターマークを自前で管理しています（ADR-0011）。ここでは Gold のテーブルはすべてマテリアライズドビューで、プラットフォームがそれぞれをどう更新したかをイベントログが記録します。フルリフレッシュでは全体を再計算し、上流に新しいデータがない更新では何もしません（NO_OP）。

# COMMAND ----------

if ON_DATABRICKS:
    show(
        spark.sql(f"""
        SELECT date_trunc('MINUTE', timestamp) AS refreshed_at,
               replace(origin.flow_name, '{SDP}.', '') AS view,
               replace(get_json_object(t, '$.maintenance_type'), 'MAINTENANCE_TYPE_', '') AS technique
        FROM {SDP}.event_log
        LATERAL VIEW explode(from_json(details:planning_information:technique_information, 'ARRAY<STRING>')) AS t
        WHERE event_type = 'planning_information' AND get_json_object(t, '$.is_chosen') = 'true'
        ORDER BY refreshed_at DESC, view
        LIMIT 30""")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Read `lakeflow/olist_sdp.sql` next to `silver.py`. Which lines replaced `merge_cdc_forward_only`, and which replaced `first_delivery`?
# MAGIC 2. Why does the declarative version sequence re-delivered order items by `-unix_date(_batch_date)` rather than by ingestion time? (Hint: what does a full refresh ingest at once? ADR-0013.)
# MAGIC 3. Change one rule in `checked_order_items` only, deploy, and run the job. Which reconciliation checks fail?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 把 `lakeflow/olist_sdp.sql` 和 `silver.py` 放在一起看。哪几行代替了 `merge_cdc_forward_only`？哪几行代替了 `first_delivery`？
# MAGIC 2. 为什么声明式版本对重新投递的订单明细按 `-unix_date(_batch_date)` 排序，而不是按读取时间？（提示：全量刷新时会一次读取哪些文件？见 ADR-0013。）
# MAGIC 3. 只在 `checked_order_items` 里改一条规则，部署后运行作业。哪些对账检查会失败？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. `lakeflow/olist_sdp.sql` を `silver.py` と並べて読んでください。`merge_cdc_forward_only` と `first_delivery` は、それぞれどの行に置き換わっていますか。
# MAGIC 2. 宣言型では、再配信された注文明細を取り込み時刻ではなく `-unix_date(_batch_date)` で順序付けているのはなぜでしょうか。（ヒント：フルリフレッシュでは何を一度に取り込むでしょうか。ADR-0013。）
# MAGIC 3. `checked_order_items` のルールを1つだけ変えてデプロイし、ジョブを実行してください。どの突き合わせが失敗しますか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "The pipeline exists twice: hand-written Structured Streaming with MERGE, and Lakeflow Declarative Pipelines. Both run every day from the same files and a reconciliation task compares 16 tables row by row, so the declarative version, which can't run locally, is tested against the one that can. The declarative code is about half the size, quality metrics and incremental refresh come from the platform, and the business rules that first looked hard, a seller history starting in 1900 and a gate that blocks only gold, fit with a reshaping view and two selective refreshes. Reconciling found real issues twice: a re-delivered snapshot rolling sellers back, and re-deliveries ordered by ingestion time, which breaks when several days are ingested at once."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "这条管道我实现了两遍：手写的 Structured Streaming 加 MERGE，以及 Lakeflow 声明式管道。两者每天读取同一批文件运行，最后由对账任务逐行比较 16 张表，所以不能在本地运行的声明式版本，是拿可以本地测试的版本来检验的。声明式的代码量大约一半，质量指标和增量刷新由平台提供；一开始看起来很难的业务规则——卖家历史从 1900 年开始、只挡 Gold 的闸门——用一个整形视图和两次选择性刷新就解决了。对账两次发现了真实问题：重新投递的快照让卖家信息倒退，以及按读取时间排序重新投递的数据，在一次读取好几天的文件时会出错。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「パイプラインは2通りあります。手書きの Structured Streaming と MERGE、そして Lakeflow Declarative Pipelines です。どちらも毎日同じファイルから実行し、突き合わせタスクが 16 テーブルを行単位で比較します。ローカルで動かせない宣言型を、ローカルでテストできる実装で検証しているわけです。宣言型のコード量は約半分で、品質メトリクスと増分更新はプラットフォームが担います。最初は難しそうだった業務ルール、つまり 1900 年から始まるセラー履歴と Gold だけを止めるゲートも、整形用のビューと2回の選択的な更新で実現できました。突き合わせでは実際の問題が2回見つかりました。再配信されたスナップショットでセラー情報が巻き戻る問題と、再配信データを取り込み時刻で順序付けると、数日分を一度に取り込んだときに誤る問題です。」
