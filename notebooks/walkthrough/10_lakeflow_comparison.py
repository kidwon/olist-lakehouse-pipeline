# Databricks notebook source
# MAGIC %md
# MAGIC # 10 The same spec in Lakeflow: reconciliation / 用 Lakeflow 实现同一规格：核对 / 同じ仕様を Lakeflow で：突き合わせ
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC `lakeflow/olist_sdp.sql` and `lakeflow/sellers_scd2.py` rebuild four mechanisms declaratively: ingestion, forward-only CDC (`AUTO CDC ... SEQUENCE BY`), seller SCD2 from snapshots (`AUTO CDC FROM SNAPSHOT`) and order-item quality with expectations plus a quarantine table. They read the same landing files and write to the schema `olist_sdp`. This notebook reconciles them with the imperative tables. Run the pipeline first: `databricks bundle run olist_sdp`. Design decision and conclusions: ADR-0012.
# MAGIC
# MAGIC Declarative pipelines run only on Databricks, so locally this notebook just says so. That is itself one of the comparison's findings.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC `lakeflow/olist_sdp.sql` 和 `lakeflow/sellers_scd2.py` 用声明式重写了四个机制：读取文件、只前进的 CDC（`AUTO CDC ... SEQUENCE BY`）、由快照生成卖家 SCD2（`AUTO CDC FROM SNAPSHOT`），以及用质量规则加隔离表检查订单明细。它们读取同一批投递文件，写到 `olist_sdp` 库。这个 notebook 把它们和命令式版本的表做核对。请先运行管道：`databricks bundle run olist_sdp`。设计决策和结论见 ADR-0012。
# MAGIC
# MAGIC 声明式管道只能在 Databricks 上运行，所以在本地这个 notebook 只会说明这一点。这本身也是对比的结论之一。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC `lakeflow/olist_sdp.sql` と `lakeflow/sellers_scd2.py` は、4つの仕組みを宣言型で作り直しています。取り込み、前進のみの CDC（`AUTO CDC ... SEQUENCE BY`）、スナップショットからのセラー SCD2（`AUTO CDC FROM SNAPSHOT`）、そしてエクスペクテーションと隔離テーブルによる注文明細の品質チェックです。同じランディングファイルを読み、スキーマ `olist_sdp` に書き込みます。このノートブックでは、それを命令型のテーブルと突き合わせます。先にパイプラインを実行してください：`databricks bundle run olist_sdp`。設計判断と結論は ADR-0012 を参照してください。
# MAGIC
# MAGIC 宣言型のパイプラインは Databricks 上でしか動かないため、ローカルではその旨を表示するだけです。これ自体も比較の結論の1つです。

# COMMAND ----------

from walkthrough_setup import ON_DATABRICKS, setup

spark, _ = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))
IMP, SDP = "workspace", "workspace.olist_sdp"

if not ON_DATABRICKS:
    print("Lakeflow pipelines run only on Databricks: open this notebook in the workspace to reconcile.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Same answers / 结果一致的部分 / 一致する部分
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Orders after CDC, order items after de-duplication, quarantined rows per rule and the gold fact are identical. `except all` returns the rows found on one side only; 0 means identical.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 经过 CDC 的订单、去重后的订单明细、按规则的隔离行数和 Gold 事实表完全一致。`except all` 返回只在一边出现的行，0 表示完全相同。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC CDC 適用後の注文、重複排除後の注文明細、ルール別の隔離件数、Gold のファクトは完全に一致します。`except all` は片方にしかない行を返すので、0 なら一致です。

# COMMAND ----------

if ON_DATABRICKS:
    checks = {
        "orders: rows only in one version": f"""SELECT count(*) FROM ((SELECT order_id, order_status, change_seq FROM {IMP}.olist_silver.orders)
                                              EXCEPT ALL (SELECT order_id, order_status, change_seq FROM {SDP}.silver_orders))""",
        "order items: keys only in one version": f"""SELECT count(*) FROM ((SELECT order_id, order_item_id FROM {IMP}.olist_silver.order_items)
                                                   EXCEPT ALL (SELECT order_id, order_item_id FROM {SDP}.silver_order_items))""",
        "gold fact: rows only in one version": f"""SELECT count(*) FROM ((SELECT order_id, order_item_id, order_status, price, customer_unique_id FROM {IMP}.olist_gold.fact_order_item)
                                                 EXCEPT ALL (SELECT order_id, order_item_id, order_status, price, customer_unique_id FROM {SDP}.fact_order_item))""",
    }
    for name, sql in checks.items():
        print(f"{name:<40} {spark.sql(sql).first()[0]}")
    show(spark.sql(f"""
        SELECT rule, imperative, lakeflow FROM
          (SELECT r AS rule, count(*) AS imperative FROM {IMP}.olist_ops.quarantine LATERAL VIEW explode(failed_rules) t AS r
           WHERE table_name = 'order_items' GROUP BY r)
        FULL JOIN
          (SELECT r AS rule, count(*) AS lakeflow FROM {SDP}.quarantine_order_items LATERAL VIEW explode(failed_rules) t AS r GROUP BY r)
        USING (rule) ORDER BY rule"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Metrics for free: the event log / 免费的指标：事件日志 / 無料のメトリクス：イベントログ
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Every expectation's passed and failed rows are recorded by the pipeline itself. The imperative version writes the same numbers to `ops.dq_metrics` with its own code.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 每条质量规则通过和失败的行数，都由管道自己记录下来。命令式版本要靠自己写代码，才能把同样的数字写进 `ops.dq_metrics`。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 各エクスペクテーションの合格・不合格の行数は、パイプライン自身が記録します。命令型では、同じ数字を自前のコードで `ops.dq_metrics` に書き込んでいます。

# COMMAND ----------

if ON_DATABRICKS:
    show(spark.sql(f"""
        SELECT e.name AS expectation, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
        FROM event_log(TABLE({SDP}.silver_order_items_valid)) l
        LATERAL VIEW explode(from_json(l.details:flow_progress.data_quality.expectations,
          'array<struct<name string, dataset string, passed_records bigint, failed_records bigint>>')) t AS e
        WHERE l.event_type = 'flow_progress' AND l.details:flow_progress.data_quality IS NOT NULL
        GROUP BY e.name ORDER BY e.name"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Where they differ / 不一致的地方 / 一致しない部分
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The spec starts a seller's first version at 1900-01-01 so that every order finds one. Lakeflow starts it at the first snapshot (version `20180531`), so orders placed before the backfill date find no seller version at all. Expressing that business rule declaratively would need an extra layer.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 规格规定卖家的第一个版本从 1900-01-01 开始，好让每个订单都能找到版本。Lakeflow 从第一份快照（版本 `20180531`）开始，所以回填日期之前的订单完全找不到卖家版本。要在声明式里表达这条业务规则，就得另外加一层。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 仕様では、すべての注文がバージョンを見つけられるよう、セラーの最初のバージョンを 1900-01-01 から有効にしています。Lakeflow では最初のスナップショット（バージョン `20180531`）から始まるため、バックフィル日より前の注文はセラーのバージョンをまったく見つけられません。この業務ルールを宣言型で表すには、もう一層追加する必要があります。

# COMMAND ----------

if ON_DATABRICKS:
    show(spark.sql(f"""
        SELECT 'imperative' AS version, count(*) AS fact_rows, count_if(seller_sk IS NULL) AS without_seller_version FROM {IMP}.olist_gold.fact_order_item
        UNION ALL
        SELECT 'lakeflow', count(*), count_if(seller_version IS NULL) FROM {SDP}.fact_order_item"""))
    show(spark.sql(f"""
        SELECT 'imperative' AS version, count(*) AS versions, count_if(NOT is_current) AS closed, cast(min(valid_from) AS string) AS first_start FROM {IMP}.olist_silver.seller_history
        UNION ALL
        SELECT 'lakeflow', count(*), count_if(__END_AT IS NOT NULL), cast(min(__START_AT) AS string) FROM {SDP}.silver_seller_history"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Read `lakeflow/olist_sdp.sql` next to `silver.py`. Which lines replaced `merge_cdc_forward_only`?
# MAGIC 2. In the event log, find the update that ran the gate (`dq_gate`). What would happen to the whole update if one batch were above 5%?
# MAGIC 3. Why could Lakeflow ignore the re-delivered 05-31 seller snapshot, while the imperative SCD2 needed a fix (ADR-0012)?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 把 `lakeflow/olist_sdp.sql` 和 `silver.py` 放在一起看。哪几行代替了 `merge_cdc_forward_only`？
# MAGIC 2. 在事件日志里找到运行闸门（`dq_gate`）的那次更新。如果有一个批次超过 5%，整次更新会怎样？
# MAGIC 3. 为什么 Lakeflow 能忽略重新投递的 05-31 卖家快照，而命令式的 SCD2 需要修复（ADR-0012）？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. `lakeflow/olist_sdp.sql` を `silver.py` と並べて読んでください。どの行が `merge_cdc_forward_only` の代わりになっていますか。
# MAGIC 2. イベントログで、ゲート（`dq_gate`）を実行した更新を見つけてください。あるバッチが 5% を超えていたら、更新全体はどうなるでしょうか。
# MAGIC 3. 再配信された 05-31 のセラーのスナップショットを、Lakeflow は無視できたのに、命令型の SCD2 は修正が必要だったのはなぜでしょうか（ADR-0012）。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "I rebuilt the core of the pipeline in Lakeflow Declarative Pipelines from the same landing files and reconciled the two: orders, items, quarantined rows per rule and the gold fact match exactly, with about a third of the code. Reconciling also found a real bug in my hand-written SCD2: a re-delivered old snapshot rolled sellers back, which Lakeflow's snapshot versioning prevented, so I fixed and tested it. But Lakeflow couldn't express two business rules without extra layers: the 1900 start of the first seller version, which left 97% of fact rows without a seller, and a gate that blocks only gold. And it can't be tested locally. So I'd default to Lakeflow for simple pipelines, and keep code I can test when the rules are specific."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "我用 Lakeflow 声明式管道，基于同一批投递文件重写了管道的核心部分，并和原版做了核对：订单、明细、按规则的隔离行数和 Gold 事实表都完全一致，代码只有大约三分之一。核对还发现了我手写的 SCD2 里的一个真实 bug：重新投递的旧快照会让卖家信息倒退，而 Lakeflow 的快照版本管理防住了这一点，于是我修复了它并补上了测试。但 Lakeflow 不额外加层就表达不了两条业务规则：卖家第一个版本从 1900 年开始（导致 97% 的事实行没有卖家），以及只阻止 Gold 的闸门。它也无法在本地测试。所以简单的管道我会默认用 Lakeflow，业务规则比较特殊时，就保留可以测试的代码。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「パイプラインの中核部分を、同じランディングファイルから Lakeflow Declarative Pipelines で作り直し、両者を突き合わせました。注文、明細、ルール別の隔離件数、Gold のファクトは完全に一致し、コード量は約3分の1でした。突き合わせによって、手書きの SCD2 の実際のバグも見つかりました。再配信された古いスナップショットでセラーの情報が巻き戻るというもので、Lakeflow はスナップショットのバージョン管理でこれを防いでいたため、修正してテストを追加しました。一方で Lakeflow では、層を追加しないと2つの業務ルールを表現できませんでした。セラーの最初のバージョンを 1900 年から有効にするルール（これがないとファクト行の97%にセラーがありません）と、Gold だけを止めるゲートです。また、ローカルでテストすることもできません。そのため、単純なパイプラインなら Lakeflow を標準にし、業務ルールが特殊な場合はテストできるコードを残します。」
