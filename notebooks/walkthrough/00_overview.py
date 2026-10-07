# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Overview: how the pipeline fits together / 总览：管道是怎样组成的 / 全体像：パイプラインの構成
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC This series walks through the production code in `src/olist_pipeline/` one layer at a time. Every notebook imports the **same functions the daily job runs** and calls them on a few hand-written rows, so you can change an input, rerun a cell, and see exactly what the pipeline does with it. Nothing here is a copy of the logic.
# MAGIC
# MAGIC The Olist dataset (about 100k orders) is static, so `replay.py` turns it into daily file drops and injects the problems real feeds have. The daily job then runs seven tasks: `replay_batch → bronze → silver → dim_seller_scd2 → dq_gate → gold → mart`.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 这一系列 notebook 按层讲解 `src/olist_pipeline/` 里的生产代码。每个 notebook 导入的都是**每日作业实际运行的同一批函数**，在几行手写的数据上调用它们。你可以改输入、重跑单元格，直接看到管道怎么处理这些数据。这里没有任何"复制一份逻辑来演示"的代码。
# MAGIC
# MAGIC Olist 数据集（约 10 万订单）是静态的，所以 `replay.py` 把它变成每天投递的文件，并注入真实数据源常见的问题。每日作业依次运行 7 个任务：`replay_batch → bronze → silver → dim_seller_scd2 → dq_gate → gold → mart`。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC このシリーズでは、`src/olist_pipeline/` の本番コードをレイヤーごとに解説します。各ノートブックは**日次ジョブが実行するものと同じ関数**をインポートし、手書きの数行のデータに対して呼び出します。入力を変えてセルを再実行すれば、パイプラインがそのデータをどう扱うかをそのまま確認できます。ロジックのコピーは一切ありません。
# MAGIC
# MAGIC Olist データセット（約10万注文）は静的なデータなので、`replay.py` が日次のファイル配信に変換し、実際のフィードでよく起きる問題を注入します。日次ジョブは7つのタスク `replay_batch → bronze → silver → dim_seller_scd2 → dq_gate → gold → mart` を順に実行します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture / 架构 / アーキテクチャ
# MAGIC
# MAGIC ```
# MAGIC Kaggle CSV ──prepare──▶ staging ──replay_batch──▶ landing/<feed>/batch_date=YYYY-MM-DD/
# MAGIC                                                        │  Auto Loader, availableNow
# MAGIC                                                        ▼
# MAGIC                         Bronze  (parsed against the contract, raw line kept, _rescued_data)
# MAGIC                                                        │  foreachBatch: rules → quarantine → dedup → MERGE
# MAGIC                                                        ▼
# MAGIC   ops.quarantine ◀──── Silver  (orders, order_items, customers, reviews, products, seller_history SCD2)
# MAGIC   ops.dq_metrics                                       │
# MAGIC                                                        ▼
# MAGIC                         dq_gate  (bad rows ≤ 5% of this run?)  ── no ──▶ stop, gold not published
# MAGIC                                                        │ yes
# MAGIC                                                        ▼
# MAGIC                         Gold    (fact_order_item, dim_*, mart_seller_delivery_performance)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code map / 代码地图 / コードマップ
# MAGIC
# MAGIC | File | Role | Walkthrough |
# MAGIC |---|---|---|
# MAGIC | `contracts.py` | Schema of the six feeds / 6 个数据源的 schema / 6つのフィードのスキーマ | 02 |
# MAGIC | `replay.py` | Static CSV → daily drops + injected anomalies / 静态 CSV → 每日投递 + 注入异常 / 静的 CSV → 日次配信 + 異常の注入 | 01 |
# MAGIC | `bronze.py` | Ingest each file once, parse, rescue unknown fields / 每个文件只摄取一次、解析、转存未知字段 / 各ファイルを一度だけ取り込み、パースし、未知フィールドを退避 | 02 |
# MAGIC | `silver.py` | Validate → dedup → MERGE, forward-only CDC / 校验 → 去重 → MERGE，只前进的 CDC / 検証 → 重複排除 → MERGE、前進のみの CDC | 03 |
# MAGIC | `scd2.py` | Seller history from daily snapshots / 由每日快照生成卖家历史 / 日次スナップショットからセラー履歴 | 04 |
# MAGIC | `quality.py`, `gate.py` | Rules, quarantine, metrics, gate / 规则、隔离、指标、闸门 / ルール、隔離、メトリクス、ゲート | 05 |
# MAGIC | `gold.py` | Star schema and mart / 星型模型与 mart / スタースキーマとマート | 06 |
# MAGIC | `gold.py` (`build_fact_order_fulfillment`) | Accumulating snapshot / 累积快照 / 累積スナップショット | 07 |
# MAGIC | `gold.py` (`build_fact_daily_order_backlog`) | Periodic snapshot / 周期快照 / 定期スナップショット | 08 |
# MAGIC | `cli.py` | One entry point per job task / 每个作业任务一个入口 / ジョブタスクごとのエントリポイント | — |

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to use these notebooks / 使用方法 / 使い方
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC - **On Databricks:** after `databricks bundle deploy` the notebooks are under `.bundle/olist_lakehouse_pipeline/dev/files/notebooks/walkthrough/`. Attach serverless compute and run top to bottom. Demo tables go to the scratch schema `workspace.olist_walkthrough`; the real pipeline tables are never touched.
# MAGIC - **Locally:** each notebook is also a plain Python script: `uv run python notebooks/walkthrough/03_silver_dedup_cdc.py` (needs Java 17 or 21).
# MAGIC - Every notebook ends with **"Try it yourself"** (changes worth making) and **"In the interview"** (how to explain the topic in 30 seconds).
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC - **在 Databricks 上：** 执行 `databricks bundle deploy` 后，notebook 位于 `.bundle/olist_lakehouse_pipeline/dev/files/notebooks/walkthrough/`。连接 serverless 计算资源，从上到下运行即可。演示用的表写入临时 schema `workspace.olist_walkthrough`，不会碰正式管道的表。
# MAGIC - **在本地：** 每个 notebook 同时也是普通 Python 脚本：`uv run python notebooks/walkthrough/03_silver_dedup_cdc.py`（需要 Java 17 或 21）。
# MAGIC - 每个 notebook 的最后都有 **"自己试试"**（值得动手改的地方）和 **"面试时怎么说"**（30 秒讲清这个主题）。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC - **Databricks 上：** `databricks bundle deploy` の後、ノートブックは `.bundle/olist_lakehouse_pipeline/dev/files/notebooks/walkthrough/` にあります。サーバーレスのコンピュートをアタッチし、上から順に実行してください。デモ用のテーブルは一時スキーマ `workspace.olist_walkthrough` に作成され、本番パイプラインのテーブルには一切触れません。
# MAGIC - **ローカル：** 各ノートブックは通常の Python スクリプトとしても実行できます：`uv run python notebooks/walkthrough/03_silver_dedup_cdc.py`（Java 17 または 21 が必要）。
# MAGIC - 各ノートブックの最後に **「やってみよう」**（試す価値のある変更）と **「面接では」**（30秒でテーマを説明する方法）があります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading order / 阅读顺序 / 読む順番
# MAGIC
# MAGIC 1. `01_replay`: where the dirty data comes from / 脏数据从哪来 / 汚れたデータはどこから来るか
# MAGIC 2. `02_bronze_contracts`: contracts and `_rescued_data` / 数据契约与 `_rescued_data` / データ契約と `_rescued_data`
# MAGIC 3. `03_silver_dedup_cdc`: dedup and forward-only CDC (**the most asked-about topic** / **面试最常问** / **面接で最もよく聞かれる**)
# MAGIC 4. `04_scd2`: seller history and point-in-time joins / 卖家历史与时点关联 / セラー履歴とポイントインタイム結合
# MAGIC 5. `05_quality_gate`: quarantine, metrics and the gate / 隔离、指标与闸门 / 隔離、メトリクス、ゲート
# MAGIC 6. `06_gold`: star schema, customer identity, GMV / 星型模型、客户身份、GMV / スタースキーマ、顧客の名寄せ、GMV
# MAGIC 7. `07_fulfillment`: accumulating snapshot of order fulfillment / 订单履约的累积快照 / 注文フルフィルメントの累積スナップショット
# MAGIC 8. `08_backlog`: periodic snapshot of the daily order backlog / 每日订单积压的周期快照 / 日次注文残の定期スナップショット
# MAGIC
# MAGIC The design decisions behind each step are in `docs/adr/` (ja / en / zh).
