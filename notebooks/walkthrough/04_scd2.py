# Databricks notebook source
# MAGIC %md
# MAGIC # 04 SCD Type 2: seller history / 卖家历史 / セラーの履歴
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC The sellers feed is a **full snapshot every day**. Sellers occasionally move. Regional delivery performance must use where the seller was **when the order was placed**, so overwriting the address would rewrite history. `src/olist_pipeline/scd2.py` keeps every version with `valid_from` / `valid_to` / `is_current`, using one hand-written MERGE per snapshot. Design decision: ADR-0003.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 卖家数据源**每天都是一份全量快照**，卖家偶尔会迁址。按地区统计配送表现时，必须用卖家**下单当时**所在的地点；直接覆盖地址等于改写历史。`src/olist_pipeline/scd2.py` 用 `valid_from` / `valid_to` / `is_current` 保留每一个版本，每份快照只用一次手写的 MERGE。设计决策见 ADR-0003。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC セラーのフィードは**毎日の全件スナップショット**です。セラーはときどき移転します。地域別の配送パフォーマンスは、**注文時点**のセラーの所在地で集計しなければならず、住所を上書きすると履歴を書き換えることになります。`src/olist_pipeline/scd2.py` は `valid_from` / `valid_to` / `is_current` で全バージョンを保持し、スナップショットごとに手書きの MERGE を1回実行します。設計判断は ADR-0003 を参照してください。

# COMMAND ----------

import datetime as dt

from pyspark.sql import functions as F

from walkthrough_setup import fresh_table, setup
from olist_pipeline import gold, scd2

spark, SCHEMA = setup()
show = globals().get("display") or (lambda df: df.show(truncate=False))
HISTORY = fresh_table(spark, SCHEMA, "demo_seller_history")


def snapshot(*rows):
    return spark.createDataFrame(list(rows), "seller_id string, seller_zip_code_prefix string, seller_city string, seller_state string")


def history():
    return spark.table(HISTORY).select("seller_id", "seller_city", "seller_state", "valid_from", "valid_to", "is_current").orderBy("seller_id", "valid_from")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. First snapshot / 第一份快照 / 最初のスナップショット
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC Every seller seen for the first time gets one current version, valid from **1900-01-01**. We have no history before the first snapshot; assuming the first-known address always held is better than orders that match no seller version at all.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 第一次出现的卖家各得到一个当前版本，从 **1900-01-01** 开始生效。第一份快照之前没有任何历史；与其让订单关联不到任何卖家版本，不如假设第一次看到的地址一直有效。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 初めて現れたセラーには、**1900-01-01** から有効な現行バージョンが1つ作られます。最初のスナップショットより前の履歴はありません。どのセラーのバージョンにも結び付かない注文が出るよりは、最初に分かった住所がそれ以前も有効だったとみなすほうが妥当です。

# COMMAND ----------

scd2.apply_snapshot(
    spark, HISTORY,
    snapshot(("s1", "01001", "sao paulo", "SP"), ("s2", "20001", "rio de janeiro", "RJ"), ("s3", "30001", "belo horizonte", "MG")),
    dt.date(2018, 5, 31),
)
show(history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. A seller moves / 卖家迁址 / セラーの移転
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC On 2018-06-03, `s1` appears in Curitiba, and `s3` is missing from the snapshot. Before the MERGE, `stage_changes` builds its source. The trick: a changed seller appears **twice**. The row with `merge_key = s1` matches the current version and closes it; the row with `merge_key = NULL` can never match, so it is inserted as the new version. One MERGE, atomic, no separate UPDATE and INSERT.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 2018-06-03，`s1` 出现在库里蒂巴（Curitiba），`s3` 没有出现在快照里。MERGE 之前，`stage_changes` 先构造数据源。技巧在于：发生变化的卖家会出现**两次**。`merge_key = s1` 的那行匹配到当前版本并把它关闭；`merge_key = NULL` 的那行永远匹配不上，于是被插入为新版本。一次 MERGE、原子完成，不需要分开写 UPDATE 和 INSERT。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 2018-06-03、`s1` がクリチバ（Curitiba）に現れ、`s3` はスナップショットから消えています。MERGE の前に、`stage_changes` がソースを組み立てます。コツは、変更のあったセラーが**2回**現れることです。`merge_key = s1` の行は現行バージョンにマッチしてそれをクローズし、`merge_key = NULL` の行は決してマッチしないため、新しいバージョンとして挿入されます。UPDATE と INSERT を分けずに、1回の MERGE でアトミックに処理します。

# COMMAND ----------

moved = snapshot(("s1", "01001", "curitiba", "PR"), ("s2", "20001", "rio de janeiro", "RJ"))
show(
    scd2.stage_changes(spark.table(HISTORY), moved, dt.date(2018, 6, 3))
    .select("merge_key", "seller_id", "seller_city", "valid_from")
)

# COMMAND ----------

scd2.apply_snapshot(spark, HISTORY, moved, dt.date(2018, 6, 3))
show(history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The same snapshot again / 再来一遍同样的快照 / 同じスナップショットをもう一度
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC A retried job applies the same snapshot again. Change detection compares a SHA-256 hash of the tracked attributes, the hashes are equal, and the staged source is empty: nothing changes. Notice too that `s3`, absent from both snapshots, is still current. A snapshot is not a delete signal.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 作业重试时会再次应用同一份快照。变更检测比较的是被追踪属性的 SHA-256 哈希，哈希相同，构造出的数据源是空的，所以什么都不会变。还要注意，两份快照里都没有的 `s3` 依然是当前版本。从快照中消失不代表删除。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC ジョブがリトライされると、同じスナップショットがもう一度適用されます。変更検知は追跡対象属性の SHA-256 ハッシュを比較するので、ハッシュが同じならステージングされたソースは空になり、何も変わりません。また、2つのスナップショットの両方にない `s3` が現行のままであることにも注目してください。スナップショットからの欠落は削除を意味しません。

# COMMAND ----------

before = spark.table(HISTORY).count()
print(f"staged rows for the repeated snapshot: {scd2.stage_changes(spark.table(HISTORY), moved, dt.date(2018, 6, 3)).count()}")
scd2.apply_snapshot(spark, HISTORY, moved, dt.date(2018, 6, 3))
print(f"versions before: {before}, after: {spark.table(HISTORY).count()}")
show(history())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Point-in-time join / 时点关联 / ポイントインタイム結合
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC This is why the history exists. `gold.build_fact_order_item` joins each order to the seller version where `valid_from <= order_date < valid_to`. The order placed on 2018-06-02 stays in São Paulo (SP); the one on 2018-06-05 goes to Paraná (PR). Overwrite the address instead and June 2nd's revenue would silently move to PR.
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 这就是保留历史的原因。`gold.build_fact_order_item` 按 `valid_from <= 下单日期 < valid_to` 把每个订单关联到对应的卖家版本。2018-06-02 下的订单仍算在圣保罗（SP）；2018-06-05 的订单算在巴拉那（PR）。如果直接覆盖地址，6 月 2 日的销售额就会悄悄挪到 PR。
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC これが履歴を残す理由です。`gold.build_fact_order_item` は、各注文を `valid_from <= 注文日 < valid_to` を満たすセラーのバージョンに結合します。2018-06-02 の注文はサンパウロ（SP）のまま、2018-06-05 の注文はパラナ（PR）に計上されます。住所を上書きしていたら、6月2日の売上は気付かれないまま PR に移ってしまいます。

# COMMAND ----------

items = spark.createDataFrame(
    [("before_move", 1, "p1", "s1", dt.datetime(2018, 6, 2, 12), 100.0, 10.0, 0),
     ("after_move", 1, "p1", "s1", dt.datetime(2018, 6, 5, 12), 80.0, 10.0, 0)],
    "order_id string, order_item_id int, product_id string, seller_id string, order_purchase_ts timestamp, price double, freight_value double, arrival_lag_days int",
)
orders = spark.createDataFrame(
    [("before_move", "c1", "delivered", None, None), ("after_move", "c2", "shipped", None, None)],
    "order_id string, customer_id string, order_status string, order_delivered_customer_ts timestamp, order_estimated_delivery_ts timestamp",
)
customers = spark.createDataFrame([("c1", "u1"), ("c2", "u2")], "customer_id string, customer_unique_id string")

fact = gold.build_fact_order_item(items, orders, customers, spark.table(HISTORY))
show(
    fact.join(spark.table(HISTORY).select("seller_sk", "seller_city", "seller_state"), "seller_sk")
    .select("order_id", "order_purchase_ts", "seller_id", "seller_city", "seller_state", "price")
    .orderBy("order_purchase_ts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself / 自己试试 / やってみよう
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC 1. Apply a third snapshot on 2018-06-08 where `s1` moves back to São Paulo. How many versions does `s1` have, and which one does an order on 2018-06-06 use?
# MAGIC 2. Change only the zip code of `s2`. Is that a new version? Look at `TRACKED` in `scd2.py`.
# MAGIC 3. Why is `valid_to` exclusive (`order_date < valid_to`) rather than inclusive? What would happen to an order placed exactly on 2018-06-03?
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC 1. 在 2018-06-08 应用第三份快照，让 `s1` 搬回圣保罗。`s1` 现在有几个版本？2018-06-06 的订单用的是哪一个？
# MAGIC 2. 只改 `s2` 的邮编。这算新版本吗？看一下 `scd2.py` 里的 `TRACKED`。
# MAGIC 3. 为什么 `valid_to` 是不含当天（`order_date < valid_to`），而不是包含？正好在 2018-06-03 下的订单会怎样？
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC 1. 2018-06-08 に3つ目のスナップショットを適用し、`s1` をサンパウロに戻してください。`s1` のバージョンはいくつになり、2018-06-06 の注文はどれを使うでしょうか。
# MAGIC 2. `s2` の郵便番号だけを変えてください。新しいバージョンになりますか。`scd2.py` の `TRACKED` を見てください。
# MAGIC 3. `valid_to` が当日を含まない（`order_date < valid_to`）のはなぜでしょうか。ちょうど 2018-06-03 に発生した注文はどうなりますか。

# COMMAND ----------

# MAGIC %md
# MAGIC ## In the interview / 面试时怎么说 / 面接では
# MAGIC
# MAGIC ---
# MAGIC #### 🇬🇧 English
# MAGIC > "Sellers come as a full daily snapshot, and I keep their history as SCD Type 2 with one hand-written MERGE: change detection by attribute hash, and each changed seller staged twice, once to close the old version and once, with a NULL merge key, to insert the new one. Re-applying a snapshot is a no-op, and a seller missing from a snapshot is not deleted. The fact table joins point-in-time on the order date, so when a seller moves, past sales stay in the old state. In the real data 78 relocations produced exactly 78 closed versions."
# MAGIC
# MAGIC #### 🇨🇳 中文
# MAGIC > "卖家数据每天是一份全量快照，我用一次手写的 MERGE 把它保存成 SCD Type 2：按属性哈希检测变更，每个有变化的卖家在数据源里出现两次，一次用来关闭旧版本，一次用空的 merge key 插入新版本。重复应用同一份快照不会有任何变化，快照里缺失的卖家也不会被删除。事实表按下单日期做时点关联，卖家迁址后，过去的销售额仍留在原来的州。真实数据里 78 次迁址正好产生了 78 个被关闭的版本。"
# MAGIC
# MAGIC #### 🇯🇵 日本語
# MAGIC > 「セラーは毎日の全件スナップショットで届くため、手書きの MERGE 1回で SCD Type 2 として履歴を保持しています。変更は属性ハッシュで検知し、変更のあったセラーはソースに2回入れます。1回目で旧バージョンをクローズし、2回目は NULL の merge key で新バージョンを挿入します。同じスナップショットを再適用しても何も変わらず、スナップショットから消えたセラーも削除しません。ファクトは注文日でポイントインタイム結合するため、セラーが移転しても過去の売上は元の州に残ります。実データでは78件の移転から、ちょうど78件のクローズ済みバージョンができました。」
