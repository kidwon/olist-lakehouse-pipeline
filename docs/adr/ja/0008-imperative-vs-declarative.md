# ADR-0008: Lakeflow 宣言型パイプラインではなく、命令型（Structured Streaming + MERGE）を採用する

**ステータス:** 採用

## 背景
Databricks では、この処理を二通りの方法で構築できます。Jobs 上で Structured Streaming と手書きの MERGE を使う命令型と、`AUTO CDC` や expectations を使う Lakeflow Spark Declarative Pipelines の宣言型です。どちらも、本プロジェクトの土台となった講座で扱いました。

## 決定
命令型を採用します。このリポジトリの目的は、冪等な MERGE、前進方向のみの CDC、SCD2、隔離といった仕組みそのものを示すことです。宣言型では、これらがそれぞれ一つのキーワードに収まってしまいます。また、命令型のコードは素の PySpark なので、すべての変換をノート PC と CI 上の pytest で検証できます。

## Lakeflow で書いた場合の対応
| 本実装 | Lakeflow での対応 |
|---|---|
| `bronze.py` の Auto Loader + チェックポイント | `STREAM read_files(...)` を読む `@dp.table` |
| `merge_cdc_forward_only` | `AUTO CDC INTO orders ... KEYS (order_id) SEQUENCE BY change_seq` |
| `scd2.apply_snapshot` | `AUTO CDC FROM SNAPSHOT ... STORED AS SCD TYPE 2` |
| ルール + 隔離 | `@dp.expect_all_or_drop` と、ルールを反転させた別の隔離テーブル |
| `dq_gate` | 直接の対応はなく、イベントログを読む別のジョブタスクで実装 |

## 影響
- 保守するコード量は増えますが、各部品は小さく、テストされています。
- チーム開発では、新しいパイプラインには Lakeflow を標準とするのが妥当です。概念はそのまま対応します。
