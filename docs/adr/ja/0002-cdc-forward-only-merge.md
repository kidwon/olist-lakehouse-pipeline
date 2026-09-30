# ADR-0002: 注文 CDC は `change_seq` に基づき前進方向にのみ適用する

**日本語** | [English](../en/0002-cdc-forward-only-merge.md) | [中文](../zh/0002-cdc-forward-only-merge.md)

**ステータス:** 採用

## 背景
注文のステータス変更は、1変更1行（変更後イメージ）で届きます。到着順は保証されず、`shipped` の2日後に `approved` が届くこともあります。同じ変更が二度届くこともあります。ファイル内の順序や到着時刻からは、どの変更が最新かを判断できません。

## 決定
- マイクロバッチ内では、`order_id` ごとに `change_seq` が最大の行を残します。
- `silver.orders` へは `WHEN MATCHED AND s.change_seq > t.change_seq THEN UPDATE` で MERGE します。既存と同じか古い変更は無視します。
- 検証に失敗した変更行（例：購入前に配達済み）は隔離し、注文は直前の正しい状態を保ちます。

## 検討した代替案
- **`change_ts` で並べる:** タイムスタンプは同値になり得ますし、システム間で時計がずれることもあります。信頼できる順序は送信元の連番だけです。
- **Silver に全変更履歴を保持し、ビューで最新状態を導出する:** 監査には有用ですが、履歴は既に Bronze にあります。Silver の役割は現在の状態を持つことです。
- **Lakeflow の `AUTO CDC ... SEQUENCE BY change_seq`:** 同じ意味論を1行で書けます。本プロジェクトで手書きしている理由は ADR-0008 を参照してください。

## 影響
- ステータスが後退することはありません。テスト: `test_late_older_change_does_not_regress_status`、およびリプレイのステージングデータから算出した正解と Silver を突き合わせる E2E テスト。
- 送信元が `change_seq` をリセットした場合、この方式は破綻します。それは契約の変更に当たります。
