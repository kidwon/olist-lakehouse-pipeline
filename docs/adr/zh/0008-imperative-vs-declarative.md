# ADR-0008：选择命令式（Structured Streaming + MERGE），而不是 Lakeflow 声明式管道

[日本語](../ja/0008-imperative-vs-declarative.md) | [English](../en/0008-imperative-vs-declarative.md) | **中文**

**状态：** 已采纳

## 背景
在 Databricks 上有两种方式构建这条管道：在 Jobs 上用 Structured Streaming 加手写 MERGE 的命令式，或者用 `AUTO CDC` 和 expectations 的 Lakeflow Spark Declarative Pipelines 声明式。本项目所基于的课程两种都讲过。

## 决策
采用命令式。这个仓库的目的是展示机制本身：幂等的 MERGE、只前进的 CDC、SCD2 和隔离。在声明式管道里，这些都会变成一个关键字。另外，命令式代码就是普通的 PySpark，所有转换都可以在笔记本电脑和 CI 上用 pytest 验证。

## 如果用 Lakeflow 来写
| 本实现 | Lakeflow 中的对应写法 |
|---|---|
| `bronze.py` 中的 Auto Loader + checkpoint | 读取 `STREAM read_files(...)` 的 `@dp.table` |
| `merge_cdc_forward_only` | `AUTO CDC INTO orders ... KEYS (order_id) SEQUENCE BY change_seq` |
| `scd2.apply_snapshot` | `AUTO CDC FROM SNAPSHOT ... STORED AS SCD TYPE 2` |
| 规则 + 隔离 | `@dp.expect_all_or_drop`，再加一张用反向规则写入的隔离表 |
| `dq_gate` | 没有直接对应，需要另写一个读取事件日志的作业任务 |

## 影响
- 需要维护的代码更多，但每个部件都很小，并且有测试。
- 在团队开发中，新管道默认用 Lakeflow 是合理的，概念可以一一对应。
