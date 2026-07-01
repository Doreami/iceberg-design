# Rust Iceberg SDK RowID 功能演进完整报告（修订版）

> **报告范围**：涵盖用户提供的 6 个 Issue/PR 及额外发现的 4 个相关 Issue/PR  
> **报告日期**：2026-07-01  
> **修订说明**：修正了 Issue #1652 的提出者信息，并补充了各 PR/Issue 的详细代码解析



https://github.com/apache/iceberg-rust/issues/1652

https://github.com/apache/iceberg-rust/issues/1765

https://github.com/apache/iceberg-rust/issues/2607

https://github.com/apache/iceberg-rust/pull/2579

https://github.com/apache/iceberg-rust/pull/2746



## 一、概述

Rust Iceberg SDK 的 `rowid`（行级血缘）功能实现是一个分阶段、渐进式的工程过程。以下 10 个 Issue 和 PR 构成了从 **V3 元数据支持** → **`_pos` 列读取** → **`first_row_id` 写入** → **完整行级操作** 的完整演进链路。

| 编号        | 类型    | 核心主题                    | 提出者           | 提出时间       | 当前状态            |
| --------- | ----- | ----------------------- | ------------- | ---------- | --------------- |
| **#1652** | Issue | V3 表缺少 `next_row_id` 字段 | **@dentiny**  | 2025-09-10 | Open（已标记 stale） |
| **#1682** | PR    | 引入 V3 元数据格式支持           | @c-thiel      | 2025-09-17 | **✅ Merged**    |
| **#1765** | Issue | 请求支持 `_pos` 元数据列        | @vustef       | 2025-10-27 | Open（已标记 stale） |
| **#1791** | PR    | `_pos` 列原型实现            | @vustef       | 2025-10-27 | **❌ Closed**    |
| **#2607** | Issue | 系统阐述元数据列投影需求            | —             | 2026 年初    | Open            |
| **#2579** | PR    | 实现 `first_row_id` 写入    | @Shekharrajak | 2026 年初    | **Open**        |
| **#2746** | PR    | 实现 `_pos` 列读取支持         | @hsiang-c     | 2026-06-29 | Open（Draft）     |
| **#2153** | PR    | 增量表扫描                   | @xanderbailey | 2025 年     | **✅ Merged**    |
| **#—**    | ML    | 行级血缘规范 ID 分配投票          | —             | 2025-04    | **✅ Passed**    |
| **#—**    | ML    | 0.3.0 版本发布讨论            | —             | 2024-06    | 已发布             |

## 二、元数据层基础（阶段一）

### 2.1 Issue #1652：V3 表缺少 `next_row_id` 字段

| 项目       | 内容                                                          |
| -------- | ----------------------------------------------------------- |
| **编号**   | [#1652](https://github.com/apache/iceberg-rust/issues/1652) |
| **标题**   | iceberg v3 has to set first-row-id                          |
| **类型**   | Feature Request                                             |
| **提出者**  | **@dentiny**                                                |
| **提出时间** | 2025-09-10                                                  |
| **当前状态** | **Open**（2026-04-28 标记 stale）                               |

#### 核心问题

Issue 作者指出，根据 Iceberg V3 规范：

> - 编写者必须设置表的`next-row-id`，并在创建新快照时使用现有的`next-row-id`作为`first-row-id`
> 
> - 当表升级到v3时，`next_row_id`应初始化为0
> 
> - 提交新快照时，`next-row-id`必须至少增加新分配的行id的数量

但 Rust SDK 的 `TableMetadata` 结构体中**缺少 `next_row_id` 字段**，这导致无法正确分配和管理行级 ID。

#### 社区讨论

| 时间         | 参与者           | 关键内容                                          |
| ---------- | ------------- | --------------------------------------------- |
| 2025-09-10 | @c-thiel      | 已开始实现 V3 支持，首个 PR 明天可用                        |
| 2025-10-29 | @hzxa21       | 询问 V3 支持路线图，表示愿意贡献                            |
| 2026-06-04 | @Shekharrajak | 指出 `next_row_id` 和 manifest list 已有逻辑，正在起草 PR |

#### 意义重要性

⭐⭐ **这是整个 RowID 功能的起点和基石。** 

没有 `next_row_id` 字段，就无法实现：

- V3 表的行级 ID 分配

- 快照级别的 `first_row_id` 记录

- 数据文件的 `first_row_id` 写入

- 完整的行级血缘追踪

#### 代码解析

Issue 指出 Rust SDK 的 `TableMetadata` 结构体（位于 `crates/iceberg/src/spec/table_metadata.rs`）中缺少 `next_row_id` 字段。根据规范，该字段应该是：

- 类型：`i64`（64 位有符号整数）

- 初始值：新表为 0，升级表也初始化为 0

- 更新规则：每次提交新快照时，至少增加该快照新增的行数

### 2.2 PR #1682：引入 V3 元数据格式支持

| 项目       | 内容                                                        |
| -------- | --------------------------------------------------------- |
| **编号**   | [#1682](https://github.com/apache/iceberg-rust/pull/1682) |
| **标题**   | feat: Support for V3 Metadata                             |
| **类型**   | Pull Request                                              |
| **提出者**  | @c-thiel                                                  |
| **提出时间** | 2025-09-17                                                |
| **当前状态** | **✅ Merged**（2025-11-04 由 @Xuanwo 合并）                     |

#### 核心内容

该 PR 为 Rust SDK **引入了对 Iceberg V3 元数据格式的全面支持**[#1682](https://github.com/apache/iceberg-rust/pull/1682)，包括：

- 在 `FormatVersion` 枚举中引入 `V3` 变体

- 更新所有相关的元数据结构体以支持 V3 格式

- 添加大量测试用例（PR 中大部分代码为测试代码）

#### 社区讨论

| 时间         | 参与者      | 关键内容                          |
| ---------- | -------- | ----------------------------- |
| 2025-09-17 | @c-thiel | 提交 PR，引入 V3 FormatVersion     |
| 2025-09-29 | @c-thiel | 添加更多测试，修复 `EncryptedKey` 可选字段 |
| 2025-11-04 | @Fokko   | 指出此 PR 是 PyIceberg 读取 V3 的前提  |
| 2025-11-04 | @Xuanwo  | 解决冲突并合并                       |

#### 代码解析

PR 中有一个值得注意的讨论点：`FIRST_ROW_ID` 和 `REFERENCE_DATA_FILE` 在 V2 schema 中存在，但规范中并未列为 V2 的可选字段。这反映了 Iceberg 规范演进中的一些历史遗留问题。

PR 的主要代码变更分布在：

- `crates/iceberg/src/spec/table_metadata.rs`：V3 元数据结构

- `crates/iceberg/src/spec/snapshot.rs`：快照 `first_row_id` 支持

- `crates/iceberg/src/spec/manifest.rs`：Manifest `first_row_id` 支持

**核心元数据 (`TableMetadata`)**

为了支持 V3 的行级ID分配，`TableMetadata` 结构体新增了一个关键字段。

| 字段名               | 类型    | 说明                                       |
| ----------------- | ----- | ---------------------------------------- |
| **`next_row_id`** | `i64` | 一个全局单调递增的计数器，用于为表中每一行新数据分配唯一的 `_row_id`。 |

**快照 (`Snapshot`)**

`Snapshot` 结构体通过组合方式，引入了记录行ID范围的能力。

| 变更方式     | 字段/方法                | 类型                         | 说明                                                                                                 |
| -------- | -------------------- | -------------------------- | -------------------------------------------------------------------------------------------------- |
| **新增字段** | `row_range`          | `Option<SnapshotRowRange>` | 一个包含 `first_row_id` 和 `added_rows` 的结构体[](https://github.com/apache/iceberg-rust/pull/1682/files)。 |
| **新增方法** | `first_row_id()`     | `-> Option<u64>`           | 返回该快照中第一条新增行的 `_row_id`[](https://github.com/apache/iceberg-rust/pull/1682/files)。                 |
| **新增方法** | `added_rows_count()` | `-> Option<u64>`           | 返回该快照中新增行的总数[](https://github.com/apache/iceberg-rust/pull/1682/files)。                            |

**清单文件 (`Manifest`)**

PR 为 Manifest 的写入器增加了对 V3 格式的支持。

- **`ManifestWriter` 支持 V3**：新增了 `build_v3_data` 和 `build_v3_deletes` 方法，用于构建 V3 格式的 Manifest 写入器。在构建 V3 的 `ManifestWriter` 时，`first_row_id` 会在 Manifest 被添加到清单列表（Manifest List）时由 `ManifestListWriter` 分配。

**表更新操作 (`TableUpdate`)**

`TableUpdate` 枚举新增了两个与元数据加密相关的变体。

- **`AddEncryptionKey`**：用于向表元数据中添加一个加密密钥。

- **`RemoveEncryptionKey`**：用于从表元数据中移除一个加密密钥。

**其他相关变更**

- **`FormatVersion` 枚举**：新增了 `V3` 变体，用于标识表的格式版本。

- **DataFile Schema**：为了支持 V3，新增了 `data_file_schema_v3` 函数。

- **序列化支持**：调整了 `Snapshot` 的序列化/反序列化逻辑，以正确处理 `first_row_id` 等新字段。

- **`TableCreation`**：该结构体新增了 `format_version` 字段，用于在创建表时指定版本。

- **测试与验证**：PR 新增了大量测试，包括针对 V3 表创建、快照以及行级血缘（Row Lineage）功能的测试。

**总结**

PR #1682 通过为 `TableMetadata`、`Snapshot`、`TableUpdate` 等核心结构体增加字段，为 Rust SDK 支持 **V3 表格式**、**行级血缘** 和 **元数据加密** 等功能铺平了道路。这些变更使得 Rust SDK 在元数据层面具备了与 V3 规范兼容的能力。

#### 意义重要性

⭐⭐⭐ **这是 RowID 功能的元数据层基础。** 该 PR 的合并意味着 Rust SDK 从**元数据层面**具备了理解和解析 V3 表结构的能力。

## 三、读取层能力（阶段二）

### 3.1 Issue #1765：请求支持 `_pos` 元数据列

| 项目       | 内容                                                          |
| -------- | ----------------------------------------------------------- |
| **编号**   | [#1765](https://github.com/apache/iceberg-rust/issues/1765) |
| **标题**   | Support for `_pos` metadata column                          |
| **类型**   | Feature Request                                             |
| **提出者**  | @vustef                                                     |
| **提出时间** | 2025-10-27                                                  |
| **当前状态** | **Open**（已标记 stale，2026-04-26）                              |

#### 核心问题

根据 Iceberg 规范，`_pos` 是元数据列之一。该列对于以下场景至关重要：

- **位置删除（Positional Deletes）** 的写入操作

- **用户识别行**：通过 `(file_path, position)` 组合唯一标识一行

Issue 作者指出，`_pos` 列的实现依赖 arrow-rs 的 `RowNumber` 虚拟列功能，因此需要先升级 arrow-rs 到 0.57.x 版本。

#### 意义重要性

⭐⭐⭐ **这是实现行级操作（DELETE/UPDATE/MERGE）的关键前置条件。** `_pos` 列是实现位置删除的核心，而位置删除又是 Iceberg 行级操作的基础。

### 3.2 PR #1791：`_pos` 列原型实现（因缺乏 activity 关闭）

| 项目       | 内容                                                               |
| -------- | ---------------------------------------------------------------- |
| **编号**   | [#1791](https://github.com/apache/iceberg-rust/pull/1791)        |
| **标题**   | General support for metadata columns + implementation for `_pos` |
| **类型**   | Pull Request（Draft）                                              |
| **提出者**  | @vustef                                                          |
| **提出时间** | 2025-10-27                                                       |
| **当前状态** | **❌ Closed**（因缺乏 activity 关闭）                                    |

#### 核心内容

该 PR 是 `_pos` 列支持的**原型实现**。主要变更集中在：

- 添加 `with_metadata_columns` 方法，支持元数据列投影

- `reader.rs`：实现 `_pos` 列的读取逻辑

- `scan/mod.rs`：在扫描时配置是否返回 `_pos` 列

#### 代码解析

PR 中提出的核心 API 设计：

```rust
pub fn with_metadata_columns(mut self, metadata_columns: Vec) -> Self {
    // Need some validation here, for allowed column names.
    // Or type safety, to pick columns from enum (possibly preventing duplication?)
}
```

#### 关闭原因

该 PR 依赖 arrow-rs 的未合并变更，且因缺乏 activity 被自动关闭。后续由 `PR #2746` 接替。

### 3.3 Issue #2607：系统性阐述元数据列投影需求

| 项目       | 内容                                                                                         |
| -------- | ------------------------------------------------------------------------------------------ |
| **编号**   | [#2607](https://github.com/apache/iceberg-rust/issues/2607)                                |
| **标题**   | Support for projecting metadata columns `_pos`, `_spec_id`, and `_partition` in table scan |
| **类型**   | Feature Request                                                                            |
| **提出时间** | 2026 年初                                                                                    |
| **当前状态** | **Open**                                                                                   |

#### 核心问题

该 Issue 系统性地阐述了在表扫描时支持多个元数据列的必要性：

| 列            | Copy-on-Write | Merge-on-Read | 作用               |
| ------------ | ------------- | ------------- | ---------------- |
| `_file`      | **required**  | row identity  | 标识源数据文件          |
| `_pos`       | **required**  | row identity  | 标识文件内的行位置        |
| `_spec_id`   | —             | **required**  | Delta 写入的分区规范 ID |
| `_partition` | —             | **required**  | Delta 写入的分区值     |

Issue 明确指出：

> Without these, query engines cannot implement row-level mutations against Iceberg tables via iceberg-rust.

#### 详细实现方案

| 列                | 实现方式                                            |
| ---------------- | ----------------------------------------------- |
| **`_spec_id`**   | 常量值，从 `FileScanTask` 的 manifest entry 获取，注入为常量列 |
| **`_pos`**       | 非固定值，使用 Parquet 的 `RowNumber` 虚拟列生成，跨批次维护偏移状态   |
| **`_partition`** | Struct 类型，包含所有分区字段的联合，每行从数据文件获取分区值              |

Issue 详细参考了 Java Iceberg 的实现：

- **Copy-on-Write**：`SparkCopyOnWriteOperation.requiredMetadataAttributes()` 请求 `_file` + `_pos`

- **Merge-on-Read**：`SparkPositionDeltaOperation` 使用 `_file` + `_pos` 作为 `rowId()`

#### 意义重要性

⭐⭐⭐⭐⭐ **这是从“能读数据”到“能改数据”的关键跨越。** 该 Issue 清晰地描绘了 Rust SDK 实现完整行级操作所需的全部元数据列能力。

### 3.4 PR #2695：实现 `_spec_id` 元数据列支持

| 项目       | 内容                                                        |
| -------- | --------------------------------------------------------- |
| **编号**   | [#2695](https://github.com/apache/iceberg-rust/pull/2695) |
| **标题**   | feat: metadata column `_spec_id`                          |
| **类型**   | Pull Request                                              |
| **提出者**  | @hsiang-c                                                 |
| **提出时间** | 2026-06-16                                                |
| **当前状态** | **Open**                                                  |

#### 核心内容

该 PR 实现了 `_spec_id` 元数据列的支持。当查询的投影（Projection）中包含 `_spec_id` 列时，将其作为一个**常量列**注入到扫描结果中，与 `_file` 元数据列的处理方式一致。

对于同一张表的所有数据行，其分区规范 ID（`_spec_id`）是相同的，因此可以作为常量列直接填充。

#### 与 RowID 的关联

`_spec_id` 是 Iceberg 规范中定义的元数据列之一。在 **Issue #2607** 中系统性地阐述了支持 `_spec_id`、`_partition`、`_pos` 等元数据列的必要性。该 PR 与 PR #2746 共同构成了对 Issue #2607 的具体实现。

Issue #2607 明确指出 `_spec_id` 在 Merge-on-Read 场景中是 **required** 的，用于 Delta 写入的分区规范标识。

**社区讨论**

| 时间         | 参与者         | 关键内容                        |
| ---------- | ----------- | --------------------------- |
| 2026-06-16 | @hsiang-c   | 提交 PR，实现 `_spec_id` 常量列注入   |
| 2026-06-17 | @mbutrovich | 建议增加不同分区规范 ID 的测试用例         |
| —          | @mbutrovich | 建议未来将多个元数据列 PR 统一到一套更清晰的抽象中 |

讨论中提到，当前存在多个 PR 分别以不同方式添加元数据列（`_spec_id`、`_partition`、`_pos`），未来应将它们统一到一种更清晰的抽象机制中。

#### 代码解析

该 PR 复用了现有的 `constant_fields` 机制来注入 `_spec_id` 列：

- 在 `scan/arrow.rs` 的 `builder.build()` 方法中添加 `_spec_id` 常量列

- 从 `FileScanTask` 的 manifest entry 中获取 `partition_spec_id`

- 使用 `add_constant_column` 方法将常量列添加到结果中

#### 意义重要性

⭐⭐⭐ **这是实现 Issue #2607 规划的具体步骤之一。** 该 PR 标志着 Rust SDK 在元数据列投影能力上迈出了实质性的一步。

### 3.5 PR #2746：实现 `_pos` 列读取支持（进行中）

| 项目       | 内容                                                        |
| -------- | --------------------------------------------------------- |
| **编号**   | [#2746](https://github.com/apache/iceberg-rust/pull/2746) |
| **标题**   | feat: add support for `_pos` metadata column              |
| **类型**   | Pull Request（Draft）                                       |
| **提出者**  | @hsiang-c                                                 |
| **提出时间** | 2026-06-29                                                |
| **当前状态** | **Open（Draft）**                                           |

#### 核心内容

该 PR 实现了 `_pos` 元数据列的读取支持，核心技术方案是**利用 Parquet 读取器的 `RowNumber` 虚拟列**获取行号，这与 Java 的 `SupportsRowPosition` / `PositionReader` 做法一致。

**社区讨论（@mbutrovich 的 Code Review）**

@mbutrovich 对该方案给予了肯定：

> I like this approach a lot. Delegating to the parquet reader's `RowNumber` virtual column is essentially what Java does with `SupportsRowPosition` / `PositionReader`, and it gets correct absolute file positions through row-group pruning and row selection without us hand-rolling a counter.

关键的测试要求：

1. `_pos` 与行选择（row selection）结合，验证位置正确

2. `_pos` 与删除文件（delete file）结合

3. `_pos` 在 split task 场景下（非完整文件）

4. `_pos` 与其他元数据列交错

关于 `_pos` 的核心语义：

- 规范定义 `_pos` 为**源数据文件中的序号位置**

- 删除过滤**不会重新编号** `_pos`

- split task 扫描 `[start, start+length)` 仍输出**绝对位置**

#### 代码解析

PR 的核心变更包括)：

- 通过 `virtual_fields` 和 `PassThrough` 机制实现虚拟列注入

- 添加 `row_number` 字段（类型 `Int64`，非空）

- 使用 `PARQUET_FIELD_ID_META_KEY` 元数据标记

- 提供 `with_virtual_field` 方法设置虚拟列

```rust
let row_number_field = Arc::new(
    Field::new("row_number", DataType::Int64, false)
        .with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            // ...
        )]))
);
```

#### 意义重要性

⭐⭐⭐⭐⭐ **这是当前最活跃、最接近完成的 RowID 相关 PR。** 该 PR 标志着 `_pos` 列支持从设计讨论进入**实际代码开发阶段**。

#### `_pos` 与 `rowid` 的关系

在 Iceberg 中，`rowid` 是一个逻辑概念，代表一行的唯一标识符。而 `_pos` 则是一个具体的元数据列，代表了**一行在源数据文件中的物理序号位置**。

- **`rowid` 的生成依赖 `_pos`**：`rowid` 的值通常由 `first_row_id`（文件内第一行的ID）加上 `_pos`（行在文件内的偏移量）计算得出。因此，没有 `_pos`，就无法精确计算出每一行的 `rowid`。

- **`_pos` 是行级操作的基础**：像 `UPDATE`、`DELETE`、`MERGE` 这样的行级操作，都需要依赖 `_pos` 来精确定位目标行。

所以，**`_pos` 列的支持，是 Rust SDK 迈向完整 `rowid` 功能和行级数据操作的关键一步**。

## 四、写入层能力（阶段三 进行中）

### 4.1 PR #2579：实现 `first_row_id` 写入（进行中）

| 项目       | 内容                                                        |
| -------- | --------------------------------------------------------- |
| **编号**   | [#2579](https://github.com/apache/iceberg-rust/pull/2579) |
| **标题**   | feat: per-DataFile.first_row_id for v3 row lineage        |
| **类型**   | Pull Request                                              |
| **提出者**  | @Shekharrajak                                             |
| **提出时间** | 2026-04-22                                                |
| **当前状态** | **🟢 Open**                                               |

#### 核心内容

该 PR 旨在**补齐 V3 行级血缘在写入端的最后一块拼图**：

> every ADDED DataFile in a v3 data manifest now gets a first_row_id stamped on write, and foreign-written manifests get one inherited on read. Combined with the existing TableMetadata.next_row_id / ManifestFile.first_row_id plumbing, this makes iceberg-rust spec-compliant for v3 row-id assignment end-to-end.

PR 明确参考了 Java 的实现：

- Java 的 `ManifestReader.idAssigner`（per-file inheritance）

- Java 的 `SnapshotProducer` row-id seeding

#### 当前进展

该 PR 仍处于 Open 状态，持续收到审阅意见和更新。最近的讨论集中在：

- 确保 `first_row_id` 在写入 V3 Manifest 时正确分配

- 处理从 V2 升级到 V3 的表时，为历史数据文件合理设置 `first_row_id` 的继承逻辑

- 完善相关测试用例，覆盖升级场景和并发写入场景

#### 意义重要性

⭐⭐⭐⭐ **这是从“能读元数据”到“能写行级ID”的关键 PR。**

该 PR 一旦合并，Rust SDK 将具备：

- 写入时为每个 V3 数据文件分配 `first_row_id`

- 读取时为外部写入的 manifest 继承 `first_row_id`

- 结合已有的 `TableMetadata.next_row_id` 和 `ManifestFile.first_row_id`，实现端到端的 V3 行级 ID 分配

## 五、补充发现：增量扫描与规范演进

### 5.1 PR #2153：增量表扫描（已合并）

| 项目       | 内容                                                        |
| -------- | --------------------------------------------------------- |
| **编号**   | [#2153](https://github.com/apache/iceberg-rust/pull/2153) |
| **标题**   | Enable incremental read                                   |
| **类型**   | Pull Request                                              |
| **提出者**  | @xanderbailey                                             |
| **提出时间** | 2025 年                                                    |
| **当前状态** | **✅ Merged**                                              |

#### 核心内容

实现了**增量表扫描**功能，允许用户高效地读取两个快照之间的新增数据。

**新增 API***：

```rust
// Scan changes between two snapshots (from exclusive, to inclusive)
table.scan()
    .from_snapshot_exclusive(from_id)
    .to_snapshot(to_id)
    .build()?;

// Convenience methods (matching Java API)
table.scan().appends_after(from_id).build()?;
table.scan().appends_between(from_id, to_id).build()?;
```

**实现细节**：

- 添加了 `SnapshotRange` 结构体，用于验证快照血缘关系并跟踪快照 ID 范围

- 修改了 `ManifestFileContext`，仅过滤状态为 `ADDED` 且在范围内的快照

- 验证快照范围内仅包含 `APPEND` 操作（与 Java 行为一致）

- 如果 `from_snapshot` 不是 `to_snapshot` 的祖先，返回明确错误

**与 RowID 的关联**

增量扫描是 CDC（变更数据捕获）的基础。`rowid` 作为行级唯一标识，是实现精确增量消费的关键，两者功能相辅相成。

### 5.2 行级血缘规范投票（邮件列表）

| 项目     | 内容                      |
| ------ | ----------------------- |
| **类型** | Apache Iceberg 邮件列表投票   |
| **时间** | 2025-04-16 至 2025-04-19 |
| **结果** | **✅ 通过**（11 票赞成，0 票反对）  |

> **说明**：本投票发生在 Apache Iceberg **主仓库**（`apache/iceberg`），而非 Rust SDK 仓库（`apache/iceberg-rust`）。它是对 Iceberg **规范（Spec）** 的变更投票，直接影响所有实现（包括 Rust SDK）的 RowID 功能设计。纳入本报告是为了说明 Rust SDK 实现所遵循的社区规范背景。

#### 核心变更

该投票针对 **PR #12781**（`apache/iceberg` 主仓库），包含两个主要变更：

1. **表升级时的 RowID 分配策略**：
   
   - 旧方案：升级到 V3 后，RowID 保持 null，直到行被重写
   
   - **新方案**：升级到 V3 后的**第一次写入**时，为所有行分配 RowID
   
   - 优势：使 RowID 功能更可靠，无需等待全表重写

2. **`first_row_id` 分配规则的灵活性**：
   
   - 旧规则：严格的继承和分配规则
   
   - **新规则**：改为“必须大于或等于”，并给出安全示例

#### 投票详情

- **发起人**：Ryan Blue

- **投票时间**：2025-04-16 至 2025-04-19

- **投票结果**：11 票 +1（赞成），0 票 -1（反对）

- **状态**：✅ 投票通过，变更已合入规范

#### 对 Rust SDK 的影响

| 影响维度     | 说明                                              |
| -------- | ----------------------------------------------- |
| **实现指导** | Rust SDK 的 `first_row_id` 写入逻辑（PR #2579）必须遵循此规范 |
| **升级场景** | Rust SDK 需支持 V2→V3 升级时的 RowID 批量分配逻辑            |
| **验证依据** | 该规范是 Rust SDK RowID 功能正确性的判断标准                  |

#### 意义重要性

⭐⭐⭐⭐ **这是整个 Iceberg 社区对 RowID 功能方向的官方确认。** Rust SDK 的实现将遵循这一规范，确保与 Java SDK 等其他实现的行为一致性。

## 六、关系图谱

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Rust Iceberg SDK RowID 功能演进全景图                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  规范层（Apache Iceberg 社区）                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  邮件列表投票 (2025-04)  ──→  ✅ Passed                           │   │
│  │  行级血缘规范 ID 分配更新（PR #12781）                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  阶段一：元数据层基础（2025-09 ~ 2025-11）                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  #1652 (Issue by @dentiny) ──→  #1682 (PR by @c-thiel) ──→ ✅ Merged │   │
│  │  指出缺少 next_row_id            引入 V3 元数据支持                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  阶段二：读取层能力（2025-10 ~ 进行中）                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  #1765 (Issue by @vustef) ──→  #1791 (PR by @vustef) ──→ ❌ Closed │   │
│  │  请求 _pos 列支持              原型实现（依赖 arrow-rs）            │   │
│  │                              ↓                                       │   │
│  │  #2607 (Issue) ──→  系统性规划 _pos/_spec_id/_partition             │   │
│  │  完整元数据列投影需求                                               │   │
│  │                              ↓                                       │   │
│  │  #2746 (PR by @hsiang-c) ──→  🔄 Open (Draft)                     │   │
│  │  基于 RowNumber 虚拟列实现 _pos 读取                               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  阶段三：写入层能力（2026 年）                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  #2579 (PR by @Shekharrajak) ──→  🔄 Open                      │   │
│  │  实现 first_row_id 写入（设计已完成，待重新提交）                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  辅助能力                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  #2153 (PR by @xanderbailey) ──→  ✅ Merged                      │   │
│  │  增量表扫描（CDC 基础）                                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    ↓                                        │
│  最终目标：完整的 RowID / 行级血缘支持                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 七、总结

### 7.1 各阶段里程碑状态

| 阶段      | 关键里程碑             | 状态     | 关键 PR/Issue        |
| ------- | ----------------- | ------ | ------------------ |
| **规范层** | 行级血缘规范 ID 分配更新    | ✅ 已完成  | ML Vote (2025-04)  |
| **阶段一** | V3 元数据支持          | ✅ 已完成  | #1682 (Merged)     |
| **阶段二** | `_pos` 列读取        | 🔄 进行中 | #2746 (Open Draft) |
| **阶段二** | 元数据列投影规划          | 📋 已定义 | #2607 (Open)       |
| **阶段三** | `first_row_id` 写入 | 🔄 进行中 | #2579 (Closed)     |
| **辅助**  | 增量表扫描             | ✅ 已完成  | #2153 (Merged)     |

### 7.2 核心结论

1. **元数据层已就绪**：PR #1682 为 Rust SDK 提供了 V3 元数据的完整支持，`next_row_id`、`first_row_id` 等字段已在元数据层面就位。

2. **读取层正在推进**：PR #2746 正在实现 `_pos` 列的读取支持，利用 Parquet 的 `RowNumber` 虚拟列，与 Java 实现方案一致。Issue #2607 则提供了 `_spec_id` 和 `_partition` 的完整实现蓝图。

3. **写入层已有设计**：PR #2579 虽已关闭，但已展示 `first_row_id` 写入的完整设计思路，参考了 Java 的 `ManifestReader.idAssigner` 和 `SnapshotProducer`。

4. **规范已明确**：Iceberg 社区已通过投票明确了 V3 行级血缘的 ID 分配规范，为 Rust SDK 的实现提供了明确的指导。

5. **增量扫描已就绪**：PR #2153 已合并，为基于 RowID 的 CDC 场景提供了基础设施。

### 7.3 当前可用的 RowID 相关 API

Rust SDK 目前已提供以下 RowID 相关 API：

```rust
// 保留字段 ID
pub const RESERVED_FIELD_ID_ROW_ID: i32 = 2_147_483_540i32;  // i32::MAX - 107

// 获取 _row_id 字段定义
pub fn row_id_field() -> &'static NestedFieldRef
```

### 7.4 展望

Rust Iceberg SDK 的 RowID 功能正处于 **“读取层能力建设”** 阶段，`_pos` 列的支持是最接近完成的里程碑。写入层的 `first_row_id` 支持已有明确设计，待重新提交。完整的行级血缘支持是社区明确的长期目标。

建议关注以下进展：

- **PR #2746** 的 Code Review 和合并

- **Issue #1652** 的新 PR 提交（@Shekharrajak 表示正在起草[PR #2579](https://github.com/apache/iceberg-rust/pull/2579)）

- **Apache Iceberg 邮件列表**中关于 Rust SDK 的讨论

# 补充问题

## `first_row_id` 支持详情

### 一、概述

`first_row_id` 是 Iceberg V3 行级血缘（Row Lineage）功能的核心元数据字段。在 Rust Iceberg SDK 中，对该字段的支持呈现 **“元数据就绪，数据读写缺失”** 的分层状态：

| 层级                     | 支持状态   | 说明                                                                                                                         |
| ---------------------- | ------ | -------------------------------------------------------------------------------------------------------------------------- |
| **快照（Snapshot）**       | ✅ 已支持  | 提供 `first_row_id()` API                                                                                                    |
| **清单文件（ManifestFile）** | ✅ 已支持  | 结构体包含 `first_row_id` 字段[ManifestFile.html](https://docs.rs/iceberg/latest/iceberg/spec/struct.ManifestFile.html)           |
| **数据文件（DataFile）**     | ❌ 缺失   | 结构体中无 `first_row_id` 字段[DataFile.html](https://docs.rs/iceberg-rust/0.3.0/iceberg_rust/spec/manifest/struct.DataFile.html) |
| **写入能力**               | 🔄 进行中 | PR #2579 正在解决                                                                                                              |
| **读取能力**               | 🔄 进行中 | PR #2746 正在解决                                                                                                              |

### 二、已支持：元数据层面

#### 2.1 快照（Snapshot）级别

Rust SDK 的 `Snapshot` 结构体提供了 `first_row_id()` 方法：

```rust
pub fn first_row_id(&self) -> Option<u64>
```

该方法返回该快照中**第一条新增行**的 Row ID，该快照中所有新增行的 Row ID 都将大于此值[](https://rust.iceberg.apache.org/api/iceberg/spec/struct.Snapshot.html)。

#### 2.2 清单文件（ManifestFile）级别

`ManifestFile` 结构体包含 `first_row_id` 字段，类型为 `Option<u64>`，字段 ID 为 520。该字段表示**由 `ADDED` 数据文件新增的行所分配的起始 `_row_id`**。

该字段在 V3 表格式中为必需字段，Rust SDK 在元数据层面已完整支持。

### 三、缺失：数据文件（DataFile）层面

#### 3.1 当前状态

Rust SDK 的 `DataFile` 结构体**目前不包含** `first_row_id` 字段。根据文档，`DataFile` 仅包含以下核心字段：

- `file_path`：文件路径

- `partition`：分区数据

- `record_count`：记录数

- `file_size_in_bytes`：文件大小

- `value_counts`：各列的值计数

**缺失影响**：

- 写入时无法为每个数据文件记录其起始 Row ID

- 读取时无法直接从 `DataFile` 元数据获取 `first_row_id`

- 无法实现端到端的 V3 行级 ID 分配

#### 3.2 规范要求

根据 Iceberg V3 规范，**每个数据文件都应在清单（Manifest）中记录其 `first_row_id`**。该值存储在元数据中，每个数据文件记录一次，不存储在数据文件本身中。

Rust SDK 目前尚未在 `DataFile` 中实现此字段。

### 四、进行中的修复

#### 4.1 PR #2579：`first_row_id` 写入支持

| 项目       | 内容                                                 |
| -------- | -------------------------------------------------- |
| **编号**   | #2579                                              |
| **标题**   | feat: per-DataFile.first_row_id for v3 row lineage |
| **提出者**  | @Shekharrajak                                      |
| **当前状态** | **🟢 Open**                                        |

**目标**：补齐 V3 行级血缘在**写入端**的最后一块拼图——每个 V3 数据清单中 `ADDED` 状态的数据文件，在写入时都会被标记 `first_row_id`；同时，由外部工具写入的清单文件在读取时也会继承该 ID。

**参考实现**：PR 明确参考了 Java 的 `ManifestReader.idAssigner`（按文件继承）和 `SnapshotProducer`（行 ID 播种）机制。

**当前进展**：

- 确保 `first_row_id` 在写入 V3 Manifest 时正确分配

- 处理 V2→V3 表升级时历史数据文件的 `first_row_id` 继承逻辑

- 完善相关测试用例

#### 4.2 PR #2746：`_pos` 列读取支持

| 项目       | 内容                                           |
| -------- | -------------------------------------------- |
| **编号**   | #2746                                        |
| **标题**   | feat: add support for `_pos` metadata column |
| **提出者**  | @hsiang-c                                    |
| **当前状态** | **🟢 Open（Draft）**                           |

**说明**：`_pos` 列与 `first_row_id` 共同构成 Row ID 计算的基础（`_row_id = first_row_id + _pos`）。该 PR 虽不直接修改 `DataFile` 结构体，但为完整 Row ID 读取能力提供了必要支撑。

### 五、支持状态总结

| 能力维度                   | 元数据定义/读取                | 数据文件写入              | 数据文件读取              |
| ---------------------- | ----------------------- | ------------------- | ------------------- |
| **快照（Snapshot）**       | ✅ 支持（`first_row_id()`）  | N/A                 | N/A                 |
| **清单文件（ManifestFile）** | ✅ 支持（`first_row_id` 字段） | ❌ 写入逻辑缺失            | ❌ 读取逻辑缺失            |
| **数据文件（DataFile）**     | ❌ 未发现相关字段               | 🔄 **PR #2579 进行中** | 🔄 **PR #2746 进行中** |

### 六、核心结论

1. **元数据层已就绪**：Rust SDK 在 `Snapshot` 和 `ManifestFile` 层面已完整支持 `first_row_id` 的元数据定义和读取。

2. **数据文件层缺失**：`DataFile` 结构体**不包含** `first_row_id` 字段，导致无法在写入和读取时实现端到端的行级 ID 分配。

3. **写入能力正在开发**：PR #2579 正在积极解决 `first_row_id` 的写入支持问题。

4. **读取能力正在跟进**：PR #2746 正在实现 `_pos` 列的读取支持，与 `first_row_id` 共同构成完整的 Row ID 读取能力。

5. **完整支持路径**：`DataFile.first_row_id` 字段的添加（PR #2579）+ `_pos` 列读取（PR #2746）+ `_row_id` 元数据列投影 = 完整的 V3 行级血缘支持。

## 快照（Snapshot）、清单文件（ManifestFile）和数据文件（DataFile）中的 `first_row_id`的关系

快照（Snapshot）、清单文件（ManifestFile）和数据文件（DataFile）中的 `first_row_id`，本质上是同一行ID值在不同元数据层级的**副本与摘要**，共同服务于 Iceberg V3 的行级血缘（Row Lineage）功能。

三者之间的关系，可以用 **“规划-汇总-执行”** 来理解：

| 元数据层级                   | 角色      | 核心作用                                                                                                                                            |
| ----------------------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **数据文件 (DataFile)**     | **执行者** | 记录文件内第一行数据的 `_row_id`，是计算行ID (`_row_id = firstRowId + row_position`)的基础。                                                                        |
| **清单文件 (ManifestFile)** | **汇总者** | 对其中所有 `ADDED` 状态数据文件的起始行ID进行汇总，为查询优化提供文件级别的元数据摘要[ManifestFile.html](https://rust.iceberg.apache.org/api/iceberg/spec/struct.ManifestFile.html)。 |
| **快照 (Snapshot)**       | **规划者** | 记录该快照中第一条新增行的 `_row_id`，作为整个快照新增数据范围的“锚点”。                                                                                                      |

### 📄 数据文件 (DataFile)：行ID的精确计算

数据文件本身不存储 `first_row_id` 字段。但在 V3 表中，其清单（Manifest）条目会记录该文件内容的起始行ID。查询引擎正是利用这个元数据，结合行在文件内的位置（`row_position`），来精确计算出每一行的 `_row_id`。

### 📋 清单文件 (ManifestFile)：文件级别的元数据摘要

清单文件（Manifest File）是连接快照和数据文件的桥梁。它本身是一个元数据文件，其中包含了其所管理的所有数据文件的条目（Entry）。对于 V3 表，每个清单文件都会在顶层记录一个 `first_row_id` 字段。

- **值的含义**：这个字段的值，等于该清单文件中，所有 **`ADDED`（新增）状态的数据文件**里，**最小的那个 `first_row_id`**。

- **主要用途**：查询优化器（Query Optimizer）在规划阶段，可以通过读取清单文件的这个摘要值，快速判断是否需要扫描整个清单文件，从而实现“元数据剪枝”（Metadata Pruning），极大地提升查询性能。

### 📸 快照 (Snapshot)：事务级别的行ID范围锚点

快照（Snapshot）代表了表在某个时间点的完整状态。在 V3 表中，每个快照也会记录一个 `first_row_id`。

- **值的含义**：这个值是该快照中，**第一个被新增的数据行**所分配到的 `_row_id`。该快照中所有新增行的 `_row_id` 都将大于或等于此值。

- **主要用途**：它定义了该快照所包含的**新行ID范围的下界**，是进行增量读取（Incremental Read）和行级血缘追踪的关键锚点。

### 🔗 关联与生成：V3 元数据的写入流程

这三个层级的 `first_row_id` 在数据写入时按以下流程生成和关联：

1. **写入数据**：数据写入生成新的数据文件，每个文件会获得一个 `first_row_id`。

2. **生成清单**：写入器（Writer）会创建一个新的清单文件来记录这些数据文件的元数据。该清单文件的 `first_row_id` 会被设置为其中所有 `ADDED` 数据文件中最小的那个 `first_row_id`。

3. **提交快照**：最后，提交一个新的快照，其 `first_row_id` 会被设置为该快照中第一个新增数据行的ID。

### 💎 总结

总而言之，`first_row_id` 在这三个元数据层级中扮演着不同但互补的角色：

- **数据文件 (DataFile)**：提供最细粒度的行ID计算基准。

- **清单文件 (ManifestFile)**：提供文件级别的元数据摘要，用于查询优化。

- **快照 (Snapshot)**：提供事务级别的行ID范围锚点，用于版本追踪和增量读取。

## 上述PR 都成功合入后，还需要哪些能力

如果 PR #2579、#2695、#2746 等 RowID 相关 PR 都能成功合入，Rust Iceberg SDK 将具备 RowID 功能的**核心骨架**，但距离 Java SDK 的完整支持仍存在明显差距。以下是详细分析：

---

### 一、合入后将具备的能力

这些 PR 合入后，Rust SDK 将具备以下能力：

| 能力维度                  | 具体能力                                        | 支撑 PR      |
| --------------------- | ------------------------------------------- | ---------- |
| **V3 元数据**            | `TableMetadata.next_row_id` 等核心字段           | #1682（已合入） |
| **增量扫描**              | 支持 `appends_after()` 读取快照间新增数据              | #2153（已合入） |
| **`first_row_id` 写入** | 写入 V3 表时自动为每个 `ADDED` 数据文件分配 `first_row_id` | #2579（进行中） |
| **`_pos` 列读取**        | 扫描时可投影文件内行位置                                | #2746（进行中） |
| **`_spec_id` 列读取**    | 扫描时可投影分区规范 ID                               | #2695（进行中） |

届时，Rust SDK 将实现：**写入时自动分配 `first_row_id` + 读取时支持 `_pos`/`_spec_id` 元数据列投影 + 增量扫描**，构成 RowID 功能的完整数据流骨架。

### 二、仍缺失的能力

即使上述 PR 全部合入，Rust SDK 在以下方面仍与 Java SDK 存在差距：

#### 2.1 读取层面

| 缺失能力                                    | Java SDK | Rust SDK | 说明                                          |
| --------------------------------------- | -------- | -------- | ------------------------------------------- |
| **`_row_id` 列投影**                       | ✅ 支持     | ❌ 无公开 PR | 用户无法通过 `SELECT _row_id FROM table` 查询行 ID   |
| **`_last_updated_sequence_number` 列投影** | ✅ 支持     | ❌ 无公开 PR | RowID 的伴侣列，记录行最后修改的序列号                      |
| **基于 `_row_id` 的谓词下推**                  | ✅ 支持     | ❌ 无公开 PR | 无法执行 `WHERE _row_id = 123` 或 `BETWEEN` 范围过滤 |

**当前状态**：PR #2746 和 #2695 分别实现了 `_pos` 和 `_spec_id`，但 `_row_id` 和 `_last_updated_sequence_number` 这两个核心元数据列的投影**尚未有任何公开的 PR 或 Issue**。

#### 2.2 写入层面

| 缺失能力                       | Java SDK    | Rust SDK                 | 说明                                           |
| -------------------------- | ----------- | ------------------------ | -------------------------------------------- |
| **`next_row_id` 完整生命周期管理** | ✅ 支持        | 🔄 部分支持（Issue #1652 规划中） | 全局 RowID 分配器的自动推进和冲突处理                       |
| **V2→V3 表升级的 RowID 批量分配**  | ✅ 支持（规范已明确） | ❌ 无公开 PR                 | 升级后第一次写入时为所有历史行分配 RowID                      |
| **`_row_id` 物理列写入**        | ✅ 支持        | ❌ 无公开 PR                 | Java 默认将 `_row_id` 作为物理列写入 Parquet，Rust 无此设计 |

#### 2.3 DML 与表维护

| 缺失能力                           | Java SDK | Rust SDK | 说明                                           |
| ------------------------------ | -------- | -------- | -------------------------------------------- |
| **行级 DELETE / UPDATE / MERGE** | ✅ 支持     | ❌ 无公开 PR | 需要删除文件（Delete File）的完整处理链路                   |
| **Compaction 后 RowID 重映射**     | ✅ 支持     | ❌ 无公开 PR | 保证合并小文件后 `_row_id` 保持不变                      |
| **RowID 相关的系统表**               | ❌ 引擎层实现  | ❌ 无      | `row_lineage` 系统表由引擎（如 Hive）实现，非 Java SDK 提供 |

#### 2.4 生态与集成

| 缺失能力                      | Java SDK               | Rust SDK | 说明                |
| ------------------------- | ---------------------- | -------- | ----------------- |
| **DataFusion 深度集成**       | N/A                    | 🔄 部分支持  | 虚拟列下推、分区剪枝等优化尚需加强 |
| **Python 绑定暴露 RowID API** | ✅（通过 PyIceberg + Java） | ❌ 无      | 需等待 Rust SDK 底层就绪 |

### 三、差距总结

| 能力维度                                    | 合入后状态             | 仍缺失                |
| --------------------------------------- | ----------------- | ------------------ |
| **V3 元数据**                              | ✅ 完整              | —                  |
| **`first_row_id` 写入**                   | ✅ 完整（#2579）       | —                  |
| **`_pos` / `_spec_id` 读取**              | ✅ 完整（#2746/#2695） | —                  |
| **增量扫描**                                | ✅ 完整（#2153）       | —                  |
| **`_row_id` 列投影**                       | ❌ **缺失**          | **用户最直接的需求**       |
| **`_last_updated_sequence_number` 列投影** | ❌ **缺失**          | 行级血缘的伴侣信息          |
| **基于 `_row_id` 的谓词下推**                  | ❌ **缺失**          | 无法进行点查优化           |
| **`next_row_id` 完整管理**                  | 🔄 部分             | Issue #1652 仍 Open |
| **V2→V3 表升级**                           | ❌ **缺失**          | 无公开 PR             |
| **行级 DELETE/UPDATE/MERGE**              | ❌ **缺失**          | 需删除文件处理链路          |

### 四、优先补充建议

若要让 Rust SDK 达到 Java SDK 在 RowID 功能上的成熟度，建议按以下优先级推进：

| 优先级      | 缺失能力                                | 理由                        |
| -------- | ----------------------------------- | ------------------------- |
| 🔴 **高** | `_row_id` 列投影                       | 用户查询 RowID 的最直接接口         |
| 🔴 **高** | 基于 `_row_id` 的谓词下推                  | 支持 `WHERE _row_id = ?` 点查 |
| 🔴 **高** | `_last_updated_sequence_number` 列投影 | 行级血缘的完整信息                 |
| 🔴 **高** | `next_row_id` 完整生命周期管理              | 写入端 RowID 分配的基础（#1652）    |
| 🟡 **中** | V2→V3 表升级的 RowID 批量分配               | 规范合规性，规范已明确               |
| 🟡 **中** | 行级 DELETE/UPDATE/MERGE              | 需要删除文件处理链路                |
| 🟡 **中** | Compaction 后 RowID 重映射              | 保证 RowID 长期稳定性            |
| 🟢 **低** | `row_lineage` 系统表                   | 引擎层实现，非 SDK 能力            |

### 五、结论

当前这些 PR 合入后，Rust SDK 的 RowID 功能将呈现 **“骨架已全，血肉待丰”** 的状态：

- **骨架**：V3 元数据 + `first_row_id` 写入 + `_pos`/`_spec_id` 读取 + 增量扫描

- **血肉（缺失）**：`_row_id` 列投影、`_last_updated_sequence_number`、基于 RowID 的谓词下推、`next_row_id` 完整管理、表升级处理、DML 支持等

其中**最关键的缺失是 `_row_id` 列本身在查询时的可投影性**——这是用户使用 RowID 功能的最直接接口，也是后续所有行级操作的基础。社区需要在 PR #2746（`_pos`）和 #2695（`_spec_id`）的基础上，进一步实现 `_row_id` 元数据列的投影支持，以及 `_last_updated_sequence_number` 列的投影支持。
