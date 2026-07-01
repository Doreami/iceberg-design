# Rust Iceberg 自定义索引实现 — 需求与设计分析文档

> **文档目的**：基于 Java Iceberg SDK 的 RowID 实现、Rust Iceberg SDK（0.10.0）的社区现状、以及 Lance 对 RowID 的设计，分析自定义索引从 RowAddr 直连到 RowID 增强的演进路径。区分 SDK 缺失功能与索引需实现功能，按阶段给出优先级、工作量评估和并行开发建议。
> 
> **适用范围**：V3 表 only，不考虑 V2→V3 兼容。基于 **Rust Iceberg SDK 0.10.0**（未发布版本）Fork 开发。
> 
> **当前架构**：`iceberg-index` 基于 `RowAddress { file_path, row_position }` 直连（快照绑定，每个 snapshot 独立索引）。
> 
> **目标架构**：`iceberg-index` 基于 RowID 存储，参考 Lance `RowIdIndex` 设计维护 RowID→RowAddr 映射，利用 RowID 稳定性解决 Deletion Vector 精确处理、Compaction 索引不失效等核心问题。
> 
> **开发人力**：2 人
> 
> **文档日期**：2026-07-02

## 一、核心概念

| 概念               | Java Iceberg SDK                      | Rust Iceberg SDK (0.10.0)                | Lance        |
| ---------------- | ------------------------------------- | ---------------------------------------- | ------------ |
| **行逻辑 ID**       | `_row_id` (field ID: `MAX_VALUE-107`) | `RESERVED_FIELD_ID_ROW_ID` 常量已定义         | `_rowid`     |
| **行物理地址**        | `(file_path, position)`               | `RowAddress { file_path, row_position }` | `_rowaddr`   |
| **行在文件内位置**      | `_pos`                                | 不支持；PoC 手动计数 `_index_row_position`       | `_rowoffset` |
| **RowID→物理地址映射** | 引擎层实现                                 | 无；**索引需实现**                              | `RowIdIndex` |

**核心理解**：

- **RowID 是稳定逻辑标识符**：一旦分配，在行生命周期内保持稳定（引擎层 UPDATE/MERGE 继承旧 ID）。Compaction 后物理位置全变，RowID 不变——这是索引稳定性的基石。
- **RowAddr 是易变物理地址**：`(file_path, row_position)`，Compaction 后必然变化。当前 PoC 的快照绑定特性恰因缺乏稳定 RowID。
- **RowID 对索引的核心价值**：
  - **Deletion Vector**：稳定 RowID 可精确标记和清理索引条目，不受物理位置偏移影响
  - **Compaction 不变性**：数据重组后 RowID 不变 → 只更新映射表，不重建索引
  - **回表定位**：RowID → RowAddr 映射是回表的关键基础设施（参考 Lance `RowIdIndex`）

## 二、Rust SDK 能力分析（0.10.0 源码验证）

> 以下基于 `iceberg-rust = "=0.10.0"` 交叉验证。`metadata_columns` 模块已定义全部 12 个元数据列常量，无需修改。

### 2.1 已支持

| 能力                  | 说明                                                                                                        |
| ------------------- | --------------------------------------------------------------------------------------------------------- |
| V3 表元数据             | `TableMetadata.next_row_id` 字段 + public getter 可用                                                         |
| V3 快照元数据            | `Snapshot.first_row_id()`、`Snapshot.added_rows_count()` 可用；V3 序列化/反序列化完备                                  |
| `first_row_id` 数据层  | `DataFile.first_row_id()`、`ManifestFile.first_row_id` 可读取                                                 |
| `first_row_id` 自动分配 | `ManifestListWriter::v3()` commit 路径自动调用 `assign_first_row_id`，分配 manifest 级 first_row_id 并推进 next_row_id |
| `_file` 元数据列        | scan 可投影（索引 PoC 已在使用）                                                                                     |
| V3 表读写              | 可创建/读取/写入 V3 格式表                                                                                          |

### 2.2 待实现（索引必需，需 Fork SDK）

| 缺失能力                             | 说明                                                                                                                               | 所属阶段 |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ---- |
| **DataFile 级 `first_row_id` 填充** | `ManifestListWriter` 已处理 manifest 级别的分配，但单个 DataFile 写入时 `first_row_id` 默认为 None（需在 write 路径补一小段逻辑，从 `next_row_id` 分配）           | 阶段一  |
| **`_pos` 列读取**                   | scan reader 不计算 `_pos`；需 Parquet `RowNumber` 虚拟列集成（参考 PR #2746）                                                                  | 阶段一  |
| **`_row_id` 列读取**                | 无 `RowIdReader`；需实现 `firstRowId + position` 动态计算 + 物理列优先双重读取（参考 Java SDK）。注意元数据层（firstRowId、nextRowId）已就绪                        | 阶段一  |
| **`_row_id` 物理列写入**              | Writer 不写 `_row_id` 物理列（参考 Java `schemaWithRowLineage()`）                                                                        | 阶段二  |
| **增量扫描 API**                     | 0.10.0 源码验证 **无** `appends_after()`、`IncrementalAppendScan` 等增量扫描 API；需实现快照 diff（参考 Java `IncrementalDataTableScan`、社区 PR #2153） | 阶段二  |

> 0.10.0 的 RowID 元数据层（`next_row_id`、`first_row_id` 定义/读取/V3 序列化、ManifestListWriter 分配）**基本完备**，缺失集中在 **scan reader 计算**（`_pos`、`_row_id`）和 **writer 物理列嵌入**，SDK 改造量比之前预估大幅减少。

## 三、功能分解总览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                      阶段一：索引基础功能                                │
│  目标：索引存储 (索引字段 → RowID)，通过 RowID→Addr 映射回表             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  SDK 待实现                             索引需实现                       │
│  ┌───────────────────────────┐       ┌──────────────────────────────┐   │
│  │ S1. _pos 列读取           │       │ I1. RowID → RowAddr 映射    │   │
│  │     (Parquet RowNumber)   │ ────→ │      (参考 Lance RowIdIndex) │   │
│  │                           │       │      (Puffin/Parquet 存储)   │   │
│  │ S2. _row_id 列读取        │       │ I2. Deletion Vector 感知     │   │
│  │     (动态计算+物理列优先)  │ ────→ │      (索引构建/查询过滤)     │   │
│  │                           │       │                              │   │
│  │ [S0. DataFile first_row_id│       │ I3. 回表扫描                 │   │
│  │  填充 — 元数据层已就绪，   │ ────→ │      (RowID→Addr→读取数据)  │   │
│  │  仅需 write 路径微调]     │       │                              │   │
│  └───────────────────────────┘       └──────────────────────────────┘   │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                      阶段二：DML & Compaction 支持                      │
│  目标：操作后 RowID 不变，物理位置变化时索引不失效                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  SDK 待实现                             索引需实现                       │
│  ┌───────────────────────────┐       ┌──────────────────────────────┐   │
│  │ M4. _row_id 物理列写入    │       │ I4. INSERT 增量更新          │   │
│  │     (Compaction 时保留)   │ ────→ │      (需增量扫描 API)        │   │
│  │                           │       │                              │   │
│  │ M5. 增量扫描 API          │       │ I5. DELETE 索引清理          │   │
│  │     (appends_after)       │ ────→ │      (按 RowID 精确删除)     │   │
│  │                           │       │                              │   │
│  │                           │       │ I6. Compaction Remap         │   │
│  │                           │       │      (映射重定向)            │   │
│  └───────────────────────────┘       └──────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## 四、阶段一：索引基础功能

> **目标**：索引存储 `(索引字段值 → RowID)` 条目，通过 RowID→RowAddr 映射实现回表，构建时正确过滤 Deletion Vector。
> 
> **交付物**：索引可对含删除文件的快照正确构建和查询，Compaction 前所有场景可用。

### 4.1 SDK 侧：DataFile 级 `first_row_id` 填充（S0，小改动）

**现状**：Rust SDK 0.10.0 的 RowID 元数据层基本完备：

- ✅ `TableMetadata.next_row_id` 字段 + public getter 已存在
- ✅ `ManifestListWriter::v3()` 在 commit 路径自动调用 `assign_first_row_id`，分配 manifest 级 first_row_id 并推进 next_row_id
- ✅ `DataFile.first_row_id()` / `Snapshot.first_row_id()` 读取路径可用
- ⚠️ **唯一缺口**：单个 DataFile 的 `first_row_id` 在写入时默认为 None，未从 `next_row_id` 自动分配

**修复要点**：在数据写入路径（创建 DataFile 时）从当前 `next_row_id` 读取并赋值 `first_row_id`，然后推进 `next_row_id += record_count`。Java SDK 中这是 `Parquet.writeData()` 的自动行为。

| 子任务                                | 代码量            |
| ---------------------------------- | -------------- |
| Write 路径中 DataFile.first_row_id 赋值 | ~50-80 行       |
| 单元测试                               | ~50-80 行       |
| **合计**                             | **~100-160 行** |

### 4.2 SDK 侧：`_pos` 元数据列读取（S1）

**参考**：Java SDK `PositionReader`、社区 PR #2746

**现状**：PoC 通过 `_file` + 手动计数器 `_index_row_position` 获取行位置。**此方案无法正确处理含删除文件的快照**（已删除行被计数，导致偏移）。S1 与 S2 合并为统一的 `PositionAndRowIdEvaluator`（共享计数器），详见 [§6.2](#62-sdk-pos--row_id-列读取s1--s2)。

| 子任务                      | 代码量            |
| ------------------------ | -------------- |
| Arrow reader `_pos` 投影逻辑 | ~200-300 行     |
| `RowNumber` 虚拟列集成        | ~150-200 行     |
| 偏移状态维护                   | ~80-120 行      |
| 单元测试（含删除文件场景）            | ~150-200 行     |
| **合计**                   | **~580-820 行** |

### 4.3 SDK 侧：`_row_id` 列读取（S2）

**参考**：Java SDK `RowIdReader`（双重读取：物理列优先 → fallback `firstRowId + position`）

**现状**：Rust SDK 常量已定义，scan reader 不计算值。与 S1 合并实现，详见 [§6.2](#62-sdk-pos--row_id-列读取s1--s2)。

| 子任务                       | 代码量            |
| ------------------------- | -------------- |
| Arrow reader `_row_id` 投影 | ~150-200 行     |
| `RowIdReader` 双重读取实现      | ~150-200 行     |
| 与 position 来源联动           | ~80-120 行      |
| 单元测试（有/无物理列）              | ~200-300 行     |
| **合计**                    | **~580-820 行** |

### 4.4 索引侧：RowID → RowAddr 映射（I1）

**数据模型**：

```
索引存储：  (索引字段值) → [RowID₁, RowID₂, ...]   ← 倒排索引
映射存储：  RowID → (file_path, row_position)      ← RowID→Addr 映射
回表链路：  index.search(query) → RowID[] → mapping.lookup_all(RowID[]) → RowAddress[] → reader.read(RowAddress[])
```

- 索引条目 key = 索引字段值（如 BTree 的 name 值），value = 该值对应的 RowID 集合
- 映射表存储 `RowID → (file_path, row_position)`，作为 Iceberg 表元数据的一部分持久化
- 索引构建时：扫描每行 → 提取 (索引字段值, RowID) → 写入索引 + 同步填充映射
- 回表时：`index.search()` 返回 RowID 列表 → 批量查映射得 RowAddr → 按物理地址读数据
- **核心价值**：Compaction 后只更新映射表（RowID 不变），索引条目无需任何改动

**映射存储方案**（均基于 Iceberg 原生存储）：

| 方案                | 描述                                                                                                   | 适用场景                            |
| ----------------- | ---------------------------------------------------------------------------------------------------- | ------------------------------- |
| **Puffin 文件**     | 映射写入 Puffin blob，通过 `StatisticsFile` 提交到 snapshot，与索引 registry 同机制                                   | 推荐，与现有索引基础设施一致                  |
| **独立 Parquet 文件** | 映射存为 `{table}/metadata/rowid-mapping-{snapshotId}.parquet`，以 `(row_id, file_path, row_position)` 列存储 | 支持高效列式扫描，适合大规模映射（百万行+）          |
| **嵌入索引段**         | 映射直接编码在索引 segment 的 artifact 中                                                                       | 每个 segment 自包含，但跨 segment 查询需聚合 |

> 推荐 Puffin 方案（与现有索引 registry 一致），数据量大时可切换 Parquet 列式存储。
>
> **详细数据结构与算法见 [§6.3](#63-rowid--rowaddr-映射i1)**。

| 子任务                                    | 代码量            |
| -------------------------------------- | -------------- |
| 映射存储接口定义（抽象 trait）                     | ~80-120 行      |
| Puffin 方案实现（映射序列化 + StatisticsFile 提交） | ~150-200 行     |
| 映射构建逻辑（构建时填充）                          | ~120-180 行     |
| 映射查询逻辑（回表时批量查询）                        | ~80-120 行      |
| 单元测试                                   | ~150-200 行     |
| **合计**                                 | **~580-820 行** |

> **依赖**：S2（`_row_id` 列读取）完成后可填充映射。

### 4.5 索引侧：Deletion Vector 感知（I2）

**说明**：Iceberg DELETE 通过删除文件标记已删除行。索引构建/查询时需过滤已删除行。

**现状**：PoC 的 `IcebergSnapshotIndexSource` 当前拒绝含删除文件的快照。S1（`_pos` 原生列）完成后可安全处理——`_pos` 自动跳过已删除行（手动计数器不行）。

| 子任务                     | 代码量            |
| ----------------------- | -------------- |
| 删除向量读取接口（封装 SDK reader） | ~100-150 行     |
| 索引构建过滤（跳过已删除行）          | ~100-150 行     |
| 查询时回表前检查删除状态            | ~80-120 行      |
| 单元测试                    | ~150-200 行     |
| **合计**                  | **~430-620 行** |

> **依赖**：S1（`_pos`）完成后可安全处理删除文件场景。基础框架可在 S1 完成前先行（使用手动计数器过渡，仅限无删除文件快照）。

### 4.6 索引侧：回表扫描（I3）

**说明**：索引查询返回 RowID 列表 → 通过 I1 映射获取 RowAddr → 按 `(file_path, row_position)` 读取数据行。

**现状**：PoC 已有 `IcebergTableReader` 基础实现（`read_file_rows()`、`materialize_candidates()` 等），可直接按 RowAddr 定位。需新增的是 **RowID → 映射查询 → RowAddr → 回表** 的完整链路。

| 子任务                  | 代码量            |
| -------------------- | -------------- |
| RowID→RowAddr→回表链路串联 | ~100-150 行     |
| 映射批量查询 + 回表批量读取优化    | ~150-200 行     |
| 结果排序还原（保持 score 顺序）  | 已有基础，~50-100 行 |
| 单元测试                 | ~100-150 行     |
| **合计（新增）**           | **~400-600 行** |

> **依赖**：I1（映射）完成后可完整串联。

### 4.7 阶段一汇总

| 功能                           | 责任  | 代码量                | 依赖  | 优先级 |
| ---------------------------- | --- | ------------------ | --- | --- |
| S0. DataFile first_row_id 填充 | SDK | ~100-160 行         | 无   | P0  |
| S1. `_pos` 列读取               | SDK | ~580-820 行         | 无   | P0  |
| S2. `_row_id` 列读取            | SDK | ~580-820 行         | S0  | P0  |
| I1. RowID → RowAddr 映射       | 索引  | ~580-820 行         | S2  | P0  |
| I2. Deletion Vector 感知       | 索引  | ~430-620 行         | S1  | P0  |
| I3. 回表扫描（RowID 链路）           | 索引  | ~400-600 行         | I1  | P0  |
| **SDK 合计**                   |     | **~1,260-1,800 行** |     |     |
| **索引合计**                     |     | **~1,410-2,040 行** |     |     |
| **阶段一总计**                    |     | **~2,670-3,840 行** |     |     |

**里程碑**：

- **M1.1（第 1-2 周）**：S0 + S1 完成 → SDK 侧可开始 S2，索引侧可开始 I2 基础框架
- **M1.2（第 5-6 周）**：S2 + I1 完成 → **RowID 可读取 + 映射可用 = 索引核心链路打通**
- **M1.3（第 7-8 周）**：I2 + I3 完成 → **阶段一交付：索引支持 Deletion Vector + RowID 回表**

## 五、阶段二：DML & Compaction 操作支持

> **目标**：在 INSERT/DELETE/Compaction 后，RowID 保持稳定，索引不失效。
> 
> **核心问题**：Compaction 重写文件后物理位置全变，动态计算 `new_firstRowId + new_position` 会得到错误的 RowID。必须通过**物理列写入**保留原始 `_row_id`。

### 5.1 SDK 侧：`_row_id` 物理列写入（M4）

**参考**：Java SDK `MetadataColumns.schemaWithRowLineage()` + `newRewrite().rewriteFiles()`

**说明**：阶段一的 S2 实现了动态计算读取。但 Compaction 后 `firstRowId + position` 与原始值不一致，必须在写入新文件时**显式嵌入 `_row_id` 物理列**。

Java SDK 的做法：读取时用 `schemaWithRowLineage()` 投影 `_row_id` → 写入时同一 schema 将 `_row_id` 作为物理列写入新 Parquet → reader 自动切换为物理列读取（`RowIdReader` 双重读取的优先分支）。

| 子任务                             | 代码量            |
| ------------------------------- | -------------- |
| Writer 支持投影 `_row_id` 列写入       | ~150-200 行     |
| `schemaWithRowLineage()` 等效工具函数 | ~50-100 行      |
| `newRewrite()` 保留 RowID 逻辑      | ~100-150 行     |
| 单元测试                            | ~100-150 行     |
| **合计**                          | **~400-600 行** |

### 5.2 SDK 侧：增量扫描 API（M5）

**参考**：Java SDK `IncrementalDataTableScan` / `appendsAfter()`，社区 PR #2153（Draft，未合并）

**说明**：INSERT 增量索引更新需要只扫描新增文件。0.10.0 源码中**不存在** `appends_after` 或任何增量扫描 API，需从零实现。核心接口：`appends_after(from_snapshot_id)`，比较 manifest 列表找出新增文件。

| 子任务                                          | 代码量            |
| -------------------------------------------- | -------------- |
| 快照 manifest diff 逻辑                          | ~150-200 行     |
| `appends_after()` API + TableScan builder 集成 | ~100-150 行     |
| 单元测试                                         | ~150-250 行     |
| **合计**                                       | **~400-600 行** |

### 5.3 索引侧：INSERT 增量更新（I4）

**说明**：INSERT 新数据后，索引只扫描新增文件（使用 M5 增量扫描），提取 RowID + 索引键，增量写入索引存储和映射表。

| 子任务             | 代码量            |
| --------------- | -------------- |
| 快照检查点管理         | ~80-120 行      |
| 增量文件扫描 + 索引条目插入 | ~150-200 行     |
| 映射表同步更新         | ~80-100 行      |
| 单元测试            | ~100-150 行     |
| **合计**          | **~410-570 行** |

> **依赖**：M5（增量扫描）+ I1（映射）

### 5.4 索引侧：DELETE 索引清理（I5）

**说明**：DELETE 操作后，通过 Deletion Vector 找出被删除行的 RowID（通过 I1 映射反查），从索引中移除对应条目。

| 子任务                       | 代码量            |
| ------------------------- | -------------- |
| DELETE 快照检测               | ~80-120 行      |
| 删除向量 → RowAddr → RowID 反查 | ~100-150 行     |
| 索引条目 + 映射条目清理             | ~80-120 行      |
| 单元测试                      | ~100-150 行     |
| **合计**                    | **~360-540 行** |

> **依赖**：I1（映射）+ I2（Deletion Vector）

### 5.5 索引侧：Compaction Remap（I6）

Compaction 重写数据文件 → 物理位置全变 → I1 映射失效。完整算法流程和方案对比见 [§6.6](#66-compaction-remapi6)。

| 子任务                     | 代码量            |
| ----------------------- | -------------- |
| Compaction 检测           | ~80-120 行      |
| 新旧文件列表获取                | ~100-150 行     |
| 映射批量重建（方案 B）或全量重建（方案 A） | ~150-250 行     |
| 单元测试                    | ~150-200 行     |
| **合计**                  | **~480-720 行** |

> **依赖**：I1（映射）+ M4（`_row_id` 物理列写入，方案 B 需要）

### 5.6 索引侧：UPDATE 索引更新（I7，可选）

**说明**：UPDATE = DELETE + INSERT。若引擎层支持 RowID 继承（新行保留旧行 `_row_id`），索引只需更新映射位置。若不支持，索引需 DELETE 旧条目 + INSERT 新条目。

**注意**：RowID 继承需要**引擎层**在 UPDATE 时显式读取旧 `_row_id` 并写入新 Parquet（参考 Java 引擎层行为）。纯 SDK 的 Overwrite 不继承。此功能优先级最低，依赖引擎侧配合。

| 子任务                     | 代码量            |
| ----------------------- | -------------- |
| DELETE 部分 + INSERT 部分协调 | 复用 I4 + I5     |
| 索引侧协调逻辑                 | ~100-150 行     |
| **合计（索引侧）**             | **~200-300 行** |

### 5.7 阶段二汇总

| 功能                   | 责任  | 代码量                | 依赖           | 优先级 |
| -------------------- | --- | ------------------ | ------------ | --- |
| M4. `_row_id` 物理列写入  | SDK | ~400-600 行         | S2           | P1  |
| M5. 增量扫描 API         | SDK | ~400-600 行         | 无            | P1  |
| I4. INSERT 增量更新      | 索引  | ~410-570 行         | M5 + I1      | P2  |
| I5. DELETE 索引清理      | 索引  | ~360-540 行         | I1 + I2      | P1  |
| I6. Compaction Remap | 索引  | ~480-720 行         | I1 + M4      | P1  |
| I7. UPDATE 索引更新      | 索引  | ~200-300 行         | I4 + I5      | P3  |
| **SDK 合计**           |     | **~800-1,200 行**   |              |     |
| **索引合计**             |     | **~1,450-2,130 行** |              |     |
| **阶段二总计**            |     | **~2,250-3,330 行** |              |     |

> **优先级说明**：M4 + I6（Compaction Remap）+ I5（DELETE 清理）是 RowID 稳定性的**核心价值交付**——Compaction 后索引不失效，DELETE 后索引精确清理。

**里程碑**：

- **M2.1（第 9-10 周）**：M4 + M5 完成 → SDK 能力完备
- **M2.2（第 11-12 周）**：I5 + I6 完成 → **核心价值交付：DELETE/Compaction 下索引不失效**
- **M2.3（第 13-14 周）**：I4 + I7 完成 → 全部功能就绪

## 六、详细设计

> 本章按**实现顺序**排列，给出关键功能的数据结构定义、接口契约和核心算法伪代码。

### 6.1 SDK: DataFile `first_row_id` 填充（S0）

S0 是 S2（`_row_id` 动态计算）的前提——RowIdReader 需要 manifest entry 中的 `firstRowId`。

**现状**：0.10.0 的 `ManifestListWriter` 已处理 manifest 级别分配，`DataFile.first_row_id()` 可读取，但单个 DataFile 写入时该字段默认为 None。

**修复**：在 `ParquetWriter` 创建 `DataFile` 时从 `TableMetadata.next_row_id()` 取值填充：

```rust
// io/parquet/writer.rs — DataFile 创建处
let first_row_id = table_metadata.next_row_id();
let data_file = DataFileBuilder::default()
    // ... existing fields ...
    .first_row_id(Some(first_row_id as i64))
    .build()?;
// next_row_id 由 ManifestListWriter 在 commit 时推进
```

**工作量**：~100-160 行（含测试）。

### 6.2 SDK: `_pos` + `_row_id` 列读取（S1 + S2）

**参考**：Java SDK `PositionReader` + `RowIdReader`，社区 PR #2746

#### 6.2.1 设计决策

两者共享同一个行计数器（`row_position_in_file`），实现为统一的 `PositionAndRowIdEvaluator`。**S1 和 S2 不独立实现**——分两个编号只是为了里程碑规划，实际是一次性工作。

#### 6.2.2 双重读取逻辑

```
读取 _row_id 列时, 对每个 Parquet 文件:
  ┌─ 检查当前文件是否包含 _row_id 物理列 (field ID = MAX_VALUE - 107)
  │
  ├─ 存在 → 走 Parquet 列读取器, 直接读物理值
  │         (如 Compaction 后显式写入的)
  │
  └─ 不存在 → 动态计算: firstRowId + position
              (如首次 INSERT 的文件)
              firstRowId ← manifest entry 中该 DataFile 的 first_row_id
              position   ← 当前行在该文件中的 0-based 偏移
```

#### 6.2.3 统一实现

```rust
/// 同时计算 _pos 和 _row_id (共享计数器，避免不一致)
struct PositionAndRowIdEvaluator {
    row_position: u64,
    file_first_row_id: Option<u64>,
}

impl PositionAndRowIdEvaluator {
    fn new() -> Self {
        Self { row_position: 0, file_first_row_id: None }
    }

    /// 切换到新文件时调用
    fn on_new_file(&mut self, data_file: &DataFile) {
        self.row_position = 0;
        if data_file_has_physical_row_id(data_file) {
            self.file_first_row_id = None;  // 走物理列读取
        } else {
            self.file_first_row_id = data_file.first_row_id().map(|id| id as u64);
        }
    }

    /// 返回 (_pos, _row_id)
    fn next(&mut self) -> (u64, u64) {
        let pos = self.row_position;
        let row_id = self.file_first_row_id
            .map(|first| first + pos)
            .unwrap_or(0);
        self.row_position += 1;
        (pos, row_id)
    }
}
```

**关键点**：
- `first_row_id` 来自 manifest entry（0.10.0 已支持读取），配合 S0 的 write 路径修复后写入侧自动填充
- 物理列存在时 `file_first_row_id = None`，`next()` 返回的 `row_id` 被忽略（物理列值优先）
- SDK reader 自动应用 delete file 过滤后，已删除行不进入计数——`row_position` 自动对齐物理行号（手动计数器 `_index_row_position` 做不到）
- 工作量：S1+S2 合计 ~1,160-1,640 行

### 6.3 RowID → RowAddr 映射（I1）

**参考**：Lance `RowIdIndex`（`lance-table/src/rowids/index.rs`）

> *"An index of row ids. This index is used to map row ids to their corresponding addresses."*

#### 6.3.1 编码策略

RowID 在 **INSERT 直接产生的文件内是连续的**（`firstRowId + position`），但 Compaction 合并多文件后会出现非连续序列。按文件粒度自动选择编码：

| 场景 | 编码 | 存储 | 文件内查找 |
|------|------|------|-----------|
| INSERT 后的文件 | `Range(first, count)` | 16 字节 | O(1) |
| Compaction 后的文件 | `SortedArray(ids)` | N×8 字节 | O(log N) |
| DELETE 后部分删除 | `SortedArray(ids)`（暂不单独优化） | N×8 字节 | O(log N) |

#### 6.3.2 数据结构

```rust
/// 单文件内的 RowID 序列编码
enum RowIdEncoding {
    /// 连续递增: row_id = first + offset
    Range { first: u64, count: u64 },
    /// 非连续: 有序数组, 二分查找
    SortedArray { ids: Vec<u64> },
}

impl RowIdEncoding {
    /// 给定 RowID，返回该行在文件内的 0-based position。
    fn position_of(&self, row_id: u64) -> Option<u64> {
        match self {
            Self::Range { first, count } =>
                (row_id >= *first && row_id < *first + *count)
                    .then(|| row_id - *first),
            Self::SortedArray { ids } =>
                ids.binary_search(&row_id).ok().map(|i| i as u64),
        }
    }

    fn min_row_id(&self) -> u64 {
        match self {
            Self::Range { first, .. } => *first,
            Self::SortedArray { ids } => ids.first().copied().unwrap_or(0),
        }
    }

    fn max_row_id(&self) -> u64 {
        match self {
            Self::Range { first, count } => *first + *count - 1,
            Self::SortedArray { ids } => ids.last().copied().unwrap_or(0),
        }
    }

    fn from_iter(ids: impl Iterator<Item = u64>) -> Self {
        let v: Vec<u64> = ids.collect();
        if v.is_empty() { return Self::Range { first: 0, count: 0 }; }
        if v.windows(2).all(|w| w[1] == w[0] + 1) {
            Self::Range { first: v[0], count: v.len() as u64 }
        } else {
            Self::SortedArray { ids: v }
        }
    }
}

/// 一个 DataFile 的映射条目
struct FileMapping {
    file_path: String,
    row_ids: RowIdEncoding,
    // row_position 是隐式的: encoding 中第 i 个元素 → 文件内第 i 行
}

/// 全局映射: 所有 FileMapping 按 min_row_id 排序
struct RowIdMapping {
    files: Vec<FileMapping>,
}
```

#### 6.3.3 核心算法

**单点查找** — O(log M + log N)：

```rust
fn lookup(&self, row_id: u64) -> Option<RowAddress> {
    let idx = self.files
        .partition_point(|f| f.row_ids.max_row_id() < row_id);
    self.files.get(idx)
        .and_then(|f| f.row_ids.position_of(row_id))
        .map(|pos| RowAddress {
            file_path: self.files[idx].file_path.clone(),
            row_position: pos,
        })
}
```

**批量查找** — O(K log K + K + M)，RowID 排序后文件指针只前进：

```rust
fn lookup_batch(&self, row_ids: &[u64]) -> Vec<Option<RowAddress>> {
    let mut indexed: Vec<(usize, u64)> = row_ids.iter()
        .copied().enumerate().collect();
    indexed.sort_by_key(|(_, id)| *id);

    let mut results = vec![None; row_ids.len()];
    let mut fi = 0usize;
    for (orig_idx, row_id) in &indexed {
        while fi < self.files.len()
            && self.files[fi].row_ids.max_row_id() < *row_id
        {
            fi += 1;
        }
        if let Some(f) = self.files.get(fi) {
            results[*orig_idx] = f.row_ids.position_of(*row_id)
                .map(|pos| RowAddress {
                    file_path: f.file_path.clone(),
                    row_position: pos,
                });
        }
    }
    results
}
```

**构建** — 扫描全表，按文件分组填充：

```rust
fn build(stream: impl Iterator<Item = (String, u64)>) -> Self {
    let mut by_file: HashMap<String, Vec<u64>> = HashMap::new();
    for (path, row_id) in stream {
        by_file.entry(path).or_default().push(row_id);
    }
    let mut files: Vec<FileMapping> = by_file.into_iter()
        .map(|(file_path, ids)| FileMapping {
            file_path,
            row_ids: RowIdEncoding::from_iter(ids.into_iter()),
        })
        .collect();
    files.sort_by_key(|f| f.row_ids.min_row_id());
    Self { files }
}
```

**Compaction 后重建** — RowID 不变，只替换映射条目：

```rust
fn rebuild_after_compaction(
    &mut self,
    removed_files: &HashSet<String>,
    new_scan: impl Iterator<Item = (String, u64)>,
) {
    self.files.retain(|f| !removed_files.contains(&f.file_path));
    let new = Self::build(new_scan);
    self.files.extend(new.files);
    self.files.sort_by_key(|f| f.row_ids.min_row_id());
    // 索引条目 (key → RowID) 无需任何改动
}
```

#### 6.3.4 序列化格式（Puffin blob）

```rust
struct MappingBlob {
    entries: Vec<BlobEntry>,  // 按 min_row_id 排序
}
struct BlobEntry {
    file_path: String,        // UTF-8
    encoding: u8,             // 0 = Range, 1 = SortedArray
    payload: Vec<u8>,         // Range: [u64 LE; 2] (first, count)
                              // SortedArray: [u64 LE; N] (ids)
}
```

#### 6.3.5 性能预算

| 参数 | 典型值 | 说明 |
|------|--------|------|
| M（文件数） | 100-1000 | 每个 snapshot 的 DataFile 数量 |
| N（每文件行数） | 10^5-10^7 | INSERT 产生大文件, Compaction 后更均质 |
| K（批量查找） | 10-10^4 | 索引查询返回的候选行数 |
| `lookup()` 耗时 | < 1μs | 跨文件 binary search + 文件内 O(1)/O(log N) |
| `lookup_batch()` 耗时 | 数百 μs | K=1000 时排序 + 单次扫描 |
| 映射内存占用 | < 10MB | M=1000, N=10^6/文件, Range 编码为主 |

### 6.4 Deletion Vector 感知（I2）

#### 6.4.1 接口契约

构建索引时，`SnapshotIndexSource::read_partition()` 返回的 `RecordBatch` 流**必须已跳过被删除行**，`_pos` 列仅对存活行计数。

```rust
/// 索引构建时使用的行迭代器
/// 前置条件: SDK reader 已应用 delete file 过滤
/// 保证:    每行对应一个唯一的 (file_path, row_position), 无空洞
trait IndexRowIter {
    fn next_batch(&mut self) -> Option<RecordBatch>;
    // 每行附带 _file, _pos, _row_id, [indexed columns]
}
```

#### 6.4.2 构建时过滤

```rust
fn build_index_with_deletion_awareness(
    source: &dyn SnapshotIndexSource,
    snapshot_id: i64,
    field_ids: &[i32],
    mapping: &mut RowIdMapping,
) -> Result<()> {
    let plan = source.plan_snapshot(snapshot_id, field_ids)?;
    for partition in &plan.partitions {
        let batch_stream = source.read_partition(
            snapshot_id, field_ids, &partition.partition, &partition.data_files,
        )?;
        while let Some(batch) = batch_stream.next() {
            // SDK reader 已跳过 deleted rows
            for row_idx in 0..batch.num_rows() {
                let row_id   = batch.column("_row_id").get_u64(row_idx);
                let file     = batch.column("_file").get_str(row_idx);
                let position = batch.column("_pos").get_u64(row_idx);
                let index_key = extract_key(&batch, field_ids, row_idx);
                index.insert(index_key, row_id);
                mapping.track(file, position, row_id);
            }
        }
    }
    mapping.finalize();
    Ok(())
}
```

#### 6.4.3 查询时检查

```rust
/// 回表后验证行未被删除 (防御性检查)
fn check_not_deleted(reader: &IcebergTableReader, addr: &RowAddress) -> Result<bool> {
    let rows = reader.read_file_rows(&[addr.clone()])?;
    Ok(!rows.is_empty())
}
```

### 6.5 回表扫描（I3）

I3 是 RowID→映射→回表链路的串联层。PoC 已有 `IcebergTableReader` 可按 `RowAddress` 直接读取，I3 只需在查询流程中插入一次映射查询：

```
index.search(query) → RowID[]
  → mapping.lookup_batch(RowID[]) → RowAddress[]
    → reader.read_file_rows_addressed(RowAddress[]) → RecordBatch
```

无额外数据结构设计，核心工作量为串联逻辑和批量优化（~400-600 行）。

---

### 6.6 Compaction Remap（I6）

#### 6.6.1 算法流程

```
输入: old_snapshot 的映射表, new_snapshot (Compaction 产生的)
输出: 更新后的映射表 (RowID 不变, 只替换物理地址)

Step 1 — 检测 Compaction
  old_files = old_snapshot.manifest_entries().data_files()
  new_files = new_snapshot.manifest_entries().data_files()
  is_compaction = (∃ old_files - new_files) AND (∃ new_files - old_files)

Step 2 — 方案选择
  if M4 (_row_id 物理列写入) 已完成:
    方案 B: 直接读新文件的 _row_id 物理列, 取 (row_id, new_position)
  else:
    方案 A: 全量重扫新 snapshot, 重建全部映射

Step 3 — 执行 (方案 B)
  for new_file in new_files:
    for row in scan_file_with_row_lineage(new_file):
      mapping.update(row.row_id, RowAddress {
        file_path: new_file.path,
        row_position: row.position,
      })
  mapping.remove_files(old_files - new_files)
```

#### 6.6.2 方案对比

| | 方案 A (保底) | 方案 B (推荐) |
|------|-------------|-------------|
| **前提** | 无 | M4 (`_row_id` 物理列写入) 完成 |
| **扫描量** | 全表 | 仅新文件 |
| **RowID 来源** | 动态计算 | 读物理列（精确） |
| **时间复杂度** | O(总行数) | O(compacted_rows) |
| **适用** | 开发早期 / M4 未就绪 | 生产环境 |

---

## 七、分阶段开发方案

### 7.1 分工建议

> SDK 和索引的分界清晰（SDK 提供 RowID 读写能力 → 索引消费），建议按模块分工减少上下文切换，但不强制。关键同步点已标注，具体分配可按实际情况灵活调整。

|            | SDK 侧                             | 索引侧                             |
| ---------- | --------------------------------- | ------------------------------- |
| **职责**     | Fork `iceberg-rust`，实现 RowID 读写能力 | 在 `iceberg-index` 中实现映射、删除感知、回表 |
| **阶段一代码量** | ~1,260-1,800 行                    | ~1,410-2,040 行                  |
| **阶段二代码量** | ~800-1,200 行                      | ~1,450-2,130 行                  |

### 7.2 关键依赖与同步点

```
SDK 侧                                  索引侧
─────────                               ─────────
S0: DataFile first_row_id 填充 ─┐
                                │
S1: _pos 列读取 ────────────────┼──→ I2: Deletion Vector（需 _pos）
                                │
S2: _row_id 列读取（需 S0） ────┼──→ I1: RowID→Addr 映射（需 _row_id）
                                │         │
                                │         └──→ I3: 回表扫描（需 I1）
                                │
──── 阶段一完成 ────              │
                                │
M4: _row_id 物理列写入 ─────────┼──→ I6: Compaction Remap（需 M4）
                                │
M5: 增量扫描 API ───────────────┼──→ I4: INSERT 增量更新（需 M5）
                                │
                                └──→ I5: DELETE 清理（需 I1+I2）
```

### 7.3 并行策略

| 时间段       | SDK 侧                                 | 索引侧                             | 并行度      |
| --------- | ------------------------------------- | ------------------------------- | -------- |
| 第 1-2 周   | S0 + S1（DataFile first_row_id + _pos） | I1 接口设计 + I2 框架（无 _pos 过渡版）     | 全并行      |
| 第 3-4 周   | S2（_row_id 列读取）                       | I1 实现（暂用 mock _row_id）+ I3 基础链路 | 全并行      |
| 第 5-6 周   | —（S2 已完成，进入联调）                        | I1 对接 S2 + I3 完整链路              | SDK→索引同步 |
| 第 7-8 周   | M4（物理列写入）                             | I2 完整实现（对接 S1）+ I3 完善           | 全并行      |
| **阶段一交付** |                                       |                                 |          |
| 第 9-10 周  | M5（增量扫描）                              | I5 + I6 并行开发                    | 全并行      |
| 第 11-12 周 | 配合 I6 联调                              | I6 完成 + I4 开发                   | 联调       |
| 第 13-14 周 | 配合 I7 联调                              | I7 完成                           | 联调       |

## 八、Fork SDK 修改清单

| 模块（基于 0.10.0 结构）       | 修改内容                                                                           | 阶段  | 对应  |
| ---------------------- | ------------------------------------------------------------------------------ | --- | --- |
| `io/parquet/writer.rs` | Write 路径：DataFile 创建时从 `next_row_id` 分配 `first_row_id`（元数据层已就绪，仅需补 write 路径赋值） | 阶段一 | S0  |
| `scan/arrow.rs`        | 添加 `_pos` 元数据列投影（`RowNumber` 虚拟列）                                              | 阶段一 | S1  |
| `scan/arrow.rs`        | 添加 `_row_id` 元数据列投影 + `RowIdReader` 双重读取                                       | 阶段一 | S2  |
| `io/parquet/writer.rs` | 支持 `_row_id` 物理列写入（`schemaWithRowLineage` 等效）                                  | 阶段二 | M4  |
| `scan/`                | 添加增量扫描 API（`appends_after()`）                                                  | 阶段二 | M5  |

> `metadata_columns.rs` 已定义全部字段常量，无需修改。`spec/table_metadata.rs` 和 `spec/snapshot.rs` 的 RowID 元数据层已在 0.10.0 中完备。

## 九、索引新增/扩展模块

对应现有 `iceberg-index` 的 7 个 crate 架构：

| Crate                   | 新增/扩展                                                                | 阶段    | 对应     |
| ----------------------- | -------------------------------------------------------------------- | ----- | ------ |
| `iceberg-index-core`    | **新增** `rowid_mapping.rs`（RowID→Addr 映射 trait + Puffin/Parquet 存储实现） | 阶段一   | I1     |
| `iceberg-index-core`    | **新增** `deletion_vector.rs`（删除向量读取 + 过滤接口）                           | 阶段一   | I2     |
| `iceberg-index-iceberg` | **扩展** `reader.rs`（RowID→映射→回表完整链路）                                  | 阶段一   | I3     |
| `iceberg-index-iceberg` | **扩展** `source.rs`（对接 SDK 增量扫描 + `_pos`/`_row_id` 投影）                | 阶段一+二 | M5, I4 |
| `iceberg-index-runtime` | **新增** `maintenance/`（Compaction Remap、DELETE 清理等）                   | 阶段二   | I5, I6 |

## 十、风险与缓解

| 风险                           | 影响                        | 缓解                                                         |
| ---------------------------- | ------------------------- | ---------------------------------------------------------- |
| **S1 `_pos` 实现困难**           | I2（Deletion Vector）无法安全处理 | 手动计数器过渡（仅限无删除文件快照）；Parquet `RowNumber` 方案成熟（Java/PR #2746） |
| **S2+M4 未完成时 Compaction 发生** | I6 Remap 只能用方案 A（全量重扫）    | 方案 A 不依赖 SDK，作为保底；方案 B 更优                                  |
| **增量扫描未实现**                  | I4 只能用全量扫描替代              | I4 优先级 P2，可暂时降级                                            |
| **Fork 后上游更新**               | 合并冲突                      | 最小 diff、定期 rebase                                          |
| **SDK 与索引侧接口不一致**            | 联调困难                      | 提前定义 trait 契约（`_row_id` 值格式、映射查询接口），mock 并行开发              |

## 十一、总结

| 维度         | 结论                                                                                                                                                 |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **当前架构**   | RowAddr 直连，快照绑定，无稳定行标识                                                                                                                             |
| **目标架构**   | RowID 增强，RowID→RowAddr 映射解耦物理位置，Compaction/Deletion 下索引不失效                                                                                         |
| **阶段一可行性** | ✅ 可行。SDK 仅需 DataFile first_row_id 填充 + `_pos` + `_row_id` 读取（~1,260-1,800 行；元数据层 0.10.0 已完备）；索引需实现 RowID 映射 + Deletion Vector + 回表（~1,410-2,040 行） |
| **阶段二可行性** | ⚠️ 依赖阶段一完成。SDK 需实现物理列写入 + 增量扫描（~800-1,200 行）；索引需实现 Compaction Remap + DML 维护（~1,450-2,130 行）                                                       |
| **总工作量**   | **~4,920-7,170 行**                                                                                                                                 |
| **预估时间线**  | 阶段一 8 周 + 阶段二 6 周 = **约 14 周**（2 人全职）                                                                                                              |
| **核心风险**   | `_pos` 实现是 Deletion Vector 的前提；物理列写入是 Compaction Remap 最优方案的前提                                                                                     |

### 开发原则

1. **索引只关注 RowID 必需**：`_last_updated_sequence_number` 等行级血缘字段不在索引范围内
2. **SDK 参考 Java，索引参考 Lance**：SDK 读取路径参考 Java `RowIdReader`/`PositionReader`；索引映射存储参考 Lance `RowIdIndex`
3. **阶段一优先打通核心链路**：S0→S2→I1→I3 是 RowID 从元数据到回表的完整闭环，优先确保可用
4. **阶段二解决稳定性场景**：Compaction 后索引不失效、DELETE 后索引精确清理是 RowID 的核心价值

## 附录：参考链接

| 参考                  | 链接                                                                       | 说明                     | 状态                   |
| ------------------- | ------------------------------------------------------------------------ | ---------------------- | -------------------- |
| Java SDK RowID 深度报告 | `docs/rowid/Java Iceberg SDK RowID 能力深度报告.md`                            | 完整 RowID 实现参考          | ✅ 已验证                |
| Rust SDK #2746      | https://github.com/apache/iceberg-rust/pull/2746                         | `_pos` 列读取             | Draft，0.10.0 **未实现** |
| Rust SDK #2153      | https://github.com/apache/iceberg-rust/pull/2153                         | 增量扫描 `appends_after()` | Draft，0.10.0 **未实现** |
| Lance RowIdIndex | https://github.com/lancedb/lance/blob/main/rust/lance-table/src/rowids/index.rs | RowID→Addr 映射 | 参考设计 |
| Lance Stable Row ID | https://docs.rs/lance/latest/lance/struct.LanceTable.html#stable-row-ids | 稳定 RowID 概念            | 参考设计                 |
