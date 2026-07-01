# RowID 索引 SDK — 设计文档 v2

> **状态**: 全链路完成（S0~M5 + I1~I7 + P1~P7），408 测试全通过，0 警告。

## 一、三层架构

```
生产引擎（内核）                  ← DML / Compaction 触发时机
  │
索引插件层 (iceberg-index-plugins) ← 具体算法：BTree / IVF / IVF-PQ
  │ 实现 IndexPlugin trait
索引框架层 (core + iceberg + runtime + table) ← RowIdMapping / 维护协调 / 回表 / 持久化
  │ 依赖 SDK 元语
Fork SDK 层 (iceberg-rust)        ← RowID 读写元语：分配 / 读取 / 写入 / 增量扫描
```

**职责边界**:
- **Fork SDK**：提供 RowID 元语（分配/读取/写入/增量扫描），不感知索引
- **索引框架层**：定义 `IndexPlugin` trait、管理 RowIdMapping、编排维护生命周期、提供回表能力。框架不关心插件内部存储格式
- **索引插件层**：实现具体算法，**必须存储 SDK RowID**（不是自建内部地址），查询时通过 RowIdMapping 解析物理地址
- **生产引擎**：调用索引框架的 `create_index` / `maintain_index` API，决定触发时机

---

## 二、Fork SDK 层 — RowID 元语

> 代码位置: `iceberg-rust/crates/iceberg/src/`

### 2.1 S0: RowID 分配（manifest 层）

`TableMetadata.next_row_id()` → `SnapshotProducer` 播种游标 → `ManifestWriter` 逐 DataFile 分配 → `ManifestListWriter` 三路验证。

| 文件 | 改动 |
|------|------|
| `spec/manifest/writer.rs` | `ManifestWriter::assign_data_file_first_row_id()` per-file 游标 |
| `spec/manifest_list/writer.rs` | 三路比较（传家宝跳过 / 等值推进 / 大于报错） |
| `spec/manifest_list/manifest_file.rs` | `load_manifest()` per-DataFile 继承游标 |
| `transaction/snapshot.rs` | `SnapshotProducer` 播种 `next_data_manifest_first_row_id` |

**与 Java 的差异**: 保留预设 `first_row_id`（Java 强制覆盖），Compaction 时旧 RowID 不丢失。

### 2.2 S1+S2: `_pos` / `_row_id` 读取

Pipeline 顺序: Parquet 读取 → delete filter → transformer（排除 `_pos`/`_row_id`）→ post-processing 追加。

- `_pos` = `row_position` (UInt64, 0-based, 每文件重置)
- `_row_id` = `firstRowId + row_position` (Int64, 动态计算)

| 文件 | 改动 |
|------|------|
| `arrow/reader/pipeline.rs` | `_pos`/`_row_id` post-processing + M4 dual-read |
| `scan/task.rs` | `FileScanTask.first_row_id` 字段 |
| `scan/context.rs` | 从 `ManifestEntry.data_file()` 填充 `first_row_id` |

### 2.3 M4: `_row_id` 物理列读写

- **写入**: `ParquetWriter` 自动检测 batch 中的 `_row_id` 列 → 扩展 Arrow schema → 写为物理列
- **读取 (dual-read)**: 优先读物理列，无则退化为动态计算

| 文件 | 改动 |
|------|------|
| `writer/file_writer/parquet_writer.rs` | auto-detect + 物理列写入 |
| `arrow/reader/pipeline.rs` | dual-read 分支 |

### 2.4 M5: 增量扫描

`TableScanBuilder::appends_after(from)` — 仅返回 `ManifestEntry::snapshot_id > from` 的 Added 条目。

**已知局限**: 暂缺 `toSnapshot` 参数，无法精确限定扫描上界。

---

## 三、索引框架层 — 映射、维护、回表

> 代码位置: `iceberg-index/crates/iceberg-index-{core,iceberg,runtime,table}/`

### 3.1 I1: RowID → RowAddr 映射

索引存 `(key → RowID)`（RowID 稳定），映射存 `(RowID → file_path, position)`（Compaction 后更新）。映射是**表级**的，多个索引共享同一个 RowIdMapping。

**编码**:

INSERT 后 RowID 天然连续（firstRowId, firstRowId+1, ...），用 Range 编码压缩:

```
Range { first: u64, count: u64 }          → 16 字节，O(1) 查: position = row_id - first
RangeWithBitmap { first, count, bitmap }  → 16B + N/8B，稀疏 DELETE（≤50%）时用
SortedArray { ids: Vec<u64> }            → N×8 字节，O(log N) 二分查找
```

`from_iter()` 自动选择: 连续 → Range，非连续 → SortedArray。
DELETE ≤50% → Range 保留为 RangeWithBitmap；DELETE >50% → 转 SortedArray。

**类型**: RowIdMapping 内部用 `u64`（`TableMetadata.next_row_id()` 和 `record_count` 都是 u64，
Range 的 `first + count` 可能超过 i64::MAX）。与 SDK `_row_id: Int64` 的转换在边界处完成
（`build_rowid_mapping()` 和 `lookup()` 调用点），零开销。

| 文件 | 改动 |
|------|------|
| `core/src/rowid_mapping.rs` | `RowIdEncoding`, `FileMapping`, `RowIdMapping`, `to_blob()`/`from_blob()` |

**持久化设计**: 作为 Registry Puffin 的第二个 blob（类型 `huawei.gauss-infra.rowid-mapping-v1`），与 index-registry blob 共存于同一个 StatisticsFile 中，保证原子更新。

> **当前状态**: ✅ 已接入——`write_registry_statistics_file()` 写入 mapping blob，`read_rowid_mapping_from_puffin_bytes()` 读取，`build_rowid_mapping()` 构建。Coordinator 加载 registry 时自动加载 mapping。

### 3.2 I2: Deletion Vector 感知

SDK pipeline 在 `_pos` 计算前已应用 delete file → `_pos` 只计存活行 → 移除手动计数器。

| 文件 | 改动 |
|------|------|
| `iceberg/src/source.rs` | 移除手动 `HashMap` 计数器，改用 SDK `_pos` 原生列 |

### 3.3 I3: 回表扫描

`RowID → mapping.lookup_batch → RowAddr[] → reader.read_file_rows_addressed → take 重排序`。

| 文件 | 改动 |
|------|------|
| `iceberg/src/reader.rs` | `read_rows_by_row_id()` + ordered 变体 |

> **当前状态**: 已接入 — BTree/IVF/IVF-PQ 返回 RowID 后由 `IndexSearchCoordinator::resolve_scored_rows()` 统一解析。

### 3.4 I4+I5+I6+I7: DML 维护

`classify_change(old, new)` 比较快照 → `MaintenanceAction` 枚举 → `maintain_index()` 分发:

| Action | 策略 | RowIdMapping 操作 |
|--------|------|-------------------|
| `AppendOnly` | I4 增量构建 | `mapping.extend(new_entries)` |
| `DeleteOnly` | I5 清理 | `mapping.remove_row_ids(deleted)` |
| `Compaction` | I6 重映射 | `mapping.rebuild_after_compaction()`，索引条目不动 |
| `FullRebuild` | I7 全量重建 | `mapping.build(all_files)` |
| `NoOp` | 返回现有 entry | 不变 |

| 文件 | 改动 |
|------|------|
| `iceberg/src/source.rs` | `classify_change()`, `plan_incremental()`, `detect_compaction()` |
| `runtime/src/build.rs` | `MaintenanceAction`, `maintain_index()` |
| `core/src/rowid_mapping.rs` | `remove()`, `remove_row_ids()`, `rebuild_after_compaction()` |
| `core/src/plugin.rs` | `IndexPlugin::prune()` with `PluginContext` + `IndexSegmentMetadata` |

### 3.5 IndexPlugin trait

框架层定义的插件接口，插件层实现：

```rust
pub trait IndexPlugin {
    // 构建与查询
    async fn build(&self, input: IndexBuildInput) -> Result<CreatedIndex>;
    async fn scalar_search(...) -> Result<Vec<IndexCandidate>>;
    async fn vector_search(...) -> Result<Vec<IndexCandidate>>;

    // 维护
    async fn prune(&self, ctx: &PluginContext, seg: &IndexSegmentMetadata, row_ids: &BTreeSet<u64>) -> Result<()> { Ok(()) }
    async fn update(&self) -> Result<()> { Err(Unsupported) }
}
```

---

## 四、索引插件层 — 算法实现的 RowID 适配

> 代码位置: `iceberg-index/crates/iceberg-index-plugins/src/{btree,ivf,ivf_pq}/`
> **状态: ✅ 已完成 (P2-P7)**

插件层遵守的 RowID 契约：
1. **构建时**读取 SDK pipeline 输出的 `_row_id` 列，存入索引
2. **SDK 固定列硬编码**: `_file` / `_row_id` 是 SDK 原生列，不通过 JSON 参数配置
3. **查询时**返回 RowID 占位符，由 `IndexSearchCoordinator::resolve_scored_rows()` 统一解析
4. RowID 不变性保证 Compaction 后索引条目无需修改

### 4.1 BTree（已完成 ✅）

| 项目 | 说明 |
|------|------|
| 存储内容 | `(key, SDK RowID: i64)` — 直接透传 |
| 构建输入 | SDK `_row_id` 列 (Int64)，硬编码 `crate::ivf::SDK_ROW_ID_COLUMN` |
| 查询解析 | 返回 `ScoredRowId { row_id, score }`（向量）或 `u64` row_id（标量），coordinator 调用 `resolve_scored_rows()`/`resolve_row_ids()` → `RowIdMapping::lookup()` |
| 数据页格式 | Int64 SDK RowID（原 UInt64 packed format） |
| 维护 | `prune()` 实现 page 级 RowID 过滤 + lookup 重建 |

**类型选择**: BTree 页存储 `Int64`，直接透传 SDK 输出避免构建时 N 行转换。
RowIdMapping 用 `u64` — `first + count` 可能溢出 i64（极端表场景），边界转换零开销。

### 4.2 IVF（已完成 ✅）

| 项目 | 说明 |
|------|------|
| 存储内容 | `(vector, RowID: u64)` — 分区数据段直接存 row_ids |
| 二进制格式 | partition：num_vectors + vectors + row_ids（无字符串表/file_indices） |
| 构建输入 | SDK `_row_id` 列 (Int64)，硬编码 `SDK_ROW_ID_COLUMN` |
| 查询解析 | 返回 RowID 占位符，coordinator 解析 |
| 维护 | `prune()` 实现分区级 RowID 过滤 |

**ARTIFACT_FORMAT_VERSION**: 2

### 4.3 IVF-PQ（已完成 ✅）

| 项目 | 说明 |
|------|------|
| 存储内容 | `(pq_codes, RowID: u64)` — 分区数据段 PQ codes + row_ids |
| 二进制格式 | partition：num_vectors + PQ codes + row_ids |
| 构建输入 | SDK `_row_id` 列 (Int64)，硬编码 `SDK_ROW_ID_COLUMN` |
| 查询解析 | 返回 RowID 占位符，coordinator 解析 |
| 维护 | `prune()` 实现分区级 RowID 过滤 |

### 4.4 插件层的 RowID 契约

```rust
// 1. 构建时：索引条目存 SDK RowID
let row_id = row_id_arr.value(row);  // Int64, 来自 SDK pipeline

// 2. 查询时：插件返回 ScoredRowId（不构造 RowAddress）
ScoredRowId { row_id: row_id as u64, score }

// 3. coordinator 通过 resolve_scored_rows() → mapping.lookup() 解析为物理地址
mapping.lookup(row_id as u64) → Some(RowAddress { file_path: "...", row_position: N })
```

---

## 五、依赖关系

```
插件层                        框架层                          SDK 层
──────────────────────────────────────────────────────────────────────
BTree (key → RowID) ────▶ RowIdMapping.lookup() ──────▶ S2 _row_id 读取
IVF  (vec → RowID) ────▶ RowIdMapping.lookup() ──────▶ S2 _row_id 读取
                            │
prune(RowID[]) ◀─────────── RowIdMapping ◀──────────── S1 _pos (间接)
                            │
映射更新 ◀──────────────── RowID 不变 ◀───────────── M4 dual-read
                            │
incremental build ◀──────── plan_incremental() ◀─────── M5 appends_after
```

---

## 六、设计决策

### SDK 层

| # | 决策 | 说明 |
|---|------|------|
| D1 | first_row_id 在 **manifest 层**分配 | 覆盖所有入表路径 |
| D2 | `_pos`/`_row_id` **排除 transformer** | field ID 不在表 schema 中 |
| D3 | **保留预设** first_row_id | 与 Java 不同，Compaction 保留旧 RowID |
| D4 | M4 **auto-detect** 物理 `_row_id` | 优于 Java 显式 `schemaWithRowLineage` |

### 框架层

| # | 决策 | 说明 |
|---|------|------|
| D5 | RowID 编码 **自动选择** | 连续→Range，非连续→SortedArray |
| D6 | **映射嵌入 Registry Puffin** | Blob 1，与 registry 原子更新，跟随 snapshot 生命周期 |
| D7 | 维护接口 **默认 no-op** | 查询正确性由 mapping 保证 |
| D8 | 映射是**表级**的 | 多索引共享同一个 RowIdMapping |

### 插件层

| # | 决策 | 说明 |
|---|------|------|
| D9 | 插件**必须存 SDK RowID** | 不自建 file_index_map 或 file_path 缓存 |
| D10 | 查询通过 `RowIdMapping.lookup()` | 统一由框架解析物理地址 |
| D11 | Compaction 后索引条目不动 | RowID 不变，只更新 RowIdMapping |

---

## 七、风险 & 优化

### SDK 层

| 项 | 类型 | 描述 |
|----|------|------|
| `appends_after()` 无 `toSnapshot` | 风险 | 无法精确限定扫描上界 |
| M4 auto-detect 依赖 batch schema | 风险 | dual-read 退化为动态计算兜底 |
| RowID 文件级位置计数 | 优化 | Java row-group 级更精确 |

### 框架层

| 项 | 类型 | 描述 |
|----|------|------|
| RowIdMapping 持久化 | ✅ | P1: build→write, load→read, coordinator 持有 |
| `prune()` trait | ✅ | P5-P7: BTree/IVF/IVF-PQ 实现 row-level 清理 |

### 插件层

| 项 | 类型 | 描述 |
|----|------|------|
| BTree/IVF/IVF-PQ 存储 SDK RowID | ✅ P2-P4 | 所有插件直接存储 SDK RowID |
| BTree page 格式 Int64 | ✅ | UInt64 packed → Int64 SDK RowID |
| IVF 二进制格式 v2 | ✅ | 字符串表移除，ARTIFACT_FORMAT_VERSION=2 |
| prune() 实现 | ✅ P5-P7 | page/partition 级过滤 + 重写 |
| SDK 固定列硬编码 | ✅ | `_file` / `_row_id` 不再通过 JSON 参数配置 |

---

## 八、实施计划

| 步骤 | 内容 | 层 | 状态 |
|------|------|----|------|
| S0~M5 | RowID 元语 | SDK | ✅ |
| I1~I7 | 框架能力 | 框架 | ✅ 代码完成 |
| P1 | RowIdMapping 持久化接入 | 框架 | ✅ |
| P2 | BTree 适配 RowID | 插件 | ✅ |
| P3 | IVF 适配 RowID | 插件 | ✅ |
| P4 | IVF-PQ 适配 RowID | 插件 | ✅ |
| P5 | `prune()` BTree 实现 | 插件 | ✅ |
| P6 | `prune()` IVF 实现 | 插件 | ✅ |
| P7 | `prune()` IVF-PQ 实现 | 插件 | ✅ |

步骤 P2/P3 可并行，P5 依赖 P2/P3。详见 [`plugin-maintenance-design.md`](plugin-maintenance-design.md)。

---

## 九、测试

| 层 | 套件 | 数量 | 说明 |
|----|------|------|------|
| SDK | 单元 | 1389 | 含 S0~M5 专项测试 |
| SDK | doctest | 85 | — |
| 框架 | core | 43 | 含 27 个 RowIdMapping 测试 |
| 框架 | iceberg | 34 | reader + source + committer |
| 框架 | runtime | 13 | 构建 + 统计 |
| 框架 | table | 5 | registry |
| 插件 | plugins | 84 | BTree + IVF + IVF-PQ（v2 格式适配后更新） |
| 集成 | SQL | 31 | iceberg-db |

---

## 十、Java SDK 参考

| Rust 实现 | 层 | 参考的 Java 类 |
|----------|----|---------------|
| `ManifestWriter::assign_data_file_first_row_id()` | SDK | `ManifestWriter` |
| `ManifestListWriter` 三路比较 | SDK | `ManifestListWriter` |
| pipeline `_pos` / `_row_id` + M4 dual-read | SDK | `PositionReader` / `RowIdReader` |
| `appends_after()` | SDK | `IncrementalDataTableScan.appendsAfter()` |
| `RowIdMapping` | 框架 | `RowIdIndex` (Lance) |
| BTree RowID 适配 | 插件 | — |

源码位于 `iceberg-java/`。

---

## 十一、相关文档

| 文档 | 说明 |
|------|------|
| `design.md` | 索引框架原始设计（英文） |
| `design.zh-CN.md` | 索引框架原始设计（中文） |
| `design-vs-implementation.md` | 设计 vs 实现偏差分析 |
| `limitations-and-future.md` | 已知限制与未来路线 |
| `plugin-maintenance-design.md` | 插件 RowID 适配 + RowIdMapping 持久化详细设计 |
| `../iceberg-index-rowid-设计文档.md` | 行级设计文档（权威） |
