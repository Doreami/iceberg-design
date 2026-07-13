# Apache Iceberg Puffin 自定义索引 — 设计文档 v5.2

**版本**: 5.2 | **日期**: 2026-06-13

**核心设计决策**:

- 纯 Rust 实现，基于 `iceberg-rust` SDK。

- **一个逻辑索引对应多个 segment 文件**（每个 segment 独立 Puffin 文件，内部一个 `custom.idx.btree-v1` Blob）。

- **注册表**：`index_metadata.puffin` 文件（位于 `metadata/` 下），内部包含多个 `custom.idx.index-meta-v1` Blob，每个 Blob 对应一个逻辑索引，其数据区 JSON 存储该索引的 `segments` 数组（含 `segment_file_path`, `coverage_files`, `stale_files`, `min_path_hash`, `max_path_hash`, `total_rows`, `source_snapshot_id` 等）。

- 文件覆盖信息（`coverage_files`, `stale_files`）**存储在注册表 JSON 中**（不设独立的 filelist Blob），已知风险：JSON 可能膨胀，后续可优化。

- 移除 Rewrite Map，部分失效通过 `stale_files` 跳过；移除阈值自动重建，仅用户命令触发重建。

- 索引更新（全量/增量/重建）均采用 **Append-only + COW**：新增 segment 文件，新写 `index_metadata.puffin`，原子切换 `statistics-files` 指针。

- 文件名格式：`{index_name}_seg{segment_id}_snap{snapshot_id}.puffin`（无需 `segment_version`，利用 `snapshot_id` 区分）。



## 目录

1. 整体架构

2. 表元数据中的 statistics-files 条目

3. 注册表文件 index_metadata.puffin

4. 独立 Segment 索引文件结构

5. Puffin 文件级 properties（可选）

6. 查询流程

7. 构建与更新流程

8. 垃圾回收（GC）

9. 与 LanceDB 的对比

10. 风险与后续优化

11. 数据结构速查



## 1. 整体架构

- Iceberg 表元数据 (`metadata.json`) 的 `statistics-files` 数组包含一条记录，指向 **索引注册表文件** `index_metadata.puffin`（位于 `metadata/` 目录下）。

- `index_metadata.puffin` 内部有多个 Blob，每个 Blob 类型为 `custom.idx.index-meta-v1`，存储一个逻辑索引的元数据，包括该索引的所有 segment 信息。

- 每个 **segment** 对应一个独立的 Puffin 文件（例如 `btree_order_id_seg0_snap1001.puffin`），文件内只包含**一个** `custom.idx.btree-v1` Blob，存储实际的 B-Tree 节点数据。

- 一个逻辑索引（如 `btree_order_id`）由多个 segment 文件组成，所有 segment 文件路径记录在注册表中。

- 所有 segment 文件统一放置在表根目录下的 `indices/` 文件夹中（可再按分区组织子目录）。



---

## 2. 表元数据中的 statistics-files 条目

`statistics-files` 中的 `blob-metadata` 只包含摘要信息，**不包含** `offset`、`length`、`compression-codec` 等物理字段（这些字段仅出现在 Puffin 文件 Footer 中）。

```json
"statistics-files": [
  {
    "snapshot-id": 1001,
    "statistics-path": "s3://.../metadata/index_metadata_v12.puffin",
    "file-size-in-bytes": 655360,
    "file-footer-size-in-bytes": 512,
    "blob-metadata": [
      { // 一个blob_metadata代表一个索引
        "type": "custom.idx.index-meta-v1", // 索引元数据类型, 描述索引元数据
        "fields": [1], // 索引字段id数组
        "snapshot-id": 1001, // 全量构建/增量构建索引时的snapshot-id
        "sequence-number": 1, // 全量构建/增量构建索引时的source-sequence-number
        "properties": {
          "index-name": "btree_order_id", // 索引名称
          "index-type": "custom.idx.btree-v1", // 索引类型
          "index-version": "1", // 索引版本
          "index-uuid": "a1b2c3d4-...", // 索引id
          "dataset_version": "1",  // 基于哪个版本数据创建索引是不是等同于source-snapshot-id
          "created_at": "2026-01-01 12:00:00",   // 创建时间
          "detail": "",  // 索引详情，json字符串表明索引参数，压缩方式等.
        }
      }，
      {
        // ... 其他索引信息
      }
    ]
  }
]
```



- `fields`：该索引关联的 Iceberg 字段 ID。

- `properties`：索引名称、类型、版本、UUID。

- 顶层 `snapshot-id`：当前快照的 ID。

- `blob-metadata[].snapshot-id`：该 Blob 所属的源快照 ID（即构建该索引时所基于的数据快照）。



---

## 3. 注册表文件 index_metadata.puffin

`index_metadata_v12.puffin` 是一个标准 Puffin 文件，其 Footer 中的 `blobs` 数组包含物理定位信息。此处只展示其核心的 `custom.idx.index-meta-v1` Blob 的 **数据区** 内容（JSON 格式，Zstd 压缩）。

每个 `custom.idx.index-meta-v1` Blob 对应一个逻辑索引，其数据区 JSON 结构如下：

```json
{
  "segments": [
    {
      "segment_id": "seg0",
      "segment_file_path": "s3://.../indices/btree_order_id_seg0_snap1001.puffin",
      "coverage_files": [
        "s3://.../part-00001.parquet",
        "s3://.../part-00002.parquet"
      ],
      "stale_files": [
        "s3://.../part-00001.parquet"
      ],
      "min_path_hash": "a3f2...",
      "max_path_hash": "c7e1...",
      "total_rows": 1024000,
      "source_snapshot_id": 1001
    },
    {
      "segment_id": "seg1",
      "segment_file_path": "s3://.../indices/btree_order_id_seg1_snap1003.puffin",
      "coverage_files": [
        "s3://.../part-00501.parquet",
        "s3://.../part-00502.parquet"
      ],
      "stale_files": [],
      "min_path_hash": "d4e5...",
      "max_path_hash": "f6a7...",
      "total_rows": 512000,
      "source_snapshot_id": 1003
    }
  ]
}
```



**字段说明**：

- `segment_id`：该 segment 的唯一标识（在同一索引内唯一）。

- `segment_file_path`：该 segment 对应的独立 Puffin 文件路径（绝对路径）。

- `coverage_files`：该 segment 覆盖的所有 Parquet 文件完整路径（字符串数组）。

- `stale_files`：该 segment 中已失效的文件路径子集（部分失效）。

- `min_path_hash` / `max_path_hash`：路径哈希的十六进制字符串，用于快速过滤（可选，但推荐）。

- `total_rows`：该 segment 索引的总行数。

- `source_snapshot_id`：构建该 segment 时所基于的数据快照 ID（全量时为该快照；增量时为上一次构建完成后的快照）。

> **注意**：索引名称、类型、字段 ID 等已在外层 `blob-metadata` 的 `properties` 中定义，此处不再重复。



---

## 4. 独立 Segment 索引文件结构

每个 segment 对应一个独立的 Puffin 文件（例如 `btree_order_id_seg0_snap1001.puffin`），内部只包含**一个** `custom.idx.btree-v1` Blob。该文件布局如下：

- **Magic** (4 字节) ：`PFA1`

- **Blob 数据区**：单个 Blob 的压缩/未压缩数据（B-Tree 节点序列化字节流）

- **Footer** (JSON，LZ4 压缩) ：包含该 Blob 的元信息

Footer JSON 示例：

```json
{
  "blobs": [
    {
      "type": "custom.idx.btree-v1",
      "fields": [1],
      "snapshot-id": 1001,
      "source-snapshot-id": 1001,
      "offset": 4,
      "length": 16384016,
      "compression-codec": "zstd",
      "properties": { // blob级properties
        "segment_id": "seg0",
        "segment-version": "1",
        "file_count": "500",
        "min_path_hash": "a3f2...",
        "max_path_hash": "c7e1...",
        "total_rows": "1024000"
      }
    }
  ]
}
```



**注意**：

- `offset`, `length`, `compression-codec` 等仅出现在 Footer 的 `blobs` 数组中，不在文件头部独立列出。

- 该文件**不包含**文件覆盖列表（`coverage_files`, `stale_files`），这些信息已存储在注册表文件中。



---

## 5. Puffin 文件级 properties（可选）

Puffin Footer 中的顶层 `properties` 可用于存放整个文件的元数据，但非必需（关键信息已在上层 `blob-metadata.properties` 或 Blob 数据区中）。以下示例仅供参考：

**对于 segment 文件**：

```json
"properties": {
  "index-name": "btree_order_id",
  "index-uuid": "a1b2c3d4-...",
  "created-by": "puffin-rs/5.2"
}
```



**对于注册表文件 `index_metadata.puffin`**：

```json
"properties": {
  "puffin-type": "index-registry",
  "registry-version": "12",
  "num-indices": "2",
  "created-by": "puffin-rs/5.2"
}
```



实现时可以忽略顶层 `properties`，不会影响功能。



---



## 6. 查询流程

1. 从 `TableMetadata.statistics-files` 找到 `index_metadata.puffin` 路径。

2. 读取 `index_metadata.puffin` 的 Footer，获取所有 `custom.idx.index-meta-v1` Blob 的 offset/length。

3. 遍历这些 Blob，读取并解压数据区，解析 JSON。根据 `fields`（在外层 `blob-metadata` 中）匹配目标列，找到对应的索引元数据。

4. 对于该索引的每个 segment：
   
   - 利用 `min_path_hash` / `max_path_hash` 快速判断候选文件是否可能属于此 segment。
   
   - 精确检查候选文件路径是否在 `coverage_files` 中且不在 `stale_files` 中。
   
   - 如果匹配，则根据 `segment_file_path` 打开对应的 segment Puffin 文件。
   
   - 读取该文件的 Footer，定位到唯一的 `custom.idx.btree-v1` Blob，读取并解压 B-Tree 数据。
   
   - 执行 B-Tree 搜索，返回 RowAddress 列表。

5. 合并所有 segment 的结果，去重后回表（利用 ParquetPageLocator）。

---

## 7. 构建与更新流程

所有更新均采用 **Append-only + COW**：

- 新增 segment 时，写入新的 segment Puffin 文件。

- 注册表文件 `index_metadata.puffin` 每次变更时重新生成（COW），旧文件保留（供时间旅行）。

- 通过 `UpdateStatistics` 原子切换指针。

### 全量构建

- 构建 B-Tree 数据，写入 segment 文件（如 `btree_order_id_seg0_snap{new_snapshot_id}.puffin`）。

- 创建注册表 JSON，包含一个 segment 条目（`coverage_files` 为所有文件路径，`source_snapshot_id` = 当前快照 ID）。

- 将新的 `custom.idx.index-meta-v1` Blob 写入新的 `index_metadata_vN.puffin`（若还有其他索引，则复制其旧 Blob）。

- 提交 `UpdateStatistics` 指向新注册表文件。

### 增量构建

- 读取旧注册表，获取已覆盖的文件集合。

- 构建新 segment 的 B-Tree 数据，写入新的 segment 文件（例如 `btree_order_id_seg1_snap{new_snapshot_id}.puffin`）。

- 复制旧注册表 JSON，追加新 segment 条目（`coverage_files` 为新增文件路径，`source_snapshot_id` = 上次构建完成时的快照 ID）。

- 生成新注册表文件，原子切换。

### 标记失效（CoW 后）

- 将失效路径加入对应 segment 的 `stale_files` 数组。

- 生成新的注册表 JSON，并写入新注册表文件（segment 文件不变）。

- 原子切换。

### 重建（用户命令）

- 收集所有有效文件（所有 segment 的 `coverage_files` 减去 `stale_files` 的并集）。

- 构建单一 segment 的 B-Tree，写入新的 segment 文件（例如 `btree_order_id_seg0_snap{new_snapshot_id}.puffin`）。

- 在注册表中，用新 segment 替换旧的 segments 列表。

- 生成新注册表文件，原子切换。

---

## 8. 垃圾回收（GC）

<mark>该功能暂不需要实现</mark>

由于 segment 文件未被 Iceberg 元数据直接引用，需自行管理清理。推荐以下组合策略：

### 8.1 对象存储生命周期规则（推荐）

- 为 `indices/` 目录设置生命周期规则，例如文件创建后 **7 天** 自动删除。

- 配合快照过期策略（例如保留最近 7 天的快照），确保时间旅行所需的旧 segment 不会被过早删除。

- 优点：简单，无需额外代码。

### 8.2 数据库内置 GC 脚本（备选）

- 定期扫描所有未过期快照中的 `index_metadata.puffin`，提取所有 `segment_file_path`，构建活跃集合。

- 扫描 `indices/` 目录，删除不在活跃集合中的 `.puffin` 文件。

- 可提供用户命令 `ALTER TABLE ... CLEANUP INDEXES` 手动触发。

---

## 9. 与 LanceDB 的对比

本方案与 LanceDB 在设计上存在根本差异：LanceDB 是自包含的列式存储格式，索引是其原生能力；而本方案基于 Iceberg 的统计文件机制构建，目标是复用 Iceberg 的事务、快照和文件管理能力。

### 多维度对比

以下从多个维度进行详细对比。

| 维度               | **本方案 (Puffin + Iceberg)**                                                                                  | **LanceDB (Lance 格式)**                                | **差异原因与影响**                                                                                                                      |
| ---------------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **元数据存储位置**      | 独立注册表文件 `index_metadata.puffin`，由 `TableMetadata.statistics-files` 引用。                                      | 嵌入在表的 `Manifest` 文件中（每个版本一个）。                         | 本方案元数据与表元数据解耦，便于独立管理；LanceDB 元数据与表版本紧耦合，读取 Manifest 即可获得全部信息。                                                                    |
| **索引元数据访问 I/O**  | **至少 2 次 I/O**：<br>1. 读 `index_metadata.puffin` Footer (获取 Blob 偏移)<br>2. 读 registry Blob 数据区<br>3. 解析 JSON | **1 次 I/O**：<br>读 `Manifest` 文件即可获得所有索引元数据。           | 本方案将 segments 信息独立存储，因此需要额外一次 I/O 定位并读取 Blob；LanceDB 的 Manifest 直接包含全部信息。虽然本方案多一次 I/O，但注册表文件通常很小（几 KB），实际影响有限。                   |
| **覆盖信息存储**       | JSON 数组 (`coverage_files`, `stale_files`)，存储完整文件路径。                                                         | RoaringBitmap (`fragment_bitmap`)，存储 Fragment ID 的位图。 | 本方案路径字符串数组内存占用大（每个路径 ~100 字节），I/O 开销高（需传输整个 JSON 数组）；LanceDB 使用位图，每个文件仅需 1 位，极度紧凑。对于 10 万个文件，本方案需 ~10 MB 存储，LanceDB 仅需 ~12.5 KB。 |
| **segment 文件组织** | 每个 segment 独立 Puffin 文件，内部一个 `custom.idx.btree-v1` Blob。                                                    | 每个 segment 独立 `.lance` 或 `.idx` 文件。                   | **完全对齐**：两者物理组织一致，均支持按需加载。                                                                                                       |
| **增量更新写入模式**     | **Append-only**：新增 segment 文件，旧文件不变。注册表文件重新生成（COW）。                                                         | **Append-only**：新增 Fragment 和索引段文件，更新 `Manifest`。     | 两者都通过追加实现高效写入，无写放大。但本方案需要重写注册表文件（通常 < 1MB），LanceDB 需要重写 `Manifest`（通常也较小）。                                                       |
| **文件数量控制**       | **无自动合并**：需后台 Compaction 或 GC 清理废弃 segment。                                                                 | **内置 Compaction**：自动合并小文件。                            | 本方案需要额外实现合并逻辑或依赖对象存储生命周期；LanceDB 对用户透明。                                                                                          |
| **部分失效处理**       | 通过 `stale_files` 字符串列表跳过，检查时需哈希查找。                                                                          | 通过 `fragment_bitmap` 直接进行位运算判断。                       | 本方案 `stale_files` 检查为 O(1) 哈希查找，性能尚可；LanceDB 位运算更快，且内存占用极低。                                                                      |
| **时间旅行支持**       | 依赖 Iceberg 快照隔离：每个快照有自己的 `index_metadata.puffin`，旧 segment 文件保留。                                            | 依赖 Lance 版本管理：Fragment 不可变，旧版本通过 `Manifest` 链访问。      | 两者均支持，但本方案充分利用 Iceberg 已有的快照机制，实现更简单。                                                                                            |
| **实现复杂度**        | **中等**：复用 Iceberg 的 `StatisticsFile`、`Transaction`、`FileIO` 等抽象，无需自研存储引擎。                                   | **较高**：需要自研存储格式、Compaction、位图索引等。                     | 本方案开发成本较低，适合已有 Iceberg 生态的系统；LanceDB 更适合全新设计的数据库。                                                                                |

###### 元数据字段对应关系

下表列出本方案中的关键元数据字段与 LanceDB 中的对应概念，便于理解两种设计的映射关系。

| 本方案 (Puffin + Iceberg)                                 | LanceDB (Lance 格式)                    | 说明                                            |
| ------------------------------------------------------ | ------------------------------------- | --------------------------------------------- |
| `snapshot-id` (在 `TableMetadata` 和 `StatisticsFile` 中) | `data_set_version` (Manifest 中的版本号)   | 都表示数据集的一个不可变版本标识。                             |
| `StatisticsFile` (包含 `statistics-path`)                | `Manifest` 文件                         | 记录索引元数据的入口文件。                                 |
| `index_metadata.puffin` 文件                             | 索引元数据内嵌在 `Manifest` 中                 | 本方案独立文件；LanceDB 将索引元数据直接写在 `Manifest` 内。      |
| `custom.idx.index-meta-v1` Blob                        | `IndexMetadata` (在 `Manifest` 中的索引描述) | 描述一个逻辑索引的元数据。                                 |
| `segments` 数组                                          | 逻辑索引中的段列表                             | 逻辑索引包含的物理段信息。                                 |
| `segment_id`                                           | 段标识 (通常由文件名或内部 ID 表示)                 | 每个段的唯一标识。                                     |
| `segment_file_path`                                    | 段文件路径 (如 `.lance` 或 `.idx` 文件)        | 物理段文件的位置。                                     |
| `coverage_files` (文件路径列表)                              | `fragment_bitmap` (RoaringBitmap)     | 覆盖信息的表示方式：本方案使用路径字符串数组（可读但膨胀），LanceDB 使用紧凑位图。 |
| `stale_files`                                          | (无直接对应，LanceDB 通过版本管理替代)              | 本方案显式标记失效文件；LanceDB 通过创建新版本 Fragment 自然淘汰旧数据。 |
| `snapshot_id` (在 `blob-metadata` 或 segment 元数据中)       | 段所基于的 `data_set_version`              | 记录构建该段所基于的数据版本，用于追溯。                          |

### 设计哲学差异

- **本方案**：遵循 Iceberg 的“元数据与数据分离”原则，索引作为优化附件，可随时重建或丢弃，不保证强一致性（查询时可跳过失效索引）。适合**分析型数据湖**场景，对更新频率要求不高，但重视与现有 Iceberg 生态的集成。

- **LanceDB**：索引是存储格式的原生能力，与数据强耦合，保证索引与数据的一致性和紧凑性。适合**实时向量检索**和**高频更新**场景，但需要完整自研存储引擎。



---

## 10. 风险与后续优化

- **JSON 膨胀风险**：`coverage_files` 和 `stale_files` 存储在注册表 JSON 中，当 segment 覆盖数万文件时可能达数 MB。优化方向：将路径列表分离为独立 `filelist` Blob，注册表只存哈希摘要。

- **写放大**：每次更新需重写注册表文件（通常 < 1MB），可接受。

- **文件数量增长**：需实现 GC 或生命周期规则控制。

- **并发冲突**：依赖 Iceberg 乐观锁，通过 `statistics-files` 原子替换解决。



## 11. 数据结构速查（Rust 示意）

```rust
pub struct IndexMetaBlob {
    pub segments: Vec<SegmentMetadata>,
}

pub struct SegmentMetadata {
    pub segment_id: String,
    pub segment_file_path: String,
    pub coverage_files: Vec<String>,
    pub stale_files: Vec<String>,
    pub min_path_hash: u64,
    pub max_path_hash: u64,
    pub total_rows: i64,
    pub source_snapshot_id: i64,
}

```



**文档版本**: 5.2 | **日期**: 2026-06-13

本设计完全基于 Rust + `iceberg-rust` SDK，无 JNI。注册表 Blob 类型为 `custom.idx.index-meta-v1`。所有文件路径、命名、GC 策略均已明确。如有疑问，请参考讨论记录。


