# Apache Iceberg Puffin 自定义索引 — 最终设计文档 v6.0

> **版本**: 6.0 | **日期**: 2026-06-13
> 
> **核心设计决策**:
> 
> - 纯 Rust 实现，基于 `iceberg-rust` SDK，作为独立索引库被 PostgreSQL、openGauss 等数据库集成。
> 
> - **一个逻辑索引对应多个 segment 文件**（每个 segment 独立 Puffin 文件，内部一个 `custom.idx.btree-v1` Blob）。
> 
> - **注册表**：`index_metadata.puffin` 文件（位于 `metadata/` 下），内部包含多个 `custom.idx.index-meta-v1` Blob，每个 Blob 对应一个逻辑索引，其数据区 JSON 存储该索引的 `segments` 数组。
> 
> - 文件覆盖信息（`coverage_files`, `stale_files`）存储在注册表 JSON 中（风险：JSON 可能膨胀，后续可优化）。
> 
> - 移除 Rewrite Map，部分失效通过 `stale_files` 跳过；移除阈值自动重建，仅用户命令触发重建。
> 
> - 索引更新（全量/增量/重建）采用 **Append-only + COW**：新增 segment 文件，新写 `index_metadata.puffin`，原子切换 `statistics-files` 指针。
> 
> - 对外提供 C API 接口，支持事务集成、异步任务、查询过滤和垃圾回收。



## 目录

1. 设计概述

2. 整体架构

3. 元数据组织与文件格式

4. 对外接口定义（C API）

5. 使用示例

6. 与 LanceDB 的对比

7. 风险与后续优化

8. 数据结构速查

9. 参考与依赖



## 1. 设计概述

### 1.1 目标

为基于 Apache Iceberg 的数据湖表提供**自定义二级索引**能力，索引库以独立库的形式被 PostgreSQL、openGauss 等数据库系统通过 C FFI 调用。索引数据利用 Iceberg 的 Puffin 文件格式存储，元数据通过 Iceberg 的 `StatisticsFile` 机制与表快照关联，支持快照隔离和时间旅行。

### 1.2 核心特性

- **增量构建**：新数据追加时，仅为新增文件构建新 segment，不修改旧 segment。

- **部分失效**：CoW UPDATE 后，失效文件通过 `stale_files` 标记，查询时跳过。

- **用户触发重建**：不基于阈值自动重建，由用户命令触发，重建后旧 segment 保留（供时间旅行）。

- **原子切换**：通过 `UpdateStatistics` 原子切换 `index_metadata.puffin` 指针。

- **垃圾回收**：提供 `idx_garbage_collect` 接口，结合对象存储生命周期规则或内部脚本清理孤儿文件。

- **无 JNI**：纯 Rust 实现，对外提供 C API。



## 2. 整体架构

```textile
┌─────────────────────────────────────────────────────────────────┐
│                     PostgreSQL / openGauss                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Parser     │  │  Planner    │  │  Executor               │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
│         │                │                      │                │
│         └────────────────┼──────────────────────┘                │
│                          │ idx_filter_files / idx_filter_rows    │
│                          ▼                                       │
│                 ┌─────────────────────┐                          │
│                 │  puffin_index C API  │                          │
│                 └──────────┬──────────┘                          │
└─────────────────────────────┼────────────────────────────────────┘
                              │
              ┌───────────────▼───────────────────────────────┐
              │           Puffin Index Library (Rust)          │
              │  ┌─────────────┐  ┌──────────┐  ┌──────────┐  │
              │  │IndexCatalog │  │SegmentMgr│  │   GC     │  │
              │  └─────────────┘  └──────────┘  └──────────┘  │
              │  ┌────────────────────────────────────────┐    │
              │  │         iceberg-rust SDK                │    │
              │  │  • TableMetadata • StatisticsFile      │    │
              │  │  • Transaction   • Puffin              │    │
              │  └────────────────────────────────────────┘    │
              └───────────────────┬─────────────────────────────┘
                                  │ FileIO (S3/GCS/HDFS)
                                  ▼
                     ┌────────────────────────┐
                     │   Object Storage / HDFS  │
                     │  • metadata.json         │
                     │  • index_metadata.puffin  │
                     │  • indices/*.puffin       │
                     │  • data/*.parquet         │
                     └──────────────────────────┘
```



## 3. 元数据组织与文件格式

### 3.1 表元数据中的 `statistics-files` 条目

`metadata.json` 中的 `statistics-files` 数组包含指向 `index_metadata.puffin` 的记录。每个 `custom.idx.index-meta-v1` Blob 对应一个逻辑索引。

```json
"statistics-files": [
  {
    "snapshot-id": 1001,
    "statistics-path": "s3://.../metadata/index_metadata_v12.puffin",
    "file-size-in-bytes": 655360,
    "file-footer-size-in-bytes": 512,
    "blob-metadata": [
      {
        "type": "custom.idx.index-meta-v1",
        "fields": [1],
        "snapshot-id": 1001,
        "source-snapshot-id": 1001,
        "sequence-number": 1,
        "properties": {
          "index-name": "btree_order_id",
          "index-type": "btree",
          "index-version": "1",
          "index-uuid": "a1b2c3d4-..."
        }
      }
    ]
  }
]
```



### 3.2 注册表文件 `index_metadata.puffin`

该文件内部包含多个 `custom.idx.index-meta-v1` Blob，每个 Blob 的数据区为 JSON（Zstd 压缩），描述一个索引的所有 segment。

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
    }
  ]
}
```



### 3.3 Segment 索引文件结构

每个 segment 对应一个独立的 Puffin 文件（例如 `btree_order_id_seg0_snap1001.puffin`），内部只包含一个 `custom.idx.btree-v1` Blob。

**文件布局**：

- Magic (4B) `PFA1`

- Blob 数据区（B-Tree 节点序列化字节流）

- Footer (JSON, LZ4 压缩) 包含 Blob 的 `offset`, `length`, `properties` 等。

Footer 示例：

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
      "properties": {
        "segment_id": "seg0",
        "file_count": "500",
        "min_path_hash": "a3f2...",
        "max_path_hash": "c7e1...",
        "total_rows": "1024000"
      }
    }
  ],
  "properties": {
    "index-name": "btree_order_id",
    "index-uuid": "a1b2c3d4-...",
    "created-by": "puffin-rs/6.0"
  }
}
```



### 3.4 文件名约定

- 注册表文件：`index_metadata_v{registry_version}.puffin`

- Segment 文件：`{index_name}_seg{segment_id}_snap{snapshot_id}.puffin`



## 4. 对外接口定义（C API）

### 4.1 头文件 `puffin_index.h`

```c
#ifndef PUFFIN_INDEX_H
#define PUFFIN_INDEX_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// 错误码
#define IDX_SUCCESS              0
#define IDX_ERR_INVALID_ARG     -1
#define IDX_ERR_NOT_FOUND       -2
#define IDX_ERR_ALREADY_EXISTS  -3
#define IDX_ERR_IO              -4
#define IDX_ERR_CORRUPTED       -5
#define IDX_ERR_CONFLICT        -6
#define IDX_ERR_NOT_IMPLEMENTED -7
#define IDX_ERR_OUT_OF_MEMORY   -8
#define IDX_ERR_BUSY            -9
#define IDX_ERR_TASK_NOT_FOUND  -10

// 初始化和关闭
int idx_init(const char* config_json);
void idx_shutdown(void);
const char* idx_version(void);
void idx_set_logger(void (*callback)(int level, const char* msg), int min_level);
const char* idx_last_error(void);

// 表句柄管理
void* idx_open_table(const char* table_path, const char* catalog_type, const char* catalog_uri);
void idx_close_table(void* table_handle);
int idx_refresh(void* table_handle);

// 索引生命周期
int idx_create_index(void* table_handle, const char* index_name, const char* index_type,
                     const int* field_ids, int num_fields, const char* index_params_json,
                     int64_t* task_id);
int idx_get_index_build_status(void* table_handle, int64_t task_id, int* status,
                               char* error_msg, size_t error_msg_len);
int idx_drop_index(void* table_handle, const char* index_name);
int idx_rebuild_index(void* table_handle, const char* index_name, int64_t* task_id);

// 数据变更通知（事务集成）
int idx_begin_transaction(void* table_handle, int64_t xid);
int idx_commit_transaction(void* table_handle, int64_t xid);
int idx_rollback_transaction(void* table_handle, int64_t xid);
int idx_notify_files_appended(void* table_handle, const char* const* added_paths, int num_added);
int idx_notify_files_rewritten(void* table_handle, const char* const* old_paths, int num_old,
                               const char* const* new_paths, int num_new);
int idx_mark_files_stale(void* table_handle, const char* index_name,
                         const char* const* stale_paths, int num_stale);

// 查询接口
int idx_filter_files(void* table_handle, const char* index_name, const char* query_expr_json,
                     int64_t snapshot_id, char*** result_file_paths, int* num_files);
int idx_filter_rows(void* table_handle, const char* index_name, const char* query_expr_json,
                    int64_t snapshot_id, uint64_t** row_addresses, int* num_rows);
void idx_free_result(void* ptr);

// 元数据和维护
int idx_list_indices(void* table_handle, char** indices_json);
int idx_get_index_stats(void* table_handle, const char* index_name, char** stats_json);
int idx_garbage_collect(void* table_handle, int64_t older_than_seconds);
int idx_get_global_stats(char** stats_json);

#ifdef __cplusplus
}
#endif

#endif // PUFFIN_INDEX_H
```



### 4.2 接口语义说明

| 接口                                      | 说明                       |
| --------------------------------------- | ------------------------ |
| `idx_init`                              | 全局初始化，配置缓存、线程池等。         |
| `idx_open_table`                        | 打开 Iceberg 表，加载注册表缓存。    |
| `idx_create_index`                      | 异步全量构建索引，返回任务 ID。        |
| `idx_rebuild_index`                     | 异步重建索引，旧索引保留。            |
| `idx_notify_files_appended`             | 通知新增文件，内部暂存，提交事务后触发增量构建。 |
| `idx_notify_files_rewritten`            | 通知重写文件，旧文件被标记 `stale`。   |
| `idx_begin/commit/rollback_transaction` | 绑定数据库事务，确保索引更新与数据变更原子性。  |
| `idx_filter_files`                      | 根据查询条件返回匹配的文件路径列表。       |
| `idx_filter_rows`                       | 返回 RowAddress 列表（精确行定位）。 |
| `idx_garbage_collect`                   | 清理孤立 segment 文件。         |



## 5. 使用示例

```c
#include <stdio.h>
#include "puffin_index.h"

int main() {
    // 1. 初始化
    if (idx_init("{\"cache_size_mb\":256}") != IDX_SUCCESS) {
        fprintf(stderr, "init failed: %s\n", idx_last_error());
        return 1;
    }

    // 2. 打开表
    void* table = idx_open_table("s3://warehouse/db/orders", "rest", "http://catalog:8181/");
    if (!table) return 1;

    // 3. 创建索引
    int fields[] = {1};
    int64_t task;
    idx_create_index(table, "btree_order_id", "btree", fields, 1,
                     "{\"page_size\":4096}", &task);

    // 轮询直到完成
    int status;
    do {
        sleep(1);
        idx_get_index_build_status(table, task, &status, NULL, 0);
    } while (status != 2);

    // 4. 新增数据（事务内）
    int64_t xid = 12345;
    idx_begin_transaction(table, xid);
    const char* new_files[] = {"s3://.../part-1000.parquet"};
    idx_notify_files_appended(table, new_files, 1);
    idx_commit_transaction(table, xid);  // 触发增量构建

    // 5. 查询过滤
    char** files;
    int num;
    if (idx_filter_files(table, "btree_order_id",
                         "{\"op\":\"eq\",\"col\":\"order_id\",\"val\":42}",
                         -1, &files, &num) == IDX_SUCCESS) {
        for (int i = 0; i < num; i++)
            printf("Scan file: %s\n", files[i]);
        idx_free_result(files);
    }

    // 6. 清理
    idx_garbage_collect(table, 86400);
    idx_close_table(table);
    idx_shutdown();
    return 0;
}
```



## 6. 与 LanceDB 的对比

| 维度         | **本方案**                 | **LanceDB**                   |
| ---------- | ----------------------- | ----------------------------- |
| 元数据存储      | 独立注册表文件                 | 嵌入 Manifest                   |
| 覆盖信息       | JSON 路径数组               | RoaringBitmap                 |
| segment 文件 | 每个 segment 独立 `.puffin` | 每个 segment 独立 `.lance`/`.idx` |
| 增量更新       | Append-only（追加 segment） | Append-only（追加 Fragment）      |
| 文件数量控制     | 需 GC 或生命周期规则            | 内置 Compaction                 |
| 部分失效       | `stale_files` 逻辑跳过      | `fragment_bitmap` 位运算         |
| 时间旅行       | Iceberg 快照              | Lance 版本管理                    |
| 实现复杂度      | 中等（复用 Iceberg）          | 较高（自研存储）                      |
| 适用场景       | 数据湖分析，与 Iceberg 生态集成    | 实时向量检索，高频更新                   |

**元数据字段对应**：

- `snapshot-id` ↔ `data_set_version`

- `StatisticsFile` ↔ `Manifest` 文件

- `custom.idx.index-meta-v1` ↔ `IndexMetadata`

- `coverage_files` ↔ `fragment_bitmap`

---

## 7. 风险与后续优化

- **JSON 膨胀**：`coverage_files` 存储完整路径，当 segment 覆盖数万文件时 JSON 可达数 MB。优化方向：分离 `filelist` Blob，注册表只存哈希摘要或 RoaringBitmap。

- **写放大**：每次更新重写注册表文件（通常 < 1MB），可接受。

- **文件数量增长**：需实现 GC 或依赖对象存储生命周期。

- **并发冲突**：依赖 Iceberg 乐观锁，通过 `statistics-files` 原子替换解决。

---

## 8. 数据结构速查（Rust 示意）

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



## 9. 参考与依赖

- Apache Iceberg Specification

- Puffin File Format: https://github.com/apache/iceberg/blob/master/format/puffin-spec.md

- iceberg-rust: https://github.com/apache/iceberg-rust

- LanceDB: [https://lancedb.com](https://lancedb.com/)

---

> **文档版本**: 6.0 | **日期**: 2026-06-13
> 
> 本设计完全基于 Rust + `iceberg-rust` SDK，对外提供 C API，可被 PostgreSQL、openGauss 等数据库集成。索引库实现了增量构建、部分失效、时间旅行、原子切换等核心能力，并通过事务集成接口与数据库事务同步。文档包含完整的 API 定义、使用示例和与 LanceDB 的对比分析。

---






