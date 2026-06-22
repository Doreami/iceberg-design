# mappedbptree介绍

`mappedbptree` 是一个 Rust 库，它实现了一个**持久化、内存映射的 B+ 树**。它的核心设计目标是为需要将数据可靠地存储在磁盘上，并提供高性能、低延迟访问的场景，提供一个开箱即用的解决方案。

### 设计目标：持久化、高性能、易用

`mappedbptree` 试图在几个关键维度上取得平衡：

- **持久化**：数据存储在文件中，进程重启后依然可用。

- **高性能**：通过内存映射（mmap）实现接近内存的访问速度，并支持零拷贝读取。

- **可靠性**：通过预写日志（WAL）和 CRC32 校验和，提供了**崩溃安全（Crash Safety）** 和数据完整性保障。

- **易用性**：提供类似标准库 `BTreeMap` 的熟悉 API。

### 核心架构：内存映射 + B+ 树

`mappedbptree` 的架构可以分解为三个关键层面：

1. **存储引擎层：内存映射（mmap）**  
   这是其高性能的基础。它直接将数据文件映射到进程的虚拟内存地址空间。对树节点的读写操作会直接转化为对内存的访问，由操作系统内核负责将数据同步回磁盘文件，极大地减少了系统调用的开销。

2. **数据结构层：B+ 树**  
   在映射的内存区域上，构建了一个标准的 B+ 树。
   
   - **内部节点**：仅存储用于路由的键。
   
   - **叶子节点**：存储实际的数据（键值对）。
   
   - **节点容量**：节点的容量会根据系统页大小（通常为 4096 字节）自动调整，以优化 I/O 性能。

3. **可靠性层：WAL 与 CRC32**  
   这是保证数据安全的关键。
   
   - **WAL (Write-Ahead Logging)**：在修改树之前，操作会先被记录到 WAL 文件并强制同步（`fsync`）到磁盘。即使写入中途崩溃，重启后也会自动重放 WAL，确保数据一致。
   
   - **CRC32 校验**：每个节点都带有 CRC32 校验和，用于检测数据损坏。

### 关键特性详解

#### 1. 🛡️ 崩溃安全（Crash Safety）

这是 `mappedbptree` 最核心的特性之一。其工作流程如下：

1. **写 WAL**：每次修改前，将操作写入 `{db}.wal` 文件并执行 `fsync`。

2. **修改树**：在内存映射区域执行修改。

3. **清理 WAL**：修改完成后，删除 WAL 文件。

4. **自动恢复**：下次打开时，若发现残留的 WAL 文件，则自动重放其中的操作。

这种机制确保了任何写操作要么完全成功，要么在重启后能自动恢复到一致状态。

#### 2. 🚀 零拷贝读取（Zero-copy Reads）

`get` 方法返回的 `MmapBTreeValueRef` 直接引用了内存映射区域的数据，避免了数据从内核空间到用户空间的拷贝。这在读取频繁的场景下能显著提升性能并降低内存占用。

#### 3. 🧵 线程安全（Thread-safe）

`MmapBTree` 内部使用 `RwLock` 实现了线程安全。它允许多个线程并发读取，而写操作会被串行化，保证了数据的一致性。

### ⚠️ 重要约束：`Pod` 类型

`mappedbptree` 要求键和值都必须实现 `bytemuck::Pod` trait。`Pod` (Plain Old Data) 意味着类型必须是“纯数据”，可以安全地直接转换为字节流。

- ✅ **支持的类型**：所有整数类型（如 `i32`, `u64`）、固定大小的数组（如 `[u8; 32]`）、以及所有字段都是 `Pod` 类型的 `#[repr(C)]` 结构体)。

- ❌ **不支持的类型**：所有包含堆内存分配的类型，例如 `String`、`Vec`、`HashMap` 等。

如果需要存储 `String` 这类数据，需要自行序列化为固定大小的字节数组。

### 快速上手

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
mappedbptree = "0.2"
```

一个简单的使用示例：

```rust
use mappedbptree::MmapBTreeBuilder;
use mappedbptree::Result;

fn main() -> Result<()> {
    // 使用构建器模式创建或打开一个B+树
    let tree = MmapBTreeBuilder::<i32, u64>::new()
        .path("my_tree.db") // 指定数据文件路径
        .build()?;

    // 插入数据
    tree.insert(1_i32, 42_u64)?;

    // 读取数据
    assert_eq!(tree.get_value(&1_i32)?, Some(42_u64));

    // 删除数据
    tree.remove(&1_i32)?;

    // 范围查询
    for (k, v) in tree.range(0..50)? {
        println!("{}: {}", k, v);
    }

    Ok(())
}
```

### 性能与限制

- **性能**：`mappedbptree` 专为高读性能场景设计。内存映射和零拷贝读取使其在读操作上非常高效。其写操作因需要 `fsync` WAL 而会有一定开销，这是为保障数据安全所做的必要权衡。

- **限制**：
  
  - **单写者**：受内部 `RwLock` 限制，写操作是串行的，不适合高并发写入场景。
  
  - **类型约束**：`Pod` 约束限制了可存储的数据类型。
  
  - **单进程**：内存映射文件通常不支持多进程同时写入。

### 总结

`mappedbptree` 是一个设计精良、专注于特定场景的 Rust 库。它通过“**内存映射 + WAL + B+树**”的组合，为需要**持久化、高读性能、数据安全**的应用提供了一个可靠的嵌入式数据库引擎选项。它的 API 设计对 Rust 开发者友好，但使用时需要留意其 `Pod` 类型约束和单写者的限制。

# Iceberg Puffin 自定义索引 — `mappedbptree` 集成方案

> **版本**: 1.0 | **日期**: 2026-06-22
> 
> **目的**: 评估并设计如何将 `mappedbptree` 库集成到 Iceberg Puffin 索引架构中，作为 B-Tree 标量索引的底层存储引擎，替代自研的 `FlatIndex + BTreeLookup` 方案。

---

## 1. 背景与动机

### 1.1 当前 B-Tree 设计方案（v2）的定位

v2 设计方案借鉴了 LanceDB 的 `FlatIndex + BTreeLookup` 双层架构：

- **查找表（Lookup）**：内存中的轻量级 B-Tree，将每页最大值映射到页偏移

- **数据页（Data Page）**：分页存储排序后的键值和行地址，使用 Arrow 格式

该方案的优势在于与 Puffin 文件格式深度集成，但需要自行实现分页、序列化、缓存等逻辑。

### 1.2 `mappedbptree` 库的引入动机

`mappedbptree` 是一个 Rust 原生、基于内存映射（mmap）的持久化 B+Tree 库，具备以下特性：

| 特性                      | 说明                                   |
| ----------------------- | ------------------------------------ |
| **文件-backed 持久化**       | 数据直接持久化到文件，进程重启后自动恢复                 |
| **WAL + CRC32 校验**      | Write-Ahead Log 保证崩溃安全，CRC32 确保数据完整性 |
| **零拷贝读取**               | `get` 方法直接从 mmap 返回数据引用，无额外复制        |
| **多线程安全**               | 读操作并发，写操作通过 `RwLock` 串行化             |
| **类似 `BTreeMap` 的 API** | 学习成本低，易于集成                           |

### 1.3 决策目标

本方案旨在评估 `mappedbptree` 是否能够作为 B-Tree 索引的底层存储引擎，在保持与现有架构（`IndexPlugin`、`ArtifactStore`、`IndexRegistryStore`）兼容的前提下，简化实现并提升可靠性。

---

## 2. 架构对比

### 2.1 v2 方案架构（自研）

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                    v2 方案架构（自研）                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Puffin 文件 (单个 segment)                       │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │  Blob 0: custom.idx.btree.lookup-v1 (查找表)                  │ │   │
│  │  │  Blob 1: custom.idx.btree.data-v1 (数据页 0)                  │ │   │
│  │  │  Blob 2: custom.idx.btree.data-v1 (数据页 1)                  │ │   │
│  │  │  ...                                                         │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  核心逻辑: 自行实现分页、序列化、查找表构建、Arrow 过滤                      │
│  数据格式: Arrow IPC (Zstd 压缩)                                          │
│  加载方式: 按需加载数据页，LRU 缓存                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 `mappedbptree` 方案架构

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                    mappedbptree 方案架构                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Puffin 文件 (单个 segment)                       │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │  Blob 0: custom.idx.btree.index-meta-v1 (索引段元数据)        │ │   │
│  │  │  ┌─────────────────────────────────────────────────────────┐  │ │   │
│  │  │  │  {                                                      │  │ │   │
│  │  │  │    "btree_file_path": "s3://.../seg0.btree",            │  │ │   │
│  │  │  │    "key_type": "Int64",                                  │  │ │   │
│  │  │  │    "value_type": "RowAddress",                           │  │ │   │
│  │  │  │    "num_entries": 1024000,                               │  │ │   │
│  │  │  │    "created_at_snapshot": 1001                           │  │ │   │
│  │  │  │  }                                                       │  │ │   │
│  │  │  └─────────────────────────────────────────────────────────┘  │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │          btree_seg0.btree (独立文件，由 mappedbptree 管理)          │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │  B+Tree 持久化数据 (mmap 映射)                                 │ │   │
│  │  │  Key: ScalarKey (编码为固定大小)                               │ │   │
│  │  │  Value: Vec<RowAddress> (编码为固定大小)                       │ │   │
│  │  │  内部: WAL + CRC32 校验                                       │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  核心逻辑: 委托给 mappedbptree 库                                          │
│  数据格式: mappedbptree 内部格式                                           │
│  加载方式: 通过 mmap 直接映射，零拷贝读取                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 3. `mappedbptree` 适配分析

### 3.1 类型约束与适配方案

`mappedbptree` 要求 Key 和 Value 必须实现 `Pod` trait（固定大小、可字节复制）。

#### 3.1.1 Key 类型适配

```rust
// 原始 ScalarKey (变长)
pub enum ScalarKey {
    Int(i64),
    Str(String),      // ❌ 变长，不满足 Pod
    Float(f64),
    Null,
}

// 适配方案: 使用固定大小枚举
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct BTreeKey {
    pub tag: u8,              // 0=Null, 1=Int, 2=Str, 3=Float
    pub int_val: i64,         // 用于 Int 和 Float (通过位转换)
    pub str_hash: u64,        // 字符串使用哈希或 ID
}

// 或使用 SmallVec (固定大小)
const STR_KEY_SIZE: usize = 64;  // 最大 63 字节 + 1 字节长度
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct BTreeKey {
    pub tag: u8,
    pub data: [u8; STR_KEY_SIZE],
}
```

#### 3.1.2 Value 类型适配

```rust
// 原始 RowAddress (固定大小，天然 Pod)
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct RowAddress {
    pub file_path_hash: u64,   // 使用哈希代替 String
    pub row_position: u64,     // 或者 (file_id, row_position)
}

// 多值情况: 每个键对应多个 RowAddress
// 方案 A: 使用单个 BTreeKey → 单个 RowAddress (存储多份)
// 方案 B: 存储 Vec<RowAddress> 的序列化 (需实现 Pod)
// 方案 C: 使用辅助数据结构 (如存储在独立文件中的倒排表)

// 推荐方案: Key = (ScalarKey, RowAddress) 复合键
// 这样每个键值对存储一个地址，查询时 range scan 获取所有地址
#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
pub struct BTreeEntryKey {
    pub key: BTreeKey,
    pub address: RowAddress,
}

// Value: 使用 () 或 u8 (仅为占位)
type BTreeValue = u8;
```

### 3.2 与 Puffin 文件的集成

```rust
/// B-Tree 索引段元数据 (存储在 Puffin Blob 中)
#[derive(Serialize, Deserialize)]
pub struct BTreeSegmentMeta {
    /// 由 mappedbptree 管理的独立文件路径
    pub btree_file_path: String,
    /// 键类型
    pub key_type: String,
    /// 总条目数
    pub num_entries: u64,
    /// 创建时的快照 ID
    pub created_at_snapshot: SnapshotId,
    /// 文件大小 (用于统计)
    pub file_size_bytes: u64,
}

/// B-Tree 插件实现
pub struct BTreePluginMapped;

impl BTreePluginMapped {
    const META_BLOB_TYPE: &str = "huawei.gauss-infra.btree-meta-v1";
    const BTREE_EXT: &str = "btree";
}
```

### 3.3 构建流程

```rust
#[async_trait]
impl IndexPlugin for BTreePluginMapped {
    async fn build(
        &self,
        context: &PluginContext,
        definition: &IndexDefinition,
        input: IndexBuildInput,
    ) -> Result<CreatedIndex> {
        // 1. 创建临时 B+Tree 文件
        let btree_path = format!("{}/{}/{}.{}",
            context.artifact_root,
            definition.index_id.0,
            uuid::Uuid::new_v4(),
            Self::BTREE_EXT
        );

        // 2. 创建 mappedbptree 实例
        let mut tree = MmapBTreeBuilder::<BTreeEntryKey, u8>::new()
            .path(&btree_path)
            .build()?;

        // 3. 消费 IndexBatchStream，插入数据
        let mut count = 0;
        while let Some(batch) = input.batches.next().await {
            let batch = batch?;
            // 提取 key, file_path, row_position
            for row in 0..batch.num_rows() {
                let key = extract_key(&batch, row)?;
                let addr = extract_address(&batch, row)?;
                let entry_key = BTreeEntryKey { key, address: addr };
                tree.insert(entry_key, 0)?;
                count += 1;
            }
        }

        // 4. 写入 Puffin 元数据 Blob
        let meta = BTreeSegmentMeta {
            btree_file_path: btree_path,
            key_type: extract_key_type(&input)?,
            num_entries: count,
            created_at_snapshot: input.snapshot_id,
            file_size_bytes: std::fs::metadata(&btree_path)?.len(),
        };

        // 5. 返回 CreatedIndex
        Ok(CreatedIndex {
            implementation: BTREE_MAPPED_IMPLEMENTATION.to_string(),
            format_version: 1,
            algorithm_details: serde_json::to_value(meta)?,
            artifact_files: vec![
                ArtifactFile {
                    uri: btree_path,
                    size_bytes: meta.file_size_bytes,
                    checksum: None,
                },
                // Puffin 文件由框架管理
            ],
            indexed_rows: count,
            completed_data_files: input_files,
        })
    }
}
```

### 3.4 加载流程

```rust
#[async_trait]
impl IndexPlugin for BTreePluginMapped {
    async fn load(
        &self,
        context: &PluginContext,
        definition: &IndexDefinition,
        segment: &IndexSegmentMetadata,
    ) -> Result<LoadedIndex> {
        // 1. 从 segment.algorithm_details 解析元数据
        let meta: BTreeSegmentMeta = serde_json::from_value(
            segment.algorithm_details.clone()
        )?;

        // 2. 打开 mappedbptree (mmap 映射)
        let tree = MmapBTreeBuilder::<BTreeEntryKey, u8>::new()
            .path(&meta.btree_file_path)
            .open()?;

        // 3. 创建运行时索引
        Ok(LoadedIndex::Scalar(Arc::new(BTreeMappedRuntimeIndex {
            implementation: definition.implementation.clone(),
            tree: Arc::new(MmapBTree::from_builder(tree)),
            key_type: meta.key_type,
            num_entries: meta.num_entries,
            stale_files: segment.stale_files.clone(),
        })))
    }
}
```

### 3.5 查询流程

```rust
#[async_trait]
impl ScalarIndex for BTreeMappedRuntimeIndex {
    async fn search(&self, request: &ScalarSearchRequest) -> Result<ScalarSearchResult> {
        // 1. 构建查询键范围
        let (start_key, end_key) = build_key_range(&request.expression)?;

        // 2. 使用 mappedbptree 的范围扫描
        let mut addresses = Vec::new();
        for entry in self.tree.range(start_key..=end_key) {
            let (key, _) = entry?;
            // 检查 stale_files
            if !self.stale_files.contains(&key.address.file_path_hash) {
                addresses.push(RowAddress {
                    file_path: resolve_path(key.address.file_path_hash)?,
                    row_position: key.address.row_position,
                });
            }
        }

        // 3. 按 limit 截断
        if let Some(limit) = request.limit {
            addresses.truncate(limit);
        }

        Ok(ScalarSearchResult {
            addresses,
            is_exact: true,
        })
    }
}
```

## 4. 两种方案对比

| 维度        | v2 方案 (自研)            | mappedbptree 方案           |
| --------- | --------------------- | ------------------------- |
| **存储位置**  | Puffin 文件内部 (多个 Blob) | Puffin 元数据 + 独立 B+Tree 文件 |
| **数据格式**  | Arrow IPC + Zstd      | mappedbptree 内部格式 + WAL   |
| **崩溃安全**  | 依赖 Iceberg 事务         | 自带 WAL + CRC32            |
| **内存占用**  | 查找表 (~2MB) + 缓存页      | mmap 映射，按需加载              |
| **查询方式**  | 查找表定位页 + Arrow 过滤     | B+Tree 范围扫描               |
| **实现复杂度** | 高 (需实现分页、序列化、缓存)      | 低 (委托给 mappedbptree)      |
| **可维护性**  | 中 (需维护多套逻辑)           | 高 (核心逻辑由库提供)              |
| **集成成本**  | 低 (全在 Puffin 内)       | 中 (需管理额外文件)               |
| **性能特征**  | 批量过滤 (SIMD)           | 点查和范围扫描 (B+Tree)          |
| **变长键支持** | 原生支持                  | 需适配为固定大小                  |

---

## 5. 集成方案建议

### 5.1 推荐方案：混合架构

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                    混合架构设计                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  场景 1: 小/中等规模 (≤ 1 亿行)                                             │
│  └─→ v2 方案 (自研)                                                        │
│      - 全在 Puffin 内，无需管理额外文件                                    │
│      - Arrow SIMD 查询性能优异                                             │
│                                                                             │
│  场景 2: 大规模 (> 1 亿行) / 需要崩溃安全                                   │
│  └─→ mappedbptree 方案                                                     │
│      - 自带 WAL + CRC32，崩溃安全                                          │
│      - 按需 mmap，内存可控                                                 │
│      - B+Tree 范围扫描在大量数据下更高效                                   │
│                                                                             │
│  场景 3: 变长键 (字符串)                                                    │
│  └─→ 需评估 mappedbptree 的 Pod 约束                                       │
│      - 若字符串基数低 → 使用哈希映射                                       │
│      - 若字符串基数高 → v2 方案                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 决策矩阵

| 条件                | 推荐方案                             |
| ----------------- | -------------------------------- |
| 数据量 < 1000 万行     | v2 方案 (自研)                       |
| 数据量 1000 万 ~ 1 亿行 | 两者均可，按需选择                        |
| 数据量 > 1 亿行        | mappedbptree 方案                  |
| 需要严格崩溃安全          | mappedbptree 方案                  |
| 列类型为变长字符串         | v2 方案 (自研) 或 哈希映射 + mappedbptree |
| 追求最快查询响应          | v2 方案 (Arrow SIMD)               |
| 追求最低内存占用          | mappedbptree 方案 (mmap 按需)        |
| 团队维护能力有限          | mappedbptree 方案 (代码量更少)          |

---

## 6. 实现路线图

### Phase 1: 核心集成 (1-2 周)

1. ✅ 在 `iceberg-index-plugins` 中创建 `btree_mapped.rs`

2. ✅ 定义 `BTreePluginMapped` 结构体

3. ✅ 实现 `IndexPlugin` trait 的基本方法 (`implementation`, `kind`, `validate_definition`)

4. ✅ 实现 `build` 方法：创建 `mappedbptree` 实例，插入数据

5. ✅ 实现 `load` 方法：打开现有 `mappedbptree` 文件

6. ✅ 实现 `ScalarIndex::search` 方法

### Phase 2: 类型适配 (1 周)

1. ✅ 设计 `BTreeKey` 固定大小表示

2. ✅ 设计 `RowAddress` 的编码方式 (使用 `u64` 哈希)

3. ✅ 实现 Key/Value 转换函数

4. ✅ 添加字符串键支持 (使用哈希或固定大小)

### Phase 3: 集成测试 (1 周)

1. ✅ 单元测试 (构建 → 加载 → 查询)

2. ✅ 集成测试 (与 `IcebergIndexService` 配合)

3. ✅ 性能基准测试 (对比 v2 方案)

### Phase 4: 优化 (可选)

1. 🔧 支持 `stale_files` 过滤

2. 🔧 支持增量构建

3. 🔧 支持多个 segment 合并

---

## 7. 结论与建议

### 7.1 结论

`mappedbptree` 库与当前 Iceberg Puffin 索引架构的集成是**可行且有价值的**：

- ✅ **简化实现**：将分页、序列化、缓存等复杂逻辑委托给库

- ✅ **提升可靠性**：自带 WAL + CRC32，崩溃安全有保障

- ✅ **内存可控**：mmap 按需加载，适合大规模数据

- ✅ **性能可期**：零拷贝读取，B+Tree 范围扫描高效

### 7.2 建议

1. **推荐采用** `mappedbptree` 方案作为 B-Tree 标量索引的主要实现：
   
   - 当前 `BTreePlugin` (全内存) 和 v2 方案 (自研分页) 的补充
   
   - 适用于大规模数据 (1 亿行以上) 和需要崩溃安全的场景

2. **保留 v2 方案的选项**：
   
   - 对于小规模数据和变长字符串列，v2 方案可能更适合
   
   - 提供两种方案的互操作性

3. **关键挑战**：
   
   - `Pod` 类型约束需要认真适配
   
   - 字符串键需要特别的哈希映射方案

---

> **总结**: `mappedbptree` 集成方案为 B-Tree 标量索引提供了**更可靠、更易维护**的实现路径，特别适合大规模数据场景。通过与现有架构的兼容设计，可以平滑集成到 `iceberg-index-sdk` 中。

# 补充问题

## 为什么数据量 > 1 亿行和需要严格崩溃安全，则选择mappedbptree 方案

### 为什么数据量 > 1 亿行时，`mappedbptree` 更好？

自研的 V2 方案（分页索引）需要通过查找表（`BTreeLookup`）和数据页缓存两个层次来管理内存：

- **查找表**：尽管相对轻量，但要回答“1 亿行的键值分布在哪些页”这个问题，它自身也需要存储所有页的元数据（如键范围）。这会随着数据量线性增长，最终占用相当可观的内存。

- **数据页缓存（LRU）**：查询时，V2 方案需要将候选数据页从磁盘加载到内存，并维持一个缓存。数据量越大，缓存命中率就越不稳定，对内存的占用和查询延迟都构成挑战。

而 `mappedbptree` 采用的内存映射（mmap）技术，从根本上改变了这个局面：

- **按需加载**：索引文件通过 mmap 映射到进程的虚拟地址空间，但**并未真正加载到物理内存**。只有当查询实际读取某个 B+Tree 节点时，操作系统才会将包含该节点的文件页加载到内存。这完全跳过了显式的数据页缓存管理。

- **近乎为零的内存上限**：理论上，其物理内存占用只与当前查询访问的索引数据页相关，而不是与整个索引大小成比例。这使得它能够轻松应对远超物理内存大小的索引。

### 为什么需要严格崩溃安全时，`mappedbptree` 更好？

这对于一个持久化的索引系统至关重要。当写入过程中发生断电或进程崩溃时，必须保证索引数据不损坏，并且能够恢复到一致的状态。

- **V2 方案的风险**：它的数据最终要写入 Puffin 文件，其完整性依赖于 Iceberg 的事务机制。但索引自身的构建和维护流程是复杂的。例如，在增量构建或合并多个段时，如果中间步骤崩溃，很可能留下一个不完整的索引，而 V2 方案自身并没有细粒度的恢复机制。

- **`mappedbptree` 的保障**：它内建了 **WAL (Write-Ahead Log)** 和 **CRC32 校验**。每次写操作都先记录日志再修改数据，这保证了在任何时刻崩溃后都能通过重放日志来恢复。CRC32 校验则能确保读取的数据没有被损坏，从而在源头上保证了数据的完整性和一致性。

### 结论

简单来说，自研的 V2 方案在处理亿级数据时，面临的**内存管理复杂度**和**大规模索引的崩溃恢复难度**，都远大于 `mappedbptree` 这类专门构建的持久化存储引擎。因此，当你的需求同时触及“大规模”和“高可靠性”这两个点时，`mappedbptree` 是一个更成熟、风险更低的选择。

## 那这些问题不能自研解决吗

### 1. 为什么 V2 方案会出现这两个问题？

#### 1.1 内存管理问题

V2 方案的内存占用由两部分组成：

```textile
总内存 = 查找表 (BTreeLookup) + 数据页缓存 (LRU)
```

- **查找表**：每页一条记录 `(max_key → page_offset)`。当数据量达到 1 亿行、每页 4096 行时，查找表约有 24,414 条记录，每条记录包含键值和偏移量，内存占用可达数 MB，虽然不大，但同样会随数据量线性增长。

- **数据页缓存**：这是主要问题。查询时加载候选页，缓存在 LRU 中。随着数据量增长，查询模式变得更加分散，缓存命中率下降，导致以下问题之一：
  
  - **增加缓存容量** → 内存占用飙升
  
  - **降低缓存容量** → 频繁触发磁盘 I/O，延迟增加

**关键瓶颈**：V2 方案的缓存是**被动式**的（加载后决定是否缓存）。它无法做到像 mmap 那样的**按需、零拷贝、由操作系统统一管理**的页面加载。

#### 1.2 崩溃安全问题

V2 方案构建索引时，需要依次执行多个步骤：

```textile
1. 扫描数据
2. 排序所有键值
3. 分页 (chunking)
4. 写入查找表 Blob
5. 写入数据页 Blob (多个)
6. 更新注册表 (index_metadata.puffin)
```

如果在步骤 3 和步骤 4 之间发生崩溃，会留下一个不完整的索引文件。要解决这个问题，必须引入：

- **WAL (Write-Ahead Log)**：记录每一步的意图

- **原子提交**：确保所有 Blob 全部写入后才标记索引为可用

- **恢复逻辑**：崩溃后能检测并清理不完整的数据

这些都需要在 V2 方案中自行实现。

---

### 2. 自研解决的成本分析

#### 2.1 解决内存管理问题

| 方案                 | 实现难度 | 效果                    |
| ------------------ | ---- | --------------------- |
| **优化 LRU 缓存策略**    | 中    | 仅能缓解，无法解决根本问题         |
| **使用 mmap 替代堆内存**  | 高    | 解决了加载问题，但需处理文件锁定和并发   |
| **实现分层存储 (热/冷)**   | 很高   | 需区分访问频率，逻辑复杂          |
| **完全重写为磁盘 B+Tree** | 极高   | 等于重写一个 `mappedbptree` |

**结论**：如果仅仅优化缓存策略，无法从根本上解决问题。真正的解决方案是拥抱 mmap 或磁盘 B+Tree。

#### 2.2 解决崩溃安全问题

需要实现的内容：

| 组件                   | 说明                              | 工作量   |
| -------------------- | ------------------------------- | ----- |
| **WAL**              | 记录每个操作 (insert, delete, update) | 2-3 周 |
| **检查点 (Checkpoint)** | 定期将 WAL 合并到索引文件                 | 1-2 周 |
| **原子提交**             | 确保索引切换是原子的                      | 1 周   |
| **恢复逻辑**             | 启动时重放 WAL                       | 1-2 周 |
| **CRC32 校验**         | 检测数据损坏                          | 0.5 周 |
| **并发控制**             | 多线程环境下安全读写                      | 1-2 周 |
| **测试**               | 模拟各种崩溃场景，验证恢复逻辑                 | 2-3 周 |

**总工作量**：约 10-15 人周（2-4 个月），且需要深入的数据库存储引擎经验。

#### 2.3 长期维护成本

- **Bug 修复**：WAL 和崩溃恢复逻辑非常复杂，边界情况极多，需要持续投入。

- **性能调优**：需要针对不同的查询模式和数据规模持续优化缓存和 I/O 策略。

- **文档和测试**：需要大量的测试用例来保证正确性。

---

### 3. 投入产出对比

| 方案                    | 开发成本        | 维护成本       | 可靠性                | 性能             |
| --------------------- | ----------- | ---------- | ------------------ | -------------- |
| **V2 自研解决**           | 极高 (2-4 个月) | 高          | 不确定 (需长期验证)        | 取决于实现质量        |
| **采用 `mappedbptree`** | 低 (1-2 周集成) | 低 (库维护者负责) | 高 (WAL + CRC32 成熟) | 高 (mmap + 零拷贝) |

---

### 4. 为什么 `mappedbptree` 已经有这些能力？

`mappedbptree` 的设计目标就是解决这些问题：

| 能力        | `mappedbptree` 的实现 | 自研需要复现的工作             |
| --------- | ------------------ | --------------------- |
| **内存管理**  | 使用 mmap，由操作系统管理页面  | 需实现完整的 mmap 封装 + 缓存策略 |
| **崩溃安全**  | WAL + CRC32 + 原子提交 | 需实现完整的 WAL + 恢复机制     |
| **并发控制**  | 内部 `RwLock` + 文件锁  | 需实现并发控制逻辑             |
| **数据完整性** | 节点级 CRC32 校验       | 需实现校验和验证              |
| **持久化**   | 文件-backed，自动持久化    | 需实现文件 I/O 和管理         |

这些能力是经过实践检验的，而自研实现需要大量时间和风险来验证。

---

### 5. 结论

**可以自研解决，但不推荐。**

#### ✅ 什么时候可以自研？

- 团队有深厚的数据库存储引擎经验

- 有充足的时间和资源 (2-4 个月)

- `mappedbptree` 的类型约束 (Pod) 无法满足你的需求

- 需要与 Puffin 文件深度集成，不希望管理额外文件

#### ✅ 什么时候应该采用 `mappedbptree`？

- 数据量大于 1 亿行

- 需要严格的崩溃安全保证

- 希望快速交付功能，将精力放在上层逻辑

- 团队规模有限，不希望维护复杂的存储引擎代码

#### 💡 务实建议

如果 `mappedbptree` 的 `Pod` 约束可以满足你的数据模型 (键值可以编码为固定大小)，那么**强烈建议采用 `mappedbptree`**。它能以 1-2 周的集成成本，换来一个经过考验、稳定可靠的存储引擎，让你能够专注于索引与 Iceberg 的集成逻辑，而不是重写一个 B+Tree 引擎。

## lance面对这些问题的处理方式

LanceDB 应对大规模数据下的内存和崩溃安全挑战，其核心策略与我们之前的讨论方向一致，但具体实现方式有所不同。它主要采用 **“磁盘优先、内存敏感”** 的架构，并结合 **MVCC（多版本并发控制）** 来保障数据一致性。

### 内存管理：索引驻内存，数据按需加载

LanceDB 通过将“索引”与“数据”分离存储来突破内存限制。

- **索引常驻内存**：其核心索引结构（如 IVF、PQ）只占原始数据量的 **1%-5%。对于 B-Tree 标量索引，其内存占用也远小于全量数据。这使得单节点即可处理十亿级向量数据。

- **数据按需加载**：原始向量数据**不常驻内存**，而是以 Lance 列式格式持久化在磁盘或对象存储中。

- **智能缓存**：查询时，数据通过**操作系统页面缓存**或用户态的 **LRU 缓存策略**从磁盘加载到内存。通过动态调整缓存，在保证查询性能的同时，将内存占用控制在很低水平。在 1 亿条向量的测试中，其 P99 延迟仍可控制在 50ms 以内。

### 崩溃安全：基于 MVCC 的版本化与事务

LanceDB 不依赖 WAL 日志，而是采用类似 Iceberg 的 **MVCC（多版本并发控制）** 机制来保证 ACID 特性。

- **版本化存储**：表的每次修改都会生成一个**不可变的新版本**（Manifest 文件），旧版本数据不会被立即删除。

- **原子提交**：新版本的提交通过原子操作（如重命名）完成，确保要么全部成功，要么全部失败。

- **崩溃恢复**：即使写入过程中崩溃，由于旧版本完好无损，系统重启后可以回退到最后一个完整版本，保证了数据的**持久性（Durability）**。

- **并发控制**：提供 **可串行化（Serializable）** 的隔离级别，并支持自动冲突解决。

### 海量数据扩展：读写分离与分层存储

LanceDB 通过计算与存储分离的架构实现海量数据扩展。

- **计算与存储分离**：数据和索引持久化在对象存储中，查询和后台任务（如索引构建、数据合并）由不同的计算节点执行。

- **分层存储**：采用 **“持久化层（对象存储）+ 缓存层（SSD/内存）+ 计算层”** 的三级架构。冷数据可存放在廉价的对象存储中，热数据则通过缓存加速访问。

- **弹性伸缩**：计算和存储资源可以独立扩展。索引构建等后台任务在专用节点上执行，避免影响在线查询。

- **写入优化**：采用 **Delta Log** 暂存写入，再异步合并到主存储，实现读写分离。

---

### 📊 解决方案对比总结

为了更直观地对比，这里将 LanceDB 与我们之前讨论的方案放在一起：

| 挑战               | LanceDB 解决方案     | V2 (自研) 方案      | mappedbptree 方案   |
| ---------------- | ---------------- | --------------- | ----------------- |
| **内存管理**         | 索引驻内存，数据通过缓存按需加载 | 查找表 + LRU 数据页缓存 | mmap 按需加载，由 OS 管理 |
| **崩溃安全**         | 依赖 MVCC 版本化与原子提交 | 需自行实现 WAL 或类似机制 | 内置 WAL + CRC32 校验 |
| **海量数据扩展**       | 存储计算分离 + 分层存储    | 受限于单机设计         | 受限于单机设计           |
| **实现复杂度**        | 高（需实现完整存储引擎）     | 中（需实现分页、缓存等）    | 低（主要集成工作）         |
| **与 Iceberg 集成** | 需额外适配层           | 原生集成            | 需管理额外文件           |

LanceDB 的方案在架构上更为彻底，通过存储计算分离和 MVCC 实现了更好的扩展性和数据一致性，但这也带来了更高的实现复杂度。

## V2 方案的安全边界

V2 方案的安全保证，取决于崩溃发生在**构建流程的哪个阶段**。我们可以用一个表格来清晰地分析：

### ✅ 受保护的阶段：元数据提交

| 阶段        | 操作                                                                                     | 崩溃后结果                             | 是否安全     |
| --------- | -------------------------------------------------------------------------------------- | --------------------------------- | -------- |
| **注册表提交** | 通过 `IndexRegistryStore::commit` 将新的 `SnapshotIndexRegistry` 写入 `index_metadata.puffin` | Iceberg 事务保证原子性：要么旧注册表保留，要么新注册表生效 | ✅ **安全** |

这部分由 Iceberg 的 MVCC 原子提交保证。如果崩溃发生在此刻，表会回滚到上一个一致状态，不会有损坏。

### ❌ 不受保护的阶段：索引数据构建

| 阶段                        | 操作                                      | 崩溃后结果                            | 是否安全                |
| ------------------------- | --------------------------------------- | -------------------------------- | ------------------- |
| **数据读取与排序**               | 从 Parquet 文件读取数据，在内存中排序                 | 进程崩溃，内存数据丢失，索引未写入任何文件            | ⚠️ **不安全**（需重试）     |
| **写入 Puffin 文件（部分）**      | 正在写入查找表 Blob 或部分数据页 Blob，尚未完成           | 留下**不完整**的 Puffin 文件，可能损坏        | ❌ **不安全**           |
| **Puffin 文件写入完成，但注册表未更新** | 索引数据已写入 Puffin 文件，但尚未提交 `statistics` 指针 | Puffin 文件已存在但**未被任何快照引用**，成为孤立文件 | ⚠️ **不安全**（需 GC 清理） |

### 图例说明

- **安全（✅）**：系统状态一致，无需人工干预

- **不安全（⚠️）**：需要重试或清理，但数据不会损坏

- **不安全（❌）**：可能导致索引文件损坏或数据丢失

## 🔍 为什么 V2 方案不完全安全？

### 1. Puffin 文件本身无事务

V2 方案将索引数据写入 Puffin 文件，但 Puffin 文件的写入是**非原子**的：

```textile
Puffin 文件写入过程：
  1. 写入 Magic (4 bytes)
  2. 写入 Blob 数据 (变长)
  3. 写入 Blob 数据 ...
  4. 写入 Footer JSON
  5. 写入 FooterPayloadSize + Flags
  6. 写入尾部 Magic (4 bytes)
```

如果在步骤 2-5 之间崩溃，文件内容不完整：

- Footer 可能未写入或部分写入，导致 Puffin 解析失败

- 部分 Blob 数据可能已写入，但 Footer 中无对应元数据

### 2. 多个 Puffin 文件之间无事务

V2 方案的构建可能会生成多个 Puffin 文件（如果按分区构建），但 Iceberg 的原子提交仅保证 `statistics` 指针的切换，不保证多个文件之间的原子性：

```textile
场景：构建包含 3 个分区的索引
  1. 写入分区1的索引数据 → Puffin A ✅
  2. 写入分区2的索引数据 → Puffin B ✅
  3. 写入分区3的索引数据 → Puffin C ❌ (崩溃)
  4. 更新注册表指向所有三个 Puffin → ❌ 未执行
```

结果：

- Puffin A 和 B 是完整但孤立的文件

- Puffin C 是部分文件

- 注册表未更新，索引不可用

### 3. 索引数据与 Parquet 文件的一致性

V2 方案构建索引时，需要确保索引数据与它所基于的 Parquet 文件版本一致：

```textile
时间线：
  T1: 读取快照 S1 的数据文件列表
  T2: 开始扫描 Parquet 文件
  T3: 对数据进行排序和分页
  T4: 写入 Puffin 文件
  T5: 更新注册表

如果在 T2 和 T4 之间，快照 S1 的数据文件发生了变化：
  - 新增了数据文件 → 索引遗漏了新数据
  - 删除了数据文件 → 索引包含已删除的数据
```

虽然 V2 方案通过传递 `input.data_files` 并在构建后验证 `completed_data_files` 来部分缓解此问题，但如果在验证前崩溃，仍可能产生不一致的索引。

### 🛡️ 如何增强 V2 方案的崩溃安全？

#### 方案 1：简单改进（不引入 WAL）

```rust
// 1. 使用临时文件名 + 原子重命名
let temp_path = format!("{}.tmp", final_path);
write_puffin_file(&temp_path, ...)?;
std::fs::rename(temp_path, final_path)?;  // 原子操作

// 2. 写入完成后验证完整性
let integrity_ok = verify_puffin_file(final_path)?;
if !integrity_ok {
    return Err(Error::Corrupted("Puffin file verification failed"));
}

// 3. 最后才更新注册表
commit_registry(registry).await?;
```

这能保证：要么完整的 Puffin 文件存在，要么不存在（没有部分文件）。

#### 方案 2：使用 Iceberg 的事务特性

```rust
// 利用 Iceberg Transaction 的原子性
let mut tx = Transaction::new(table);
// 先写入所有 Puffin 文件到临时位置
// 在 Transaction 中统一提交
tx.add_update(SetStatisticsUpdate { ... });
tx.commit().await?;  // 原子提交所有变更
```

#### 方案 3：引入 WAL（成本较高）

在 V2 方案基础上引入 WAL，记录每个构建步骤：

```textile
WAL 记录：
  1. BEGIN BUILD (snapshot_id, index_id)
  2. WRITE PAGE (page_idx, offset, length, checksum)
  3. WRITE PAGE ...
  4. COMMIT BUILD

崩溃恢复：
  1. 扫描 WAL，找到未完成的事务
  2. 删除所有已写入的 Puffin 文件
  3. 移除注册表中的 pending 条目
  4. 清理 WAL
```

### 结论

| 方案                        | 崩溃安全级别           | 实现成本     |
| ------------------------- | ---------------- | -------- |
| **V2 方案（当前）**             | 低（元数据安全，数据不安全）   | 低        |
| **V2 + 临时文件 + 原子重命名**     | 中（文件级原子性）        | 中（1-2 天） |
| **V2 + Transaction 统一提交** | 中（依赖 Iceberg 事务） | 中（1-2 天） |
| **V2 + WAL**              | 高                | 高（2-4 周） |
| **mappedbptree 方案**       | 高（自带 WAL）        | 低（集成工作）  |

**简单回答**：

- ❌ V2 方案**不能完全避免**崩溃问题

- ✅ 但可以通过**临时文件 + 原子重命名 + 事务提交**等简单手段，将不安全窗口缩得很小

- ⚠️ 如果需要**严格保证**，则需要引入 WAL 或采用 `mappedbptree`

## 性能对比：V2 方案 vs. `mappedbptree` 方案

| 性能维度              | V2 方案 (自研) | `mappedbptree` 方案 | 核心原因                                                                                                                           |
| ----------------- | ---------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| **读取性能 (点查/范围查)** | **更快**     | 相对较慢              | V2 方案在内存中通过 `BTreeLookup` 定位页后，使用 **Arrow + SIMD** 进行向量化过滤，计算效率极高。`mappedbptree` 是通用的持久化 B+Tree，需通过多次磁盘 I/O（尽管有 mmap 缓存）遍历树节点。 |
| **写入性能 (构建索引)**   | 较慢         | **更快**            | V2 方案需在内存中完成排序、分页等操作后，再整体写入 Puffin 文件。`mappedbptree` 的写入是**增量式**的，得益于其 B+Tree 结构，数据可以边处理边写入，且其设计目标之一就是高性能写入。                   |
| **崩溃安全与一致性**      | 较弱         | **强**             | V2 方案的构建过程非原子，崩溃可能导致文件损坏或不完整。`mappedbptree` 通过 **WAL (Write-Ahead Log)** 和 **CRC32 校验** 提供了事务性保证，确保了数据的完整性和一致性。                |
| **内存占用**          | 高          | 低                 | V2 方案需要将查找表常驻内存，并维护数据页缓存（LRU）。`mappedbptree` 使用 **mmap** 按需加载数据，内存占用更可控。                                                       |
| **I/O 模式**        | 批量顺序写入     | 随机写入              | V2 方案的写入是批量的、顺序的。`mappedbptree` 作为 B+Tree，其写入和读取都涉及**随机 I/O**，这是其性能开销的主要来源。                                                    |

---

### 📈 详细分析：为何会有这样的差异？

#### 1. V2 方案的性能优势来源

V2 方案专为分析型（OLAP）场景设计，其性能优势在于：

- **批量处理与向量化**：它先将所有数据在内存中排序，再分页写入。查询时，加载整个数据页后使用 **Arrow 和 SIMD** 进行过滤，非常适合处理大规模数据扫描。

- **可控的内存与 I/O**：索引数据集中存储在 Puffin 文件中，写入是**顺序的**，对磁盘友好。查询时，通过查找表精确定位到少数几个数据页，I/O 开销可控。

#### 2. `mappedbptree` 方案的设计取舍

`mappedbptree` 的设计更通用，旨在平衡读写性能、内存占用和数据安全：

- **通用的 B+Tree 结构**：它是一棵标准的、可持久化的 B+Tree，其随机 I/O 特性在分析型场景下，通常不如 V2 方案针对性的批量扫描高效。

- **写入性能的优势**：增量写入避免了 V2 方案在内存中排序和构建查找表的“冷启动”开销。尤其是在处理大量数据时，这种流式处理方式可能更快。

- **安全优先的设计**：为了保证数据在崩溃后依然安全，`mappedbptree` 需要执行 WAL 日志写入等额外操作，这会带来一些性能开销，但换来了更高的可靠性。

### 💡 选择建议

这个选择实际上是在 **“极致读取性能”** 与 **“写入性能+数据安全”** 之间做权衡：

- 如果你的场景是**读多写少**，且对查询延迟有极致要求，并能接受一定的实现复杂度和崩溃风险，那么 **V2 方案可能更优**。

- 如果你的场景是**写入频繁**，或者对数据的**持久化和一致性**有更高要求，希望获得更稳健的工程实现，那么 **`mappedbptree` 方案是更稳妥的选择**。

### 💎 总结

总的来说，使用 `mappedbptree` 方案，**读取性能可能会稍逊于高度优化的 V2 方案**，但它**在写入性能、内存效率和数据可靠性方面具有明显优势**。

最终选择哪个方案，取决于你对性能、安全性和开发成本的综合考量。如果决定采用 `mappedbptree`，其写入性能优势和对数据安全的保障，可能会是更重要的考量因素。



## 使用mappedbptree方案也是1个segment对应1个mappedbptree文件吗

是的，在 `mappedbptree` 方案中，**1 个 segment 仍然对应 1 个 mappedbptree 文件**，这与当前架构中 1 个 segment 对应 1 个 Puffin 文件的设计保持一致。

### 架构对应关系

```textile
当前架构 (V2 自研)                    mappedbptree 方案
─────────────────────────────────────────────────────────────

逻辑索引 (IndexGroup)
    │
    ├── Segment 0                        ├── Segment 0
    │   └── btree_order_id_seg0.puffin   │   └── btree_order_id_seg0.btree
    │       ├── Blob: lookup             │       (mappedbptree 文件)
    │       └── Blob: data page 0        │
    │       └── Blob: data page 1        │
    │
    ├── Segment 1                        ├── Segment 1
    │   └── btree_order_id_seg1.puffin   │   └── btree_order_id_seg1.btree
    │
    └── Segment 2                        └── Segment 2
        └── btree_order_id_seg2.puffin       └── btree_order_id_seg2.btree
```

**对应关系**：

- 1 个 Segment = 1 个 mappedbptree 文件 (`.btree` 或 `.idx`)

- 1 个逻辑索引 = 多个 Segment = 多个 mappedbptree 文件

- 文件命名：`{index_name}_seg{segment_id}_snap{snapshot_id}.btree`

### 为什么这样设计？

#### 1. 生命周期一致性

每个 Segment 是独立的、不可变的单元，对应一次全量或增量构建：

```rust
// Segment 元数据 (存储在 index_metadata.puffin 的 registry 中)
pub struct SegmentMetadata {
    pub segment_id: SegmentId,
    pub segment_file_path: String,        // 指向 .btree 文件
    pub coverage_files: BTreeSet<String>, // 覆盖的 Parquet 文件
    pub stale_files: BTreeSet<String>,    // 失效文件
    pub created_at_snapshot: SnapshotId,
    // ...
}
```

`segment_file_path` 指向的就是 `mappedbptree` 文件，而不是 Puffin 文件。

#### 2. 隔离性与独立性

- 每个 Segment 的 `mappedbptree` 文件是**完全独立**的

- 构建时互不干扰

- 查询时**并行加载**各个 Segment

- GC 时可以独立删除

#### 3. 与现有架构的兼容性

| 组件                   | V2 方案 (自研)                  | mappedbptree 方案                  |
| -------------------- | --------------------------- | -------------------------------- |
| **Segment 元数据**      | 存储在 `index_metadata.puffin` | 存储在 `index_metadata.puffin` (不变) |
| **Segment 物理文件**     | `.puffin` (内含查找表 + 数据页)     | `.btree` (mappedbptree 文件)       |
| **覆盖文件列表**           | `covered_data_files`        | `covered_data_files` (不变)        |
| **失效文件列表**           | `stale_files`               | `stale_files` (不变)               |
| **ArtifactStore 管理** | 通过 `ArtifactStore::put/get` | 通过 `ArtifactStore::put/get` (不变) |

**现有代码无需大幅修改**：

- `IndexRegistryStore`：不变，仍存储 segment 元数据

- `IndexLoader`：不变，仍按 segment 加载

- `IndexSearchCoordinator`：不变，仍遍历所有 segment 合并结果

- `ArtifactStore`：不变，仍负责文件的读写和删除

唯一变化的是：

1. `segment_file_path` 指向 `.btree` 而非 `.puffin`

2. `IndexPlugin` 的 `build` 和 `load` 方法使用 `mappedbptree` API

#### 4. 垃圾回收

与 V2 方案一致，`mappedbptree` 文件同样需要 GC：

```rust
// 删除 Segment 时
pub async fn drop_segment(segment: &SegmentMetadata) -> Result<()> {
    artifact_store.delete(&segment.segment_file_path).await?;
    // 删除 registry 中的条目
}
```

### 与 Puffin 文件的关系

`mappedbptree` 方案中，Puffin 文件仍然存在，但仅用于存储**注册表**（`index_metadata.puffin`），不再存储索引数据：

```textile
metadata/
├── index_metadata_v12.puffin              ← 注册表 (仅存 segment 元数据)
├── btree_order_id_seg0_snap1001.btree    ← mappedbptree 文件 (索引数据)
├── btree_order_id_seg1_snap1003.btree    ← mappedbptree 文件 (索引数据)
└── ivf_embedding_seg0_snap1001.puffin    ← 其他索引类型 (仍用 Puffin)
```

> 注意：其他索引类型（IVF、Bitmap 等）仍可使用 Puffin 文件，B-Tree 索引切换到 mappedbptree 方案。这体现了**插件化设计的灵活性**。

### 混合架构的可能性

如果某些场景下仍希望使用 Puffin 存储 B-Tree 数据，可以同时保留两种实现：

```rust
// 用户创建索引时指定
CREATE INDEX btree_idx ON orders USING btree_v2 (order_id);
CREATE INDEX btree_idx2 ON orders USING btree_mapped (order_id);

// 插件注册
registry.register(Arc::new(BTreePluginV2))?;          // Puffin 存储
registry.register(Arc::new(BTreePluginMapped))?;      // mappedbptree 存储
```

### 总结

| 维度                   | V2 方案 (自研)       | mappedbptree 方案 |
| -------------------- | ---------------- | --------------- |
| **1 Segment = 1 文件** | ✅ 是 (.puffin)    | ✅ 是 (.btree)    |
| **文件内容**             | 查找表 + 多个数据页 Blob | 单一 B+Tree 数据    |
| **与现有架构兼容**          | ✅ 完全兼容           | ✅ 完全兼容          |
| **文件管理**             | ArtifactStore    | ArtifactStore   |
| **GC 机制**            | 需要               | 需要              |

**结论**：`mappedbptree` 方案在架构层面上与现有设计完全一致，**1 个 segment 对应 1 个 mappedbptree 文件**，这是对现有 segment 管理模式的自然延续。区别仅在于索引数据的物理存储格式，而 segment 的生命周期管理、覆盖文件追踪、失效标记、GC 清理等机制保持不变。
