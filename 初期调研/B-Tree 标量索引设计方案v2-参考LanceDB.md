## Iceberg 自定义索引 — B-Tree 标量索引设计方案（v2）

> 基于 LanceDB B-Tree 设计，适配 Iceberg Puffin + segment 架构，替换现有 BTreePlugin



### 一、概述

为 Iceberg 表提供高性能标量索引，针对高基数列的等值、范围和 IN 查询。借鉴 LanceDB 的 **FlatIndex + BTreeLookup** 双层架构，利用 Puffin 文件的 Blob 存储数据页和查找表，复用现有 segment 管理和注册表机制。

#### 1.1 核心设计原则

- **磁盘存数据，内存建目录**：索引数据分页存储在 Puffin 文件中，内存中仅保留轻量级查找表（`BTreeLookup`）。

- **按需加载数据页**：查询时通过查找表定位候选页，仅加载必要的页，而非整个索引。

- **向量化精确过滤**：候选页加载为 Arrow RecordBatch，使用 Arrow 的 SIMD 计算进行精确过滤。

- **不可变文件 + 增量 segment**：每个 segment 对应一个 Puffin 文件，写入后不可变；增量构建生成新 segment，查询时合并多个 segment 的结果。

- **与现有架构无缝集成**：复用 `IndexPlugin` SPI、`ArtifactStore`、`IndexBatchStream`、`IndexLoader`、`ScalarSearchCoordinator` 等组件。

#### 1.2 与现有 BTreePlugin 的对比

| 维度    | 现有 BTreePlugin         | 新 BTreePlugin (本方案)     |
| ----- | ---------------------- | ----------------------- |
| 内存占用  | 全量加载，O(N)              | 查找表 O(√N)，数据页按需加载       |
| 适用规模  | < 1000 万行              | > 1000 万行，可扩展至数十亿行      |
| 序列化格式 | JSON (全量)              | Arrow IPC (分页) + 二进制查找表 |
| 查询方式  | `BTreeMap::range` 直接返回 | 定位候选页 → Arrow SIMD 过滤   |
| 增量构建  | 全量重建                   | 生成新 segment (与现有机制一致)   |

---

### 二、整体架构

#### 2.1 双层架构图

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                        B-Tree 索引架构 (Puffin 适配)                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        内存层 (Memory)                              │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │         BTreeLookup (轻量级 B-Tree)                           │ │   │
│  │  │         每页最大值 → (blob_offset, blob_length)                │ │   │
│  │  │         内存占用: ~2 MB / 10亿行 (Int64)                     │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │         数据页缓存 (LRU)                                      │ │   │
│  │  │         仅缓存最近使用的若干页                                │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        磁盘层 (Disk)                               │   │
│  │  ┌───────────────────────────────────────────────────────────────┐ │   │
│  │  │        Puffin 文件 (每个 segment 一个)                         │ │   │
│  │  │  ┌─────────────────────────────────────────────────────────┐  │ │   │
│  │  │  │  Blob 0: huawei.gauss-infra.btree.lookup-v1             │  │ │   │
│  │  │  │  (查找表: max_value → blob_offset/blob_length)          │  │ │   │
│  │  │  ├─────────────────────────────────────────────────────────┤  │ │   │
│  │  │  │  Blob 1: huawei.gauss-infra.btree.data-v1              │  │ │   │
│  │  │  │  (数据页 0: 4096 行)                                    │  │ │   │
│  │  │  ├─────────────────────────────────────────────────────────┤  │ │   │
│  │  │  │  Blob 2: huawei.gauss-infra.btree.data-v1              │  │ │   │
│  │  │  │  (数据页 1: 4096 行)                                    │  │ │   │
│  │  │  ├─────────────────────────────────────────────────────────┤  │ │   │
│  │  │  │  ...                                                   │  │ │   │
│  │  │  └─────────────────────────────────────────────────────────┘  │ │   │
│  │  └───────────────────────────────────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        注册表 (Registry)                            │   │
│  │  每个 segment 记录：                                                │   │
│  │  - segment_file_path: 指向 Puffin 文件                             │   │
│  │  - coverage_files: 覆盖的 Parquet 文件列表                         │   │
│  │  - key_type: 键类型 (Int64/Utf8/...)                              │   │
│  │  - page_size: 4096                                                │   │
│  │  - num_pages: 总页数                                              │   │
│  │  - total_rows: 总行数                                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```



#### 2.2 与 LanceDB 的对比

| 维度        | LanceDB B-Tree                          | 本方案 (Iceberg Puffin)                                                                         |
| --------- | --------------------------------------- | -------------------------------------------------------------------------------------------- |
| **索引文件**  | `page_lookup.lance` + `page_data.lance` | 单个 Puffin 文件，多个 Blob。puffin文件的第1个blob对应`page_lookup.lance`，后续blob对应`page_data.lance里的一个page` |
| **查找表存储** | `page_lookup.lance` (Lance 格式)          | `huawei.gauss-infra.lookup-v1` Blob                                                          |
| **数据页存储** | `page_data.lance` (Lance 格式)            | `huawei.gauss-infra.data-v1` Blob (多个)                                                       |
| **行地址**   | Lance 内部 row_id                         | `RowAddress { file_path, row_position }`                                                     |
| **分段管理**  | 单索引文件                                   | 多 segment 文件 (与现有架构一致)                                                                       |
| **覆盖追踪**  | Fragment bitmap                         | `covered_data_files` (registry)                                                              |

---





### 三、核心数据结构

#### 3.1 Blob 类型命名

- **查找表 Blob**: `huawei.gauss-infra.btree.lookup-v1`

- **数据页 Blob**: `huawei.gauss-infra.btree.data-v1`

#### 3.2 查找表 (`BTreeLookup`)

内存中的轻量级 B-Tree，存储每个数据页的最大值和对应的 Blob 位置。

```rust
/// 查找表条目 (内存)
#[derive(Debug, Clone)]
pub struct LookupEntry {
    /// 该页的最大键值 (用于二分查找)
    pub max_key: ScalarKey,
    /// 该页在 Puffin 文件中的 Blob 偏移
    pub blob_offset: u64,
    /// 该页的 Blob 长度 (压缩后)
    pub blob_length: u64,
}

/// B-Tree 查找表 (内存结构)
pub struct BTreeLookup {
    entries: Vec<LookupEntry>,  // 按 max_key 升序排列
    key_type: ScalarKeyType,
    page_size: usize,
}

impl BTreeLookup {
    /// 二分查找定位候选页索引范围
    /// 返回 [start, end) 索引区间
    pub fn locate(&self, query: &BTreeQuery) -> (usize, usize) {
        match query {
            BTreeQuery::Eq { value } => {
                let start = self.lower_bound(value);
                let end = self.upper_bound(value);
                (start, end)
            }
            BTreeQuery::Range { lo, hi } => {
                let start = self.lower_bound(lo);
                let end = self.upper_bound(hi);
                (start, end)
            }
            BTreeQuery::IsNull => {
                // null 值统一存储在开头或结尾，取决于排序规则
                // 这里简化，返回所有页 (或特殊处理)
                (0, self.entries.len())
            }
        }
    }

    fn lower_bound(&self, key: &ScalarKey) -> usize {
        self.entries.partition_point(|e| &e.max_key < key)
    }

    fn upper_bound(&self, key: &ScalarKey) -> usize {
        self.entries.partition_point(|e| &e.max_key <= key)
    }

    pub fn memory_bytes(&self) -> usize {
        self.entries.len() * (std::mem::size_of::<LookupEntry>() + self.key_type.size())
    }
}
```



#### 3.3 数据页 (`DataPage`)

每个数据页存储为 Arrow RecordBatch，包含两列：

```rust
/// 数据页 (磁盘存储格式)
/// 序列化为 Arrow IPC RecordBatch
pub struct DataPage {
    /// 键值列 (按升序排列)
    pub values: ArrayRef,
    /// 行地址列 (与 values 一一对应)
    pub row_ids: ArrayRef,
}

/// 每页固定行数
pub const BTREE_PAGE_SIZE: usize = 4096;
```



#### 3.4 键类型 (`ScalarKey`)

实现全序比较，支持 Int64、Int32、Utf8、Float64 等常见类型。

```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ScalarKey {
    Int64(i64),
    Int32(i32),
    Float64(f64),  // 注意浮点数比较可能产生 NaN 问题，需特殊处理
    Utf8(String),
    // 可扩展...
}

/// 从 Arrow 数组中提取键
impl ScalarKey {
    pub fn from_array(arr: &dyn Array, idx: usize) -> Result<Self> {
        // 实现...
    }
    pub fn to_arrow(&self) -> ArrayRef { /* 实现 */ }
    pub fn size(&self) -> usize { /* 实现 */ }
}
```



#### 3.5 构建参数 (`BTreeParameters`)

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BTreeParameters {
    /// 索引列名
    pub key_column: String,
    /// 每页行数 (默认 4096)
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    /// 文件列名 (由 source 注入)
    pub file_column: Option<String>,
    /// 位置列名
    pub position_column: Option<String>,
}

fn default_page_size() -> usize { 4096 }
```



---

### 四、索引构建流程

#### 4.1 构建流程图

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                      B-Tree 索引构建流程                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  1. 消费 IndexBatchStream     │
                      │  读取 key_column + _file +    │
                      │  _index_row_position           │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  2. 构建 (key, RowAddress)    │
                      │  收集所有行                    │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  3. 按 key 排序               │
                      │  ScalarKey 全序比较           │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  4. 分页 (Chunking)           │
                      │  每 BTREE_PAGE_SIZE 行一页    │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  5. 提取每页最大值            │
                      │  构建 BTreeLookup 元数据      │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  6. 写入 Puffin 文件          │
                      │  每个数据页为一个 Blob        │
                      │  Lookup 为独立 Blob           │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  7. 返回 CreatedIndex         │
                      │  artifact_files 包含 Puffin   │
                      │  completed_data_files 完整    │
                      └───────────────────────────────┘
```

#### 4.2 构建详细步骤 (伪代码)

```rust
async fn build(&self, context: &PluginContext, definition: &IndexDefinition, mut input: IndexBuildInput) -> Result<CreatedIndex> {
    let params = BTreeParameters::from_value(&definition.build_parameters)?;
    let input_files = input.data_files.iter().map(|f| f.file_path.clone()).collect::<BTreeSet<_>>();

    // Step 1: 收集所有 (key, RowAddress)
    let mut entries: Vec<(ScalarKey, RowAddress)> = Vec::new();
    while let Some(batch) = input.batches.next().await {
        let batch = batch?;
        let key_arr = batch.column_by_name(&params.key_column).ok_or(...)?;
        let file_arr = batch.column_by_name(params.file_column.as_deref().unwrap_or("_file")).ok_or(...)?;
        let pos_arr = batch.column_by_name(params.position_column.as_deref().unwrap_or("_index_row_position")).ok_or(...)?;
        // 提取每个 key, file_path, row_position
        for i in 0..batch.num_rows() {
            let key = ScalarKey::from_array(key_arr, i)?;
            let file_path = /* 从 file_arr 提取字符串 */;
            let row_position = /* 从 pos_arr 提取 u64 */;
            entries.push((key, RowAddress { file_path, row_position }));
        }
    }

    if entries.is_empty() {
        return Err(Error::InvalidDefinition("BTree build received no rows".into()));
    }

    // Step 2: 排序
    entries.sort_by(|a, b| a.0.cmp(&b.0));

    // Step 3: 分页
    let page_size = params.page_size;
    let total_rows = entries.len();
    let num_pages = (total_rows + page_size - 1) / page_size;

    // Step 4: 构建查找表条目 (先占位)
    let mut lookup_entries: Vec<LookupEntry> = Vec::with_capacity(num_pages);

    // Step 5: 写入 Puffin 文件
    let puffin_path = format!("{}/{}/btree-{}.puffin", context.artifact_root.trim_end_matches('/'), definition.index_id.0, Uuid::new_v4());
    let mut writer = PuffinWriter::new(...)?;

    let mut current_offset = 0u64;
    let mut blob_metadata = Vec::new();

    // 先写入所有数据页
    for (page_idx, chunk) in entries.chunks(page_size).enumerate() {
        let max_key = chunk.last().unwrap().0.clone();

        // 构建 RecordBatch
        let values: ArrayRef = chunk.iter().map(|(k, _)| k.to_arrow()).collect();
        let row_ids: ArrayRef = chunk.iter().map(|(_, addr)| addr.encode()).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("values", values.data_type().clone(), false),
            Field::new("row_ids", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![values, row_ids])?;

        // 序列化为 Arrow IPC (Zstd 压缩)
        let data = arrow_ipc::writer::write_batch(&batch)?;
        let compressed = zstd::encode_all(&data[..], 3)?;

        let blob_offset = current_offset;
        let blob_length = compressed.len() as u64;
        // 写入 Blob
        writer.write_blob(&compressed)?;
        current_offset += blob_length;

        // 记录查找表条目
        lookup_entries.push(LookupEntry {
            max_key: max_key.clone(),
            blob_offset,
            blob_length,
        });

        // 记录 Blob 元数据 (用于 Footer)
        blob_metadata.push(BlobMetadata {
            blob_type: "huawei.gauss-infra.btree.data-v1".to_string(),
            fields: vec![definition.field_ids[0]],
            offset: blob_offset,
            length: blob_length,
            compression_codec: Some("zstd".to_string()),
            properties: {
                let mut props = HashMap::new();
                props.insert("page_idx".to_string(), page_idx.to_string());
                props.insert("max_value".to_string(), max_key.to_string());
                props
            },
            snapshot_id: input.snapshot_id,
            sequence_number: 0,
        });
    }

    // 写入查找表 Blob
    let lookup_data = serialize_lookup(&lookup_entries)?;
    let lookup_compressed = zstd::encode_all(&lookup_data[..], 3)?;
    let lookup_offset = current_offset;
    let lookup_length = lookup_compressed.len() as u64;
    writer.write_blob(&lookup_compressed)?;

    blob_metadata.push(BlobMetadata {
        blob_type: "huawei.gauss-infra.btree.lookup-v1".to_string(),
        fields: vec![definition.field_ids[0]],
        offset: lookup_offset,
        length: lookup_length,
        compression_codec: Some("zstd".to_string()),
        properties: {
            let mut props = HashMap::new();
            props.insert("num_pages".to_string(), num_pages.to_string());
            props.insert("page_size".to_string(), page_size.to_string());
            props.insert("key_type".to_string(), format!("{:?}", entries[0].0));
            props
        },
        snapshot_id: input.snapshot_id,
        sequence_number: 0,
    });

    // 写入 Footer
    writer.finish()?;

    // Step 6: 返回 CreatedIndex
    Ok(CreatedIndex {
        implementation: BTREE_IMPLEMENTATION.to_string(),
        format_version: 1,
        algorithm_details: serde_json::to_value(BTreeAlgorithmDetails {
            key_type: format!("{:?}", entries[0].0),
            page_size,
            num_pages,
            total_rows,
        })?,
        artifact_files: vec![ArtifactFile { uri: puffin_path, size_bytes: current_offset, checksum: None }],
        indexed_rows: total_rows as u64,
        completed_data_files: input_files,
    })
}
```



#### 4.3 查找表序列化格式

查找表 Blob 的二进制格式 (Zstd 压缩前)：

```textile
┌──────────────────────────────────────────────────────────────┐
│  Magic: 0x4254 ("BT")  (2 bytes)                           │
│  Version: 1              (2 bytes)                         │
│  KeyType: u8             (1 byte)                          │
│  PageSize: u32          (4 bytes)                         │
│  NumPages: u32          (4 bytes)                         │
│  Reserved: [u8; 3]                                        │
├──────────────────────────────────────────────────────────────┤
│  Entries (num_pages 个):                                    │
│  └── for each entry:                                       │
│       ├── max_key: 变长 (根据 key_type)                    │
│       ├── blob_offset: u64 (8 bytes)                      │
│       └── blob_length: u64 (8 bytes)                      │
└──────────────────────────────────────────────────────────────┘
```



### 五、索引加载与缓存

#### 5.1 运行时索引 (`BTreeRuntimeIndex`)

```rust
pub struct BTreeRuntimeIndex {
    implementation: String,
    key_type: ScalarKeyType,
    page_size: usize,
    lookup: BTreeLookup,              // 常驻内存
    puffin_path: String,               // 用于按需加载数据页
    artifact_store: Arc<dyn ArtifactStore>,
    page_cache: Arc<RwLock<LruCache<usize, RecordBatch>>>,
    total_rows: u64,
}

impl BTreeRuntimeIndex {
    /// 加载指定页 (带缓存)
    async fn load_page(&self, page_idx: usize) -> Result<RecordBatch> {
        // 1. 查缓存
        if let Some(batch) = self.page_cache.write().unwrap().get(&page_idx) {
            return Ok(batch.clone());
        }

        // 2. 从查找表获取 offset/length
        let (offset, length) = self.lookup.get_page_location(page_idx)?;

        // 3. 通过 ArtifactStore 读取 Blob
        let data = self.artifact_store.get_range(&self.puffin_path, offset, length).await?;

        // 4. 解压 + 反序列化
        let decompressed = zstd::decode_all(&data[..])?;
        let batch = arrow_ipc::reader::read_batch(&decompressed)?;

        // 5. 写入缓存
        self.page_cache.write().unwrap().put(page_idx, batch.clone());
        Ok(batch)
    }
}
```



#### 5.2 `RuntimeIndex` 实现

```rust
#[async_trait]
impl RuntimeIndex for BTreeRuntimeIndex {
    fn kind(&self) -> IndexKind { IndexKind::Scalar }
    fn implementation(&self) -> &str { &self.implementation }
    async fn prewarm(&self) -> Result<()> {
        // 可选：预加载第一页或最常用页
        Ok(())
    }
    fn statistics(&self) -> Result<RuntimeIndexStatistics> {
        let mut props = BTreeMap::new();
        props.insert("key_type".to_string(), format!("{:?}", self.key_type));
        props.insert("page_size".to_string(), self.page_size.to_string());
        props.insert("num_pages".to_string(), self.lookup.len().to_string());
        props.insert("total_rows".to_string(), self.total_rows.to_string());
        props.insert("cache_entries".to_string(), self.page_cache.read().unwrap().len().to_string());
        let memory_bytes = self.lookup.memory_bytes() + self.page_cache.read().unwrap().iter().map(|(_, b)| b.get_memory_size()).sum::<usize>();
        Ok(RuntimeIndexStatistics {
            memory_bytes: Some(memory_bytes as u64),
            properties: props,
        })
    }
}
```



#### 5.3 `ScalarIndex` 实现

```rust
#[async_trait]
impl ScalarIndex for BTreeRuntimeIndex {
    async fn search(&self, request: &ScalarSearchRequest) -> Result<ScalarSearchResult> {
        // 1. 将 ScalarExpression 转换为 BTreeQuery
        let query = BTreeQuery::from_expression(&request.expression)?;

        // 2. 定位候选页索引范围
        let (start, end) = self.lookup.locate(&query);

        // 3. 并行加载候选页
        let mut handles = Vec::new();
        for page_idx in start..end {
            let this = self;
            handles.push(tokio::spawn(async move {
                this.load_page(page_idx).await
            }));
        }
        let mut batches = Vec::new();
        for handle in handles {
            let batch = handle.await??;
            batches.push(batch);
        }

        // 4. 合并候选页
        let combined = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;

        // 5. Arrow SIMD 精确过滤
        let filter_mask = build_filter_mask(&combined, &query)?;
        let filtered = arrow::compute::filter_record_batch(&combined, &filter_mask)?;

        // 6. 提取 RowAddress
        let row_ids_col = filtered.column_by_name("row_ids").unwrap();
        let row_ids_arr = row_ids_col.as_any().downcast_ref::<UInt64Array>().unwrap();
        let addresses: Vec<RowAddress> = row_ids_arr.iter().map(|&id| RowAddress::decode(id)).collect();

        Ok(ScalarSearchResult {
            addresses,
            is_exact: true,
        })
    }
}
```

---

## 六、查询流程

### 6.1 查询流程图

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                      B-Tree 查询流程                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  1. 解析查询条件               │
                      │  ScalarSearchRequest           │
                      │  (Eq / Range / In)             │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  2. BTreeLookup 定位候选页    │
                      │  O(log P) 二分查找            │
                      │  返回页索引列表                │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  3. 并行加载候选数据页         │
                      │  (从缓存或磁盘)               │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  4. Arrow SIMD 精确过滤        │
                      │  合并 RecordBatch 并 filter   │
                      └───────────────────────────────┘
                                      │
                                      ▼
                      ┌───────────────────────────────┐
                      │  5. 返回 RowAddress 列表      │
                      │  ScalarSearchResult           │
                      └───────────────────────────────┘
```

### 6.2 `ScalarIndex` 实现

```rust
#[async_trait]
impl ScalarIndex for BTreeRuntimeIndex {
    async fn search(&self, request: &ScalarSearchRequest) -> Result<ScalarSearchResult> {
        // 1. 构建 B-Tree 查询
        let query = BTreeQuery::from_expression(&request.expression)?;

        // 2. 定位候选页
        let candidate_pages = self.lookup.locate(&query);

        // 3. 并行加载候选页
        let mut batches = Vec::new();
        for page_idx in candidate_pages {
            let batch = self.load_page(page_idx).await?;
            batches.push(batch);
        }

        // 4. 合并所有候选页
        let combined = concat_batches(&batches)?;

        // 5. Arrow SIMD 精确过滤
        let filter_mask = build_filter_mask(&combined, &query)?;
        let filtered = filter_record_batch(&combined, &filter_mask)?;

        // 6. 提取 RowAddress 列表
        let addresses = extract_row_addresses(&filtered)?;

        Ok(ScalarSearchResult {
            addresses,
            is_exact: true,
        })
    }
}
```

### 6.3 查询类型支持

| 查询类型        | 示例                      | 候选页定位                        | 精确过滤                      |
| ----------- | ----------------------- | ---------------------------- | ------------------------- |
| **等值**      | `col = 42`              | `max >= 42` 的页               | `col == 42`               |
| **范围**      | `col BETWEEN 10 AND 20` | `max >= 10` 且 `min <= 20` 的页 | `col >= 10 AND col <= 20` |
| **IN**      | `col IN (1,2,3)`        | `max >= 1` 且 `min <= 3` 的页   | `col IN (1,2,3)`          |
| **IS NULL** | `col IS NULL`           | 直接扫描所有页或特殊处理                 | `col IS NULL`             |

---



## 七、与现有架构的集成

#### 7.1 插件注册

```rust
// iceberg-index-plugins/src/lib.rs

pub const BTREE_IMPLEMENTATION: &str = "huawei.gauss-infra.btree-v1";

pub fn register_builtin_plugins(registry: &PluginRegistry) -> Result<()> {
    registry.register(Arc::new(BTreePlugin))?;  // 新的 B-Tree
    registry.register(Arc::new(IvfPlugin))?;
    registry.register(Arc::new(ExactFakeVectorPlugin))?;
    // ... 不再注册旧版 BTreePlugin
    Ok(())
}
```



#### 7.2 注册表 (Registry) 中的 segment 条目

```json
{
  "segment_id": "seg0",
  "segment_file_path": "s3://.../indices/btree_order_id_seg0_snap1001.puffin",
  "coverage_files": ["part-00001.parquet", ...],
  "stale_files": [],
  "min_path_hash": "...",
  "max_path_hash": "...",
  "total_rows": 1024000,
  "source_snapshot_id": 1001,
  "index_type": "btree",
  "key_type": "Int64",
  "page_size": 4096,
  "num_pages": 250
}
```



---

### 八、性能预期

#### 8.1 内存占用

| 数据类型      | 查找表内存 (10亿行) | 单页内存   | 典型查询加载页数 |
| --------- | ------------ | ------ | -------- |
| `Int64`   | ~2 MB        | ~32 KB | 1-3 页    |
| `Utf8`    | ~4 MB        | ~64 KB | 1-3 页    |
| `Float64` | ~2 MB        | ~32 KB | 1-3 页    |

#### 8.2 查询延迟

| 查询类型                         | 候选页数   | 延迟 (估算)  |
| ---------------------------- | ------ | -------- |
| 等值 (`col = 42`)              | 1-2 页  | < 1 ms   |
| 范围 (`col BETWEEN 10 AND 20`) | 2-10 页 | 1-5 ms   |
| 大范围 (`col > 100`)            | 50% 页  | 10-50 ms |

#### 8.3 构建性能

| 数据量     | 构建时间 (估算) | Puffin 大小 (估算) |
| ------- | --------- | -------------- |
| 100 万行  | 2-5 秒     | ~20 MB         |
| 1000 万行 | 10-30 秒   | ~200 MB        |
| 1 亿行    | 2-5 分钟    | ~2 GB          |

---

### 九、总结

本方案借鉴 LanceDB B-Tree 的核心设计——查找表 + 分页数据，适配 Iceberg Puffin 存储格式和现有索引架构。关键特性：

- **内存可控**：查找表常驻内存（~2 MB/10亿行），数据页按需加载。

- **查询高效**：Arrow SIMD 向量化过滤，延迟毫秒级。

- **增量友好**：每次增量构建生成新 segment，与现有机制一致。

- **可扩展**：支持多种数据类型，页大小可配置。

- **可替换现有实现**：直接替换 `BTreePlugin`，无需兼容旧版。

该方案将大幅提升大规模标量列的索引性能，同时保持与 Iceberg 生态的深度集成。





# 问题

## 为什么statistics函数返回的是一个BTreeMap

`statistics` 的结果常被序列化为 JSON，存入 `index_metadata.puffin` 或用于系统表展示：

- 为什么用 **`BTreeMap` 而不是 `HashMap`**？  
  `BTreeMap` 是有序的（按 key 排序），确保每次序列化输出的 JSON 键顺序完全一致，方便版本对比和 diff 操作。`HashMap` 的迭代顺序是随机的，会导致元数据文件产生不必要的差异（即使内容未变）。



## huawei.gauss-infra.lookup-v1不能写在blob的metadata里吗

> 为什么不建议放在 Blob Metadata 中

### 1. **数据量问题**

```json
Blob Metadata 的位置：Puffin Footer JSON 中

对于 10 亿行数据：
- 页数 = 10亿 / 4096 ≈ 244,141 页
- 查找表条目数 ≈ 244,141
- 每个条目至少：max_value (8-16B) + offset (8B) + length (8B) = 24-32B
- 总大小 ≈ 244,141 × 32 ≈ 7.8 MB (未压缩)

将这个放入 Footer JSON 会导致：
- Footer 从 ~1KB 膨胀到 ~8MB
- 每次读取 Puffin 文件时都要解析这个大 JSON
- 查询时无法按需加载查找表（必须全量解析）
```

### 2. **Puffin 规范限制**

Puffin Footer 是 **UTF-8 JSON**，设计用于存储少量元数据（Blob 的 offset/length/type/properties），而非**大型结构化数据**。

### 3. **性能影响**

| 操作            | 查找表在 Blob Metadata | 查找表作为独立 Blob   |
| ------------- | ------------------ | -------------- |
| **读取 Footer** | 需解析 ~8MB JSON      | 解析 ~1KB JSON   |
| **首次查询**      | 必须加载全部             | 按需加载 (仅 1-2MB) |
| **内存占用**      | 常驻内存 (8MB)         | 可缓存管理          |

### 4. **Properties 值类型限制**

`properties` 是 `HashMap<String, String>`，无法直接存储结构化数组，必须序列化为 JSON 字符串或二进制编码，进一步增加解析开销。

---

### ✅ 推荐做法：作为独立 Blob

```json
// Footer JSON (小巧)
{
  "blobs": [
    {
      "type": "huawei.gauss-infra.lookup-v1",
      "fields": [1],
      "offset": 4,
      "length": 16384,
      "compression-codec": "zstd",
      "properties": {
        "num_pages": "244141",
        "page_size": "4096",
        "key_type": "Int64"
      }
    },
    {
      "type":huawei.gauss-infraee.data-v1",
      "fields": [1],
      "offset": 16388,
      "length": 1048576,
      "compression-codec": "zstd",
      "properties": {
        "page_idx": "0",
        "max_value": "999"
      }
    },
    // ... 更多数据页 Blob
  ]
}
```

**查找表作为独立 Blob 的优势**：

- Footer 保持轻量（仅含摘要信息）

- 查找表可按需加载（首次查询时加载，或 `prewarm` 预加载）

- 查找表可压缩存储（Zstd 压缩）

- 与现有 Puffin 设计模式一致

---

### 📊 对比总结

| 方案                   | Footer 大小 | 加载速度 | 内存占用     | 可维护性 |
| -------------------- | --------- | ---- | -------- | ---- |
| **放在 Blob Metadata** | 大 (8MB+)  | 慢    | 全量常驻     | 差    |
| **作为独立 Blob**        | 小 (1KB)   | 快    | 可控 (可缓存) | 好    |

---

### 💡 结论

`huawei.gauss-infra.lookup-v1` **必须作为独立的 Blob 存储**，Footer 中仅存放 `num_pages`、`page_size`、`key_type` 等摘要信息。这样做符合 Puffin 设计理念，也提供了更好的性能和扩展性。

## 不能放在index_metadata.puffin里吗

### 为什么不建议？

1. **职责错位**
   
   - `index_metadata.puffin` 回答的问题是：“这个表有哪些索引？每个索引有哪些段？”
   
   - B-Tree 查找表回答的问题是：“在**这个具体的段**中，如何快速定位某个键值所在的页？”  
   将查找表塞进注册表，违背了关注点分离原则。

2. **文件膨胀**  
   对于大索引，查找表可能达到数 MB。若将其放入 `index_metadata.puffin`，该文件会随着索引数量和数据量增长而急剧膨胀，而它本应保持轻量（几 KB 到十几 KB）。

3. **更新频率不同**
   
   - `index_metadata.puffin` 在**每次索引状态变更**（新增段、标记失效、重建）时重写。
   
   - B-Tree 查找表在**段创建时**写入，之后只读，**从不修改**。  
   若将查找表放入注册表，每次新增段时都要重写整个注册表文件，但查找表本身并不需要被修改。

4. **加载时机不同**
   
   - `index_metadata.puffin` 在查询规划阶段加载，用于发现有哪些索引可用。
   
   - B-Tree 查找表仅在**实际使用该段**时加载（按需）。  
   若将查找表放入注册表，查询规划时就必须加载所有段的查找表，即使最终可能用不上，造成不必要的 I/O。

5. **独立 Blob 更灵活**
   
   - 查找表作为独立 Blob，可以单独压缩、单独缓存、按需加载。
   
   - 无需重写整个 `index_metadata.puffin` 即可更新查找表（实际上查找表从不更新，只读）。

---

### ✅ 推荐结构

| 文件                      | 内容                  | 更新方式      |
| ----------------------- | ------------------- | --------- |
| `index_metadata.puffin` | 索引注册表（段路径、覆盖文件、状态）  | 每次索引变更时重写 |
| `{segment}.puffin`      | 查找表 Blob + 数据页 Blob | 写入一次，永不修改 |

这样，`index_metadata.puffin` 保持轻量，查找表按需加载，两者各司其职。

## 文件布局对比

### 文件布局示例

```textile
┌─────────────────────────────────────────────────────────────────┐
│                    btree_order_id_seg0_snap1001.puffin          │
├─────────────────────────────────────────────────────────────────┤
│  Magic (4B) = "PFA1"                                            │
├─────────────────────────────────────────────────────────────────┤
│  Blob 0: huawei.gauss-infra.lookup-v1                            │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  对应 LanceDB 的 page_lookup.lance                        │  │
│  │  存储：每页最大值 → (blob_offset, blob_length) 的映射    │  │
│  │  大小：~2 MB (10亿行)                                     │  │
│  │  压缩：Zstd                                               │  │
│  └───────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Blob 1huawei.gauss-infraee.data-v1                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  对应 LanceDB 的 page_data.lance 中的 Page 0              │  │
│  │  存储：4096 行 (values + row_ids)                         │  │
│  │  大小：~32 KB                                             │  │
│  │  压缩：Zstd                                               │  │
│  └───────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Blobhuawei.gauss-infratree.data-v1                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  对应 LanceDB 的 page_data.lance 中的 Page 1              │  │
│  └───────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  ...                                                           │
├─────────────────────────────────────────────────────────────────┤
│  Blhuawei.gauss-infra.btree.data-v1                             │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  对应 LanceDB 的 page_data.lance 中的 Page N-1            │  │
│  └───────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│  Footer (JSON)                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  记录每个 Blob 的 offset, length, type, properties        │  │
│  │  properties 包含：page_idx, max_value, 等摘要信息         │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 与 LanceDB 文件对比

| LanceDB                      | 本方案 (Puffin)                                | 说明               |
| ---------------------------- | ------------------------------------------- | ---------------- |
| `page_lookup.lance`          | **Blob 0** (`huawei.gauss-infra.lookup-v1`) | 查找表（每页最大值 → 页位置） |
| `page_data.lance` (Page 0)   | **Blob 1** (`huawei.gauss-infra.data-v1`)   | 数据页 0（4096 行）    |
| `page_data.lance` (Page 1)   | **Blob 2** (`huawei.gauss-infra.data-v1`)   | 数据页 1（4096 行）    |
| ...                          | ...                                         | ...              |
| `page_data.lance` (Page N-1) | **Blob N** (`huawei.gauss-infra.data-v1`)   | 数据页 N-1（4096 行）  |

---

### 关键对应关系

| 组件       | LanceDB                      | 本方案                                        |
| -------- | ---------------------------- | ------------------------------------------ |
| **文件格式** | Lance 格式（列式）                 | Puffin 格式（Blob 容器）                         |
| **查找表**  | `page_lookup.lance` 独立文件     | `huawei.gauss-infra.lookup-v1` Blob（第 1 个） |
| **数据页**  | `page_data.lance` 单个文件（内部分页） | 每个数据页一个独立 Blob（第 2..N 个）                   |
| **分页大小** | 4096 行                       | 4096 行（一致）                                 |
| **行地址**  | Lance 内部 row_id              | `RowAddress { file_path, row_position }`   |
| **压缩**   | Lance 列式压缩                   | Zstd（每个 Blob 独立压缩）                         |

---

### ✅ 设计确认

主要设计决策已梳理如下：

1. **每个 B-Tree segment 对应一个独立的 Puffin 文件**

2. **文件内的第 1 个 Blob 是查找表**（`huawei.gauss-infra.lookup-v1`）

3. **后续每个 Blob 是一个数据页**（`huawei.gauss-infra.data-v1`）

4. **每个数据页固定 4096 行**

5. **Footer 存储所有 Blob 的元数据**（offset、length、properties 等）

6. **查找表和数据页均使用 Zstd 压缩**（这个应该可选？）
