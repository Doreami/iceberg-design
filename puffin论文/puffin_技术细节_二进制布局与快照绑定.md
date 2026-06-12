# Puffin 索引技术深入：二进制布局 & 快照绑定机制

> 基于论文 "Puffin-Backed Vector Indexes" (arXiv:2606.04196v1) 第4节和第2.1/3.2节的详细技术解读。

---

## 第一部分：Snapshot Summary 作为绑定机制

### 1.1 Iceberg 快照模型回顾

Apache Iceberg 的表由一条**不可变快照链（chain of immutable snapshots）**描述。每个快照引用一个 manifest list，manifest list 再引用 manifest 文件，manifest 文件最终描述该快照下存活的数据文件（Parquet）。快照提交在 REST Catalog 层通过**乐观并发控制（OCC）**实现原子性。

关键结构关系：

```
Table
 └── Snapshot S_n (不可变)
      ├── manifest list
      │    └── manifest files → Parquet 文件列表
      └── summary (key-value map, 自由格式)
           ├── "operation" = "append"
           ├── "added-files-size" = "..."
           ├── "statistics-file" = "s3://bucket/.../ann-embedding_idx-snap-S_n.puffin"  ← 关键属性
           └── ...
```

### 1.2 `statistics-file` 属性：Puffin 与快照之间的唯一纽带

论文设计的核心洞察是：**Puffin 文件与快照之间通过 snapshot summary 中的单个属性 `statistics-file` 绑定**。

这个属性原本是 Puffin 规范的一部分，用于指向包含表统计信息（Theta sketches、Bloom filters）的 Puffin 文件。但 Puffin 格式刻意设计为**类型可扩展**——每个 blob 携带一个 opaque 类型字符串，引擎只读取它理解的类型，**未知类型被静默忽略**。论文正是利用这个扩展点来承载 ANN 索引。

```json
// Snapshot summary 示例
{
  "operation": "append",
  "statistics-file": "s3://warehouse/db/table/metadata/ann-embedding_idx-snap-42.puffin"
}
```

### 1.3 绑定机制带来的六大免费属性

绑定机制的精妙之处在于：**不需要实现任何新的生命周期管理**。ANN 索引完全继承 Iceberg 现有的六项能力：

```
绑定机制 (一行 summary 属性)
    │
    ├── 1. 原子性 (Atomicity)
    │      Puffin 文件的指向与数据 commit 在同一个 OCC 事务中。
    │      读取者要么看到 (数据_snap_N + 索引_snap_N)，要么看到
    │      (数据_snap_{N-1} + 索引_snap_{N-1})，不会出现
    │      "新数据 + 旧索引"的不一致状态。
    │
    ├── 2. 时间旅行 (Time Travel)
    │      查询 `SELECT ... AS OF '<snapshot_id>'` 时，
    │      引擎自动读取该快照对应的 `statistics-file`，
    │      从而使用该版本的 ANN 索引。无需额外代码。
    │
    ├── 3. 多引擎可读性 (Multi-engine Readability)
    │      任意 Iceberg 兼容引擎读取快照时都能发现 statistics-file。
    │      如果它不理解 `flockdb-ann-index-v1` blob 类型，
    │      它会静默跳过——不影响读取 Parquet 数据本身。
    │
    ├── 4. 孤儿文件回收 (Orphan-file GC)
    │      当旧快照过期时，旧 Puffin 文件不再被任何快照引用，
    │      Iceberg 现有的孤儿文件清理机制自动将其删除。
    │      索引清理 = 快照过期，零新代码。
    │
    ├── 5. 乐观并发控制 (OCC)
    │      两个 coordinator 同时刷新索引 → 一个在 REST Catalog
    │      提交成功，另一个检测到冲突必须重试。与数据写入的
    │      并发语义完全一致。
    │
    └── 6. 元数据仅提交 (Metadata-only Commit)
    │      增量刷新后的新 Puffin 文件写入新 S3 对象，
    │      快照 summary 更新指向新路径。数据 manifest 完全不变。
    │      这是 Iceberg REST 规范原生支持的 `set-properties` 操作。
```

### 1.4 绑定-解绑的生命周期

```
        CREATE INDEX                   REFRESH INDEX                  DROP INDEX
            │                              │                              │
            ▼                              ▼                              ▼
    ┌───────────────┐   时间流逝    ┌───────────────┐            ┌───────────────┐
    │ Snapshot S_n  │ ────────────► │ Snapshot S_m  │ ─────────► │ Snapshot S_p  │
    │ statistics-   │   数据追加    │ statistics-   │            │ statistics-   │
    │ file: puffin_1│              │ file: puffin_2│            │ file: null    │
    └───────────────┘              └───────────────┘            └───────────────┘
            │                              │                              │
            ▼                              ▼                              ▼
    puffin_1 被引用               puffin_1 变为孤儿             puffin_2 变为孤儿
                                  (等待 GC 清理)               (等待 GC 清理)
```

### 1.5 Puffin 文件本身的物理特性

Puffin 容器的二进制结构(这是理解"为什么适合存放 ANN 索引"的基础):

```
┌──────────────────────────────────────┐
│  Magic: "PFA1" (4 bytes)             │  ← 文件头
├──────────────────────────────────────┤
│  Blob 0 payload (routing, 几 MB)      │
├──────────────────────────────────────┤
│  Blob 1 payload (shard 0, 60-250 GB) │  ← 拼接的 blob 载荷序列
├──────────────────────────────────────┤
│  Blob 2 payload (shard 1, 60-250 GB) │
├──────────────────────────────────────┤
│  ...                                 │
├──────────────────────────────────────┤
│  Footer (UTF-8 JSON):                │  ← 描述每个 blob 的元数据
│  {                                   │
│    "blobs": [                        │
│      {                               │
│        "type": "flockdb-ann-routing",│
│        "fields": [7],                │
│        "offset": 4,                  │  ← 字节偏移量
│        "length": 3145728,            │  ← 字节长度
│        "compression": "zstd",        │
│        "properties": {               │
│          "algorithm": "diskann",     │
│          "snapshot_id": "42"         │
│        }                             │
│      },                              │
│      {                               │
│        "type": "flockdb-ann-index",  │
│        "offset": 3145732,            │
│        "length": 64424509440,        │
│        ...                           │
│      },                              │
│      ...                             │
│    ]                                 │
│  }                                   │
├──────────────────────────────────────┤
│  Footer length (4 bytes LE)          │  ← 文件尾
│  Flags (4 bytes)                     │
│  Magic: "PFA1" (4 bytes)             │
└──────────────────────────────────────┘
```

**关键特性**：
- **Footer 很小（几 KB）**：Coordinator 可以通过 HTTP Range Request 只下载文件尾，解析 JSON 获取所有 blob 的偏移和长度
- **每个 blob 独立压缩**（zstd / lz4）：Shard blob 可单独解压
- **字节寻址**：Executor 通过 `Range: bytes=<offset>-<offset+length-1>` 只下载属于自己的 shard blob，无需下载整个 PB 级 Puffin 文件

---

## 第二部分：Puffin 内部的 Sharded Graph 二进制布局

### 2.1 总体策略：N+1 Blob 布局

论文定义了三类新的 Puffin blob 类型：

| Blob 类型 | 用途 | 大小量级 | 读取者 |
|-----------|------|---------|--------|
| `flockdb-ann-routing-v1` | 路由元数据：码书、分片映射、覆盖文件列表 | 几 MB | Coordinator + 所有 Executor（每次探测都读） |
| `flockdb-ann-index-v1` | Vamana 图分片 | 60–250 GB / 分片 | 仅所属 Executor |
| `flockdb-ann-centroid-v1` | 文件级质心索引（小型表） | ~30 MB（压缩后 8–15 MB） | 仅 Coordinator |

**一个完整的 sharded 索引 = 1 个 Puffin 文件，内含 1 个 routing blob + N 个 shard blob**：

```
ann-embedding_idx-snap-42.puffin
├── Blob 0: flockdb-ann-routing-v1   ← 每个探测都读
├── Blob 1: flockdb-ann-index-v1 (shard 0, owned by executor 0)
├── Blob 2: flockdb-ann-index-v1 (shard 1, owned by executor 1)
├── Blob 3: flockdb-ann-index-v1 (shard 2, owned by executor 2)
└── Blob 4: flockdb-ann-index-v1 (shard 3, owned by executor 3)
```

### 2.2 Shard Blob 的线格式详解 (`flockdb-ann-index-v1`)

每个 shard blob 内部按以下严格顺序排列：

```
┌──────────────────────────────────────────────────────────────┐
│                     SECTION 1: Header (固定)                  │
├──────────────────────────────────────────────────────────────┤
│  magic         : [4B]  "DANN" (0x44414E4E)                   │
│  version       : [4B]  uint32, 当前 = 1                       │
│  dimensions    : [4B]  uint32, 向量维度数 D (如 768)          │
│  vector_count  : [8B]  uint64, 此 shard 中的向量数 N          │
│  graph_degree  : [4B]  uint32, Vamana 图度数 R (如 64)       │
│  beam_width    : [4B]  uint32, 搜索 beam width L (如 100)    │
│  medoid_id     : [8B]  uint64, 图中心点（搜索起点）            │
│  pq_m          : [4B]  uint32, PQ 子量化器数 (如 48)          │
│  pq_nbits      : [4B]  uint32, 每子量化器编码位数 (如 8)      │
│  metric        : [1B]  uint8,  距离度量 (0=L2, 1=IP, ...)    │
│  reserved      : [23B] 对齐填充                               │
│  TOTAL HEADER  : 64 bytes                                     │
├──────────────────────────────────────────────────────────────┤
│                   SECTION 2: PQ Codebook                       │
├──────────────────────────────────────────────────────────────┤
│  用于内存中距离近似的量化码书。                                  │
│                                                                 │
│  结构: K × m 个质心，每个 D/m 维 float32                        │
│    K = 2^pq_nbits = 256 个聚类中心                              │
│    m = pq_m = 48 个子空间                                       │
│    每子空间维度 = D/m = 768/48 = 16 维                          │
│                                                                 │
│  Codebook 大小 = K × m × (D/m) × 4 bytes                       │
│                = 256 × 48 × 16 × 4                             │
│                = 786,432 bytes ≈ 768 KB                        │
│                                                                 │
│  布局: 按子空间分组排列                                         │
│    [subspace_0_centroids[256][16]] [subspace_1_centroids...]   │
│                                                                 │
│  查询时，整个 codebook 加载到 RAM（每 shard 仅 768 KB）          │
│  PQ 距离计算: 对查询向量各子空间查表 → O(m × K) 查表 +        │
│                O(m) 加法 = 极快的距离近似                       │
├──────────────────────────────────────────────────────────────┤
│              SECTION 3: Adjacency Offset Table                │
├──────────────────────────────────────────────────────────────┤
│  长度为 N+1 的 uint64 数组（前缀和偏移），                       │
│  使得节点 i 的邻接表 = data[offset[i] : offset[i+1]]            │
│                                                                 │
│  offset[0]     : [8B]  总是 0                                   │
│  offset[1]     : [8B]  节点 0 的邻接表结束位置                   │
│  offset[2]     : [8B]  节点 1 的邻接表结束位置                   │
│  ...                                                            │
│  offset[N]     : [8B]  邻接表总字节数                            │
│                                                                 │
│  大小 = (N + 1) × 8 bytes                                       │
│   250M 向量 → 约 2 GB                                           │
├──────────────────────────────────────────────────────────────┤
│              SECTION 4: Adjacency Lists (压缩)                 │
├──────────────────────────────────────────────────────────────┤
│  节点邻接关系的连接序列。每个节点：                              │
│    degree   : [varint]  该节点的邻居数 (通常 ≤ R=64)            │
│    neighbors: [varint × degree]  邻居节点 ID（Vamana 有向边）   │
│                                                                 │
│  Varint 编码: 小 ID 用 1 字节，大 ID 用多字节                    │
│  整体经过 zstd 压缩块。                                         │
│                                                                 │
│  典型大小: N × R × ~3 bytes (varint 平均) × zstd 压缩率         │
│    250M × 64 × 3 × 0.5 ≈ 24 GB                                 │
│                                                                 │
│  搜索行为: 从 medoid 出发，按邻接表跳转，                         │
│  用 PQ 距离近似引导贪心 beam search。                            │
│  每次查询约访问 500–1000 个节点。                                │
├──────────────────────────────────────────────────────────────┤
│       SECTION 5: Full-precision Vectors (可选，可配置)          │
├──────────────────────────────────────────────────────────────┤
│  N 个原始的 float32 向量，用于 Stage B 精确重排。                │
│                                                                 │
│  大小 = N × D × 4 bytes                                         │
│    250M × 768 × 4 = 768 GB (!)                                  │
│                                                                 │
│  这是 shard blob 的**主导存储成本**。                            │
│                                                                 │
│  保留策略 (可配置):                                              │
│    - 包含模式: blob 自带完整向量 → 重排阶段零 I/O                │
│      shard 大小: 60 GB (图) + 768 GB (向量) ≈ 820 GB           │
│    - 省略模式: 重排时从 Parquet 按需读取向量                     │
│      shard 大小: 仅 60 GB (图)                                  │
│      代价:  重排阶段多一次 S3 round-trip                        │
│                                                                 │
│  论文推荐的折中: 热表用包含模式（追求延迟），                      │
│  冷表用省略模式（节省存储）。                                    │
├──────────────────────────────────────────────────────────────┤
│          SECTION 6: Vector-ID-to-Location Map                  │
├──────────────────────────────────────────────────────────────┤
│  将每个向量映射回 (Parquet文件, Row Group, 行偏移) 三元组。      │
│                                                                 │
│  按 vector_id 排序后 delta 编码:                                 │
│    - 文件路径引用 routing blob 的路径表索引（varint）             │
│    - row_group_id 和 row_offset 也用 delta 编码                  │
│                                                                 │
│  大小 ≈ N × (平均条目大小)                                       │
│    假设每条目约 12 bytes (压缩后),                               │
│    250M × 12 = 3 GB                                             │
│                                                                 │
│  用途: Stage B 精确重排时，Coordinator 用此映射                    │
│  告知每个 Executor 需要读取哪些 Parquet 文件的哪些 row group      │
│  ________________________________________________________________│
│                      SECTION 2-6 layout summary (reordered)      │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Section            │ Purpose                │ Size (est.)   │ │
│  ├─────────────────────────────────────────────────────────────┤ │
│  │ Header             │ Metadata                │ 64 B         │ │
│  │ PQ Codebook        │ Fast approx. distance   │ 768 KB       │ │
│  │ Offset Table       │ O(1) neighbor lookup    │ 2 GB         │ │
│  │ Adjacency (zstd)   │ Graph traversal         │ ~24 GB       │ │
│  │ Full Vecs (opt.)   │ Exact rerank            │ 768 GB       │ │
│  │ ID→Location Map    │ Parquet row lookup      │ ~3 GB        │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Lean 模式 total: ~30 GB (不含 full vectors，码书和邻接表为主)    │
│  Full 模式 total: ~800 GB                                        │
└──────────────────────────────────────────────────────────────────┘
```

### 2.3 Routing Blob 的线格式 (`flockdb-ann-routing-v1`)

Routing blob 小而关键，是分布式探测的"目录"：

```
┌──────────────────────────────────────────────────────┐
│  magic          : [4B]  "ANNR"                        │
│  version        : [4B]  uint32                        │
│  num_shards     : [4B]  uint32, 分片数 N              │
│  base_snapshot  : [8B]  uint64, 索引构建时的快照 ID    │
│  algorithm      : [16B] UTF-8, "diskann" 或 "hnsw"    │
│  dimensions     : [4B]  uint32                        │
│  metric         : [1B]  uint8                         │
│  reserved       : [23B]                               │
├──────────────────────────────────────────────────────┤
│  Per-shard entries (N 条):                            │
│    shard_id          : [4B]  uint32                   │
│    blob_offset       : [8B]  uint64 (在 Puffin 中的偏移)│
│    blob_length       : [8B]  uint64                   │
│    vector_count      : [8B]  uint64                   │
│    tombstone_ratio   : [4B]  float32 (删除比例)       │
│    centroid          : [D×4B] float32[]  (该分片的质心)│
│    covered_files     : [varint count][varint refs...] │
│                        (指向下方路径表的索引)           │
├──────────────────────────────────────────────────────┤
│  File path table:                                    │
│    num_paths         : [4B]  uint32                  │
│    paths             : [长度前缀 UTF-8] × num_paths  │
├──────────────────────────────────────────────────────┤
│  Centroids codebook (K × D float32), zstd 压缩       │
│  用于 Stage 0 的 IVF 式向量分配                      │
└──────────────────────────────────────────────────────┘

总大小: 数 MB（主要是 codebook），单次探测全部读入内存
```

Routing blob 的三种关键作用：

1. **Shard 发现**：Coordinator 解析 footer JSON → 找到 routing blob → 获知 N 个 shard 的字节偏移 → 每个 executor 只需下载自己的 shard
2. **增量刷新的基快照**：`base_snapshot_id` 告诉 RE︎FRESH INDEX 操作与哪个快照做 diff
3. **Tombstone 监控**：`tombstone_ratio` 超过 20% 时触发该分片的局部重建

### 2.4 Centroid Index Blob (`flockdb-ann-centroid-v1`)

用于 coordinator 本地探测的小型索引（文件级剪枝）：

```
┌──────────────────────────────────────────────────────┐
│  magic             : [4B]  "ANNI"                     │
│  version           : [4B]  uint32                     │
│  dimensions        : [4B]  uint32                     │
│  entry_count       : [4B]  uint32, 质心条目数          │
│  file_count        : [4B]  uint32, 文件数              │
│  metric            : [1B]  uint8                      │
│  entry_size        : [4B]  uint32, 每条目字节数        │
│  paths_offset      : [8B]  uint64, 路径表在 blob 中的偏移│
├──────────────────────────────────────────────────────┤
│  Entries (entry_count 条, 每条 entry_size 字节):      │
│    centroid     : D × float32  (768×4 = 3072 bytes)   │
│    file_index   : uint32      (路径表的索引)           │
│    max_distance : float32     (质心到文件中最远向量的距离)│
│                                                        │
│    每条目 = D×4 + 8 bytes                              │
│    10K 文件 × 768 维 = 10K × 3080 ≈ 30.8 MB           │
├──────────────────────────────────────────────────────┤
│  File paths table (长度前缀 UTF-8):                    │
│    num_paths     : [4B]  uint32                       │
│    path_0_len    : [4B]  uint32                       │
│    path_0        : [N bytes] UTF-8                    │
│    path_1_len    : [4B]  uint32                       │
│    ...                                                │
└──────────────────────────────────────────────────────┘

整体 zstd 压缩后: 8–15 MB

查询语义（阈值查询剪枝）:
  for each file entry:
    if distance(query, centroid) - max_distance > threshold:
        skip this file   ← 安全剪枝（保证无假阴性）
```

`max_distance` 字段是**精确剪枝的关键**：质心到文件中最远向量的距离。如果 `distance(query, centroid) - max_distance > threshold`，则该文件中不可能存在满足阈值的向量，可安全跳过。这是论文中唯一在 Coordinator 端执行的断言式剪枝。

### 2.5 二进制布局的整体设计原则总结

```
┌─────────────────────────────────────────────────────────────────┐
│                    设计决策          │     设计目的                │
├─────────────────────────────────────────────────────────────────┤
│  Footer 中的 byte offset + length   │  只下载需要的 blob,         │
│  支持 HTTP Range Request            │  不下载整个 Puffin 文件     │
├─────────────────────────────────────────────────────────────────┤
│  Routing blob 与 Shard blob 分离    │  Routing 小(几MB),          │
│                                     │  每次探测都读;              │
│                                     │  Shard 大(60+GB),           │
│                                     │  仅所属 executor 读          │
├─────────────────────────────────────────────────────────────────┤
│  邻接表用 varint + zstd 压缩        │  图遍历是查询的最热路径,     │
│                                     │  压缩率与解压速度的折中      │
├─────────────────────────────────────────────────────────────────┤
│  PQ codebook 嵌入 blob 头部         │  探测时一次加载到 RAM,       │
│                                     │  供所有查询共享 (每分片一次) │
├─────────────────────────────────────────────────────────────────┤
│  Full vector section 可选且可配置    │  热表: 包含 → 零重排 I/O    │
│                                     │  冷表: 省略 → 节省 90% 存储  │
├─────────────────────────────────────────────────────────────────┤
│  Vector→Location map 按 ID 排序 +   │  支持 Stage B 高效定位       │
│  delta 编码                         │  Parquet row group          │
├─────────────────────────────────────────────────────────────────┤
│  Tombstone bitmap + ratio 上报      │  避免昂贵的节点级图修复,     │
│                                     │  >20% 才触发分片重建         │
└─────────────────────────────────────────────────────────────────┘
```

### 2.6 与 Puffin 规范的关系

Puffin 规范对 blob 载荷内容**不做任何约束**——它就是一把字节。论文所做的工作是：

1. **定义了三个合法的 `type` 字符串**：`flockdb-ann-centroid-v1`、`flockdb-ann-index-v1`、`flockdb-ann-routing-v1`
2. **在这些 blob 的 payload 中放置了自描述的二进制结构**（以 magic number 开头，版本号紧随其后，以便未来演进）
3. **利用 Puffin 的 `properties` 和 `fields` 元数据**传递索引参数（算法名、快照ID、向量列字段ID），确保与 Iceberg schema 的正确关联

```
Puffin blob envelope (Puffin 规范负责):
  {
    "type": "flockdb-ann-index-v1",    ← Puffin 层: 类型标识
    "fields": [7],                     ← Puffin 层: 关联的 Iceberg 列
    "offset": 3145732,                 ← Puffin 层: 物理寻址
    "length": 64424509440,             ← Puffin 层: 物理寻址
    "compression": "zstd",             ← Puffin 层: 传输压缩
    "properties": {                    ← Puffin 层: 扩展元数据
      "algorithm": "diskann",
      "snapshot_id": "42"
    },
    "payload": "<bytes>"  ──────────►  blob payload (论文定义):
                                        magic "DANN" → header →
                                        PQ codebook → offset table →
                                        adjacency → vectors? →
                                        ID map
  }
```

这种分层设计意味着：**Puffin 规范不需要为 ANN 索引做任何修改**。只要 blob type 字符串不冲突，任意引擎可以并存任意种类的派生数据。

