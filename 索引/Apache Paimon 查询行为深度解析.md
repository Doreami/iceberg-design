# Apache Paimon 查询行为深度解析

> **文档版本**：v1.0 | **更新日期**：2026-08-20 | **适用版本**：Apache Paimon 0.9+
> 
> **说明**：本文档基于 Apache Paimon 官方文档、PIP（Paimon Improvement Proposal）及社区 PR 整理，深入解析 Paimon 的 Global Index 体系、索引物理存储、查询执行计划及向标混合查询机制。

## 目录

1. 概述

2. 索引架构总览

3. 标量索引（Scalar Index）

4. 向量索引（Vector Index）

5. 全文索引（Full-Text Index）

6. 混合查询实现

7. 索引生命周期管理

8. 性能调优指南

9. 总结与限制

## 1. 概述

Apache Paimon 是一个流批一体的数据湖表格式，其 **Global Index（全局索引）** 体系为 Append-Only 表提供了高效的**行级索引能力**，无需全表扫描即可实现快速的标量过滤、向量检索和全文搜索  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)。

Paimon 的索引设计遵循 **“索引与数据分离”** 的架构理念：

- **索引独立存储**：索引文件与数据文件解耦，可独立创建、删除和管理  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)

- **基于 Row ID 的关联**：索引通过 `_ROW_ID` 系统列来定位数据行

- **快照版本化管理**：索引与 Paimon 快照绑定，支持增量构建和时间旅行

- **异步构建**：索引构建通过 Flink 批作业异步执行，不阻塞数据写入

**Global Index 的核心定位**：为 Data Evolution（Append-Only）表提供行级查找与过滤能力，支持 BTree、Bitmap、Vector、Full-Text 四种索引类型  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)。

## 2. 索引架构总览

### 2.1 Global Index 体系

Paimon 的 Global Index 是一个独立的索引子系统，专为 Append-Only 表设计  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)。

**核心前置条件**：

| 表属性                      | 值      | 说明                                                                                                                    |
| ------------------------ | ------ | --------------------------------------------------------------------------------------------------------------------- |
| `bucket`                 | `-1`   | 无感知桶模式（unaware-bucket mode）  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index) |
| `row-tracking.enabled`   | `true` | 启用行级跟踪，添加 `_ROW_ID` 隐藏列  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)     |
| `data-evolution.enabled` | `true` | 支持增量索引构建和长期维护  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)               |
| `global-index.enabled`   | `true` | 启用全局索引功能  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)                    |

### 2.2 索引类型总览

Paimon 支持四种全局索引类型  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-  [引用链接](ex)[](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)：

| 索引类型                | 适用场景            | 核心数据结构                                                                                                                                                                   |
| ------------------- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **BTree 索引**        | 高基数列的等值、IN、范围查询 | 基于多级 SST 文件的逻辑 B-Tree  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)                                                                |
| **Bitmap 索引**       | 枚举型维度、标签列的低基数查询 | RoaringBitmap 压缩位图  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)                                                             |
| **向量索引（Vector）**    | 向量相似性搜索（ANN）    | IVF-FLAT、IVF-PQ、IVF-HNSW、Lumina  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae) |
| **全文索引（Full-Text）** | 文本关键词检索         | Lucene / Tantivy 倒排索引  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)                                                                           |

### 2.3 索引存储结构

Paimon 的索引文件独立于数据文件存储：

```textile
table/
├── data/                          # 数据文件目录
│   └── *.parquet                  # 数据文件
├── index/                         # 索引文件目录
│   ├── btree_<uuid>/              # BTree 索引文件
│   │   └── *.sst                  # SST 文件（多级）
│   ├── bitmap_<uuid>/             # Bitmap 索引文件
│   │   └── *.bitmap               # RoaringBitmap 文件
│   ├── vector_<uuid>/             # 向量索引文件
│   │   ├── index.idx              # IVF 模型/分区元数据
│   │   └── auxiliary.idx          # PQ 码本/图结构
│   └── fulltext_<uuid>/           # 全文索引文件
│       ├── segments               # Lucene 段文件
│       └── ...
└── manifest/                      # 清单文件
    └── manifest-<id>              # 快照与索引元数据
```

**存储特点**：

- 索引文件与数据文件**生命周期一致**，随数据文件创建和删除

- 小索引（如 100 字节）可直接存储在 Manifest 文件中

- 索引文件支持**块缓存（Block Cache）**、**文件级 min/max 键剪枝**、**延迟加载**和**块压缩**  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)

### 2.4 `_ROW_ID` 系统列

当表开启 `row-tracking.enabled = true` 后，Paimon 会为每行分配一个**单调递增的 64 位整数**作为 `_ROW_ID`：

- **稳定性**：`_ROW_ID` 在数据整个生命周期内保持不变，不受 Compaction 影响

- **唯一性**：每个 `_ROW_ID` 唯一标识一行

- **索引映射**：所有 Global Index 都建立 `索引键值 → _ROW_ID 集合` 的映射

## 3. 标量索引（Scalar Index）

标量索引用于加速对数值、字符串、日期等结构化字段的过滤查询。

### 3.1 BTree 索引

**适用场景**：高基数列（如 ID、时间戳、类别等）的等值、IN 和范围查询  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)

**实现原理**：

- 基于多级 SST 文件构建**逻辑 B-Tree 结构**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)

- 支持丰富的谓词下推（Predicate Pushdown）  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)

- 支持块缓存、文件级 min/max 键剪枝、延迟加载和块压缩  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)

**支持的谓词类型**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)：

| 谓词类型         | 示例                               |
| ------------ | -------------------------------- |
| 等值（Equality） | `name = 'a200'`                  |
| IN           | `name IN ('a200', 'a300')`       |
| 范围（Range）    | `price >= 10 AND price < 100`    |
| NULL 检查      | `name IS NOT NULL`               |
| AND/OR 组合    | `name = 'a200' OR name = 'a300'` |

> **注意**：`LIKE`、`startsWith`、`contains` 和 `NOT IN` 谓词可能需要更广泛的索引文件读取，建议使用全文索引替代  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)。

**创建示例**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)：

```sql
-- 在 'name' 列上创建 BTree 索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'name',
    index_type => 'btree'
);

-- 仅对指定分区构建
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'name',
    index_type => 'btree',
    partitions => 'dt=2026-06-18;dt=2026-06-19'
);
```

**关键配置参数**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)：

| 参数                                   | 默认值        | 说明                        |
| ------------------------------------ | ---------- | ------------------------- |
| `sorted-index.records-per-range`     | 10,000,000 | BTree/Bitmap 每个索引文件的预期记录数 |
| `btree-index.block-size`             | 64 KB      | BTree 索引文件的块大小            |
| `btree-index.cache-size`             | 128 MB     | BTree 索引读取器的缓存大小          |
| `btree-index.fallback-scan-max-size` | 256 MB     | 允许回退索引扫描的最大候选文件总大小        |

**查询使用**：BTree 索引构建完成后，当 `WHERE` 条件匹配索引列时，**扫描会自动使用索引加速**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)。

### 3.2 Bitmap 索引

**适用场景**：枚举型维度、标签列等**低基数**列的等值、IN 和 NULL 检查  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)

**实现原理**：

- 为每个 distinct 值存储一个**压缩的 RoaringBitmap**

- 每一位代表一行，标记该行是否包含此值

- 支持字符串列的前缀匹配  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)

**支持的查询类型**  [引用链接](https://paimon.apache.org/docs/master/primary-key-table/global-index/)：

- 等值查询（`=`）

- `IN` 查询

- NULL 检查

- 补集谓词（Complement Predicates）

- 字符串前缀匹配

**创建示例**：

```sql
-- 在 'status' 列上创建 Bitmap 索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'status',
    index_type => 'bitmap'
);
```

### 3.3 标量索引类型选择指南

| 数据类型                   | 过滤类型                       | 推荐索引                                                                                      |
| ---------------------- | -------------------------- | ----------------------------------------------------------------------------------------- |
| 数值、字符串（高基数，>1000 唯一值）  | `=`、`IN`、`<`、`>`、`BETWEEN` | **BTREE**  [引用链接](https://paimon.apache.org/docs/master/primary-key-table/global-index/)  |
| 枚举型、标签列（低基数，<1000 唯一值） | `=`、`IN`、NULL 检查           | **BITMAP**  [引用链接](https://paimon.apache.org/docs/master/primary-key-table/global-index/) |

## 4. 向量索引（Vector Index）

向量索引用于加速高维向量的近似最近邻（ANN）搜索  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)。

### 4.1 索引类型总览

Paimon 支持多种向量索引类型  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)：

| 索引类型              | 特点                | 适用场景                                                                                                                                                                    |
| ----------------- | ----------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ivf-flat**      | 原始向量存储，召回率最高      | 存储和内存可接受时追求最高召回率  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)               |
| **ivf-pq**        | PQ 量化压缩，索引文件小     | 平衡召回率、延迟和存储的权衡  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)                 |
| **ivf-hnsw-flat** | IVF + HNSW，原始向量存储 | 分区内 HNSW 搜索，高召回率  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)               |
| **ivf-hnsw-sq**   | IVF + HNSW，标量量化   | HNSW 搜索质量 + SQ 压缩  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)              |
| **lumina**        | DiskANN 图索引       | 大规模 ANN 搜索，支持 rawf32/sq8/pq 编码  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae) |

### 4.2 IVF-PQ 深度解析

Paimon 的 IVF-PQ 向量索引采用 **纯 Rust 实现**，专为数据湖（S3/HDFS/OSS）设计：

- **基于 Seek 的 I/O**：适配对象存储的随机读取模式

- **SIMD 加速**：支持 8-bit 和 4-bit PQ 的 SIMD 加速

- **JNI 集成**：通过 `paimon-ivfpq-jni` 模块集成到 Paimon 的 GlobalIndex SPI 框架

**关键构建参数**：

| 参数                         | 默认值   | 说明        |
| -------------------------- | ----- | --------- |
| `ivfpq.train.sample_ratio` | —     | 训练采样比例    |
| `ivfpq.index.dimension`    | 128   | 向量维度      |
| `ivfpq.add.batch_size`     | 10000 | 批量添加向量的大小 |

**创建示例**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)：

```sql
-- 创建 IVF-PQ 向量索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'embedding',
    index_type => 'ivf-pq',
    options => 'ivf-pq.distance.metric=cosine,ivf-pq.nlist=256,ivf-pq.pq.m=16'
);

-- 创建 Lumina DiskANN 索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'embedding',
    index_type => 'lumina',
    options => 'lumina.index.dimension=768,lumina.distance.metric=l2,lumina.encoding.type=sq8'
);
```

### 4.3 Refine Factor 重排序

Paimon 支持 **LanceDB 风格的 refine factor** 机制：

- **目的**：用原始向量对 ANN 候选结果进行重排序

- **适用场景**：压缩向量索引（如 IVF-PQ）中，索引分数可能与原始向量精确分数有偏差

- **效果**：通过 `refine_factor` 参数扩大候选池，再用原始向量精排，提升召回率

### 4.4 向量索引限制

- 当前每张表**仅支持一个向量列**  [引用链接](https://paimon.apache.org/docs/master/primary-key-table/global-index/)

- 向量列的元素类型必须为 `FLOAT`  [引用链接](https://paimon.apache.org/docs/master/primary-key-table/global-index/)

- 向量维度必须在创建索引时通过 `<index-type>.dimension` 指定  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)

## 5. 全文索引（Full-Text Index）

全文索引基于 **Lucene++** 或 **Tantivy**（实验性）实现  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)。

**支持的搜索模式**  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)：

| 模式            | 说明                                                                                        | 示例                              |
| ------------- | ----------------------------------------------------------------------------------------- | ------------------------------- |
| **MATCH_ALL** | 所有词都必须出现（AND 语义）  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html) | —                               |
| **MATCH_ANY** | 任意词匹配即可（OR 语义）  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)   | —                               |
| **PHRASE**    | 精确短语匹配  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)           | —                               |
| **PREFIX**    | 前缀匹配  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)             | `"run*"` → `running`, `runner`  |
| **WILDCARD**  | 通配符 `*` 和 `?`  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)    | `"ap*e"`, `"app?e"` → `"apple"` |

**创建示例**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)：

```sql
-- 创建全文索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'full-text'
);
```

## 6. 混合查询实现

Paimon 的混合查询通过 **Hybrid Search API** 实现，支持在一个请求中组合多个向量路由、多个全文路由或两者的混合  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-se  [引用链接](h/)[](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)。

### 6.1 核心机制：多路并行搜索 + 结果融合

Paimon 的混合查询执行流程如下  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)：

```textile
用户查询（标量条件 + 向量/全文搜索）
        ↓
┌───────────────────────────────────────────────────────┐
│ 1. 查询解析与路由                                    │
│    - 识别标量条件 → 路由到 BTree/Bitmap 索引        │
│    - 识别向量列 → 路由到 Vector 索引                │
│    - 识别文本列 → 路由到 Full-Text 索引             │
└───────────────────────────────────────────────────────┘
        ↓
┌───────────────────────────────────────────────────────┐
│ 2. 标量索引预过滤（LanceDB 风格）                    │
│    - BTree/Bitmap 索引返回满足条件的 Row ID 集合    │
│    - 组织成 RoaringBitmap（Row ID 掩码）            │
│    - 传入向量/全文索引作为预过滤条件                   │
└───────────────────────────────────────────────────────┘
        ↓
┌───────────────────────────────────────────────────────┐
│ 3. 多路索引并行搜索                                  │
│    - 各向量/全文路由独立执行 ANN/FTS 搜索           │
│    - 每条路由返回带分数的 Row ID 列表               │
└───────────────────────────────────────────────────────┘
        ↓
┌───────────────────────────────────────────────────────┐
│ 4. 结果融合（Ranker）                                   │
│    - 使用 RRF/weighted_score/MRR 融合多路结果            │
│    - 生成最终排序的 Row ID 列表                          │
└───────────────────────────────────────────────────────┘
        ↓
┌───────────────────────────────────────────────────────┐
│ 5. 数据读取                                          │
│    - 根据最终 Row ID 列表从数据文件中读取完整行      │
└───────────────────────────────────────────────────────┘
```

### 6.2 Row ID 掩码预过滤（核心机制）

Paimon 的标量预过滤采用 **LanceDB 风格的 Row ID 预过滤**机制：

> **PR #15 描述**：“Add LanceDB-style row-id prefilter support using serialized 64-bit Roaring bitmap bytes. This lets the Paimon query layer evaluate metadata predicates first and pass the matching row IDs into IVF-PQ reader search without embedding scalar metadata into the vector index file.”

**核心实现**：

1. **标量索引先行**：Paimon 查询层首先评估元数据谓词（即 `WHERE` 中的标量条件），利用 BTree 或 Bitmap 索引快速定位满足条件的行

2. **生成 Row ID 掩码**：将满足标量条件的 Row ID 组织成一个 **RoaringBitmap64**（压缩位图）

3. **传入向量索引**：将这个 Row ID 掩码作为预过滤条件，通过 `with_include_row_ids()` 方法传入向量索引读取器

4. **向量索引按掩码执行**：向量索引在搜索时，**只对掩码中标记的 Row ID 进行距离计算**

这种设计的核心优势在于：**标量元数据不需要嵌入到向量索引文件中**，索引文件保持纯粹，两者解耦。

### 6.3 三种排名器（Ranker）

Paimon 支持三种排名器用于融合多路结果  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)：

| 排名器                | 说明                                                                                                                             | 适用场景           |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------ | -------------- |
| **RRF**（默认）        | 倒数排名融合，基于各路由中的排名顺序，不依赖分数归一化  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)        | 不同路由分数尺度差异大时   |
| **weighted_score** | 基于归一化分数和权重进行融合，Min-Max 归一化到 [0, 1]  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/) | 需要精细控制各路由权重时   |
| **MRR**            | 平均倒数排名，强调每个路由中的头部命中结果  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)              | 关注 Top 结果的准确性时 |

### 6.4 混合查询 SQL 示例

```sql
-- 混合搜索：2 个向量路由 + 1 个全文路由
SELECT * FROM hybrid_search(
    table_name => 'db.my_table',
    vector_routes => ARRAY[
        named_struct(
            'field', 'title_embedding',
            'query_vector', ARRAY[0.1, 0.2, ...],
            'limit', 50,
            'weight', 1.0,
            'options', MAP('ivf.nprobe', '10')
        ),
        named_struct(
            'field', 'body_embedding',
            'query_vector', ARRAY[0.3, 0.4, ...],
            'limit', 50,
            'weight', 0.8,
            'options', MAP('ivf.nprobe', '10')
        )
    ],
    full_text_routes => ARRAY[
        named_struct(
            'column', 'content',
            'query', 'paimon hybrid search',
            'limit', 50,
            'weight', 0.5,
            'options', MAP()
        )
    ],
    limit => 10,
    ranker => 'weighted_score'
);
```

### 6.5 多向量搜索

Paimon 支持多向量搜索，即在一个查询中对多个向量列同时进行 ANN 搜索：

- 将查询**扇出（fan out）**到每个向量列的独立全局向量索引

- 融合打分后的 Row ID

- **不需要任何索引格式变更**，复用现有的向量索引构建器

## 7. 索引生命周期管理

### 7.1 索引创建

索引构建通过 **Flink 批作业异步执行**：

```sql
-- 创建 BTree 索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'name',
    index_type => 'btree'
);

-- 创建向量索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'embedding',
    index_type => 'ivf-pq',
    options => 'ivf-pq.distance.metric=cosine,ivf-pq.nlist=256,ivf-pq.pq.m=16'
);

-- 创建全文索引
CALL sys.create_global_index(
    table => 'db.my_table',
    index_column => 'content',
    index_type => 'full-text'
);
```

**异步构建特点**：

- 写入数据后需**等待索引构建完成**，查询才能使用索引加速

- 可通过 DLF 控制台查看全局索引构建情况

### 7.2 索引查询与验证

```sql
-- 查看执行计划，验证索引是否被使用
EXPLAIN SELECT * FROM my_db.user_actions WHERE id = 1;
```

### 7.3 索引删除

```sql
-- 删除 BTree 索引
CALL sys.drop_global_index(
    table => 'db.my_table',
    index_column => 'name',
    index_type => 'btree'
);

-- 仅删除指定分区的索引（dry_run 预览）
CALL sys.drop_global_index(
    table => 'db.my_table',
    index_column => 'name',
    index_type => 'btree',
    partitions => 'dt=2026-06-18;dt=2026-06-19',
    dry_run => true
);
```

### 7.4 索引更新与增量构建

Paimon 的索引支持**增量构建和长期维护**  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)：

- 每生成一个快照（Snapshot），就会为增量数据创建新的索引文件

- 查询时需要**按顺序读取当前快照和所有历史快照的索引文件**

- 支持时间旅行（Time Travel）：可查询任意历史版本的索引数据

## 8. 性能调优指南

### 8.1 BTree 索引调优  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)

| 参数                                   | 建议           | 说明                                                                                                |
| ------------------------------------ | ------------ | ------------------------------------------------------------------------------------------------- |
| `btree-index.read-buffer-size`       | 范围查询：增大到 1MB | 提高 I/O 带宽和顺序读取性能  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)         |
|                                      | 点查询：保持未设置    | 缓冲可能导致读取放大  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)               |
| `btree-index.cache-size`             | 根据内存调整       | 增大可提高缓存命中率  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)    |
| `btree-index.fallback-scan-max-size` | 根据查询模式调整     | 控制回退扫描的最大文件大小  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/) |

### 8.2 向量索引调优  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)

| 索引类型            | 适用场景            | 调优方向                                                                                                                                                                        |
| --------------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **ivf-flat**    | 最高召回率           | 增加 `nlist` 提高精度，接受更大存储                                                                                                                                                      |
| **ivf-pq**      | 平衡召回/延迟/存储      | 调整 `pq.m` 控制压缩比，`nlist` 控制分区数  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)      |
| **ivf-hnsw-sq** | HNSW 质量 + SQ 压缩 | 调整 `ef_construction` 和 `ef_search`  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae) |
| **lumina**      | 大规模 ANN 搜索      | 调整编码类型（rawf32/sq8/pq）  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)              |

**查询时参数**：

- `ivf.nprobe`：搜索的分区数量，增大提高召回率，增加延迟  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)

- `hnsw.ef_search`：HNSW 搜索的候选列表大小  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)

### 8.3 混合查询调优

| 场景           | 建议                                                                                                                       |
| ------------ | ------------------------------------------------------------------------------------------------------------------------ |
| 标量过滤选择性高     | 利用 BTree/Bitmap 预过滤，大幅减少向量搜索空间                                                                                           |
| 多路结果融合       | 使用 `RRF` 避免分数归一化问题  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)           |
| 需要精细控制权重     | 使用 `weighted_score` 并配置各路由权重  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/) |
| 关注 Top 结果准确性 | 使用 `MRR` 强调头部命中  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)              |
| 向量索引召回不足     | 使用 `refine_factor` 扩大候选池并用原始向量精排                                                                                         |

## 9. 总结与限制

### 9.1 索引体系总览

| 索引类别     | 类型         | 适用场景          | 核心数据结构                                                                                                                                               |
| -------- | ---------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **标量索引** | BTREE      | 高基数列的等值/范围查询  | 多级 SST 文件 + B-Tree  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)                                               |
|          | BITMAP     | 低基数列的等值/IN 查询 | RoaringBitmap  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)                                              |
| **向量索引** | IVF-FLAT   | 最高召回率         | IVF + 原始向量  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)  |
|          | IVF-PQ     | 平衡召回/延迟/存储    | IVF + 乘积量化  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)  |
|          | IVF-HNSW-* | HNSW + IVF 组合 | IVF + HNSW  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae)  |
|          | Lumina     | 大规模 DiskANN   | DiskANN 图索引  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/?spm=a2c6h.13046898.publish-article.4.56436ffaC7kWae) |
| **全文索引** | Full-Text  | 关键词搜索         | Lucene / Tantivy 倒排索引  [引用链接](https://paimon.apache.org/docs/cpp/user_guide/global_index.html)                                                       |

### 9.2 混合查询总结

| 混合类型        | 索引组合                     | 核心机制                                                                                                           |
| ----------- | ------------------------ | -------------------------------------------------------------------------------------------------------------- |
| **标量 + 向量** | BTree/Bitmap + Vector    | Row ID 掩码预过滤                                                                                                   |
| **标量 + 全文** | BTree/Bitmap + Full-Text | Row ID 掩码预过滤                                                                                                   |
| **向量 + 向量** | Vector + Vector          | 多路扇出 + 结果融合                                                                                                    |
| **向量 + 全文** | Vector + Full-Text       | 多路并行搜索 + Ranker 融合  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/) |

### 9.3 关键特性

- ✅ 支持四种全局索引类型：BTREE、BITMAP、Vector、Full-Text  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)

- ✅ 支持 LanceDB 风格的 Row ID 预过滤

- ✅ 支持多向量、多全文的混合搜索  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)

- ✅ 支持三种排名器：RRF、weighted_score、MRR  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)

- ✅ 索引异步构建，不阻塞写入

- ✅ 索引与数据分离，独立存储和管理  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)

- ✅ 支持增量构建和时间旅行

- ✅ 支持 Refine Factor 重排序

### 9.4 已知限制

| 限制          | 说明                                                                                                                                      |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| **表类型限制**   | Global Index 仅支持 Append-Only（Data Evolution）表  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index) |
| **向量列数量**   | 每张表**仅支持一个向量列**  [引用链接](https://paimon.apache.org/docs/master/primary-key-table/global-index/)                                          |
| **索引覆盖不完整** | 当索引仅覆盖部分数据时，查询可能不完整  [引用链接](https://paimon.apache.org/docs/master/multimodal-table/global-index/#btree-index)                           |
| **索引构建等待**  | 写入后需等待索引构建完成才能使用                                                                                                                        |
| **多列复合索引**  | 正在开发中（PR #7933 支持 Lucene 多列索引）                                                                                                          |

## 附录：参考资料

- [Apache Paimon 官方文档 - Global Index](https://paimon.apache.org/docs/master/multimodal-table/global-index/)

- [Apache Paimon 官方文档 - Hybrid Search](https://paimon.apache.org/docs/master/multimodal-table/global-index/hybrid-search/)

- [Apache Paimon 官方文档 - Vector Index](https://paimon.apache.org/docs/master/multimodal-table/global-index/vector/)

- [Apache Paimon 官方文档 - BTree Index](https://paimon.apache.org/docs/master/multimodal-table/global-index/btree/)

- [PIP-38: Introduce Global Index for Paimon Table](https://cwiki.apache.org/confluence/display/PAIMON/PIP-38%253A+Introduce+Global+Index+for+Paimon+Table)

- [PR #15: Add Roaring bitmap filter pushdown (LanceDB-style row-id prefilter)](https://github.com/apache/paimon-vector-index/pull/15)

- [PR #7933: Support multi-column GlobalIndex framework](https://github.com/apache/paimon/pull/7933)

*本文档基于 Apache Paimon 官方文档、PIP 及社区 PR 整理，如有更新请以官方最新文档为准。*


