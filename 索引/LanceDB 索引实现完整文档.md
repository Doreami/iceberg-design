# LanceDB 索引实现完整文档

> **文档版本**：v3.0 | **更新日期**：2026-08-17 | **适用版本**：LanceDB 0.37.1
> 
> **说明**：本文档基于 LanceDB 0.37.1 官方文档及实际测试结果整理，所有结论均经过验证。

## 目录

1. 概述

2. 索引架构总览

3. 标量索引（Scalar Index）

4. 向量索引（Vector Index）

5. 全文搜索索引（Full-Text Search Index）

6. 混合查询实现

7. 索引生命周期管理

8. 性能调优指南

9. 测试验证报告

10. 0总结与限制

11. 参考资料

## 1. 概述

LanceDB 是一个基于 **Lance** 列式存储格式构建的向量数据库，其索引系统采用 **双轨索引架构**，同时支持向量索引、标量索引和全文搜索索引三大体系。

LanceDB 最核心的设计哲学是 **"索引与数据分离"** 和 **"以磁盘为中心"** 的索引策略。与传统内存型向量数据库不同，LanceDB 的索引主要存储在磁盘上，通过精心设计的数据结构和缓存策略，在保证查询性能的同时大幅降低内存占用。

**标量索引的核心定位**：标量索引是 LanceDB 的基础优化层，可加速向量搜索（预过滤/后过滤）、全文搜索、SQL 扫描和键值查找等多种负载。

## 2. 索引架构总览

### 2.1 双轨索引架构

LanceDB 实现了一个**双轨索引系统**，向量索引和标量索引共享共同的基础设施，但优化不同的查询模式：

| 索引类别     | 主要用途       | 查询模式       | 索引结构                                 |
| -------- | ---------- | ---------- | ------------------------------------ |
| **向量索引** | K近邻搜索      | 近似最近邻（ANN） | IVF分区 + 子索引（HNSW/Flat）+ 量化（PQ/SQ/RQ） |
| **标量索引** | 等值、范围、成员查询 | 精确过滤       | BTree页、Bitmap集合、LabelList            |
| **全文索引** | 关键词搜索      | BM25相关性排序  | 倒排索引                                 |

### 2.2 索引存储结构

所有索引独立存储在数据目录下的 `_indices/` 目录中，每个索引拥有唯一的 UUID：

```textile
dataset/
├── _indices/
│   ├── <index-uuid-1>/          # 向量索引
│   │   ├── index.idx            # 主索引（IVF模型、分区元数据）
│   │   └── auxiliary.idx        # 辅助数据（HNSW图、量化码本）
│   ├── <index-uuid-2>/          # BTree标量索引
│   │   ├── page_lookup.lance    # 页边界查找表
│   │   └── page_data.lance      # 排序的键值页
│   ├── <index-uuid-3>/          # Bitmap标量索引
│   │   └── bitmap_<field>.lance # 值→行ID位图
│   └── <index-uuid-4>/          # FTS全文索引
│       ├── metadata.lance       # 分区列表和参数
│       ├── tokens_<pid>.lance   # Token→ID映射
│       ├── posting_<pid>.lance  # 倒排列表（压缩）
│       └── docs_<pid>.lance     # 文档元数据
│
└── data/
    └── *.lance                  # 数据文件
```

## 3. 标量索引（Scalar Index）

标量索引用于加速对数值、类别、时间戳等结构化字段的过滤和排序操作。LanceDB 支持三种标量索引类型。

### 3.1 BTree 索引

**适用场景**：高基数（唯一值多）的列，如 ID、时间戳等。

**实现原理**：

- 存储列的排序副本，采用两级结构

- 上层 BTree 缓存于内存，映射值范围到数据页

- 每页包含 4096 行

- 下层存储排序值和行ID

**查询加速**：

- 等值查询（`=`）

- 范围查询（`<`、`>`、`BETWEEN`）

- 排序查询（`ORDER BY`）

**创建示例**：

```python
# 默认创建 BTree 索引
table.create_scalar_index("user_id")

# 或使用 create_index API
table.create_index(column="user_id", config=BTree())
```

### 3.2 Bitmap 索引

**适用场景**：低基数（唯一值少）的列，如类别、状态码等。

**实现原理**：

- 为每个 distinct 值存储一个 **Roaring Bitmap**

- 每一位代表一行，标记该行是否包含此值

- 查询时先检查缓存，未命中则从磁盘加载

**查询加速**：

- 等值查询（`=`）

- `IN` 查询

- `BETWEEN` 查询

**创建示例**：

```python
table.create_scalar_index("category", index_type="BITMAP")
# 或
table.create_index(column="category", config=Bitmap())
```

### 3.3 LabelList 索引

**适用场景**：数组类型列（如标签列表）的包含查询。

**实现原理**：

- 基于 Bitmap 索引结构实现

- 专门优化数组元素的隶属度查找

**查询加速**：

- `array_has_any`：数组包含任意指定值

- `array_has_all`：数组包含所有指定值

**创建示例**：

```python
table.create_scalar_index("tags", index_type="LABEL_LIST")
```

### 3.4 标量索引类型选择指南

| 数据类型                  | 过滤类型                            | 推荐索引           |
| --------------------- | ------------------------------- | -------------- |
| 数值、字符串（高基数，>1000 唯一值） | `=`、`<`、`>`、`BETWEEN`           | **BTREE**      |
| 数值、字符串（低基数，<1000 唯一值） | `=`、`IN`、`BETWEEN`              | **BITMAP**     |
| 低基数的列表                | `array_has_any`、`array_has_all` | **LABEL_LIST** |

### 3.5 复杂查询下的标量索引利用

LanceDB 0.37.1 的查询优化器基于 Apache DataFusion，具备**成熟的布尔表达式拆解能力**。

#### 3.5.1 AND 组合

对于由 `AND` 连接的多个条件，LanceDB 可以有效利用多个单列索引：

| 查询类型    | 示例                        | 索引利用方式                    |
| ------- | ------------------------- | ------------------------- |
| 等值 + 等值 | `WHERE a = 1 AND b = 2`   | 分别走 `a` 和 `b` 的索引，取**交集** |
| 等值 + 范围 | `WHERE a = 1 AND b > 100` | 分别走索引，取交集                 |
| 范围 + 范围 | `WHERE a > 10 AND b < 50` | 分别走索引，取交集                 |

**执行计划示例**：

```textile
ScalarIndexQuery: query=AND([a = 1]@a_idx(BTree),[b = 2]@b_idx(BTree))
```

#### 3.5.2 OR 组合

对于 `OR` 连接的多个条件，LanceDB 同样可以走**索引合并（Union）**：

**简单 OR 示例**：

```sql
WHERE a = 1 OR b = 2
```

**执行计划**：

```textile
ScalarIndexQuery: query=OR([a = 1]@a_idx(BTree),[b = 2]@b_idx(BTree))
```

**复杂嵌套 OR 示例**：

```sql
WHERE (a = 1 AND b = 2) OR (c > 500 AND d < 5000) OR (a = 3 AND e = 1)
```

**执行计划**：

```textile
ScalarIndexQuery: query=OR(
  OR(
    AND([a = 1]@a_idx(BTree),[b = 2]@b_idx(BTree)),
    AND([c > 500]@c_idx(BTree),[d < 5000]@d_idx(BTree))
  ),
  AND([a = 3]@a_idx(BTree),[e = 1]@e_idx(BTree))
)
```

✅ 多个索引全部被利用，形成嵌套逻辑树

**大量 OR 分支测试**：在 20 个 OR 分支的测试中，优化器成功生成了 40 个索引节点的逻辑树，所有涉及的索引均被利用。

#### 3.5.3 非索引条件与 refine_filter 机制

当查询条件中包含**无法使用索引的谓词**（如 `LIKE '%...%'`、函数包裹列等），且与可索引条件通过 `OR` 组合时，优化器为了确保结果正确性，会**放弃对整个 `OR` 条件使用索引**，退化为全表扫描。

**示例查询**：

```sql
WHERE (a = 1 AND b = 2) OR (label LIKE '%test%')
```

**执行计划**：

```textile
LanceRead: full_filter=a = 1 AND b = 2 OR label LIKE "%test%", 
           refine_filter=a = 1 AND b = 2 OR label LIKE "%test%"
```

**执行机制**：

- 执行**全表扫描**（`LanceRead`），读取所有数据

- 在内存中逐行应用 `refine_filter` 中的完整条件进行过滤

- 满足条件的行才被返回

**机制说明**：

- `refine_filter` 表示过滤发生在数据被读取到内存**之后**

- 这**不是“部分索引利用 + 二次过滤”**，而是**全表扫描 + 内存过滤**

- `refine_filter` 的存在本身并不代表索引被使用，只代表过滤条件被推迟到数据读取后执行

**触发条件**：

- `OR` 条件中至少有一个分支包含无法索引的谓词

- 查询中包含函数包裹的列（如 `SUBSTR(col, 1, 2) = 'xx'`）

### 3.6 优化器决策机制与执行计划分析

#### 3.6.1 核心决策过程：Scanner 与 DataFusion

LanceDB 的查询规划核心是 **`Scanner`** 组件，位于 `rust/lance/src/dataset/scanner.rs`，负责将用户查询转换为优化的物理执行计划。

这个过程依赖 **Apache DataFusion** 的查询引擎，提供 SQL 解析、逻辑计划生成和优化规则（谓词下推、投影下推等）。

#### 3.6.2 优化器的决策依据

LanceDB 优化器在生成计划时，主要依据以下信息：

1. **可用索引**：检查查询涉及的列上是否存在标量索引

2. **数据统计信息**：利用行数、列基数、数据分布等估算成本

3. **查询特性**：过滤条件复杂度（`AND`/`OR` 组合）、是否包含向量搜索等

4. **用户参数**：`prefilter`、`nprobes` 等

#### 3.6.3 决策实例

| 决策因素                   | 对执行计划的影响                           |
| ---------------------- | ---------------------------------- |
| **过滤条件下推**             | 将 `WHERE` 中的过滤条件下推到数据源层执行          |
| **预过滤（`prefilter`）模式** | `prefilter=True` 生成"先标量过滤再向量搜索"的计划 |
| **投影下推**               | 只读取查询中指定的必要列                       |

#### 3.6.4 如何观察优化器的决策

LanceDB 提供两个工具：

| 工具                 | 用途             | 特点                          |
| ------------------ | -------------- | --------------------------- |
| **`explain_plan`** | 展示逻辑和物理执行计划    | 用于**分析**查询结构、验证索引是否被使用      |
| **`analyze_plan`** | 实际执行查询并返回运行时指标 | 用于**性能调优**，包含耗时、处理行数、I/O 统计 |

**示例**：

```python
# 查看执行计划
print(table.search([100, 102])
      .where("category = 'electronics' AND price > 100")
      .explain_plan(True))
```

## 4. 向量索引（Vector Index）

向量索引用于加速对高维向量的近似最近邻（ANN）搜索。

### 4.1 索引类型总览

LanceDB 提供两类向量索引算法：**IVF**（Inverted File）和 **HNSW**（Hierarchical Navigable Small World）。HNSW 不作为顶级索引暴露，而是作为 IVF 分区内的子索引使用。

| 索引类型              | 适用场景                 | 压缩方式       | 特点                  |
| ----------------- | -------------------- | ---------- | ------------------- |
| **IVF_PQ**        | 大规模向量搜索，小维度（≤256）高精度 | PQ（乘积量化）   | 平衡索引大小与召回率          |
| **IVF_HNSW_FLAT** | 追求最高召回率              | 无量化        | 使用原始向量，无量化损失        |
| **IVF_HNSW_SQ**   | 最佳召回/延迟权衡            | SQ（标量量化）   | 结合 IVF 分区与 HNSW 图搜索 |
| **IVF_HNSW_PQ**   | 需要压缩的 HNSW 搜索        | PQ         | HNSW 图内使用 PQ 压缩     |
| **IVF_RQ**        | 最大压缩                 | RQ（RabitQ） | 约原始大小的 **1/32**     |

### 4.2 IVF-PQ 深度解析

IVF-PQ 是 LanceDB 的**默认向量索引类型**。

**乘积量化（PQ）**：

1. 将高维向量划分为等大小的子向量

2. 每个子向量映射到最近的聚类中心（码本中的码字）

3. 使用码字 ID 替代原始向量

**倒排文件索引（IVF）**：

1. 对向量空间运行 K-means 聚类

2. 每个向量分配到最近的聚类中心

3. 查询时只搜索最相关的几个分区（由 `nprobe` 控制）

**关键参数**：

- `nprobe`：搜索的分区数量，值越大召回率越高但速度越慢

- `num_partitions`：分区总数

- `num_sub_vectors`：子向量数量

### 4.3 向量索引创建与调优

```python
# 创建默认 IVF_PQ 索引
table.create_index(
    metric="l2",
    num_partitions=256,
    num_sub_vectors=16
)
```

**索引调优起始值**：

| 索引类型            | num_partitions 起始值      | 其他参数                              |
| --------------- | ----------------------- | --------------------------------- |
| IVF_HNSW_*      | `num_rows // 1,048,576` | `ef_construction: 150`            |
| IVF_PQ / IVF_RQ | `num_rows // 4096`      | `num_sub_vectors: dimension // 8` |

**索引类型选择建议**：

| 优先级          | 推荐索引                |
| ------------ | ------------------- |
| 最高召回率        | `IVF_HNSW_FLAT`     |
| 最佳召回/延迟权衡    | `IVF_HNSW_SQ`       |
| 最大压缩（高维）     | `IVF_RQ`            |
| 小维度（≤256）高精度 | `IVF_PQ`            |
| 频繁带元数据过滤     | `IVF_RQ` 或 `IVF_PQ` |

## 5. 全文搜索索引（Full-Text Search Index）

全文搜索索引基于 **倒排索引（Inverted Index）** 实现，使用 **BM25** 算法进行相关性评分。

**核心组件**：

- 使用 BM25 排名算法

- 支持可配置的分词（tokenization）、词干提取（stemming）、停用词移除和语言特定处理

**查询类型**：

- `MatchQuery`：包含一个或多个 Token 的文档搜索（AND/OR）

- `PhraseQuery`：精确短语匹配

- `BooleanQuery`：组合多个查询（AND/OR/NOT 逻辑）

- `BoostQuery`：对查询结果进行分数加权

- `MultiMatchQuery`：跨多列搜索

**创建示例**：

```python
# 单列 FTS 索引
table.create_fts_index("content", tokenizer_name="en_stem")

# 多列 FTS 索引
table.create_fts_index(["title", "content"])
```

## 6. 混合查询实现

LanceDB 支持将不同类型索引组合使用。根据索引组合方式，分为三种场景：**标标混合**、**向标混合**、**向向混合**。

### 6.1 标标混合：标量索引 + 标量过滤

**定义**：查询中包含多个标量条件，利用标量索引加速过滤。

**实现机制**：

- 每个标量条件独立使用对应的标量索引

- AND 组合：多个索引取交集

- OR 组合：多个索引取并集

**示例**：

```python
# 为多个标量列分别创建索引
table.create_scalar_index("category")
table.create_scalar_index("price")

# 标标混合查询
result = table.search().where("category = 'electronics' AND price > 100").to_pandas()
```

**执行计划示例（AND 组合）**：

```sql
-- 查询
SELECT * FROM table WHERE a = 1 AND b = 2
```

```textile
ScalarIndexQuery: query=AND([a = 1]@a_idx(BTree),[b = 2]@b_idx(BTree))
```

 **解读**：`a` 和 `b` 的索引各自返回行号集合，然后取交集。

**执行计划示例（OR 组合）**：

```sql
-- 查询
SELECT * FROM table WHERE a = 1 OR b = 2
```

```textile
ScalarIndexQuery: query=OR([a = 1]@a_idx(BTree),[b = 2]@b_idx(BTree))
```

**解读**：`a` 和 `b` 的索引各自返回行号集合，然后取并集。

### 6.2 向标混合：向量索引 + 标量过滤

**定义**：查询中同时包含**向量相似度搜索**和**标量条件过滤**。

#### 两种执行模式

| 特性        | 预过滤（Prefilter）   | 后过滤（Post-filter） |
| --------- | ---------------- | ---------------- |
| **执行顺序**  | 先标量过滤，再向量搜索      | 先向量搜索，再标量过滤      |
| **结果精确性** | 保证返回 `limit` 条结果 | 可能少于 `limit` 条   |
| **延迟表现**  | 过滤条件严格时更快        | 通常延迟更低           |
| **默认行为**  | **默认开启**         | 需显式指定            |

**示例**：

```python
# 预过滤（默认）
result = (table.search(query_vector)
          .where("category = 'electronics' AND price > 100")
          .limit(10)
          .to_pandas())

# 后过滤（显式指定）
result = (table.search(query_vector)
          .where("category = 'electronics' AND price > 100", prefilter=False)
          .limit(10)
          .to_pandas())
```

**执行计划示例（预过滤）**：

```sql
-- 查询
SELECT * FROM table WHERE a = 1 AND b = 2 ORDER BY vector_distance(vector, query) LIMIT 10
```

```textile
ProjectionExec: ...
  LanceRead: ...
    SortExec: TopK(fetch=10)
      ANNSubIndex: name=vector_idx, k=10
        ANNIvfPartition: ...
        ScalarIndexQuery: query=AND([a = 1]@a_idx(BTree),[b = 2]@b_idx(BTree))
```

**解读**：`ScalarIndexQuery` 位于 `ANNSubIndex` 内部，表示先执行标量过滤，再在过滤后的候选集上执行向量搜索。**标量过滤发生在向量搜索之前**。

**执行计划示例（后过滤）**：

```sql
-- 查询
SELECT * FROM table WHERE a = 1 AND b = 2 ORDER BY vector_distance(vector, query) LIMIT 10
-- 且 prefilter=False
```

```textile
ProjectionExec: ...
  GlobalLimitExec: fetch=10
    FilterExec: a = 1 AND b = 2
      LanceRead: projection=[a, b]
        SortExec: TopK(fetch=10)
          ANNSubIndex: name=vector_idx, k=10
            ANNIvfPartition: ...
```

**解读**：`FilterExec` 位于 `ANNSubIndex` 之后，表示先执行向量搜索，再对 Top-K 结果应用标量过滤。**标量过滤发生在向量搜索之后**，因此返回结果可能少于 `limit` 条。

### 6.3 向向混合：向量索引 + 向量索引

#### 场景一：多向量查询（Multivector Search）

**适用场景**：ColBERT 等晚期交互模型，每个文档生成多个向量嵌入。

**核心机制：MaxSim**  
MaxSim(Q,D)=∑i=1∣Q∣​maxj=1∣D∣​sim(qi​,dj​)

**示例**：

```python
# 每个文档包含多个向量
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("vector", pa.list_(pa.list_(pa.float32(), 256))),
])

# 多向量搜索
query_vectors = np.random.random(size=(2, 256)).tolist()
result = table.search(query_vectors).limit(10).to_pandas()
```

**执行计划**：

```textile
KNNVectorDistance: metric=cosine, multivector=true
  LanceScan: projection=[vector]
```

**解读**：`multivector=true` 表示启用多向量模式，使用 MaxSim 计算查询向量与文档向量组的相似度。

#### 场景二：混合检索（Hybrid Search）：向量 + 全文搜索

**核心机制：RRF（倒数排名融合）**

1. 分别执行**向量搜索**和**全文搜索（FTS）**

2. 各自独立返回 Top-N 结果

3. 使用 RRF 算法合并重排

**示例**：

```python
from lancedb.rerankers import RRFReranker

results = (table.search("user query", query_type="hybrid")
           .rerank(RRFReranker())
           .limit(10)
           .to_pandas())
```

**执行计划（概念性）**：

```textile
HybridSearchExec: query_type=hybrid
  FtsSearchExec: query="user query"
  VectorSearchExec: query_vector=[...]
  RrfRerankerExec: k=60
```

**解读**：分别执行 FTS 和向量搜索，各自返回 Top-K 结果，然后通过 RRF 合并排序。

## 7. 索引生命周期管理

### 7.1 索引创建

```python
# 标量索引创建
table.create_scalar_index("column_name")

# 向量索引创建（异步）
table.create_index(metric="l2", num_partitions=256, num_sub_vectors=16)
```

`create_index` API 立即返回，索引构建是**异步的**。

### 7.2 索引更新

- **新数据追加**：索引**不会自动更新**

- **正常搜索**：会检查未索引的行（较慢的回退路径）

- **快速搜索**（`fast_search()`）：跳过回退，只搜索已索引的行

- **索引刷新**：调用 `optimize()` 将新数据合并到现有索引

```python
# 等待索引构建完成
table.wait_for_index(["a", "b", "c"], timeout=timedelta(seconds=600))

# 查看索引状态
table.index_stats("column_name")

# 优化索引（合并新数据）
table.optimize()
```

### 7.3 索引重建

```python
# 强制重建索引
table.create_index(replace=True)
```

## 8. 性能调优指南

### 8.1 标量索引调优

| 建议                       | 说明                     |
| ------------------------ | ---------------------- |
| **为高频过滤列建索引**            | 强烈建议在用于过滤的列上创建标量索引     |
| **选择合适的索引类型**            | 高基数用 BTREE，低基数用 BITMAP |
| **保持过滤表达式简单**            | 避免复杂变换                 |
| **使用 `explain_plan` 验证** | 确认索引是否被使用              |

### 8.2 向量索引调优

**IVF_PQ 调优参数**：

| 参数                | 起始值                | 调优方向          |
| ----------------- | ------------------ | ------------- |
| `num_partitions`  | `num_rows // 4096` | 增大提高召回，减小降低延迟 |
| `num_sub_vectors` | `dimension // 8`   | 增大提高召回，减小降低延迟 |
| `nprobes`（查询时）    | 1-10               | 增大提高召回，减小降低延迟 |

**IVF_HNSW 调优参数**：

| 参数                | 起始值                     | 调优方向            |
| ----------------- | ----------------------- | --------------- |
| `num_partitions`  | `num_rows // 1,048,576` | 降低可减少搜索延迟       |
| `ef_construction` | 150                     | 增大提高召回，减小加快索引构建 |

### 8.3 混合查询调优

| 场景            | 建议                             |
| ------------- | ------------------------------ |
| 过滤条件**高度选择性** | 使用 `bypass_vector_index()`     |
| 预过滤**召回不足**   | 增加 `nprobes`                   |
| 后过滤**结果太少**   | 增加 `nprobes` 和 `refine_factor` |
| 不确定选哪种模式      | 使用**预过滤**（默认）                  |

### 8.4 使用 explain_plan 分析

```python
# 查看执行计划
print(table.search(query_vector)
      .where("category = 'electronics'")
      .explain_plan(True))
```

## 9. 测试验证报告

本章节基于 **LanceDB 0.37.1** 在 50 万行数据集上的实际测试结果。

### 9.1 测试环境

| 项目         | 规格                                                            |
| ---------- | ------------------------------------------------------------- |
| LanceDB 版本 | 0.37.1                                                        |
| 数据量        | 500,000 行                                                     |
| 向量维度       | 128                                                           |
| 标量列        | a（低基数 0-9）、b（中基数 0-99）、c（高基数 0-999）、d（高基数 0-9999）、e（极低基数 0-1） |
| 索引类型       | 全部为 BTree（`create_scalar_index` 默认）                           |

### 9.2 标量索引测试结果

| 测试场景                                                 | 执行计划特征                                                 | 索引利用                      | 耗时      |
| ---------------------------------------------------- | ------------------------------------------------------ | ------------------------- | ------- |
| **AND 查询** `a=1 AND b=2`                             | `ScalarIndexQuery: AND([a=1]@a_idx,[b=2]@b_idx)`       | ✅ 2 个索引取交集                | 0.0108s |
| **简单 OR** `a=1 OR b=2`                               | `ScalarIndexQuery: OR([a=1]@a_idx,[b=2]@b_idx)`        | ✅ 2 个索引合并                 | 0.0088s |
| **复杂嵌套 OR** 5 列混合                                    | `ScalarIndexQuery: OR(OR(AND(...),AND(...)),AND(...))` | ✅ 5 个索引全部利用               | 0.0592s |
| **20 个 OR 分支**                                       | `ScalarIndexQuery: OR(OR(...))` 40 个索引节点               | ✅ 全部走索引                   | 0.0216s |
| **LIKE 污染** `(a=1 AND b=2) OR (label LIKE '%test%')` | `refine_filter=...`                                    | ⚠️ 部分索引利用 + refine_filter | 0.0079s |

### 9.3 向量索引测试结果

| 测试项                           | 结果                                   |
| ----------------------------- | ------------------------------------ |
| **IVF_PQ 索引创建**               | 5.11s，索引大小 ~10 MB（500,000 行 × 128 维） |
| **无过滤向量搜索**                   | 0.0103s，返回 10 条                      |
| **有过滤向量搜索**（`a=1 AND b=2`）    | 0.0076s，返回 10 条                      |
| **nprobe=1**                  | 0.0050s                              |
| **nprobe=10**                 | 0.0054s                              |
| **nprobe=50**                 | 0.0095s                              |
| **nprobe=100**                | 0.0124s                              |
| **IVF_PQ（10,000 行小数据集）**      | 0.0078s                              |
| **IVF_HNSW_SQ（10,000 行小数据集）** | 0.0128s                              |

### 9.4 预过滤 vs 后过滤测试结果

| 模式                        | 执行计划特征                                | 结果数                    |
| ------------------------- | ------------------------------------- | ---------------------- |
| **预过滤** `prefilter=True`  | `ScalarIndexQuery` 在 `ANNSubIndex` 之前 | 10 条（全部满足条件）           |
| **后过滤** `prefilter=False` | `FilterExec` 在 `ANNSubIndex` 之后       | 0 条（Top-10 向量结果无一满足条件） |

## 10. 总结与限制

### 10.1 索引体系总览

| 索引类别     | 类型         | 适用场景          | 核心数据结构         |
| -------- | ---------- | ------------- | -------------- |
| **标量索引** | BTREE      | 高基数列的范围/等值查询  | BTree + 排序页    |
|          | BITMAP     | 低基数列的等值/IN 查询 | Roaring Bitmap |
|          | LABEL_LIST | 数组列的包含查询      | Bitmap 变体      |
| **向量索引** | IVF_PQ     | 大规模向量搜索（默认）   | IVF + PQ       |
|          | IVF_HNSW_* | 高召回向量搜索       | IVF + HNSW     |
|          | IVF_RQ     | 最大压缩向量搜索      | IVF + RaBitQ   |
| **全文索引** | FTS        | 关键词搜索         | 倒排索引 + BM25    |

### 10.2 混合查询总结

| 混合类型     | 索引组合    | 核心机制                     | 默认行为 |
| -------- | ------- | ------------------------ | ---- |
| **标标混合** | 标量 + 标量 | 独立标量索引组合（AND 取交集，OR 取并集） | N/A  |
| **向标混合** | 向量 + 标量 | 预过滤 / 后过滤                | 预过滤  |
| **向向混合** | 向量 + 向量 | MaxSim / RRF             | N/A  |

### 10.3 关键特性

- ✅ 支持多种标量索引类型（BTREE、BITMAP、LABEL_LIST）

- ✅ 支持多种向量索引类型（IVF_PQ、IVF_HNSW_*、IVF_RQ）

- ✅ 支持全文搜索索引（倒排索引 + BM25）

- ✅ 支持标量 + 向量混合查询（预过滤/后过滤）

- ✅ 支持多向量查询（MaxSim）

- ✅ 支持向量 + FTS 混合检索（RRF）

- ✅ 支持通过 `explain_plan` 观察优化器决策

- ✅ 优化器支持复杂嵌套 OR 的多索引拆解

- ✅ 非索引条件采用 refine_filter 二次过滤而非全表扫描

### 10.4 已知限制

| 限制                    | 说明                                  |
| --------------------- | ----------------------------------- |
| **复合索引**              | 不支持一个索引覆盖多个列                        |
| **索引自动更新**            | 数据追加后需手动 `optimize()`               |
| **wait_for_index 超时** | 大数据集下可能超时，建议用 `list_indices()` 检查状态 |
