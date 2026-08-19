# LanceDB 查询行为深度解析

> **文档版本**：v2.0 | **更新日期**：2026-08-19 | **适用版本**：LanceDB 0.37.1
> 
> **说明**：本文档基于 LanceDB 0.37.1 实际测试结果整理，深入解析 LanceDB 物理存储格式、索引结构、查询执行计划及预过滤/后过滤机制。所有执行计划均来自实际测试输出。

## 目录

1. 物理存储格式

2. 索引物理存储结构

3. Row ID 与索引的关系

4. 查询执行计划解析

5. 执行计划对比分析

6. 预过滤与后过滤深度解析

7. 总结与最佳实践

## 1. 物理存储格式

LanceDB 底层使用 **Lance 列式存储格式**，一个 LanceDB 表在磁盘上是一个 **Lance Dataset**。

### 1.1 顶层目录结构

```textile
your_table.lance/
├── data/               # 数据文件目录
│   └── *.lance         # 一个或多个数据文件
├── _indices/           # 索引文件目录
│   ├── <index_uuid>/   # 每个索引一个子目录
│   │   ├── *.idx       # 索引文件
│   │   └── ...
├── _versions/          # 版本元数据目录
│   └── *.manifest      # 清单文件 (Manifest)
└── _transactions/      # 事务日志目录
    └── *.txn           # 事务文件
```

### 1.2 各目录作用

| 目录                   | 作用               | 说明                                       |
| -------------------- | ---------------- | ---------------------------------------- |
| **`data/`**          | 存储实际表数据的列式文件     | 每个文件存储部分列的数据，同一行在不同文件间通过 `Row Offset` 对齐 |
| **`_indices/`**      | 存储所有索引数据         | 每个索引由 UUID 唯一标识，存储为独立的 Lance 文件          |
| **`_versions/`**     | 存储清单文件（Manifest） | 描述表在特定版本下的“完整状态快照”，包含片段、索引和 Schema 信息    |
| **`_transactions/`** | 存储事务日志           | 记录每一次修改表状态的操作，实现 **MVCC 多版本并发控制**        |

### 1.3 核心数据单元：片段 (Fragment) 与数据文件

| 概念                  | 类型   | 说明                                                        |
| ------------------- | ---- | --------------------------------------------------------- |
| **片段（Fragment）**    | 逻辑概念 | 由清单文件定义，代表一组行的集合，是数据管理的基本单元                               |
| **数据文件（Data File）** | 物理文件 | 一个片段可由多个数据文件组成，存储不同列的数据。同一片段内所有数据文件行数相同，按 `Row Offset` 对齐 |

一个片段内的数据文件结构示意：

```textile
Fragment 0:
  ├── data/0_0.lance    (列: id, b, c)
  ├── data/0_1.lance    (列: vector)
  └── data/0_2.lance    (列: d, label)
  # 所有文件行数相同，通过 Row Offset 对齐
```

## 2. 索引物理存储结构

所有索引独立存储在 `_indices/<uuid>/` 目录中，每个索引由唯一的 UUID 标识。

### 2.1 向量索引（IVF-PQ）

```textile
_indices/<uuid>/
├── index.idx          # 主索引文件（IVF 模型、分区元数据）
└── auxiliary.idx      # 辅助数据文件（PQ 码本）
```

**存储内容**：

- **`index.idx`**：IVF 聚类中心（Centroids）、分区列表、每个分区的倒排列表，倒排列表中存储 `(PQ编码, Row Address)` 对

- **`auxiliary.idx`**：PQ 码本（Codebook），存储子向量到码字的映射

### 2.2 标量索引（BTree）

```textile
_indices/<uuid>/
├── page_lookup.lance  # 页查找表（BTree 头条目）
└── page_data.lance    # 排序的键值页（存储 (value, row_id) 对）
```

**存储内容**：

- **`page_lookup.lance`**：BTree 的头节点，缓存于内存，用于定位数据页

- **`page_data.lance`**：按列值排序的数据页，每页 4096 行，存储排序后的值和对应的 `Row Address`

### 2.3 标量索引（Bitmap）

```textile
_indices/<uuid>/
└── bitmap_<field>.lance  # 值→行ID 位图文件
```

### 2.4 全文搜索索引（FTS）

```textile
_indices/<uuid>/
├── metadata.lance     # 分区列表和 Tokenizer 配置
├── tokens_<pid>.lance # Token → ID 映射（FST 或 Arrow 格式）
├── posting_<pid>.lance# 倒排列表（每个 Token 的文档 ID 和位置，压缩存储）
└── docs_<pid>.lance   # 文档元数据（文档长度、Token 数量，用于 BM25 评分）
```

## 3. Row ID 与索引的关系

Row ID 是连接数据与索引的“通用语言”，在 LanceDB 中扮演着**行级指针**的角色。

### 3.1 Row Address（当前实现）

这是 LanceDB 当前版本主要使用的标识符，是一个 **64 位整数**，编码了行的物理存储位置：

```textile
Row Address = (fragment_id << 32) | row_offset
```

| 组成部分              | 位数     | 说明            |
| ----------------- | ------ | ------------- |
| **`fragment_id`** | 高 32 位 | 标识该行属于哪个片段    |
| **`row_offset`**  | 低 32 位 | 标识该行在片段内的行偏移量 |

**特点**：

- ✅ **定位快**：通过位运算可直接定位到数据，实现常数时间的随机访问

- ✅ **内存高效**：64 位整数，存储和传输开销低

- ❌ **不稳定**：数据发生 Compaction（数据重组）或更新时，物理位置变化，Row Address 随之改变

### 3.2 Stable Row ID（规划中）

Lance 正在支持的新特性，作为**逻辑主键**使用，在行的一生中保持不变。

**特点**：

- ✅ **稳定**：即使行因 Compaction 而移动，Stable Row ID 不变

- ✅ **解耦**：索引与物理存储位置解耦，数据重组后只需更新映射表

- ⚠️ **映射开销**：需要维护 Stable Row ID → Row Address 的映射索引

### 3.3 索引中存储的内容

索引的核心作用就是**快速返回一组满足条件的 Row Address**：

| 索引类型            | 存储内容                            | 返回结果                      |
| --------------- | ------------------------------- | ------------------------- |
| **BTree 标量索引**  | 排序后的 `(value, row_address)` 对   | 满足条件的 `row_address` 集合    |
| **Bitmap 标量索引** | 每个 distinct 值的 Roaring Bitmap   | 对应值的 `row_address` 集合（位图） |
| **IVF-PQ 向量索引** | 倒排列表中存储 `(PQ编码, row_address)` 对 | 最相似的 `row_address` 列表     |
| **FTS 全文索引**    | 倒排索引 `term → posting list`      | 包含关键词的 `row_address` 列表   |

### 3.4 Row ID 掩码（Row Mask）

Row ID 掩码是**行号（Row Address）的集合**，用于标记哪些行需要被处理。

**两种形式**：

| 形式                  | 说明               | 适用场景              |
| ------------------- | ---------------- | ----------------- |
| **允许列表（AllowList）** | 存储**需要被包含**的行 ID | 高选择性过滤（能筛选掉大部分数据） |
| **阻止列表（BlockList）** | 存储**需要被排除**的行 ID | 低选择性过滤（只能排除少量数据）  |

**存储形式**：

- **小结果集（< 几万行）**：直接存储为整数数组 `[101, 205, 307, ...]`

- **中等规模（几十万行）**：Roaring Bitmap（压缩位图）

- **大规模（数百万行）**：位图（每个 bit 代表一行）

## 4. 查询执行计划解析

### 4.1 执行计划结构总览

LanceDB 向量查询的执行计划通常包含以下核心节点：

```textile
ProjectionExec          ← 最终投影，选择输出列
  └── LanceRead         ← 数据扫描
      └── SortExec/TopK ← 排序取 Top-K
          └── ANNSubIndex ← 向量索引搜索（近似最近邻）
              └── ANNIvfPartition ← IVF 分区扫描
              └── ScalarIndexQuery / LanceRead ← 标量过滤 / 数据扫描
```

### 4.2 关键字段与算子

#### 4.2.1 `ScalarIndexQuery`

表示**标量索引被使用**，出现在执行计划中说明过滤条件利用了索引。

```textile
# 单索引
ScalarIndexQuery: query=[b = 2]@b_idx(BTree)

# 多索引 AND 组合
ScalarIndexQuery: query=AND([b = 2]@b_idx(BTree), [c = 3]@c_idx(BTree))

# 多索引 OR 组合
ScalarIndexQuery: query=OR([b = 2]@b_idx(BTree), [c = 3]@c_idx(BTree))
```

#### 4.2.2 `refine_filter`

表示**部分过滤条件无法使用索引，在数据读取后进行内存过滤**。

```textile
LanceRead: full_filter=b = Int64(2) AND d = Int64(3), refine_filter=d = Int64(3)
```

**⚠️ 关键**：`refine_filter` 的位置决定了过滤发生的阶段：

| `refine_filter` 位置                 | 过滤阶段              | 结果数量                          |
| ---------------------------------- | ----------------- | ----------------------------- |
| `ANNSubIndex` **内部**               | 向量搜索**之前**（预过滤阶段） | **保证 `limit` 条**（受 ANN 召回率影响） |
| `ANNSubIndex` **外部**（`FilterExec`） | 向量搜索**之后**（后过滤阶段） | **可能少于 `limit` 条**            |

#### 4.2.3 `LanceRead`

数据扫描节点，扫描 Lance 格式的数据文件。

```textile
LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[id, b, c, d, vector], source=stream(_rowid)
```

- `uri`：数据文件路径

- `projection`：需要读取的列

- `full_filter`：下推到存储层的完整过滤条件

- `refine_filter`：无法下推、在内存中二次过滤的条件

- `row_id=true`：表示需要读取 Row ID（用于生成掩码）

#### 4.2.4 `ANNSubIndex` / `ANNIvfPartition`

向量索引搜索节点，执行近似最近邻搜索。

```textile
ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
  ANNIvfPartition: uuid=25ae3e18-0e26-4362-9777-574cb7339ddd, minimum_nprobes=20, maximum_nprobes=Some(20), deltas=1
```

- `name`：向量索引名称

- `k`：返回的 Top-K 结果数

- `metric`：距离度量（L2、Cosine 等）

- `minimum_nprobes` / `maximum_nprobes`：搜索的分区数范围

## 5. 执行计划对比分析

基于以下测试环境：

- **表结构**：`vector`（向量列，IVF-PQ 索引）、`b`（BTree 索引）、`c`（BTree 索引）、`d`（无索引）

- **数据量**：1000 行

- **查询**：`SELECT * FROM tab WHERE ... ORDER BY vector LIMIT 5`

### 5.1 场景一：`WHERE b = 2`

**完整执行计划**：

```textile
ProjectionExec: expr=[id@2 as id, b@3 as b, c@4 as c, d@5 as d, vector@6 as vector, _distance@0 as _distance]
  LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[id, b, c, d, vector], source=stream(_rowid)
    SortExec: TopK(fetch=5), expr=[_distance@0 ASC NULLS LAST, _rowid@1 ASC NULLS LAST], preserve_partitioning=[false]
      ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
        ANNIvfPartition: uuid=25ae3e18-0e26-4362-9777-574cb7339ddd, minimum_nprobes=20, maximum_nprobes=Some(20), deltas=1
        ScalarIndexQuery: query=[b = 2]@b_idx(BTree)
```

**逐行解读**：

| 行   | 算子/字段                                                    | 含义                                          |
| --- | -------------------------------------------------------- | ------------------------------------------- |
| 1   | `ProjectionExec: expr=[id@2 as id, ...]`                 | 最终输出列，`@2` 表示该列在扫描结果中的位置索引                  |
| 2   | `LanceRead: uri=..., projection=[id, b, c, d, vector]`   | 从数据文件读取指定列，`source=stream(_rowid)` 表示按行流式读取 |
| 3   | `SortExec: TopK(fetch=5), expr=[_distance@0 ASC ...]`    | 对距离列升序排序，只保留前 5 行                           |
| 4   | `ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2` | 使用名为 `vector_idx` 的向量索引，返回 Top-5，距离度量 L2    |
| 5   | `ANNIvfPartition: uuid=..., minimum_nprobes=20, ...`     | IVF 分区扫描，实际搜索 20 个分区                        |
| 6   | `ScalarIndexQuery: query=[b = 2]@b_idx(BTree)`           | 使用 BTree 索引 `b_idx` 过滤 `b=2`，生成 Row ID 掩码   |

**执行流程**：

1. `ScalarIndexQuery` 在 BTree 索引中查找 `b=2`，返回满足条件的 **Row ID 集合**

2. 将 Row ID 集合组织为 **允许列表（AllowList）** 掩码

3. `ANNSubIndex` 读取掩码，在 IVF 分区扫描时**只对掩码标记的行计算向量距离**

4. `SortExec` 对距离排序取 Top-5，`ProjectionExec` 输出结果

**结果数量**：✅ 保证 5 条（过滤在向量搜索前完成）

### 5.2 场景二：`WHERE c = 3`

**完整执行计划**：

```textile
ProjectionExec: expr=[id@2 as id, b@3 as b, c@4 as c, d@5 as d, vector@6 as vector, _distance@0 as _distance]
  LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[id, b, c, d, vector], source=stream(_rowid)
    SortExec: TopK(fetch=5), expr=[_distance@0 ASC NULLS LAST, _rowid@1 ASC NULLS LAST], preserve_partitioning=[false]
      ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
        ANNIvfPartition: uuid=25ae3e18-0e26-4362-9777-574cb7339ddd, minimum_nprobes=20, maximum_nprobes=Some(20), deltas=1
        ScalarIndexQuery: query=[c = 3]@c_idx(BTree)
```

**解读**：与场景一完全对称，只是索引列从 `b` 换成了 `c`。`c` 的 BTree 索引返回满足 `c=3` 的 Row ID 集合。

### 5.3 场景三：`WHERE b = 2 AND c = 3`

**完整执行计划**：

```textile
ProjectionExec: expr=[id@2 as id, b@3 as b, c@4 as c, d@5 as d, vector@6 as vector, _distance@0 as _distance]
  LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[id, b, c, d, vector], source=stream(_rowid)
    SortExec: TopK(fetch=5), expr=[_distance@0 ASC NULLS LAST, _rowid@1 ASC NULLS LAST], preserve_partitioning=[false]
      ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
        ANNIvfPartition: uuid=25ae3e18-0e26-4362-9777-574cb7339ddd, minimum_nprobes=20, maximum_nprobes=Some(20), deltas=1
        ScalarIndexQuery: query=AND([b = 2]@b_idx(BTree),[c = 3]@c_idx(BTree))
```

**执行流程**：

1. `b` 索引返回满足 `b=2` 的 Row ID 集合 `Set_B`

2. `c` 索引返回满足 `c=3` 的 Row ID 集合 `Set_C`

3. 内存中计算 `Set_B ∩ Set_C`，生成最终的 Row ID 掩码

4. `ANNSubIndex` 按掩码执行向量搜索

**结果数量**：✅ 保证 5 条

### 5.4 场景四：`WHERE b = 2 AND c = 3 AND d = 5`

**完整执行计划**：

```textile
ProjectionExec: expr=[id@2 as id, b@3 as b, c@4 as c, d@5 as d, vector@6 as vector, _distance@0 as _distance]
  LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[id, b, c, d, vector], source=stream(_rowid)
    SortExec: TopK(fetch=5), expr=[_distance@0 ASC NULLS LAST, _rowid@1 ASC NULLS LAST], preserve_partitioning=[false]
      ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
        ANNIvfPartition: uuid=25ae3e18-0e26-4362-9777-574cb7339ddd, minimum_nprobes=20, maximum_nprobes=Some(20), deltas=1
        LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[], num_fragments=1, range_before=None, range_after=None, row_id=true, row_addr=false, full_filter=b = Int64(2) AND c = Int64(3) AND d = Int64(5), refine_filter=d = Int64(5)
          ScalarIndexQuery: query=AND([b = 2]@b_idx(BTree),[c = 3]@c_idx(BTree))
```

**逐行解读**：

| 行   | 算子/字段                                                                       | 含义                                                                                 |
| --- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| 1-4 | 同上                                                                          | 与场景一相同的外层结构                                                                        |
| 5   | `LanceRead: projection=[], row_id=true, full_filter=..., refine_filter=d=5` | 内层 `LanceRead` 用于**读取 Row ID 并应用 refine 过滤**，`projection=[]` 表示不读取任何数据列，只使用 Row ID |
| 6   | `ScalarIndexQuery: query=AND([b=2]@b_idx, [c=3]@c_idx)`                     | 双索引取交集生成 Row ID 掩码                                                                 |

**关键观察**：

- `refine_filter=d=5` 出现在内层 `LanceRead` 中

- 该 `LanceRead` 位于 `ANNSubIndex` 内部

- 说明 `d=5` 的过滤在向量搜索**之前**执行

**结果数量**：✅ 保证 5 条（因为 refine 在向量搜索前完成）

### 5.5 预过滤 vs 后过滤

#### 预过滤（`prefilter=True`，默认）

**完整执行计划**：

```textile
ProjectionExec: expr=[id@2 as id, b@3 as b, c@4 as c, d@5 as d, vector@6 as vector, _distance@0 as _distance]
  LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[id, b, c, d, vector], source=stream(_rowid)
    SortExec: TopK(fetch=5), expr=[_distance@0 ASC NULLS LAST, _rowid@1 ASC NULLS LAST], preserve_partitioning=[false]
      ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
        ANNIvfPartition: uuid=25ae3e18-0e26-4362-9777-574cb7339ddd, minimum_nprobes=20, maximum_nprobes=Some(20), deltas=1
        ScalarIndexQuery: query=[b = 2]@b_idx(BTree)
```

#### 后过滤（`prefilter=False`）

**完整执行计划**：

```textile
ProjectionExec: expr=[id@3 as id, b@2 as b, c@4 as c, d@5 as d, vector@6 as vector, _distance@0 as _distance]
  LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[id, c, d, vector], source=stream(_rowid)
    GlobalLimitExec: skip=0, fetch=5
      FilterExec: b@2 = 2
        LanceRead: uri=tmp/lancedb_demo_96y51b95/tab.lance/data, projection=[b], source=stream(_rowid)
          SortExec: TopK(fetch=5), expr=[_distance@0 ASC NULLS LAST, _rowid@1 ASC NULLS LAST], preserve_partitioning=[false]
            ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
              ANNIvfPartition: uuid=25ae3e18-0e26-4362-9777-574cb7339ddd, minimum_nprobes=20, maximum_nprobes=Some(20), deltas=1
```

**对比解读**：

| 特征                    | 预过滤（prefilter=True）  | 后过滤（prefilter=False）                         |
| --------------------- | -------------------- | -------------------------------------------- |
| `ScalarIndexQuery` 位置 | `ANNSubIndex` **内部** | 无 `ScalarIndexQuery`                         |
| `FilterExec` 位置       | 无                    | `ANNSubIndex` **外部**（在 `GlobalLimitExec` 之下） |
| 过滤执行顺序                | 标量过滤 → 向量搜索          | 向量搜索 → 标量过滤                                  |

## 6. 预过滤与后过滤深度解析

### 6.1 什么是预过滤

预过滤是指**先执行标量过滤，再执行向量搜索**的执行模式。这是 LanceDB 的默认行为（`prefilter=True`）。

**核心机制**：利用标量索引生成 Row ID 掩码，向量索引按掩码执行。

### 6.2 预过滤的执行流程

```flow
flowchart TD
    A[SQL 查询<br>WHERE b = 2 ...] --> B(LanceDB 优化器)
    
    B --> C[识别标量条件 b=2]
    B --> D[识别向量列 a]

    C --> E[查询 BTree 标量索引]
    E --> F[生成 Row ID 掩码<br>（允许列表）]

    F --> G[向量索引<br>（IVF-PQ）]
    D --> G

    G --> H{对每个 IVF 分区}
    H --> I[检查 Row ID 掩码]
    I --> J{分区内是否有<br>Row ID 在掩码中？}
    J -- 否 --> K[跳过整个分区<br>（无 I/O，无距离计算）]
    J -- 是 --> L[读取掩码中 Row ID 的向量<br>计算距离]

    L --> M[合并结果，排序，返回]
    K --> H
```

### 6.3 预过滤是否会导致结果不足？

**是的，预过滤同样可能导致返回结果不足。**

原因不在于预过滤机制本身，而在于**向量索引（ANN）的近似搜索特性**：

1. **分区搜索**：IVF 索引只会搜索与查询向量最接近的 `nprobes` 个分区

2. **遗漏风险**：如果满足标量条件的行恰好集中在未被搜索的分区中，它们就会被遗漏

**结果不足的双重条件**：

- 数据集中有 `limit` 条满足标量条件的行（标量层面满足）

- 这些行必须分布在向量索引所搜索的 `nprobes` 个分区内（向量索引层面满足）

### 6.4 后过滤的行为

后过滤（`prefilter=False`）是指**先执行向量搜索，再执行标量过滤**。

**执行流程**：

1. 向量索引搜索全部数据，返回全局最相似的 Top-K 结果

2. 对 Top-K 结果应用标量过滤条件

3. 返回过滤后的结果

**结果不足的原因**：如果 Top-K 结果中满足标量条件的行少于 `limit`，返回结果数将少于 `limit`，甚至为 0。

### 6.5 两种模式对比

| 维度            | 预过滤（Prefilter）       | 后过滤（Post-filter）   |
| ------------- | -------------------- | ------------------ |
| **执行顺序**      | ① 标量过滤 → ② 向量搜索      | ① 向量搜索 → ② 标量过滤    |
| **Row ID 掩码** | 标量索引生成，向量索引使用        | 向量索引不使用掩码          |
| **结果数量**      | 可能少于 `limit`（ANN 遗漏） | 可能少于 `limit`（过滤不足） |
| **I/O 效率**    | 高（跳过无关分区）            | 低（向量搜索全部数据）        |
| **默认行为**      | ✅ 是                  | ❌ 否                |

### 6.6 调优参数

| 参数                        | 作用                | 调优建议                                     |
| ------------------------- | ----------------- | ---------------------------------------- |
| **`nprobes`**             | 控制向量索引搜索的分区数量     | 增大可提高召回率，降低结果不足风险；建议覆盖 5%-10% 的分区数       |
| **`refine_factor`**       | 扩大候选池进行重排序        | 设置 `refine_factor=10`，先取 10×limit 个候选再过滤 |
| **`bypass_vector_index`** | 强制暴力搜索（Flat Scan） | 数据量不大或对精确性有极高要求时使用，100% 召回率              |

## 7. 总结与最佳实践

### 7.1 `refine_filter` 位置决定行为

| `refine_filter` 位置             | 过滤阶段              | 结果数量                      |
| ------------------------------ | ----------------- | ------------------------- |
| `ANNSubIndex` 内部               | 向量搜索**之前**（预过滤阶段） | 可能少于 `limit`（受 ANN 召回率影响） |
| `ANNSubIndex` 外部（`FilterExec`） | 向量搜索**之后**（后过滤阶段） | 可能少于 `limit`（受过滤条件影响）     |

### 7.2 场景总结

| 场景                     | 索引利用             | Row ID 掩码来源  | 过滤时机                   | 结果数量                     |
| ---------------------- | ---------------- | ------------ | ---------------------- | ------------------------ |
| 有索引列 `AND` 有索引列        | ✅ 多索引取交集         | 多个索引交集       | 向量搜索前                  | 可能少于 `limit`（ANN 影响）     |
| 有索引列 `AND` 无索引列        | ⚠️ 部分索引 + refine | 单索引 + refine | 向量搜索前（refine 在 ANN 内部） | 可能少于 `limit`（ANN 影响）     |
| 预过滤（`prefilter=True`）  | ✅ 利用可用索引         | 标量索引生成       | 向量搜索前                  | 可能少于 `limit`（ANN 影响）     |
| 后过滤（`prefilter=False`） | ⚠️ 可能部分利用        | 不使用          | 向量搜索后                  | **可能少于 `limit`**（过滤条件影响） |
| `OR` 含无索引分支            | ❌ 放弃整个 OR        | 不使用          | 向量搜索后                  | **可能少于 `limit`**         |

### 7.3 最佳实践

1. **默认使用预过滤**：`prefilter=True`（默认）是更安全的选择

2. **为高频过滤列建索引**：索引越多，Row ID 掩码越精确

3. **调优 `nprobes` 平衡召回率**：增大可降低结果不足风险，但会增加延迟

4. **使用 `explain_plan` 验证**：确认 `refine_filter` 的位置和索引利用情况

5. **避免 `OR` 中包含无索引列**：会导致整个 `OR` 无法走索引

6. **如需后过滤，接受结果可能不足**：或通过增大 `refine_factor` 缓解

## 附录：测试验证

本文档所有结论均基于 LanceDB 0.37.1 实际测试验证，测试环境如下：

| 项目         | 规格                                              |
| ---------- | ----------------------------------------------- |
| LanceDB 版本 | 0.37.1                                          |
| 数据量        | 1000 行                                          |
| 向量维度       | 128                                             |
| 索引配置       | `vector`（IVF-PQ）、`b`（BTree）、`c`（BTree）、`d`（无索引） |
| 测试场景       | 4 种不同 `WHERE` 条件 + 预过滤/后过滤对比                    |

*本文档基于 LanceDB 0.37.1 实际测试结果整理，如有更新请以官方最新文档为准。*
