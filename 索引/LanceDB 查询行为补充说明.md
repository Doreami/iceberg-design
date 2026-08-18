# LanceDB 查询行为补充说明

> **文档版本**：v1.0 | **更新日期**：2026-08-18 | **适用版本**：LanceDB 0.37.1
> 
> **说明**：本文档基于 LanceDB 0.37.1 实际测试结果整理，补充主文档中未详细展开的查询行为细节。

## 目录

1. 后过滤（Post-filtering）行为详解

2. SQL 执行计划解析

3. 执行计划对比分析

4. 总结

## 1. 后过滤（Post-filtering）行为详解

### 1.1 什么是后过滤

后过滤是指**先执行向量搜索，再用标量条件过滤结果**的执行模式。与预过滤相反。

### 1.2 核心行为

**当后过滤过滤后的结果少于 `limit` 时，LanceDB 会直接返回过滤后剩余的所有结果，数量可能少于 `limit`，甚至返回空结果。**

这是由执行顺序决定的：

1. 先执行向量搜索，从整个数据集中找出全局最相似的 Top-K 个结果

2. 再对这 K 个结果应用标量过滤条件

3. 返回满足条件的行

因此，最终结果数量完全取决于 Top-K 结果中有多少能满足过滤条件。

### 1.3 两种过滤模式对比

| 特性       | 后过滤（Post-filtering）        | 预过滤（Pre-filtering）         |
| -------- | -------------------------- | -------------------------- |
| **执行顺序** | ① 向量搜索 → ② 标量过滤            | ① 标量过滤 → ② 向量搜索            |
| **结果数量** | **可能少于 `limit`，甚至为 0**     | **保证返回 `limit` 条**（如果数据足够） |
| **默认行为** | 否（需显式指定 `prefilter=False`） | **是**（默认）                  |

### 1.4 处理结果不足的方法

| 方法                     | 说明                                                                            |
| ---------------------- | ----------------------------------------------------------------------------- |
| **使用预过滤**              | 设置 `prefilter=True`（默认），确保向量搜索仅在满足过滤条件的数据上进行                                  |
| **增大 `refine_factor`** | 后过滤模式下，增大此值可让向量搜索返回更多候选（如 `limit=10`，`refine_factor=10` 可取 100 个候选），提高满足条件的概率 |
| **检查过滤条件**             | 如果过滤条件过于严苛，满足条件的数据本身就不足 `limit` 条，任何模式都无法返回足够结果                               |

## 2. SQL 执行计划解析

### 2.1 执行计划结构总览

LanceDB 向量查询的执行计划通常包含以下核心节点：

```textile
ProjectionExec          ← 最终投影，选择输出列
  └── SortExec/TopK     ← 排序取 Top-K
      └── KNNVectorDistance / ANNSubIndex  ← 向量距离计算
          └── LanceRead / ScalarIndexQuery ← 数据扫描 / 标量索引
```

### 2.2 关键字段与算子

#### 2.2.1 `ScalarIndexQuery`

表示**标量索引被使用**，出现在执行计划中说明过滤条件利用了索引。

```textile
ScalarIndexQuery: query=[b = 2]@b_idx(BTree)
ScalarIndexQuery: query=AND([b = 2]@b_idx(BTree), [c = 3]@c_idx(BTree))
ScalarIndexQuery: query=OR([b = 2]@b_idx(BTree), [c = 3]@c_idx(BTree))
```

#### 2.2.2 `refine_filter`

表示**部分过滤条件无法使用索引，在数据读取后进行内存过滤**。

```textile
LanceRead: full_filter=b = Int64(2) AND d = Int64(3), refine_filter=d = Int64(3)
```

**关键**：`refine_filter` 的位置决定了过滤发生的阶段：

- 在 `ANNSubIndex` **内部**：过滤发生在**向量搜索之前**

- 在 `ANNSubIndex` **外部**（如 `FilterExec`）：过滤发生在**向量搜索之后**

#### 2.2.3 `LanceRead`

数据扫描节点，扫描 Lance 格式的数据文件。

```textile
LanceRead: uri=..., projection=[id, b, c, d, vector], full_filter=..., refine_filter=--
```

#### 2.2.4 `ANNSubIndex` / `KNNVectorDistance`

向量索引搜索节点，执行近似最近邻搜索。

```textile
ANNSubIndex: name=vector_idx, k=5, deltas=1, metric=L2
  ANNIvfPartition: uuid=..., minimum_nprobes=20, maximum_nprobes=Some(20)
```

## 3. 执行计划对比分析

以下基于测试用例 `b` 有索引、`c` 有索引、`d` 无索引、`vector` 有向量索引的场景。

### 3.1 场景一：`WHERE b = 2`

**SQL**：

```sql
SELECT * FROM tab WHERE b = 2 ORDER BY vector LIMIT 5;
```

**执行计划**：

```textile
ProjectionExec: ...
  LanceRead: ...
    SortExec: TopK(fetch=5)
      ANNSubIndex: name=vector_idx
        ANNIvfPartition: ...
        ScalarIndexQuery: query=[b = 2]@b_idx(BTree)
```

**解读**：

- `b` 有索引，走索引预过滤

- 向量搜索仅在 `b=2` 的候选行上进行

- 结果数量：**保证 5 条**

### 3.2 场景二：`WHERE b = 2 AND c = 3`

**SQL**：

```sql
SELECT * FROM tab WHERE b = 2 AND c = 3 ORDER BY vector LIMIT 5;
```

**执行计划**：

```textile
ProjectionExec: ...
  LanceRead: ...
    SortExec: TopK(fetch=5)
      ANNSubIndex: name=vector_idx
        ANNIvfPartition: ...
        ScalarIndexQuery: query=AND([b = 2]@b_idx(BTree), [c = 3]@c_idx(BTree))
```

**解读**：

- `b` 和 `c` 都有索引，AND 组合走多索引取交集

- 向量搜索仅在交集候选行上进行

- 结果数量：**保证 5 条**

### 3.3 场景三：`WHERE b = 2 AND d = 3`

**SQL**：

```sql
SELECT * FROM tab WHERE b = 2 AND d = 3 ORDER BY vector LIMIT 5;
```

**执行计划**：

```textile
ProjectionExec: ...
  LanceRead: ...
    SortExec: TopK(fetch=5)
      ANNSubIndex: name=vector_idx
        ANNIvfPartition: ...
        LanceRead: full_filter=b = 2 AND d = 3, refine_filter=d = 3
          ScalarIndexQuery: query=[b = 2]@b_idx(BTree)
```

**解读**：

- `b` 有索引，`d` 无索引

- `b=2` 通过索引预过滤，`d=3` 在 `refine_filter` 中内存过滤

- **关键**：`refine_filter` 出现在 `ANNSubIndex` 内部，说明过滤发生在向量搜索**之前**

- 结果数量：**保证 5 条**（因为 `d=3` 的 refine 在向量搜索前完成）

### 3.4 场景四：后过滤模式（`prefilter=False`）

**SQL**：

```sql
SELECT * FROM tab WHERE b = 2 ORDER BY vector LIMIT 5;
-- 且 prefilter=False
```

**执行计划**：

```textile
ProjectionExec: ...
  GlobalLimitExec: fetch=5
    FilterExec: b = 2
      LanceRead: projection=[b]
        SortExec: TopK(fetch=5)
          ANNSubIndex: name=vector_idx
            ANNIvfPartition: ...
```

**解读**：

- `FilterExec` 出现在 `ANNSubIndex` 之后

- 先向量搜索，再标量过滤

- 结果数量：**可能少于 5 条**

### 3.5 场景五：`OR` 中包含无索引列

**SQL**：

```sql
SELECT * FROM tab WHERE (b = 2 AND c = 3) OR (d = 4) ORDER BY vector LIMIT 5;
```

**执行计划**：

```textile
ProjectionExec: ...
  LanceRead: full_filter=..., refine_filter=...
    SortExec: TopK(fetch=5)
      KNNVectorDistance: metric=l2
        LanceScan: ...
```

**解读**：

- `d` 无索引，`OR` 条件中有一个分支无法走索引

- 优化器**放弃对整个 `OR` 使用索引**，退化为全表扫描

- 结果数量：**可能少于 5 条**（向量搜索后过滤）

## 4. 总结

### 4.1 `refine_filter` 位置决定行为

| `refine_filter` 位置             | 过滤阶段       | 结果数量               |
| ------------------------------ | ---------- | ------------------ |
| `ANNSubIndex` 内部               | 向量搜索**之前** | **保证 `limit` 条**   |
| `ANNSubIndex` 外部（`FilterExec`） | 向量搜索**之后** | **可能少于 `limit` 条** |
| `LanceRead` 中（`OR` 场景）         | 向量搜索**之后** | **可能少于 `limit` 条** |

### 4.2 核心结论

| 场景                     | 索引利用             | 过滤时机                   | 结果数量               |
| ---------------------- | ---------------- | ---------------------- | ------------------ |
| 有索引列 `AND` 有索引列        | ✅ 多索引取交集         | 向量搜索前                  | 保证 `limit` 条       |
| 有索引列 `AND` 无索引列        | ⚠️ 部分索引 + refine | 向量搜索前（refine 在 ANN 内部） | 保证 `limit` 条       |
| 预过滤（`prefilter=True`）  | ✅ 利用可用索引         | 向量搜索前                  | 保证 `limit` 条       |
| 后过滤（`prefilter=False`） | ⚠️ 可能部分利用        | 向量搜索后                  | **可能少于 `limit` 条** |
| `OR` 含无索引分支            | ❌ 放弃整个 OR 的索引    | 向量搜索后                  | **可能少于 `limit` 条** |

### 4.3 最佳实践

1. **默认使用预过滤**：`prefilter=True`（默认）能保证结果数量

2. **为高频过滤列建索引**：索引越多，预过滤越精确

3. **使用 `explain_plan` 验证**：确认索引是否被利用，以及 `refine_filter` 的位置

4. **避免 `OR` 中包含无索引列**：会导致整个 `OR` 无法走索引

5. **如需后过滤，接受结果可能不足**：或通过增大 `refine_factor` 缓解
