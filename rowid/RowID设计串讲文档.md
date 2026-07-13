# RowID 设计串讲

> **文档目的**：串讲 RowID 特性的设计理念、核心数据结构、新增与修改的接口、执行流程与存储结构。
> 
> **配套文档**：架构与阶段规划的权威来源是 [`iceberg-index-rowid-设计文档.md`](./iceberg-index-rowid-设计文档.md)，本文是其"对外使用"视角的浓缩串讲。
> 
> **适用范围**：基于 `iceberg-rust`（0.10.0，含 RowID 增强）+ `iceberg-index` 索引 SDK。

---

## 术语表

| 术语                            | 含义                                                          |
| ----------------------------- | ----------------------------------------------------------- |
| **RowID**（`_row_id`）          | 行的**逻辑 ID**，分配后终生不变（Compaction/UPDATE 都不变）                  |
| **RowAddr**（`_file` + `_pos`） | 行的**物理地址** `(file_path, row_position)`，会随重写变化               |
| **`_pos`**                    | 行在文件内的 0-based 行号（**删除过滤之后**计数，只计算存活行）                      |
| **first_row_id**              | 一个 DataFile 首行的 RowID；文件内满足 `_row_id = first_row_id + _pos` |
| **next-row-id**               | 表级全局游标：下一个可分配的 `_row_id`；分配后 `+= 新增行数`，只增不减                 |
| **游标 (cursor)**               | 顺序分配 RowID 用的"下一个可用值"指针，分配后向前推进                             |
| **二级/倒排索引**                   | 插件（BTree/IVF/IVF-PQ）存储的 `索引键 → RowID` 结构；检索它得到候选 RowID      |
| **COW**（Copy-on-Write）        | 更新时**重写整个数据文件**（老文件删、新文件写）                                  |
| **MOR**（Merge-on-Read）        | 更新时写 **delete 文件**标记旧行 + 写**新数据文件**放新行，读时合并                 |
| **no-op**                     | 空操作 —— 不做任何改动，直接复用现状                                        |
| **blob**                      | Puffin 文件里一段带类型标识的独立负载（二进制或 JSON）                           |
| **StatisticsFile**            | Iceberg 快照上挂载 Puffin 文件的**原生元数据入口**                         |

---

## 一、RowID 基础与设计动机

> 本章 1.1–1.4 是 Iceberg RowID 的通用背景；1.5 起是本项目二级索引为什么依赖它。

### 1.1 什么是 `_row_id`（Iceberg 行级血缘）

`_row_id` 是 Iceberg 表的一个**元数据列**（`BIGINT`），给每一行分配一个**稳定、唯一的逻辑 ID**：理念上在整个生命周期内**永不改变**。它由数据文件的 `first_row_id` 加行内偏移导出：`_row_id = first_row_id + row_position`。（update/merge/compaction等操作需要由引擎来支持继承旧 ID， 把_row_id写入物理列）

它与 `_last_updated_sequence_number`（最后更新序列号）共同构成**行级血缘（Row Lineage）**：`_row_id` 回答"这是哪一行"，后者回答"这行最后在哪个快照被改"。典型价值：行级血缘/CDC 增量、合规审计（GDPR 删除确认）、精确行级 DML、**二级索引的稳定锚点**。

> 当前索引只消费 `_row_id`；`_last_updated_sequence_number` 等血缘字段不在索引范围内。

### 1.2 什么是 `next-row-id`

`next-row-id` 是**表级的全局游标**，指向"下一个可分配的 `_row_id`"，存在**表元数据 `metadata.json`**（`TableMetadata` 字段，SDK 读取接口 `TableMetadata::next_row_id()`）里。每次写入新数据时：取当前 `next-row-id` 作为新数据文件/快照的 `first_row_id`，写完后 `next-row-id += 本批新增行数`。它保证全表 `_row_id` **全局单调递增、永不冲突**，且**只增不减**——DELETE 不回收被删 RowID（不复用），这正是 RowID 稳定的前提。

### 1.3 什么是 `first_row_id`

`first_row_id` 是**第一条新写入数据行被分配到的 `_row_id`**（值取自 `next-row-id`），是一个**元数据属性**（不逐行存进数据文件）。它有两个同名但作用不同的层级：

| 层级                     | 存储位置           | 记录什么                     | 核心作用                                                     | 类比       |
| ---------------------- | -------------- | ------------------------ | -------------------------------------------------------- | -------- |
| **文件级** `first_row_id` | 清单文件（Manifest） | 该数据文件**首行**的 `_row_id`   | **执行层**：逐行算出 `_row_id` 的具体值（"怎么算"）                       | 每本书的页码   |
| **快照级** `first_row_id` | 表元数据（Snapshot） | 该快照**新增数据首行**的 `_row_id` | **管理层**：全局 ID 分配锚点、按 `_row_id` 范围剪枝快照、事务溯源（"从哪来 / 要不要读"） | 图书馆采购批次号 |

> **常见误区**：快照级 `first_row_id` 记录的是该快照**新增数据**的起始 ID，**不是**快照内所有数据的最小 ID。例：表已有 rowid 1–10，再 INSERT 5 行 → 新快照 `first_row_id = 11`（不是 1）。

**三者关系与落盘位置**：

```
next-row-id（表级：下一个可用）
   └─分配→ first_row_id（文件/快照级：这一批的起点）
              └─加行内偏移→ _row_id（每行 = first_row_id + row_position）
```

| 概念                     | 落盘位置                                                             |
| ---------------------- | ---------------------------------------------------------------- |
| `next-row-id`          | 表元数据 `metadata.json`（`TableMetadata`）                            |
| **快照级** `first_row_id` | `metadata.json`（snapshot 对象）                                     |
| **文件级** `first_row_id` | 清单文件 Manifest（Avro），不在 json                                      |
| `_row_id`              | 一般不落盘（读时动态算）；Compaction/Update 后 rowid 不连续，需写进 Parquet 物理列以保留原值。 |

### 1.4 DML 下的 RowID 行为

DML从rowid不变的理念上需要有以下行为

| 操作                       | 旧行 `_row_id`         | 新行 `_row_id`                        | 新快照 `firstRowId`                       |
| ------------------------ | -------------------- | ----------------------------------- | -------------------------------------- |
| **INSERT**               | —                    | 从 `next-row-id` 连续分配                | = 本批起点                                 |
| **DELETE**               | 不变（仅被 delete 文件标记删除） | 无新行                                 | **null**（addedRows=0，operation=delete） |
| **UPDATE**               | 不变（旧行标记删除）           | 默认分配**新 ID**；由引擎自行支持继承保留旧 ID        | = 本批起点，旧 ID 由物理列保留                     |
| **Compaction / rewrite** | —                    | 默认分配**新 ID**；由引擎自行支持继承保留旧 ID（写入物理列） | = 本批起点，旧 ID 由物理列保留                     |

> UPDATE/Compaction/rewrite操作会改变物理位置和first_row_id，导致动态通过first_row_id计算出来的_row_id不准确，所以需要把 `_row_id` 写进新文件的**物理列**→ 物理位置全变保持 `_row_id` 不变，这是行级追踪与索引稳定性的基石。
> 
> 继承不冻结游标：新文件**仍分配**文件级 `first_row_id`、`next-row-id` **仍推进**；继承靠读路径的物理列优先（shadow 掉计算值），而非跳过游标分配。这会导致文件中实际 `_row_id` **可能小于** 文件级 `first_row_id`（如继承来的旧 ID=5，新文件 first=11），属正常现象，读取以物理列为准。因此 **文件级 `first_row_id` 不可当作该文件的最小 RowID 来做剪枝**——`RowIdMapping` 按实际值构建，不依赖此假设。

### 1.5 为什么二级索引需要 RowID（设计动机）

**旧架构痛点**：索引直接存 `RowAddress`（物理地址），与快照绑定。一旦数据改变了物理位置，索引整体失效，必须重建。

**RowID 架构的价值**：

1. **表级映射，统一维护** —— 维护`RowIdMapping`映射表，记录`RowID → RowAddr`映射，`RowIdMapping` 是**表级别**的，被该表上所有索引共享。旧架构每个索引各自维护物理地址，N 个索引要维护 N 份；现在只需维护 1 份映射，更快更简单。

2. **Compaction 不失效** —— 数据重组后 RowID 不变，只需更新 `RowIdMapping` 映射表，**索引条目 0 改动**。

3. **UPDATE 可继承** —— 引擎把旧 RowID 写入更新后的行，则索引只需更新映射（见 §5.8 MOR/COW）。

4. **DELETE 精确清理** —— SDK 的 delete 文件过滤已保证删除行不会被读到（索引层不用关心 delete 文件）。`prune` + `remove_row_ids` 的作用是**防止索引膨胀**：不清理的话已删 RowID 仍在索引和映射里，每次检索都白白做一轮查找和回表，虽不丢正确性但浪费 IO。RowID 能精确定位这些无效条目，零伤及无辜。（见 §5.6）

5. **避免全量重建** —— DELETE / Compaction / UPDATE 不再要求索引整体重建：DELETE 只 `prune` 对应条目，Compaction 只更新映射（索引不动），UPDATE 继承旧 ID 后索引可完全不变。对比旧架构"任何物理变更 → 索引失效 → 全量重建"，RowID 把维护代价从 O(全表) 降到了 O(增量)。

---

## 二、核心设计

RowID 增强的二级索引围绕一个核心思想：**索引只存稳定的逻辑 ID，物理地址变化通过独立的映射层解耦**。

```
索引存：(索引键 → RowID)   ← 稳定逻辑 ID，不含物理地址
映射存：RowID → RowAddress(file_path, row_position)   ← 物理地址，会随重写变化
```

### 2.1 RowID 的分配

表级全局游标 `next-row-id`（存 `metadata.json`）指向"下一个可分配的 `_row_id`"。写入时，每个数据文件取当前 `next-row-id` 作为自己的 `first_row_id`，文件内每行 `_row_id = first_row_id + row_position`（row_position 为文件内 0-based 偏移）。写完后 `next-row-id += 本批新增行数`。

### 2.2 RowID 的读取（双读）

读路径**物理列优先**：若 Parquet 文件含物理 `_row_id` 列（Compaction / UPDATE-COW 重写时保留的旧 ID），直接读物理值；否则动态计算 `first_row_id + row_position`。

双读的意义：Compaction 后文件被重写，新文件的 `first_row_id` 是游标新分配的，但行保留的旧 `_row_id` 存在物理列里。若没有物理列优先，`first_row_id + position` 会算出错误的新值，行级追踪断裂。继承带来的副作用是文件中实际 `_row_id` 可能小于文件级 `first_row_id`（继承旧 ID=5，新文件 first=11），属正常现象，读取以物理列为准。

> **动态计算的局限**：MOR DELETE 后，`_pos` 跳过已删行重新编号 → 被删行之后的行 `_row_id` 前移。纯动态计算的 `_row_id` 只在快照内稳定；跨快照行级血缘必须依赖 Compaction 写入的物理列兜底。

### 2.3 RowIdMapping 映射表

`RowIdMapping` 是**表级别**的 `RowID → RowAddress` 映射表，被该表上所有索引共享。它是 RowID 架构的**中心枢纽**——索引只存 `(键 → RowID)`，物理地址全部托管给映射表。

**结构**：一个按 `min_row_id` 升序排列的 `FileMapping` 列表，每个 `FileMapping` 对应一个数据文件，记录 `(file_path, row_ids 编码)`。对外接口：

| 方法                          | 用途                      | 复杂度                |
| --------------------------- | ----------------------- | ------------------ |
| `lookup(row_id)`            | 单个 RowID → RowAddress   | O(log M + log N)   |
| `lookup_batch(row_ids)`     | 批量解析，先排序后单次扫描           | O(K log K + K + M) |
| `remove(row_ids)`           | DELETE 后移除已删 RowID      | O(K log N)         |
| `rebuild(files)`            | Compaction 后整体重建        | O(总行数)             |
| `to_blob()` / `from_blob()` | 序列化到 Puffin blob / 反序列化 | O(总行数)             |

**生命周期**：

```
INSERT → 追加 FileMapping（Range 编码，16 字节）
DELETE → remove() 在位图中清位，或降级为 SortedArray
Compaction → rebuild() 用新文件路径重建整个映射
```

**为什么高效**：N 个索引共享 1 份映射，Compaction 后只需更新这 1 份（O(文件数)），而不是重建 N 个索引（O(N × 全表)）。详见 §3.2 结构体定义、§6.3 blob 编码。

### 2.4 映射的编码策略

`RowIdMapping` 按文件粒度自动选择最紧凑的编码：

| 场景                      | 编码                                         | 存储       | 查找       |
| ----------------------- | ------------------------------------------ | -------- | -------- |
| INSERT 直出文件（RowID 连续）   | `Range { first, count }`                   | 16 字节    | O(1)     |
| DELETE 后 ≤50% 行被删       | `RangeWithBitmap { first, count, bitmap }` | 16B+N/8B | O(1) 查位图 |
| Compaction 合并 / 删除 >50% | `SortedArray { ids }`                      | N×8 字节   | O(log N) |

`lookup(row_id)` 先按 `min_row_id` 二分定位到文件（O(log M)），再按文件编码取值。映射整体典型 <10MB（M=1000 文件、Range 为主）。

**编码切换不只是空间优化，更是 DELETE 后 `position_of` 正确性的保证**：

回表需要根据`rowid`计算出`_pos`，`_pos`在定义中只计算存活的行 → 被删行之后的行位置前移。

举个例子：如果有`1,X,X,4` 4行数据，且中间两行被删除，那么rowid等于4的这一行`_pos`等于2

因此 DELETE 触发编码迁移：`Range` → `RangeWithBitmap` 或 `SortedArray`。新编码的 `position_of` 只计数存活行：

| 编码                | `position_of` 实现                              | 是否跳过已删行                |
| ----------------- | --------------------------------------------- | ---------------------- |
| `Range`           | `row_id - first`                              | ❌ 不跳（但 Range 只用于无删除场景） |
| `RangeWithBitmap` | `popcount(bitmap[0..offset])` — 数目标位之前有多少个存活行 | ✅                      |
| `SortedArray`     | `ids.binary_search(row_id)` — 数组只含存活行，索引即位置   | ✅                      |

后两种编码的 `position_of` 返回值与 `_pos` 过滤后的行号天然一致，`RowAddress.row_position` 始终准确。

### 2.5 构建链路

```
source.scan_data_files(snapshot) → (col_values, RowID)[]
  → plugin.build((col_values, RowID)[]) → segment.puffin
```

构建时，SDK 扫描数据文件产出 `(列值, RowID)` 流，插件逐行提取索引键写入索引段。详见 §5.1（首次全量）、§5.5（INSERT 增量维护）。

### 2.6 检索链路

```
index.search(query) → RowID[]
  → RowIdMapping.lookup_batch(RowID[]) → RowAddress[]
    → reader.read_file_rows(RowAddress[]) → RecordBatch
```

检索时，查索引得候选 RowID 列表 → 经 `RowIdMapping` 批量解析为物理地址 → 回表读数据行。插件只认识 RowID，从不接触物理地址。详见 §5.2。

### 2.7 DML 下的稳定性与维护

RowID 在行生命周期内保持不变，DML 只影响映射层，索引条目不动：

| 操作             | RowID       | RowIdMapping映射层                            | 索引条目                   |
| -------------- | ----------- | ------------------------------------------ | ---------------------- |
| **INSERT**     | 新分配         | 追加新 FileMapping                            | 追加新条目                  |
| **DELETE**     | 不变          | `remove_row_ids()` 移除                      | `prune()` 按 RowID 精确删除 |
| **UPDATE**（继承） | 保留旧 ID（物理列） | `rebuild_after_compaction()` 换地址（即remap操作） | 0 改动                   |
| **Compaction** | 不变          | `rebuild_after_compaction()` 换地址（即remap操作） | 0 改动                   |

**DELETE 精确清理的含义**：SDK 的 delete 文件过滤已确保已删行永不被读到（索引层不感知）。清理的目的是**性能而非正确性**——不 `prune` 不会出错，但索引和映射里堆积的已删 RowID 会导致每次检索都白做无效查找。因 RowID 稳定，可精确定位并删除这些条目（`prune` + `remove_row_ids`），无需整段重建。

**UPDATE：**

- COW：旧文件整个删掉，新文件写入更新后的行（RowID 继承到物理列）
- MOR：旧行被 delete 文件标记，新行写入新文件（RowID 继承到物理列）

和 **Compaction** 本质是同一件事——"文件轮转，RowID 保留"，所以走同一个 rebuild_after_compaction() 接口。

**原子性要求**：`prune`（清索引条目）和 `remove_row_ids`（切映射编码）必须与 DELETE 在**同一个快照内原子提交**。若 prune 先完成而 `remove_row_ids` 未提交，此时查询存活行会走到旧 `Range` 编码 → `position_of` 不跳已删行 → `row_position` 偏移 → 回表读到错误行。当前引擎需手动串联这三者，端到端原子性尚未接通（见 §8.1）。

### 2.8 两层分工与调用入口

引擎经 **ABI（`iceberg-index-abi`）→ `IndexedTableView::open_metadata_location(...)`→ `search_*` / `prepare_*_index(...)`** 访问，写操作返回 `StatisticsFile` 由 bridge 原子提交。检索链路在 `search_*` 内闭环。

- **SDK 层**（`iceberg-rust`）：提供 RowID 读写基础能力 —— `_row_id`/`_pos` 列投影、`first_row_id` 游标分配、`appends_after` 增量扫描。详见 §三。
- **Index 层**（`iceberg-index`）：在 SDK 之上实现索引与映射 —— `RowIdMapping` 解析、`IndexPlugin` 构建/prune、`classify_change` DML 判定。详见 §三。

**需引擎主动调用的接口**（不自动触发）：`prune` / `remove_row_ids` / `rebuild_after_compaction` / `plan_incremental` / `maintain_index`。详见 §三（接口）、§五（流程）、§八（现状）。

---

## 三、关键结构体

### 3.1 SDK 层结构体

#### `FileScanTask.first_row_id` —— 扫描任务上的行血缘种子

```rust
pub struct FileScanTask {
    // ... 既有字段 ... 
    pub first_row_id: Option<u64>,
}
```

由 `scan/context.rs` 从 `manifest_entry.data_file().first_row_id()` 自动填充，读取管线据此动态计算 `_row_id`。

> 注：`_file` / `_pos` / `_row_id` 在 SDK 侧是**列（保留 field ID + 列名常量）**

### 3.2 Index 层结构体（**核心**）

#### `RowAddress` —— 物理行地址

```rust
pub struct RowAddress {
    pub file_path: String,
    pub row_position: u64,
}
```

回表读取的入参：检索协调器经 `RowIdMapping::lookup(&self, row_id: u64)` 把 RowID 解析为此结构体。插件不直接产出物理地址 —— 向量插件返回 `ScoredRowId { row_id, score }`，标量插件返回 `Vec<u64>`。

> 向量插件（IVF / IVF-PQ）的 score 是 查询向量与候选向量的平方 L2 距离（squared Euclidean distance）。搜索时按 score 升序排列，score 相同则按 RowID 升序保证确定性排序。

#### `RowIdEncoding` —— 单文件内 RowID 序列的三态编码

```rust
pub enum RowIdEncoding {
    Range { first: u64, count: u64 },                       // 连续递增，16 字节
    RangeWithBitmap { first: u64, count: u64, bitmap: Vec<u8> }, // 稀疏 DELETE (≤50%)，16B+N/8B
    SortedArray { ids: Vec<u64> },                           // 非连续，N×8 字节
}
```

**自动选择策略**：连续 → `Range`；DELETE ≤50% → `RangeWithBitmap`；非连续/删除 >50% → `SortedArray`。进一步删除时 `RangeWithBitmap` 清位；剩余行重新连续时自动压回 `Range`。

> **命名歧义**：`Range { first }` 是映射中该文件**实际 RowID 的最小值**（`min_row_id()`），与 manifest 元数据的文件级 `first_row_id` 不是同一个概念。INSERT 直出文件两者碰巧相等，Compaction 继承后实际值可能远小于元数据值（继承旧 ID=5，新文件 first=11）。映射全部基于实际值构建和排序，不依赖文件元数据。

> **物理存储怎么区分是哪一种编码？** 序列化时每个文件的 payload 前有 **1 字节 `encoding` tag**：`0=Range`、`1=SortedArray`、`2=RangeWithBitmap`。反序列化按 tag 分派。完整字节布局见 **§6.3**。

#### `FileMapping` —— 单个 DataFile 的映射条目

```rust
pub struct FileMapping {
    pub file_path: String,
    pub row_ids: RowIdEncoding,
}
```

#### `RowIdMapping` —— 全局 RowID → RowAddr 映射表

**RowID 特性的基石**

```rust
pub struct RowIdMapping {
    files: Vec<FileMapping>,   // 私有，按 min_row_id 升序排列
}
```

> **为什么只有一个字段？** 它本质就是"一批按 `min_row_id` 排序的 `FileMapping`"。排序保证跨文件可二分（`lookup` O(log M)），文件内再按编码二分/直算。`files` 私有，对外通过 `file_count()` / `total_rows()` / `iter()` / `lookup*()` 访问。持久化到 Puffin blob（类型 `huawei.gauss-infra.rowid-mapping-v1`）。

**RowID → RowAddress 解析机制**：

1. **跨文件定位**：`files` 按 `min_row_id` 升序排列，二分找到包含目标 RowID 的文件（O(log M)）；`lookup_batch` 则先对输入 RowID 排序，文件指针只前进不后退（均摊 O(1) 跨文件查找）。

2. **文件内定位**：调用 `FileMapping::position_of(row_id)`，按编码类型分派：
   
   - `Range`：`row_id - first`（O(1)）
   
   - `SortedArray`：`ids.binary_search(&row_id)`（O(log N)）
   
   - `RangeWithBitmap`：先验证 `row_id - first` 落在范围内，再检查 bitmap 该位是否为 1（存活）（O(1)）

3. **输出**：`position_of` 返回文件内的 0-based 行位置，组装 `RowAddress { file_path, row_position }`。

#### `ScoredRowId` —— 插件返回的"地址无关"查询结果

```rust
// ...
pub struct ScoredRowId {
    pub row_id: u64,
    pub score: f32,
}
```

> **关键设计**：插件构建/检索时只认识稳定 RowID。向量插件返回 `ScoredRowId`（含 row_id + score），标量插件返回 `Vec<u64>`，由 table 层统一经 `RowIdMapping` 解析成 `RowAddress`。

#### `IndexSegmentMetadata` —— 索引段元数据

```rust
// iceberg-index-core/src/model.rs（节选）
pub struct IndexSegmentMetadata {
    pub segment_id: SegmentId,                              // ← 既有
    pub built_at_snapshot_id: SnapshotId,                   // ← 既有
    pub covered_data_files: BTreeSet<String>,               // ← 既有
    pub artifact_files: Vec<ArtifactFile>,                  // ← 既有：段实体文件的 URI 指针
    pub indexed_rows: u64,                                   // ← 既有
    /// 【本次 RowID/COW 相关新增】每覆盖文件的行数。
    pub covered_data_file_rows: Option<BTreeMap<String, u64>>,
    // ... algorithm_details / format_version / created_at_ms 等既有字段 ...
}
```

- **作用**：`covered_data_file_rows` 的 key 是文件路径，value 是该文件中的行数。构建时记录每文件有多少行，检索时用来算死行占比。没有这个字段时，段只能在其覆盖的所有文件**全部存活**时才复用。有了它，即使部分行被删，只要死行占比低于阈值（`CoveragePolicy.max_dead_row_ratio 50%`），就可以**部分复用**——从旧段查出候选 RowID（含已删行的），经 `RowIdMapping` 过滤掉死行后回表，避免整段重建。

#### `SnapshotChange` / `CompactionDiff` / `MaintenanceAction` —— DML 维护三件套

```rust
pub enum SnapshotChange { AppendOnly, DeleteOnly, Compaction, FullRewrite, NoChange }

pub struct CompactionDiff {
    pub removed_files: HashSet<String>,
    pub added_files: Vec<DataFileRef>,
}

pub enum MaintenanceAction { NoOp, AppendOnly, DeleteOnly, Compaction, FullRebuild }
```

三者协同流程（详见 §5.5–5.9，其中 §5.9 含 SnapshotChange → 映射接口的对应表）：

1. `classify_change(old, new)` 判断这是什么操作：纯追加(`AppendOnly`)、纯删除(`DeleteOnly`)、Compaction、全量重写(`FullRewrite`)或无变化(`NoChange`)。
2. 若为 `Compaction`，`detect_compaction(old, new)` 返回 `CompactionDiff { removed_files, added_files }`，喂给 `rebuild_after_compaction()`。
3. 按结果选策略，转成 `MaintenanceAction` 传入 `maintain_index()`：`AppendOnly`→增量构建、`DeleteOnly`→清理、`Compaction`/`FullRewrite`→remap、`NoOp`→直接复用旧条目。

---

## 四、新增 / 修改的接口

> 能力概览（设计见 §二，详细签名见后续签名）。

| 层     | 能力                         | 接口                                                                          | 新增/修改 | 引擎需主动调用 |
| ----- | -------------------------- | --------------------------------------------------------------------------- |:-----:|:-------:|
| SDK   | `_row_id` / `_pos` 列读取（双读） | `select([..., "_row_id", "_pos"])`                                          | 新增    | ❌       |
| SDK   | `first_row_id` 写入路径填充      | `with_first_row_id()` / `v3(...)`                                           | 新增    | ❌       |
| SDK   | 增量扫描                       | `appends_after(from_snapshot_id)`                                           | 新增    | ❌       |
| Index | RowID → RowAddr 映射         | `lookup(row_id)` / `lookup_batch(row_ids)`                                  | 新增    | ❌       |
| Index | 索引构建                       | `IndexPlugin::build(ctx, def, input)`                                       | 修改    | ❌       |
| Index | 检索回表                       | `search_vector(name, query)` / `search_scalar(...)`                         | 新增    | ❌       |
| Index | 变更分类                       | `classify_change(old, new)`                                                 | 新增    | ❌       |
| Index | Compaction 检测              | `detect_compaction(old, new)`                                               | 新增    | ❌       |
| Index | DELETE 清理                  | `prune(ctx, segment, &deleted)` + `remove_row_ids(&mut self, &deleted)`     | 新增    | ✅       |
| Index | Compaction 重建              | `rebuild_after_compaction(&mut self, removed_files, new_scan)`              | 新增    | ✅       |
| Index | INSERT 增量                  | `plan_incremental(from, to, field_ids)` + `maintain_index(AppendOnly, ...)` | 新增    | ✅       |
| Index | 维护分发                       | `maintain_index(action: MaintenanceAction, request, previous_registry)`     | 新增    | ✅       |

### 4.1 SDK 层（`iceberg-rust`）

#### (A) 元数据 / 写入路径

| 接口                                                                                                                    | 说明                                                                   | 变更  |
| --------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |:---:|
| `TableMetadata::next_row_id(&self) -> u64`                                                                            | 表级游标：下一个待分配 RowID，用于给 writer 设置起始值                                   | 既有  |
| `DataFile::first_row_id(&self) -> Option<i64>`                                                                        | 读取该数据文件首行 RowID（delete 文件返回 None）                                    | 新增  |
| `ManifestWriterBuilder::with_first_row_id(self, first_row_id: u64) -> Self`                                           | 写 data manifest 前设置 manifest 级 first_row_id 游标的起始值                   | 新增  |
| `ManifestListWriter::v3(writer, snapshot_id, parent_snapshot_id, sequence_number, first_row_id: Option<u64>) -> Self` | 构造 manifest-list writer，设置 next_row_id 游标起始值（delete manifest 传 None） | 修改  |
| `ManifestListWriter::add_manifests(&mut self, manifests: impl Iterator<Item = ManifestFile>) -> Result<()>`           | 追加 manifest，触发三向比较（carry-over / Equal 分配 / Greater 报错）               | 修改  |
| `ManifestFileMetadata::load_manifest(&self, file_io: &FileIO) -> Result<Manifest>`                                    | 读时按游标回填缺失的 per-DataFile first_row_id（旧 manifest 兼容）                  | 修改  |

> 内部私有例程 `assign_data_file_first_row_id()`（每文件游标）与 `assign_first_row_id()`（三向比较）**不直接对外**，由上述公开写入路径自动驱动。

#### (B) 扫描 / 读取

| 接口                                                               | 说明                                                                            | 变更  |
| ---------------------------------------------------------------- | ----------------------------------------------------------------------------- |:---:|
| `TableScanBuilder::appends_after(from_snapshot_id: i64) -> Self` | **增量扫描**：只返回 `status==Added && snapshot_id > from` 的数据文件（供索引增量维护）             | 新增  |
| `FileScanTask::first_row_id` 字段                                  | 每个扫描任务携带首行 RowID                                                              | 新增  |
| `_pos` / `_row_id` 列投影（双读）                                       | `select([..., "_row_id", "_pos"])`，物理列存在走物理读，否则 `first_row_id + position` 动态算 | 新增  |

#### (C) 常量与列（`spec/metadata_columns.rs`，均 `pub const`）

| 常量                         | 值                | 说明                                                 |
| -------------------------- | ---------------- | -------------------------------------------------- |
| `RESERVED_FIELD_ID_FILE`   | `i32::MAX - 1`   | `_file` 列的保留 field ID                              |
| `RESERVED_FIELD_ID_POS`    | `i32::MAX - 2`   | `_pos` 列的保留 field ID                               |
| `RESERVED_FIELD_ID_ROW_ID` | `i32::MAX - 107` | `_row_id` 列的保留 field ID（与 Java `MAX_VALUE-107` 对齐） |
| `RESERVED_COL_NAME_FILE`   | `"_file"`        | 数据文件路径列的列名                                         |
| `RESERVED_COL_NAME_POS`    | `"_pos"`         | 文件内 0-based 行号列的列名                                 |
| `RESERVED_COL_NAME_ROW_ID` | `"_row_id"`      | 逻辑行 ID 列的列名                                        |

> **用法**：`select(["col_a", "_row_id", "_pos"])` — 元数据列 opt-in，传列名即可。`select_all()` 只投影数据列，默认不带 `_row_id` / `_pos`。不想读则不传该列名。

### 4.2 Index 层（`iceberg-index`）

#### (A) RowID 映射核心 —— `RowIdMapping`（`iceberg-index-core`）

| 接口                                                                                                                                  | 说明                                            |
| ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| `RowIdMapping::lookup(&self, row_id: u64) -> Option<RowAddress>`                                                                    | 单点解析，row_id -> RowAddress，O(log M + log N)    |
| `RowIdMapping::lookup_batch(&self, row_ids: &[u64]) -> Vec<Option<RowAddress>>`                                                     | 批量解析，[row_id] ->  [RowAddress]，保序，未命中为 None   |
| `RowIdMapping::build(stream: impl Iterator<Item = (String, u64)>) -> Self`                                                          | 由 `(file_path, row_id)` 对全量构建映射               |
| `RowIdMapping::rebuild_after_compaction(&mut self, removed_files: &HashSet<String>, new_scan: impl Iterator<Item = (String, u64)>)` | **Compaction / UPDATE-COW 重建**：换物理地址，RowID 不变 |
| `RowIdMapping::remove_row_ids(&mut self, row_ids: &BTreeSet<u64>)`                                                                  | **DELETE 清理**：按 RowID 移除映射条目                  |
| `RowIdMapping::remove_row_ids_in_file(&mut self, file_path: &str, row_ids: &BTreeSet<u64>) -> bool`                                 | 限定单文件的按 RowID 移除                              |
| `RowIdMapping::to_blob(&self) -> Vec<u8>`                                                                                           | Puffin 序列化（二进制 LE）                            |
| `RowIdMapping::from_blob(data: &[u8]) -> Result<Self, String>`                                                                      | Puffin 反序列化（二进制 LE）                           |

#### (B) 插件 SPI —— `IndexPlugin::prune`（`iceberg-index-core/src/plugin.rs`）

| 接口                                                                                                                | 说明                                                            |
| ----------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `fn prune(&self, context: &PluginContext, segment: &IndexSegmentMetadata, row_ids: &BTreeSet<u64>) -> Result<()>` | DELETE 后按 RowID 清理索引条目。**注意**：此接口目前无生产调用方，需引擎经 ABI 手动驱动（见 §八） |

#### (C) 增量 / Compaction 数据源（`iceberg-index-iceberg/src/source.rs`）

| 接口                                                                                                               | 说明                           |
| ---------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `classify_change(&self, old: SnapshotId, new: SnapshotId) -> Result<SnapshotChange>`                             | 比对文件集 + 表 UUID，判定变更类型        |
| `detect_compaction(&self, old: SnapshotId, new: SnapshotId) -> Result<Option<CompactionDiff>>`                   | 同时有增删文件才判为 Compaction        |
| `plan_incremental(&self, from: SnapshotId, to: SnapshotId, field_ids: &[FieldId]) -> Result<SnapshotBuildPlan>`  | 只规划 `from` 之后新增文件（INSERT 增量） |
| `scan_row_ids_for_files(&self, snapshot_id: SnapshotId, data_files: &[DataFileRef]) -> Result<IndexBatchStream>` | 扫指定文件的 RowID（remap 用）        |
| `build_rowid_mapping(&self, snapshot_id: SnapshotId) -> Result<RowIdMapping>`                                    | 全量构建某快照的映射                   |

#### (D) 回表读取（`iceberg-index-iceberg/src/reader.rs`，`IcebergTableReader`）

| 接口                                                                                                                                   | 说明                              |
| ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------- |
| `IcebergTableReader::read_rows_by_row_id(&self, row_ids: &[u64], mapping: &RowIdMapping) -> Result<IndexBatchStream>`                | RowID → 映射 → 流式读整行（未命中丢弃）       |
| `IcebergTableReader::read_rows_by_row_id_ordered(&self, row_ids: &[u64], mapping: &RowIdMapping) -> Result<(RecordBatch, Vec<u64>)>` | 同上，但按输入顺序返回单个 batch + 对齐的 RowID |
| `IcebergTableReader::read_file_rows(&self, addresses: &[RowAddress]) -> Result<IndexBatchStream>`                                    | 直接按物理地址读                        |

#### (E) 维护编排（`iceberg-index-runtime/src/build.rs`，`IndexBuildCoordinator`）

| 接口                                                                                                                                                                           | 说明                                                                                                                                                                        |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pub async fn maintain_index(&self, action: MaintenanceAction, request: BuildIndexRequest, previous_registry: Option<&SnapshotIndexRegistry>) -> Result<IndexRegistryEntry>` | 顶层分发：`NoOp` 复用旧条目（空操作）；`AppendOnly/DeleteOnly/Compaction` → 增量构建；`FullRebuild` → 全量重建。`DeleteOnly / Compaction` 分支需引擎另行驱动 `prune` / `rebuild_after_compaction`（见 §5.9、§八） |

#### (F) 对外门面（`iceberg-index-table/src/metadata.rs`，`IndexedTableView`）

| 接口                                                      | 说明                                                          |
| ------------------------------------------------------- | ----------------------------------------------------------- |
| `open_metadata_location(config) -> Self`                | **ABI 真实入口**：按 metadata_location 打开表，自动加载/重建 `RowIdMapping` |
| `search_vector(name, query) -> VectorSearchResult`      | 向量检索，RowID 已在协调器内解析为地址                                      |
| `search_vector_materialized(...)`                       | 同上并回表物化整行                                                   |
| `search_scalar(name, query) -> ScalarIndexSearchResult` | 标量检索                                                        |
| `search_scalar_materialized(...) -> RecordBatch`        | 标量检索 + 回表物化                                                 |
| `prepare_create_index / _optimize_index / _drop_index`  | 索引生命周期管理：创建/优化/删除索引，操作后自动重建并持久化映射                           |

> 外部调用方是**引擎（经 ABI）**，由 ABI 调用 `IndexedTableView` 的 `open_metadata_location` + `search_*` / `prepare_*`。`resolve_scored_rows` / `resolve_row_ids`（协调器内 RowID→地址解析）、`plan_coverage`、`load_mapping_from_source` 均为**内部/crate 级**实现细节，不直接暴露。

**快速上手**：

```rust
let view = IndexedTableView::open_metadata_location(MetadataLocationTableConfig {
    table_namespace: ns, table_name: name, metadata_location: metadata_loc,
}).await?;
let hits = view.search_vector("my_ivf_index",
    VectorSearchRequest { vector: query_vec, top_k: 10 }).await?;
let batch = view.search_scalar_materialized("my_btree_index", scalar_query).await?;
```

---

## 五、执行流程（端到端）

> 每个流程标注穿越的层与关键函数。

### 5.1 索引构建（首次全量）

```
引擎(ABI) → IndexedTableView::prepare_create_index(def)
  │
  ├─▶ IndexBuildCoordinator::create_and_build_index(request)
  │     ├─▶ [Index源] source.plan_snapshot(snapshot, field_ids)
  │     │     └─ SDK scan：reader 已应用 delete filter，产出含
  │     │        _file / _pos / _row_id 的 RecordBatch（仅存活行）
  │     ├─▶ [插件] IndexPlugin::build(ctx, def, input)
  │     │     └─ 逐行提取 (索引键, RowID) → 写索引段 artifact（独立 Puffin，见 §6.1）
  │     └─▶ [框架] build_rowid_mapping(snapshot)
  │           └─ 收集 (file_path, row_id) → RowIdMapping::build()
  │              新文件 RowID 连续 → Range 编码（16B/文件）
  └─▶ 提交：注册表 blob 写进 registry Puffin，作为 StatisticsFile 原子挂到快照（见 §5.9、§6.1）。映射 blob 随注册表一并持久化
  返回 IndexRegistryEntry
```

### 5.2 检索回表（search，最常见路径）

```
引擎(ABI) → IndexedTableView::search_vector_materialized(name, query)
  │
  ├─▶ match_index(name) → 命中的 IndexRegistryEntry
  ├─▶ IndexSearchCoordinator::search_vector(request)
  │     ├─▶ [插件] load(segment) + 查询 → Vec<ScoredRowId>{ row_id, score }
  │     └─▶ resolve_scored_rows(scored)                  ← 协调器内部
  │           └─ RowIdMapping::lookup_batch(row_ids) → Vec<RowAddress>
  └─▶ IcebergTableReader::materialize_candidates(candidates)
        └─ 按 RowAddress 回表读整行 → 保 score 顺序返回
```

### 5.3 SDK 写入路径：`first_row_id` 分配

```
写数据 (ParquetWriter 创建 DataFile)
  │  first_row_id ← TableMetadata::next_row_id()
  ▼
commit → ManifestWriterBuilder::with_first_row_id(起始值)
  │  ManifestWriter 每文件游标：仅对 Added 数据项分配 / 已预设的保留 / 推进 record_count
  ▼
ManifestListWriter::v3(.., first_row_id) + add_manifests()
  │  三向比较：Less→保留 / Equal→分配并推进 / Greater→报错
  ▼
推进 TableMetadata.next_row_id
```

### 5.4 SDK 读取路径：`_row_id` / `_pos` 双读

```
scan → 每个 FileScanTask
  │  FileScanTask.first_row_id ← data_file().first_row_id()
  ▼
pipeline 处理每个 Parquet 文件（双读，物理列优先）：
  ├─ 含物理 _row_id 列? → 是→直读物理列值（Compaction/UPDATE-COW 后保留的原值）
  └─ 否→动态计算：_pos=存活行 0-based 计数 / _row_id=first_row_id+_pos
  ▼
把 _file / _pos / _row_id 作为列附加到输出 RecordBatch
```

### 5.5 INSERT 增量维护

```
classify_change(old, new) → AppendOnly
  ├─▶ source.plan_incremental(from, to, field_ids)  （底层 appends_after）
  ├─▶ 插件增量插入新文件的 (键, RowID)
  ├─▶ RowIdMapping 追加新 FileMapping（新文件连续 → Range）
  └─▶ maintain_index(AppendOnly, ..) → 新 IndexRegistryEntry
```

### 5.6 DELETE 清理

**为什么要清理**：SDK 的 delete 文件过滤已保证删除行不会被读取——即使索引返回了已删 RowID，回表时 SDK 也会过滤掉，结果正确。但索引条目和映射里堆积无效 RowID 会导致每次检索都白跑查找（映射解析 → 回表 → 空结果），空间和时间都浪费。因 RowID 稳定，可**精确**定位并删掉这几条而无需整段重建。

```
classify_change(old, new) → DeleteOnly
  ├─▶ 求出被删 RowID 集合 deleted: BTreeSet<u64>
  ├─▶ [插件] IndexPlugin::prune(ctx, segment, &deleted)
  │     └─ BTree：page 级过滤 + lookup 重建；IVF/IVF-PQ：分区级过滤 + 重写
  ├─▶ [映射] RowIdMapping::remove_row_ids(&deleted)
  │     └─ Range → 删 ≤50% 转 RangeWithBitmap；删 >50% 转 SortedArray
  └─▶ maintain_index(DeleteOnly, ..) 提交
```

**prune 的写入粒度：整段文件重写（同 URI 覆盖）。**

| 对象                        | DELETE 时                             |
| ------------------------- | ------------------------------------ |
| 命中删除的段 artifact Puffin    | ✅ 整文件重写（同 URI 覆盖）                    |
| 未命中的段 artifact Puffin     | ❌ 不动                                 |
| 段被全删空                     | 🗑️ 不写文件，由上层从注册表移除该段                 |
| registry Puffin（注册表 + 映射） | 🔄 每次提交新写一份（§6.1 两层）                 |
| `RowIdMapping` 内存态        | `remove_row_ids()` 原地改，随 registry 落盘 |

> BTree：读全段→命中页 filter+重建，未命中页原样拷贝，`write_blobs(uri, ...)` 写回。IVF/IVF-PQ：逐分区过滤→`serialize_ivf(...)` 重建→`put(uri, ...)` 写回。**计算是页/分区级，物理是整段落盘。**

**调用方与时机**：由**引擎方经 ABI 驱动**——引擎执行 `DELETE` 后，依次调用 `plugin.prune()` + `mapping.remove_row_ids()`，再提交注册表。框架侧只提供接口，不自动触发（见 §八）。

### 5.7 Compaction 重建（RowID 稳定性的核心价值）

```
classify_change(old, new) → Compaction
  ├─▶ source.detect_compaction(old, new) → Some(CompactionDiff{ removed_files, added_files })
  ├─▶ source.scan_row_ids_for_files(new, added_files)
  │     └─ 读新文件的物理 _row_id 列 → RowID 值不变，只是物理位置变了
  └─▶ RowIdMapping::rebuild_after_compaction(&removed_files, new_scan)
        ├─ retain 未删文件的 FileMapping
        ├─ build 新文件的 FileMapping（多文件合并 → 常为 SortedArray）
        └─ 按 min_row_id 重排序
  ★ 索引段条目 0 改动 —— 插件永不 remap，只有映射表被替换
```

**调用方与时机**：由**引擎方经 ABI 驱动**——引擎执行 `Compaction` / `OPTIMIZE` 后，依次调用 `detect_compaction()` + `scan_row_ids_for_files()` + `mapping.rebuild_after_compaction()`，再提交注册表。框架侧不自动触发（见 §八）。

### 5.8 UPDATE（MOR 与 COW）

UPDATE 在 Iceberg 里语义上是"更新若干行"。RowID 是否继承取决于**引擎是否把旧行的 `_row_id` 写回更新后的行**。

```
── UPDATE (COW) ── 重写整个数据文件
  ├─ 引擎继承 RowID（物理列保留旧 ID）
  │    → 与 Compaction 同构：rebuild_after_compaction() 换映射，索引 0 改动
  └─ 引擎不继承 → 退化 DELETE(旧行) + INSERT(新行)

── UPDATE (MOR) ── 写 delete 文件 + 新数据文件
  ├─ 旧行：被 delete 文件标记 → 走 DELETE 路径
  └─ 新行：新数据文件 → 走 INSERT 路径
```

**一句话**：**COW + RowID 继承**是最优路径（等价 Compaction）；**MOR** 或**不继承的 COW** 则落回 `DELETE + INSERT` 组合。是否继承由**引擎层**决定。

### 5.9 RowIdMapping 变更与持久化时序（内存 + 物理）

**按操作选择的映射变更接口**：

| 操作                   | 引擎触发场景                       | 触发 SnapshotChange          | 映射变更接口（仅内存）                  | 编码效果（内存态）                |
| -------------------- | ---------------------------- | -------------------------- | ---------------------------- | ------------------------ |
| 首次构建                 | `CREATE INDEX`               | 建索引                        | `build()` 全量                 | 新文件 → Range              |
| INSERT               | 引擎 `INSERT` / `MERGE`        | `AppendOnly`               | 追加 FileMapping               | Range                    |
| DELETE               | 引擎 `DELETE`                  | `DeleteOnly`               | `remove_row_ids()`           | Range→Bitmap→SortedArray |
| UPDATE-COW（继承）       | 引擎 `UPDATE`(COW，§5.8)        | `Compaction`/`FullRewrite` | `rebuild_after_compaction()` | 整体替换                     |
| UPDATE-COW（不继承）/ MOR | 引擎 `UPDATE`(MOR，§5.8)        | `DeleteOnly`+`AppendOnly`  | `remove_row_ids()` + 追加      | 混合                       |
| Compaction           | 引擎 `Compaction` / `OPTIMIZE` | `Compaction`               | `rebuild_after_compaction()` | 多文件 → SortedArray        |

> 上表的接口**只修改内存中的 `RowIdMapping`**，不直接写物理存储。持久化在下方时序第④步（`to_blob` → 写 registry Puffin）完成。

**变更 + 持久化时序**：

```
① 快照提交产生 new_snapshot
    │
② ABI 打开表（会话期内一次）：open_metadata_location → 加载/重建 RowIdMapping，缓存为 Arc<RwLock<>>，后续 search_* 复用
    │
③ 内存态更新（Arc<RwLock<RowIdMapping>>）：
   · create/optimize后 build_rowid_mapping 全量重建
   · DML 增量由引擎经 ABI 调对应接口（当前手动串联，见 §八）
   · 统一 *mapping.write() = new 原地替换
    │
④ 提交：prepare_*_index 返回 StatisticsFile，交 bridge 原子提交
    │
⑤ 读回：下次 open_metadata_location → load_mapping_from_source
        读 blob；缺失则 build_rowid_mapping 从数据重建
```

**一致性原则**：每快照对应一份与其存活行一致的映射（per-snapshot）。

**物理存储层的变化**（映射 blob 字节如何在操作后改写）：

| 操作             | 受影响文件                 | blob 编码变化                                | 说明                                            |
| -------------- | --------------------- | ---------------------------------------- | --------------------------------------------- |
| 首次构建           | 全部新文件                 | → Range                                  | 新文件 RowID 连续                                  |
| INSERT         | 新增文件                  | → Range                                  | 追加新 FileMapping 到 blob                        |
| DELETE         | 命中删除的文件               | Range → RangeWithBitmap 或 SortedArray    | 编码重选，`to_blob()` 时以新编码序列化                     |
| Compaction     | removed 文件 + added 文件 | 旧 mapping 删去 + 新 mapping（常为 SortedArray） | `rebuild_after_compaction` 后 `to_blob()` 整份重写 |
| UPDATE-COW（继承） | 同 Compaction          | 同 Compaction                             | 物理列保留旧 ID，映射换地址                               |

---

## 六、Puffin 存储结构

### 6.1 两层 Puffin：注册表 Puffin vs 段实体 Puffin

**索引段的实体数据在各自独立的 artifact Puffin 文件里；注册表 Puffin 只存"元数据 + 指针"。**

```
Snapshot
  └─ statistics: [ StatisticsFile ] ──▶  <table>/indices/xxxx-<snap>.puffin   【注册表 Puffin】
        blob:
          ├─ index-meta-v1     = SnapshotIndexRegistry (JSON)
          │      └─ 每个索引：implementation(插件类型) + IndexSegmentMetadata{ artifact_files:[URI…], … }
          │                                        │ URI 指针
          │                                        ▼
          │                          <table>/indices/seg-*.puffin   【段实体 Puffin，各自独立】
          │                             blob: ivf-flat-segment-v1 / BTree 页 / …
          │                             （zstd 压缩，插件自定义格式）
          ├─ rowid-mapping-v1  = RowIdMapping.to_blob()  （二进制 LE，不压缩）
          └─ (carry-over 的非索引 blob)
```

| Blob 类型常量                 | 值                                        | 落在哪层           | 内容                            |
| ------------------------- | ---------------------------------------- | -------------- | ----------------------------- |
| `REGISTRY_BLOB_TYPE`      | `huawei.gauss-infra.index-meta-v1`       | 注册表 Puffin     | `SnapshotIndexRegistry`（JSON） |
| `ROWID_MAPPING_BLOB_TYPE` | `huawei.gauss-infra.rowid-mapping-v1`    | 注册表 Puffin     | `RowIdMapping`（二进制 LE）        |
| `IVF_SEGMENT_BLOB_TYPE`   | `huawei.gauss-infra.ivf-flat-segment-v1` | **段实体 Puffin** | IVF/IVF-PQ 段数据（zstd）          |

### 6.2 Puffin 物理文件布局

Puffin 是**写一次的不可变文件**：`Magic "PFA1"` → 各 blob 字节负载 → `Footer`（`Magic` + `blob_metadata[]` JSON：每个 blob 的 `{type, offset, length, compression, properties}` + payload 长度 + flags + `Magic`）。读取时先解析 footer 拿 `offset/length`，切片后反序列化。

### 6.3 RowID 映射 blob 内部字节布局（little-endian）

```
file_count : u32 LE
每个 FileMapping 重复：
  path_len : u32 LE     path : UTF-8 bytes
  encoding : u8         0=Range  1=SortedArray  2=RangeWithBitmap
  ── encoding=0 ──  first : u64 LE   count : u64 LE           （16B）
  ── encoding=2 ──  first : u64 LE   count : u64 LE
                     bmp_len : u32 LE   bitmap : u8×bmp_len   （16B+4B+N/8）
  ── encoding=1 ──  id_count : u32 LE   ids : u64 LE×id_count （4B+N×8B）
```

编码 tag：`ENCODING_RANGE=0` / `ENCODING_SORTED_ARRAY=1` / `ENCODING_RANGE_WITH_BITMAP=2`。映射整体典型 <10MB。

### 6.4 读写接口

- 写：`write_registry_statistics_file`→映射 blob 与注册表同写一个 registry Puffin，`prepare_*_index` 返回 `StatisticsFile` 交 bridge 提交。
- 读：`load_mapping_from_source()` → `from_blob()`；如果blob 缺失→`build_rowid_mapping` 从数据重建，最坏空映射。
- ABI入口：`IndexedTableView::open_metadata_location()` 自动完成上述加载。

---

## 七、关键设计约束

1. **插件不 remap** —— `IndexPlugin::remap()` 已移除。Remap 统一由框架层 `RowIdMapping::rebuild_after_compaction()` 完成，Compaction 只换映射物理地址，索引条目 0 改动。

2. **i64/u64 边界** —— `RowIdMapping` 用 `u64`；插件存 `i64`(BTree) 或 `u64`(IVF)，`as` 转换，小端零开销。
   
   **为什么有两套类型**：
   
   | 组件              | 类型                          | 原因                                                                            |
   | --------------- | --------------------------- | ----------------------------------------------------------------------------- |
   | SDK `_row_id` 列 | `Int64`（Arrow）/ `i64`（Rust） | Iceberg 元数据列定义即 `BIGINT` → 对应 `Int64`                                         |
   | BTree 插件        | `i64`                       | 直接读写 Arrow `Int64Array`，零序列化开销                                                |
   | IVF / IVF-PQ 插件 | `u64`                       | 二进制 LE 格式 `write_u64_le` / `read_u64_le`，与 Arrow 类型无关                         |
   | `RowIdMapping`  | `u64`                       | `Range { first, count }` 中 `first + count` 可能超 `i64::MAX`（≥92 亿亿行），需 `u64` 保护 |
   
   **转换点**：BTree 搜索返回 `Int64Array` → 在 runtime 层 `as u64` 转为 `ScoredRowId { row_id: u64 }`；构建时 BTree 从 `Int64Array` 读 `i64` → 写索引存为 `i64`。IVF 构建时 `Int64Array` 读 `i64` → `as u64` 写二进制。所有转换在小端平台零开销。

3. **元数据列天然隔离** —— 回表只投影数据列，`_row_id`/`_pos`/`_file` 上层不感知。

4. **`_file` / `_pos` / `_row_id` 是 SDK 原生列** —— 硬编码常量，不通过 JSON 参数配置。

5. **Bridge 是薄转发层** —— 不感知 `_row_id`，纯转发给 Index SDK。

6. **双读优先级** —— 物理 `_row_id` 列 > 动态计算（见 §5.4）。

7. **映射按快照自包含** —— per-snapshot，跨快照不共享。

### 7.1 RowIdMapping 的加载与缓存模型（无淘汰）

`RowIdMapping` **不是缓存**，而是「按快照全量加载、常驻内存、整体替换」的结构，当前无任何淘汰机制。

```
open_metadata_location(config)
  └─ load_mapping_from_source() → from_blob() 全量反序列化
       └─ 存为 Arc<RwLock<RowIdMapping>>
```

- **持有**：`Arc<RwLock<RowIdMapping>>`（`metadata.rs`），整表常驻。
- **更新**：create/optimize/drop index 后**整体替换** `*mapping.write() = new_mapping`。
- **查询**：`self.mapping.read()` 直接内存二分。
- **没有**：LRU / TTL / 容量上限 / 惰性驻留 / 失效回收。

**与"有淘汰"的缓存区分**（淘汰对象都不是映射表）：

| 机制            | 位置                               | 淘汰对象                |
| ------------- | -------------------------------- | ------------------- |
| 1024 页 LRU    | `BTreeRuntimeIndex::load_page()` | BTree Arrow IPC 数据页 |
| Segment cache | runtime 层（`lru`/`moka`）          | 索引段                 |

设计预算 <10MB（§6.3），全量常驻换纯内存二分。超大表时存在线性增长风险，未来可做分片惰性加载 + LRU（见 §八）。

---

## 八、实现状态与未来工作

### 8.1 成熟度速览

| 能力                          | 状态         | 说明                                                                                                                                                                                                      |
| --------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| RowID 元数据读写                 | ✅          | first_row_id 填充、`_pos`/`_row_id` 双读、物理列写入、`appends_after`                                                                                                                                               |
| RowIdMapping（编码/lookup/持久化） | ✅          | 三态编码、`to_blob`/`from_blob`、`rebuild`/`remove_row_ids`                                                                                                                                                   |
| 插件 RowID 适配 + `prune`       | ✅（有单测）     | BTree/IVF/IVF-PQ；`prune` 整段重写                                                                                                                                                                           |
| 检索回表                        | ✅          | `search_*` 内闭环                                                                                                                                                                                          |
| INSERT 增量                   | ✅          | `maintain_index(AppendOnly)` + `plan_incremental`                                                                                                                                                       |
| **端到端 DML 维护编排**            | ⚠️ **未接通** | `prune`/`remove_row_ids`/`rebuild_after_compaction`/`maintain_index(DeleteOnly/Compaction)` 是积木，框架已实现但 **ABI 层未暴露**对应入口（当前 ABI 只有 `build/optimize/drop_index_by_metadata`），引擎无法通过 ABI 调用；`prune` 无生产调用方 |
| **prune 后注册表统计刷新**          | ⚠️ 未接通     | 段重写后 `indexed_rows`/`size_bytes` 应刷新，编排未接                                                                                                                                                               |
| **映射持久化（注册表提交路径）**          | ✅ 完成       | `prepare_registry_with` / `prepare_drop_index` 将 `self.mapping` 传给 `prepare_registry_commit` → `write_registry_statistics_file`，映射 blob 随 registry Puffin 一并持久化。`open_metadata_location` 优先从 blob 恢复，缺失时回退重建。ABI 无需感知此改动 |
| **prune 写新 URI**            | ❌ 未做       | 当前同 URI 覆盖，波及旧快照                                                                                                                                                                                        |
| **映射分片惰性加载**                | ❌ 未做       | 全量常驻，超大表无背压                                                                                                                                                                                             |

### 8.2 边界 / 错误行为

- **映射 blob 缺失** → 空映射；检索返回空，不报错（可由 `build_rowid_mapping` 重建）。
- **数据文件缺 `first_row_id` 元数据** → SDK 查询 `_row_id` 直接**报错**。
- **段被全删空** → `prune` 不写文件，由上层从注册表移除。
- **RowID 不在映射中** → `lookup` 返回 `None`，回表丢弃该行。

### 8.3 后续方向

- 端到端 DML 维护编排：ABI 暴露 `prune` / `rebuild_after_compaction` / `maintain_index` 入口，DELETE/Compaction 自动触发 `prune` + 刷新注册表；`IndexedTableView` 提交时传入映射。
- 映射分片惰性加载 + LRU（大表背压）。
- `prune` 写新 URI，支持旧快照时间旅行。
- 多分区支持、RowID 继承在引擎侧的落地。

---

## 附：接口速查表

| 我想做的事         | 用哪个接口                                                                        |
| ------------- | ---------------------------------------------------------------------------- |
| 打开索引表         | `IndexedTableView::open_metadata_location()`                                 |
| 向量/标量检索并回表    | `search_vector_materialized()` / `search_scalar_materialized()`              |
| RowID → 物理地址  | `RowIdMapping::lookup(row_id)` / `lookup_batch(row_ids)`                     |
| RowID → 整行数据  | `read_rows_by_row_id(row_ids, mapping)` / `read_rows_by_row_id_ordered(...)` |
| 构建映射          | `RowIdMapping::build()` / `source.build_rowid_mapping()`                     |
| DELETE 清理     | `prune()` + `remove_row_ids()`                                               |
| Compaction 重建 | `rebuild_after_compaction()`                                                 |
| INSERT 增量规划   | `plan_incremental()`（底层 `appends_after`）                                     |
| 判定快照变更        | `classify_change()` / `detect_compaction()`                                  |
| 维护分发          | `maintain_index(action, request, previous_registry)`                         |
| 映射持久化         | `to_blob()` / `from_blob()`                                                  |
| SDK 读 RowID 列 | `select([..., "_row_id", "_pos"])`                                           |
| SDK 增量扫描      | `appends_after(from_snapshot_id)`                                            |
