# 多列 BTree 索引 vs 单列 BTree 索引 — 实现/设计对比

> 基于 `feat/multi-column-btree`（已合入 main）。设计动机与备选方案见 [多列BTree索引设计方案.md](多列BTree索引设计方案.md)，本文聚焦**实现与设计差异**：改动点、存储布局、排序机制、索引查找、搜索路径、分区行为、API 演进，不含性能基准。

## 1. 核心结论：单列是 N=1 的退化，不是两条独立路径

**多列实现没有为单列保留任何特判分支**（`crates/iceberg-index-plugins/src/btree/` 中无 `len==1` 分支）。单列索引就是 `key_columns` 长度为 1 的复合键：

- **排序**：同一个 `CompositeKey::cmp`（逐分量字典序 + 长度决胜），N=1 时自然退化为单值比较。
- **存储**：同一份 Lookup v2 格式，`NumKeyColumns` 字段从 1 变 N；数据页每列一个 Arrow 数组，单列恰好是 1 个键数组。
- **查找/搜索**：同一个 `locate_bounds` + `locate_composite` + `build_filter_mask_multi`，`col_map` 从"映射到列 0"变"映射到 0..N-1"；FDW 侧同一套 `match_index` 探测 + 前缀可用性判定。
- **分区**：分区处理在 core 索引引擎层（`PartitionGroup` + `plan_coverage` + `full_scan_scalar`），单列/多列共用，多列没有也不需要额外分区适配（见 §6）。

> 推论：**写单列索引的代码 = 写多列索引的代码**。多列不是单列的超集改造，而是把"键"从单个值泛化为"值数组"，单列恰好落入其中；且顺带修复了单列旧实现"丢弃多表达式"的隐患。

---

## 2. 改动点全景：多列到底改了什么

以"单列时期 → 多列时期"逐层列改动，加粗为**语义变化点**（不只是实现细节）。

| # | 层 / 模块 | 单列时期 | 多列时期（改了什么） | 影响 / 为什么 |
|---|-----------|----------|----------------------|---------------|
| 1 | 构建参数 `build_parameters` | `key_column`（单个列名） | **`key_columns`（列名数组）**，顺序 = 排序优先级 | 旧 props `key_column` 被静默忽略（API 重构坑，见 §8） |
| 2 | 搜索接口 `ScalarSearchRequest` | `expression`（单表达式） | **`expressions`（表达式数组，AND 语义）** | FDW/bridge/引擎三层统一；单对象兼容形态已移除 |
| 3 | 键类型（btree 插件） | 单值 / `ScalarValue::List` | **`CompositeKey(Vec<ScalarValue>)`**，逐分量字典序 + 长度决胜 | 不复用 `ScalarValue::List`（其 Ord 仅按长度比较，会破坏字典序） |
| 4 | 构建 `build.rs` | 每行读 1 个键列 | 每行读 N 个键列组成 `components` → `CompositeKey`；`sort_by(CompositeKey::cmp)` | 单列 = 1 元素，代码路径不变 |
| 5 | 页面格式（Data Page） | `[key, row_address]`（2 列） | **`[key_0, …, key_{N-1}, row_address]`（N+1 列）**，每键列一个 Arrow 数组 | 单列 = 1 个键数组 |
| 6 | Lookup 二进制 | v1 | **v2**：新增 `NumKeyColumns`、每列 `KeyTypes`、每页 N 对 `(min_key, max_key)` | v2 与 v1 不兼容（有意决策，无迁移路径） |
| 7 | 页面定位 `lookup.rs` | 单列 `locate()` | **`locate_composite()`**（N 元素 `CompositeKey` 的 `max_key` 二分 + 边界回溯） | 统一路径，旧 `locate()` 已删除 |
| 8 | 边界计算 `runtime.rs` | 单列 bounds | **`locate_bounds()` 前缀语义**：等值列 lo=hi → 首个非等值列收拢 → 后续列全范围 | 多列独有的定位规则 |
| 9 | 页内过滤 `page.rs` | 单列 mask | **`build_filter_mask_multi()`**：列内多表达式 AND + 跨列 AND | 顺带修复单列"丢弃多表达式"隐患 |
| 10 | FDW 路由 `scalar_index_scan_adapter.cpp` | 逐谓词找索引 | **一次 `match_index` 取回全量** → 按列过滤 → 同索引收集 → **首列前缀可用性判定**（首列无谓词则拒路由） | 跳过首列 → 原生全扫描 |
| 11 | 映射校验 | field_ids 对齐单列 | `field_ids.len() == key_columns.len()`，**同序同长** | 构建期强制；运行时取自 lookup blob 的 Puffin `fields` |

**涉及仓库**：`iceberg-index`（core 接口 + abi 解析 + plugins/btree 全模块 + db/sql DDL）、`iceberg-rust-bridge`（expression 数组透传）、`iceberg_fdw`（标量索引适配）。

> 读法：**语义变化点只有 5 个**——① 键参数从单值变数组；② 搜索入参从单表达式变数组；③ 键类型从单值变 `CompositeKey`；④ Lookup 格式 v1→v2；⑤ 定位/过滤从单列变前缀语义 + 多列 mask。其余（④⑤ 的实现细节、FDW 路由、校验）都是这些变化的落地。

---

## 3. 排序机制：单列 vs 多列

### 3.1 排序键类型 `CompositeKey`

```rust
// mod.rs
/// Lexicographic ordering: compare component-by-component left-to-right,
/// then by length. Intentionally a btree-local type — does NOT reuse
/// `ScalarValue::List` whose `Ord` is length-only.
pub struct CompositeKey(pub Vec<ScalarValue>);

impl Ord for CompositeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let min_len = self.0.len().min(other.0.len());
        for i in 0..min_len {
            let c = self.0[i].cmp(&other.0[i]);
            if c != std::cmp::Ordering::Equal {
                return c;
            }
        }
        self.0.len().cmp(&other.0.len())   // 前缀相同 → 长度小的在前
    }
}
```

| 维度 | 单列 | 多列 |
|------|------|------|
| 键形态 | `CompositeKey([v])`（1 元素） | `CompositeKey([v0, v1, …, v_{N-1}])`（N 元素） |
| 比较规则 | 直接比 `v`（`ScalarValue::Ord`） | 逐分量字典序：先比 v0，再比 v1…；全相等则长度小的在前 |
| 长度决胜 | 单列恒等长，长度分支不触发 | 前缀完全相同（如 `[1]` vs `[1,2]`）时 `[1]` 排前 |

**关键点**：排序语义必须与 `ScalarValue::Ord` 保持一致（NaN-first、跨类型按判别值），这是索引有序性的硬约束。之所以不直接复用 `ScalarValue::List`，是因为它的 `Ord` **仅按长度比较**（无排序意义），直接拿来会破坏字典序。

### 3.2 构建时的排序

```rust
// build.rs Step 2
raw_entries.sort_by(|a, b| a.key.cmp(&b.key));   // 单列多列同一条语句
```

- 每行把各键列读成 `components`（`Vec<ScalarValue>`）→ `CompositeKey` → 整体 `sort_by`。
- 单列时 `components` 只有 1 个元素，排序退化为单值排序，但**代码路径完全相同**。

---

## 4. 存储布局：单列 vs 多列

### 4.1 数据页（Data Page）

构建时按复合键序分页（每 `page_size` 行一页），每页一个 RecordBatch：

```rust
// build.rs Step 4（伪代码）
for chunk in raw_entries.chunks(page_size) {
    for col_idx in 0..ncols {                    // 每列一个 Arrow 数组
        arrays.push(build_key_array(chunk, col_idx));
    }
    arrays.push(row_addresses);                  // 最后一个 UInt64Array（file_idx + row_position 打包）
    let batch = RecordBatch::new(page_schema, arrays);
    page_bytes = serialize_page(&batch);
    lookup_entries.push(LookupEntry{ min_key, max_key });   // 页首/页尾复合键
}
```

| 维度 | 单列 | 多列 |
|------|------|------|
| 页面列数 | `[key_0, row_address]`（2 列） | `[key_0, key_1, …, key_{N-1}, row_address]`（N+1 列） |
| 行地址 | `UInt64Array`：`file_idx(u32) << 32 | row(u32)` 打包 | 同（与列数无关） |
| 每列存储 | 一个 Arrow 数组 | 每列各自一个 Arrow 数组（同规则） |
| 键类型 | 取首行 Arrow 列类型（NULL 不携带类型） | 每列各自推断 |

### 4.2 Lookup 表（Lookup v2 二进制）

```rust
// lookup.rs 头部布局注释
// Magic(2)="BT"  Version(2)=2  NumKeyColumns(4)
// KeyTypes: [u8; N]（每列类型：0=Int64, 1=Utf8, 2=Float64）+ padding
// PageSize(4)  NumPages(4)  NumFiles(4)  Reserved(4)
// ├─ file_table: NumFiles × (FileLen:u32 + UTF-8 path)
// └─ entries:    NumPages × N × (min_key, max_key)   // 每页每列各一对 min/max
```

| 维度 | 单列 | 多列 |
|------|------|------|
| `NumKeyColumns` | 1 | N |
| `KeyTypes` | 1 个字节 | N 个字节（+ 对齐 padding） |
| 页条目 | 每页 1 对 `(min,max)`（1 元素键） | 每页 N 对 `(min,max)`（每列各一对） |
| v1/v2 兼容 | **不兼容**（v2 是单列时期的超集，无迁移路径，有意决策） | — |

- `LookupEntry { min_key, max_key }` 是 `CompositeKey`（N 元素），序列化后页条目变成 N 对 min/max。
- 序列化时的 NULL 边界处理（lookup.rs）：NULL 在二进制编码里没有 tag，会把 NULL 边界提升为最紧的可表示值，保证 `min <= hi` 保守检查在负区间不误删页面。单/多列同规则。

---

## 5. 怎么找索引：两层定位

"找索引" 分两层：**FDW 在规划期发现该用哪个索引**，**引擎在搜索时定位哪些页面**。

### 5.1 第一层：FDW 发现与路由（`scalar_index_scan_adapter.cpp`）

```
openGauss 查询（WHERE a=1 AND b>2）
  │
  ├─ 1. match_index 一次取回全部活跃索引（index_name=NULL）
  │      → match_json（所有索引的 column_names / field_ids / kind）
  │
  ├─ 2. 逐谓词：
  │      ├─ 解析出该列真实 field_id（未解析 → 跳过，recheck 兜底）
  │      ├─ 翻译成表达式 JSON（{"field_id","op","value"}）
  │      └─ 从同一份 match_json 按列过滤，找第一个 kind=="Scalar" 且
  │         column_names 含该列的索引，一并取回【索引首列】column_names[0]
  │
  ├─ 3. 同索引收集：只保留命中【第一个索引】的谓词，
  │      其他索引的谓词丢弃（local recheck 兜底）→ expression_json 数组
  │
  └─ 4. 前缀可用性判定：若没有谓词命中【索引首列】→ 返回 false
         → 走原生数据面全扫描（文件级裁剪 + recheck），不加载任何索引页
```

**单列 vs 多列的差异**：
- 单列索引 `(a)`：`column_names[0] == a`，任何 a 上的谓词天然命中首列，前缀判定恒通过。
- 多列索引 `(a,b,c)`：只有谓词覆盖到 **column_names[0] = a** 才通过前缀判定；`WHERE b=2`（跳过首列）在**路由期即被拒**——因为引擎侧 `locate_bounds` 对首列无表达式时必然全范围页面扫描，路由进索引是纯开销。

**两个非确定性**（多候选索引时）：`first-match` 锁死由"谓词迭代顺序 × match_json 顺序"决定（tracking: iceberg_fdw #38）。正确性由 recheck 兜底，纯性能问题。

### 5.2 第二层：引擎在索引内定位页面（`runtime.rs` + `lookup.rs`）

```
表达式数组（AND 语义，按 field_id 映射）
  │
  ├─ locate_bounds(exprs, key_types, col_map)  →  (lo, hi) 复合键边界
  │    按列从左到右：
  │      无表达式列            → 全范围 [min, max]，结束等值前缀
  │      等值前缀内的 Eq 列     → lo = hi = value
  │      首个非等值列           → 合并该列全部范围表达式为最紧 lo/hi
  │      后续列                → 全范围（精确性交给 mask）
  │
  ├─ locate_composite(lo, hi)  →  页面范围 [start, end)
  │    页面按 max_key 字典序单调，partition_point 二分：
  │      start = 首个 max_key >= lo 的页
  │      end   = 末个 max_key <= hi 的页 + 1（扩展一个边界页）
  │      回溯：从 end 往前找最后一个 min_key <= hi 的页
  │    结果恒为保守超集（绝不漏选可能命中的页）
  │
  └─ build_filter_mask_multi(batch, exprs, key_types, col_map)
        → 页内逐行多列 mask：列内多表达式 AND，跨列再 AND
        → 命中行地址 → FDW 本地 recheck
```

**单列 vs 多列的差异**：
- `locate_bounds`：单列 = 1 列，等值即 lo=hi（或首个非等值列合并范围）；多列 = 前缀语义（等值列收紧 → 首个非等值列收拢 → 后续列全范围）。
- `locate_composite`：单列 = 1 元素 `CompositeKey` 的二分；多列 = N 元素键二分（旧的单列 `locate()` 已随统一路径删除）。
- `build_filter_mask_multi`：单列只作用于列 0；多列按 `col_map` 分组，列内 AND + 跨列 AND。

### 5.3 两层合起来：一条查询的完整寻径

```
WHERE a=1 AND b>2   (索引 (a,b,c))
  ① FDW: match_index 发现 (a,b,c) 可用，a 命中首列 → 前缀可用 → 走索引
  ② FDW: 收集 a、b 两个谓词 → expressions = [{a,eq,1},{b,gt,2}]
  ③ 引擎: locate_bounds → a 收紧 lo=hi=1，b 收拢 (2, +∞]，c 全范围
  ④ 引擎: locate_composite 二分 → 候选页面范围
  ⑤ 引擎: 页内 mask 按 a=1 AND b>2 逐行过滤 → 候选行地址（保守超集）
  ⑥ FDW: 对所有谓词本地 recheck → 精确结果
```

---

## 6. 分区表行为：为什么多列无需额外分区适配

多列索引在分区表上的行为常被误解为"缺适配"。实际上**分区处理与 BTree 键结构是正交的两个层面**，多列只改了键结构，因此天然继承已有的分区支持，不需要（也不应该）在 btree 插件里做分区适配。

### 6.1 正交性：分区与键结构互不感知

```
分区  = 哪些文件进哪个 segment   ← 索引结构层（iceberg-index-core / runtime）
BTree键 = 段内排序/过滤           ← 插件层（plugins/btree）
```

| 层面 | 关心什么 | 多列索引改了吗 |
|------|---------|--------------|
| 分区/段 | 文件按 `PartitionIdentity` 分组 → 每分区一个 `PartitionGroup` 持有 segments；搜索时 `plan_coverage` 分已覆盖/未覆盖 | **没改**（沿用 vector/单列已有的模型） |
| BTree 键 | `key_columns` 复合键在段内排序、前缀定位、mask 过滤 | **只改了这里**（单值 → 复合键） |

一个 segment 里的键就按 `CompositeKey::cmp` 排序，段本身不感知自己在哪个分区；分区列要不要进 key_columns 由你决定。因此 btree 插件里**不该有**分区代码。

### 6.2 多列索引在分区表上的实际行为

- **构建**：`PartitionBuildPlan` 按分区分组文件 → 每分区一个 segment（键仍是 key_columns 的复合键）。
- **搜索**：`plan_coverage` 把快照分成两类，结果合并：
  - **已覆盖 segments** → 走 btree `index.search()`（前缀定位照常，无需知道分区）；
  - **未覆盖分区** → `full_scan_scalar` 回退（重读该分区数据文件、重新收集键、内存过滤）。
- **文件裁剪**：FDW/规划器把基于分区谓词的 `pruned_files` 传给引擎，coverage planner 跳过无关分区文件（`parallel_scan_2x2` 测了 `identity(c_shard)` 谓词裁剪其他文件）。

### 6.3 证据：单列索引同样没有分区适配

单列 BTree 代码里同样**没有**任何分区相关逻辑——它和多列共用同一套 `PartitionGroup` + `plan_coverage` + `full_scan_scalar`。这说明不是"多列缺适配"，而是**整个 BTree 标量索引都建立在"按分区分段"这个已有的索引模型之上**，分区支持是架构白送的。

### 6.4 设计文档 §7.5 已有相关描述

设计文档并非完全没提分区——§7.5「覆盖分区回退」明确写到多列在回退路径上的行为：

> 未覆盖分区走 `full_scan_scalar`：对实时 batch 重新收集键、**构建多列 batch**，并**按 field_id 映射表达式过滤**（与索引路径同一套 col_map），保证与索引路径行为一致。

它被当作"回退一致性"写，而不是"分区适配"——因为已覆盖路径根本不需要分区处理，只有回退路径需要保证与索引路径行为一致。

### 6.5 两个前提与一个验证盲区

1. **"天然支持"的前提**是：多列沿用了已有的**按分区分段**模型（设计 §3/§4 隐含了构建按分区分组）。如果当初设计成"整表一个全局 BTree"（不按分区分段），多列就**必须**自己做分区裁剪。正因选了 segment/PartitionGroup 模型，分区支持是架构白送的。
2. **分区列与键列的关系**：分区谓词的文件裁剪在 coverage 层（`pruned_files`），**不依赖索引**；若分区列也放进 key_columns，BTree 键再叠一层裁剪（双重效果），但这不是必需的。
3. **验证盲区**：多分区列 + 多列键的组合、分区**变换列**（`bucket`/`truncate`，分区值 ≠ 原始列值）作为键列的行为——均未测试。这些不是"需要适配"，而是"未覆盖验证"，值得补用例钉住。

---

## 7. 搜索路径对比（摘要）

| 维度 | 单列 | 多列 |
|------|------|------|
| 接口 | `expressions: Vec`（1 元素） | 同（N 元素，AND 语义） |
| 列映射 | `col_map: field_id → 0` | 同（→ 0..N-1） |
| 前缀定位 | 等值即 lo=hi | 等值收紧 → 首个非等值收拢 → 后续全范围 |
| 页内过滤 | 单列 mask | 列内 AND + 跨列 AND |
| 路由决策 | 该列有谓词 → 走索引 | 首列有谓词 → 走索引；跳过首列 → 路由期拒绝 |

**前缀语义（多列唯一的语义差异）**：

```
索引 (a,b,c)：
  WHERE a=1            → 可用（单列等价）
  WHERE a=1 AND b>2    → 可用：a 定位，b 收拢，c 页内过滤
  WHERE b=2            → 不可用：跳过首列 → full scan（单列索引无此限制）
  WHERE c=1            → 不可用：非前缀 → full scan
```

---

## 8. API 演进（多列引入的连带改动）

多列不是内部悄悄改，而是动了三层接口：

| 改动 | 单列时期 | 多列时期 | 影响 |
|------|------|------|------|
| `key_column`（单数） | 键列名 | **`key_columns`（数组）** | 旧 props `key_column` 被静默忽略（API 重构坑，见 common-pitfalls #二十四） |
| `expression`（单对象） | 搜索入参 | **`expressions`（恒数组）** | FDW / bridge / 引擎三层统一；单对象兼容形态已移除 |
| `field_ids` | 与单列对齐 | 与 `key_columns` **同序同长** | 构建期校验；运行时取自 lookup blob 的 Puffin `fields` 字段 |
| `inject_key_columns` | — | 调用方未给 `key_columns` 时从 `column_names` 自动推导 | 单列 CREATE INDEX 的默认路径（镜像向量索引的 `inject_vector_column`） |

---

## 9. 正确性机制对比

两层防线对单/多列**完全相同**：

1. **引擎返回保守超集**：`locate_composite` / `locate_bounds` 绝不**少**选页面；mask 逐行精确判定。
2. **FDW 本地 recheck**：对所有谓词做二次过滤（只删不补）。超集性质是硬约束。

多列新增的**前缀可用性判定**（§5.1）是纯性能优化，不改变正确性：首列无谓词时 `locate_bounds` 必然全范围，路由进索引是纯开销，原生全扫描总是至少一样便宜，因此在路由期直接拒绝。

---

## 10. 设计权衡小结

| | 单列 | 多列 |
|--|------|------|
| 适用查询 | 任意该列谓词 | 前缀覆盖的复合谓词 + ORDER BY 前缀 |
| 命中裁剪 | 单列范围裁剪 | 多列联合裁剪（一次排序、一次页面扫描） |
| recheck 压力 | 其余谓词引擎外逐行 recheck | 前缀之外的列由 mask 页内过滤，recheck 更少 |
| 复杂度 | 低（实现上已与多列统一，复杂度被共享） | 前缀语义 + 复合排序 + Lookup v2 |
| 代价 | 复合谓词无法同时利用 | 非前缀查询退化全扫描；多候选索引 first-match 择优问题（存量，非多列引入） |
| 实现方式 | **N=1 的复合键，无独立路径** | 统一路径 |

## 11. 一句话总结

**实现上**：多列把 BTree 键从"单值"泛化为 `CompositeKey(Vec<ScalarValue>)`，排序用**逐分量字典序 + 长度决胜**；存储上数据页**每键列一个 Arrow 数组** + 行地址，Lookup v2 用 `NumKeyColumns` 参数化（单列=1，多列=N）；**找索引**分两层——FDW 一次 `match_index` 探测选索引并按**首列命中**做前缀可用性判定，引擎 `locate_bounds`（前缀 bounds）+ `locate_composite`（max_key 二分）定位页面；**分区**处理在 core 层（`PartitionGroup` + `plan_coverage` + `full_scan_scalar`），单列/多列共用，多列无需额外适配。**单列没有独立代码路径**，就是 `key_columns=["a"]` 的复合键。

**设计上**：多列引入**前缀语义**作为唯一的语义差异——谓词必须覆盖从左起的连续前缀才走索引，否则退化为原生全扫描；单列因为没有"首列之外"的概念，天然不受此限制。正确性机制（保守超集 + FDW recheck）两层防线对两者完全一致。
