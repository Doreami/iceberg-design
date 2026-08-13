# 多列 BTree 复合索引 — 设计方案

> 最后更新: 2026-08-07 | 分支: `feat/multi-column-btree`

## 1. 背景与动机

现有 BTree 标量索引只支持单列键。实际查询常携带跨列条件（如 `WHERE a = 1 AND b > 2`），在单列索引下只能命中其中一个谓词，其余谓词必须在引擎外逐行 recheck；引擎无法利用复合谓词做一次性的键范围裁剪，也无法服务 `ORDER BY` 前缀场景。

本方案将 BTree 索引推广为**复合键** `(a, b, c)`，采用标准 SQL 复合索引的**前缀语义**：只有从左到右连续等值、第一个范围条件及更早的列能用于定位页面，其余列仅用于页内精确过滤。这样 `WHERE a=1 AND b>2` 可用索引裁剪，而 `WHERE b=2`（跳过首列）退回等价全扫描。

### 备选方案对比

| 方案 | 优点 | 缺点 | 结论 |
|------|------|------|------|
| 多个单列索引 + 结果合并 | 各索引独立、构建简单 | 需要多索引交叉合并、谓词→索引的分配逻辑复杂；对 `a=1 AND b>2` 无法同时利用两列 | 放弃 |
| **复合键 + 前缀语义** | 单次排序、单次页面扫描；语义与 PostgreSQL/SQL Server 复合索引一致；复用既有页面/LRU/二分基础设施 | 非前缀查询（跳过首列）无法利用索引 | **采用** |

## 2. 设计目标与非目标

### 目标
- 支持复合键 `(c1, c2, …)` 的构建与搜索，前缀语义正确。
- **单列索引作为 1 列复合键退化**：搜索走统一路径，无需双分支，且顺带修复"单列索引丢弃多表达式"的隐患。
- 表达式→索引列映射用 `field_id`（曾因位置映射出过 bug），与建表 schema 解耦。
- 引擎接口 `ScalarSearchRequest` 由单表达式升级为表达式数组（AND 语义）。
- 不要求 v1 格式兼容。

### 非目标
- 非前缀查询的加速（`WHERE b=2` 等价全扫描，交给调用方决定是否绕开索引）。
- 修改 core `ScalarValue` 的类型系统（复合键是 BTree 插件的局部关切）。
- 支持 NULL 语义的 SQL 三值逻辑（NULL 键按"最小"参与排序，详见 §7.3）。

## 3. 总体架构与数据流

```
openGauss 查询
  └─ iceberg_fdw: 收集同一索引的谓词 → 表达式 JSON 数组（恒为数组）
       └─ iceberg-rust-bridge: parse_scalar_query 透传 expression_json（仅数组）
            └─ iceberg-index
                 ├─ build: 逐列读键 → CompositeKey → 排序 → 分页 → Lookup v2
                 └─ search: 表达式数组 → 前缀定位页范围 → 页内多列 mask 过滤 → 候选行地址
```

- **构建链**: `CREATE INDEX (a,b,c)` → Catalog SQL → `IndexDefinition`（`field_ids` 与 `key_columns` 顺序一致）→ 插件收集键 → 排序 → 分页 → Lookup v2 落盘。
- **搜索链**: FDW 把 `WHERE a=1 AND b>2` 翻译为 `[{field_id:a,eq,1},{field_id:b,gt,2}]`，经 bridge 到达引擎；引擎按前缀语义定位页面，再按全部谓词逐行过滤，返回候选行地址；**FDW 在引擎外对全部谓词做本地 recheck**（见 §6.2）。

## 4. 构建侧设计

### 4.1 CompositeKey
插件内复合键类型，不修改 core `ScalarValue`：

```rust
pub struct CompositeKey(pub Vec<ScalarValue>);
// Ord: 逐分量字典序；前缀相同则长度小的在前（长度决胜）
```

**为何放插件内而非 core**：core 的 `ScalarValue::List` 其 `Ord` 仅按长度比较（任意但无排序意义），直接复用会破坏字典序；复合排序是 BTree 索引的局部关切，放进 core 会污染通用类型。代价是排序语义在插件内 ad-hoc 定义，必须与 `ScalarValue::Ord`（NaN-first、跨类型按判别值）保持一致——这是索引有序性的硬约束。

### 4.2 建索引参数
- `build_parameters.key_columns: Vec<String>`（替代单列 `key_column`），顺序即排序优先级。
- Catalog 侧 `CREATE INDEX (a,b,c)` 逐列解析，产出与 `key_columns` 顺序一致的 `field_ids`。
- ABI 侧 `inject_key_columns`：调用方未显式给 `key_columns` 时，从 `column_names` 自动推导（镜像向量索引的 `inject_vector_column`）。
- **校验**: `field_ids.len() == key_columns.len()`，否则拒绝定义。

### 4.3 页面格式
数据页面从 `[_btree_key, _row_address]` 泛化为 `[_btree_key, _btree_key_1, …, _btree_key_{N-1}, _row_address]`，每列一个 Arrow 数组，键按复合序排序、每 `page_size` 行一页。

### 4.4 Lookup 二进制格式 v2
新增 `NumKeyColumns` 与每列 `KeyType`，用于多列键的编码解码；单列时退化为 v1 布局的超集。**v2 与 v1 不兼容，属有意决策。**

```
Magic(2) Version=2(2) NumKeyColumns(4) KeyTypes(N) padding PageSize(4)
NumPages(4) NumFiles(4) Reserved(4)
├─ file_table: NumFiles × (len:u32 + UTF-8 path)
└─ entries:    NumPages × N × (min_key, max_key)
```

### 4.5 构建流程
```
collect_btree_entries → 每行逐列 → CompositeKey
  → sort_by(CompositeKey::cmp)
  → 分页：逐列构建 key array → RecordBatch（含 _row_address）
  → BTreeLookup::new(entries, key_types, page_size, file_table)
  → serialize → Lookup blob → 写 Puffin segment
```

**键类型推断**：key_types 取自首行的 Arrow 列类型；NULL 值不携带类型信息，类型一律由 Arrow 列类型决定（避免字符串列首行 NULL 被误判为 Int64）。

## 5. 搜索侧设计

### 5.1 接口变更
`ScalarSearchRequest` 由单 `expression` 升级为 `expressions: Vec<ScalarExpression>`（AND 语义）。JSON 入口 `parse_expression_json` 与 bridge `parse_scalar_query` 均**只接受数组**（FDW 恒输出数组；单对象仅是早期单列兼容形态，已移除）。

### 5.2 统一搜索路径
单列索引 = 1 列复合键，搜索**不区分单列/多列**，共用同一套前缀定位 + 多列 mask：

```
search(expressions)
  ├─ 空表达式 → 返回空（调用方回退）
  ├─ 类型校验：每个表达式 value 与目标键列类型兼容，否则报错
  ├─ col_map: field_id → 列下标（field_ids 与 key_columns 顺序一致）
  ├─ locate_bounds(exprs, key_types, col_map) → (lo, hi)
  ├─ lookup.locate_composite(&lo, &hi) → 页面范围 [start, end)
  ├─ 加载页面 → 拼接
  └─ build_filter_mask_multi(batch, exprs, key_types, col_map) → 精确过滤 → 行地址
```

### 5.3 前缀语义（locate_bounds）
对每列依次：
1. **等值列**（`=`，且其左侧全为等值）：`lo = hi = value`。
2. **首个非等值列**：从该列全部表达式合并出最紧 `lo`/`hi`（如 `b>45000 AND b<55000`）。
3. **后续列**：无论是否有表达式，均取全范围 `[MIN, MAX]`（精确性交给 mask）。
4. **首列无表达式**（如 `WHERE b=2`）：所有列全范围，等价整表页面扫描。

结果恒为**保守超集**——绝不漏掉可能命中的页面。

### 5.4 页面范围（locate_composite）
`entries` 按 `max_key` 单调排序；二分找到 `max < lo` 的起点与 `max <= hi` 的终点，再扩展一个边界页并回溯检查 `min <= hi`，保证跨页边界（同一键分布在相邻页、页面同时含 `lo` 前与 `hi` 后键）不丢失。单列定位即 1 元素 `CompositeKey` 的 `locate_composite`（旧的单列 `locate()` 已随统一路径删除）。

### 5.5 精确过滤（build_filter_mask_multi）
按 `col_map` 把表达式分组到各列，列内多表达式 AND，跨列再 AND。支持全部运算符（`=, <, <=, >, >=, IN, BETWEEN, BETWEEN_EXCLUSIVE`）；不匹配任何索引列的表达式不参与（由调用方 recheck）。

### 5.6 运算符在搜索各环节的参与度

| 运算符 | 参与前缀定位 | 页内精确过滤 | FDW 是否下发 |
|--------|:---:|:---:|:---:|
| `=`, `<`, `<=`, `>`, `>=` | ✅ | ✅ | ✅ |
| `IN` | ✅（min/max 收拢） | ✅ | ❌（当前不下发） |
| `BETWEEN` / `BETWEEN_EXCLUSIVE` | ✅（紧致两端） | ✅ | ❌（当前不下发） |
| `!=` / IS NULL | — | — | ❌（不走索引） |

### 5.7 FDW 路由决策（前缀感知，本分支实现）

FDW 规划期判断"走不走索引"，分两步：

1. **探测（probe）**：整个查询**只调一次** bridge `match_index`（`index_name=NULL` 返回全部活跃索引；每次调用都会读表元数据 + 索引注册表 + 当前快照 manifest，逐谓词调用是重复 I/O），随后每列从同一份 JSON 里过滤。在结果里找**第一个** `kind=="Scalar"` 且 `column_names` 含该列的索引；`column_names` 由 bridge 按 `field_ids`/`key_columns` 顺序下发，**第 0 列即索引首列**，一并取回。
2. **收集 + 前缀判定**：
   - 只有探测到**同一索引**（索引名比较）的谓词进入 `expression_json` 数组；其他索引的谓词丢弃，由 FDW 本地 recheck 兜底。
   - **前缀可用性判定**：收集完成后，若**没有表达式命中该索引首列** → 直接返回 false → 走原生数据面全扫描。因为首列无谓词时 `locate_bounds` 必然全范围，索引路由是纯开销；原生全扫描（文件级裁剪 + recheck）总是至少一样便宜。该判定保证：路由进索引 ⟺ 索引至少能裁剪一页。

**路由不变量**：FDW 恒输出 JSON 数组、恒保留全部谓词做本地 recheck；索引结果只作候选超集。**局限**：索引选择是"先到先得"——只取第一个命中的索引并锁死，多个可行索引并存时可能选到次优（见 §11 多候选索引择优）。

## 6. 正确性不变量

### 6.1 索引结果是保守超集
`locate_composite` 与 `locate_bounds` 绝不**少**选页面；`build_filter_mask_multi` 对每个命中谓词逐行精确判定、列间 AND。因此索引返回的候选集 ⊇ 真实命中集。这是引擎正确性的第一层。

### 6.2 FDW 本地 recheck（第二层）
FDW 的标量索引路径**恒对所有谓词做本地 recheck**（iceberg_fdw.cpp 明确注释）。即便引擎层某个谓词未参与过滤（如非索引列表达式），最终结果仍精确。**代价**：引擎返回的候选必须保持超集性质，recheck 只能删、不能补——因此 `locate_composite` 的保守性是硬约束。

### 6.3 映射一致性
- `field_ids` 与 `key_columns` 同序同长（构建期校验）。
- 运行时 `field_ids` **取自 lookup blob 的 Puffin 标准 `fields` 字段**（构建时写入的 field_ids），而非加载时的 `IndexDefinition`——保证 field_id → 列映射始终与构建一致，即便 definition 事后漂移；仅当 blob 无字段时回退 definition。

### 6.4 类型一致性
搜索前对每个表达式做 `is_compatible_with(目标列类型)` 校验，类型错配立即报错而非静默算出错误范围。

## 7. 边界情况与限制

### 7.1 非前缀查询的性能
`WHERE b=2`（索引 `(a,b,c)`，b 非首列）经 §5.7 的前缀可用性判定在**路由期即被拒**，改走原生数据面全扫描（文件级裁剪 + recheck），不再加载全部索引页面。若绕过 FDW（直连引擎 API）仍会按全范围处理，等价全扫描、正确但低效。

### 7.2 非索引列表达式
表达式列不在索引中时被 `col_map` 丢弃，候选集为超集，由 FDW recheck 兜底。直接调用引擎 API 且依赖 `is_exact=true` 的调用方需自行 recheck。

### 7.3 NULL 键
NULL 参与排序（`ScalarValue::Null` 为最小判别值），构建、分页、编码均支持 NULL。SQL 三值逻辑语义不在索引层保证。

### 7.4 浮点边界
`ScalarValue::successor/predecessor`（`Gt`/`Lt` 用于收紧边界）对 Float64 使用 `f64::next_up/next_down`（精确的相邻可表示浮点，main #186 修复）。旧实现 `v ± EPSILON` 在 |v|<1 会跳过真实相邻值导致漏行、|v|≥2 退化为 no-op；现已精确处理。`successor(f64::MAX)` 溢出为 `+inf`、NaN 原样传播。

### 7.5 覆盖分区回退
未覆盖分区走 `full_scan_scalar`：对实时 batch 重新收集键、构建多列 batch，并**按 field_id 映射表达式**过滤（与索引路径同一套 col_map），保证与索引路径行为一致。

## 8. 测试策略

| 层级 | 覆盖 |
|------|------|
| 单元测试（插件内） | CompositeKey 排序、`locate_bounds` 前缀/In/Between、`locate_composite` 页范围与序列化、`build_filter_mask_multi` 列内/列间 AND 与 IN、NULL 键数组构建 |
| e2e（真实 Iceberg 表） | 单列构建+搜索；服务重启后索引恢复；三列构建；多列联合搜索（`a=1 AND b='x'`、`a=1 AND c>2`、前缀范围+次列过滤） |
| Bridge | expression 数组解析（恒数组） |
| FDW | 多谓词收集 → JSON 数组（恒数组）；前缀可用性判定（首列命中才路由） |

## 9. 涉及仓库与模块（概览）

| 仓库 | 模块 | 职责 |
|------|------|------|
| iceberg-index | `plugins/btree`（build/lookup/page/runtime/mod） | 核心：CompositeKey、构建、Lookup v2、前缀定位、多列过滤 |
| iceberg-index | `core` | `ScalarSearchRequest.expressions` 接口 |
| iceberg-index | `abi`（metadata_ops） | `parse_expression_json`（数组）、`inject_key_columns` |
| iceberg-index | `db/sql` | DDL 多列解析 → `key_columns` + `field_ids` |
| iceberg-rust-bridge | `scan` | `parse_scalar_query` 透传 expression_json |
| iceberg_fdw | 标量索引适配 | 整个查询 fetch 一次 match_index、逐列过滤（取回首列）、收集同一索引多谓词 → 表达式 JSON 数组、前缀可用性判定 |

> 注意：本特性伴随少量纯 API 连带改动（`expression` → `expressions`），均已在本方案范围内，无功能外改动。

## 10. 兼容性与格式版本

- **Lookup 二进制 v2**：新增多列键信息，与 v1 不兼容（有意决策，无需迁移路径）。
- **表达式 JSON**：仅接受数组（FDW / bridge / 引擎三层一致，单对象兼容形态已移除）。
- **`is_exact` 语义**：引擎恒返回 `is_exact=true`（不按表达式覆盖度区分）。实际可达路径上不存在"超集标 exact"——FDW 探测保证发到引擎的表达式必命中索引列，且 FDW 全程本地 recheck，结果恒精确（#1 评估为非问题）；直接调用引擎 API 的调用方需自行 recheck。

## 11. 已知问题与后续工作

1. **多候选索引择优**（**main 同样存在的存量问题**，非多列实现引入）：
   - **先到先得**：探测只返回 match JSON 里**第一个**含该列的索引，首次命中的索引被锁死，其余索引的谓词全部丢弃。多列实现前（main）的单列索引同样如此——`WHERE a=1 AND b=1` 在 `(a)`、`(b)` 并存时走哪个索引由谓词迭代顺序决定。
   - **多列新增的一个结果**：`(a)` 与 `(b,c)` 并存时，`WHERE a=1 AND c=1` 若 c 先迭代，first-match 锁死 `(b,c)`、随后前缀可用性判定（§5.7）拒绝 → **不选任何索引走原生全扫描**，错过本可服务 a=1 的 `(a)`。这种"有可用索引却全扫描"的结果在 main 上不会出现（main 总能命中某个单列索引）。（`(b,c)` 无法服务 `c=1` 单独查询是前缀原则的直接推论，§5.3，非本问题。）
   - 最终选择依赖"谓词迭代顺序 × match JSON 顺序"两个非确定性因素，正确性由 recheck 兜底，纯性能问题。
   - **改进方向**：探测返回含该列的全部候选索引，按"谓词集可覆盖的前缀列数 / 覆盖文件数"择优。（跟踪：[iceberg_fdw #38](https://github.com/DataInfraLab/iceberg_fdw/issues/38)）
