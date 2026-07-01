# Rust Iceberg SDK 自定义索引方案

---

## 1. 背景与问题定义

### 1.1 当前 Rust SDK 对 `_row_id` 的支持现状

Apache Iceberg Rust SDK（截至 0.9.1 版本）对 V3 表格式的 `_row_id` 支持处于“元数据就绪，数据操作缺失”的状态：

| 层面        | 支持状态  | 说明                                             |
| --------- | ----- | ---------------------------------------------- |
| **元数据读取** | ✅ 已支持 | 可读取快照的 `first_row_id`、数据文件的 `first_row_id` 等   |
| **数据写入**  | ❌ 未实现 | Writer 不会自动将 `_row_id` 作为物理列写入 Parquet         |
| **数据读取**  | ❌ 未实现 | Reader 不会自动计算 `first_row_id + row_position` 返回 |

这意味着在 Rust 生态中，**无法直接通过查询获得 `_row_id` 列的值**，也无法利用 Iceberg 原生的行级血缘特性。

### 1.2 目标与约束

在 Rust 项目中实现对 Iceberg 表数据的**自定义二级索引**，支持高效点查，并能应对表数据的变更（INSERT / UPDATE / DELETE / Compaction）。约束条件：

- 尽量不侵入 Iceberg SDK 核心代码；

- 索引数据需要与 Iceberg 快照保持一致性；

- 尽可能减少索引失效范围。

---

## 2. 可行方案总览

| 方案                          | 索引 Key | 索引 Value                    | 是否需要 `_row_id` | 索引精度 | 增量更新支持 | 实现复杂度 | 推荐度    |
| --------------------------- | ------ | --------------------------- | -------------- | ---- | ------ | ----- | ------ |
| **A. 物理存储 + `_row_id` 中间层** | 业务字段   | `_row_id`                   | 是（写入物理列）       | 行级   | 可增量    | 极高    | ⭐（不推荐） |
| **B. 动态计算 + `_row_id` 中间层** | 业务字段   | `_row_id`                   | 是（动态计算）        | 行级   | 可增量    | 中高    | ⭐⭐⭐⭐   |
| **C. 物理位置索引**               | 业务字段   | `(file_path, row_position)` | **否**          | 行级   | 可增量    | 中     | ⭐⭐⭐⭐⭐  |
| **D. 文件级索引（Puffin）**        | 文件统计   | 文件级统计值                      | 否              | 文件级  | 天然增量   | 中     | ⭐⭐⭐⭐   |
| **E. 纯元数据剪枝**               | 无      | 无                           | 否              | 文件级  | 无需维护   | 低     | ⭐⭐⭐    |

---

## 3. 为什么索引需要存储 `(file_path, row_position)` 而非仅存储 `_row_id`？

这是一个贯穿方案 A/B/C 的关键设计决策。若索引只存 `_row_id`，查询时必须**额外扫描 Manifest 文件**来定位该 `_row_id` 属于哪个数据文件，从而引入额外 I/O，抵消索引加速收益。

### 3.1 `_row_id` 不是指针，无法直接定位

| 认知误区                  | 实际情况                                                                 |
| --------------------- | -------------------------------------------------------------------- |
| `_row_id` 是数组下标       | `_row_id` 是一个**逻辑 ID**，它与物理存储位置**解耦**                                |
| 可直接计算文件偏移             | 不同文件大小不同，`_row_id` 连续的行可能跨多个文件                                       |
| 文件内 `_row_id` 连续但文件未知 | 必须知道哪个文件包含了 `first_row_id <= target <= first_row_id + row_count` 的范围 |

### 3.2 从 `_row_id` 到物理位置的转换成本高

若索引只存 `_row_id`，查询时每查到一个 `_row_id` 都需要：

1. 扫描 Manifest 文件（或缓存的元数据），找到包含该 `_row_id` 的数据文件；

2. 打开该文件，计算 `row_position = _row_id - first_row_id`，读取行。

这等于每次点查都附带一次 Manifest 扫描，对于千级文件以上的表性能急剧下降。

### 3.3 映射关系因 Compaction 而变化

即使额外维护 `_row_id → file_path` 映射，Compaction 后文件被重写，该映射也会整体失效，必须重建。

### 3.4 直接存储物理位置的优势

| 索引存储内容                      | 查询步骤                    | 依赖的额外操作              |
| --------------------------- | ----------------------- | -------------------- |
| 仅 `_row_id`                 | 查索引 → 扫描 Manifest → 读文件 | **扫描 Manifest**（高开销） |
| `(file_path, row_position)` | 查索引 → 直接读文件             | **无**（O(1) 直接访问）     |

因此，存储 `(file_path, row_position)` 是实现 O(1) 点查的关键。

---

## 4. 方案详解

### 4.1 方案 A：物理存储 + `_row_id` 中间层（不推荐）

**索引 Key**：业务字段  
**索引 Value**：`_row_id`  
**回表方式**：`_row_id` → Manifest 扫描（或第二层映射） → 物理位置

**风险**：

- 需要自行在写入时物理存储 `_row_id`，并与 Iceberg 事务原子绑定；

- Compaction 后必须保留 `_row_id` 列；

- 与社区未来实现可能不兼容。

**结论**：投入产出比极低，强烈不推荐。

---

### 4.2 方案 B：动态计算 + `_row_id` 中间层

#### 4.2.1 核心原理

采用**两层索引**：

```textile
第一层（倒排索引）：业务字段 → _row_id 列表
第二层（位置映射）：_row_id → (file_path, row_position)
```

- **第一层构建**：扫描数据文件，对每一行，通过 `first_row_id + row_position` 动态计算 `_row_id`，然后将 `(业务字段值, _row_id)` 存入倒排索引。

- **第二层构建**：同时将 `(_row_id, file_path, row_position)` 存入位置映射（可用 RocksDB 等 KV 存储）。

- **查询**：`WHERE name = 'Alice'` → 查第一层得 `_row_id` 列表 → 查第二层得物理位置 → 读数据。

#### 4.2.2 与方案 C 的对比

| 维度             | 方案 B（`_row_id` 中间层）            | 方案 C（直接物理位置）                   |
| -------------- | ------------------------------ | ------------------------------ |
| 索引 Key         | 业务字段                           | 业务字段                           |
| 索引 Value       | `_row_id` 列表                   | `(file_path, row_position)` 列表 |
| 回表步骤           | 两步（先查 ID，再查位置）                 | 一步（直接得位置）                      |
| 业务字段值变更        | 仅需更新第一层（删除旧 key，插入新 key），第二层不变 | 必须更新所有相关物理位置条目（key 变了）         |
| Compaction 后   | 仅需更新第二层（`_row_id` → 新位置），第一层不变 | 必须更新所有受影响的业务字段条目               |
| 与 CDC / 行级血缘结合 | 天然利用 `_row_id` 语义              | 无法利用                           |

---

#### 4.2.3 全量构建示例

```rust
fn build_index_b(table: &Table, index_layer1: &mut InvertedIndex, index_layer2: &mut KvStore) -> Result<()> {
    let scan = table.scan().build()?;
    let tasks = scan.plan_files()?;
    for task in tasks {
        let first_row_id = task.file().first_row_id().unwrap();
        let reader = task.open()?;
        for (row_pos, row) in reader.enumerate() {
            let row_id = first_row_id + row_pos as i64;
            let key = row.get("name")?.to_string();  // 业务字段
            let file_path = task.file().path();
            index_layer1.add(key, row_id);
            index_layer2.put(row_id, (file_path, row_pos));
        }
    }
    Ok(())
}
```

#### 4.2.4 增量维护（详见第 5 节）

利用增量扫描，仅处理新增或重写的文件，分别更新两层索引。

---

### 4.3 方案 C：物理位置索引（推荐）

#### 4.3.1 核心原理

**直接建立业务字段到物理位置的映射**，无中间层：

```textile
业务字段 → [(file_path, row_position), ...]
```

- **构建**：扫描数据文件，读取业务字段值和 `(file_path, row_position)`，存入 KV 存储（Value 为列表）。

- **查询**：`WHERE name = 'Alice'` → 查索引得到位置列表 → 直接读数据。

- **优点**：一次索引查找 + 一次文件读取，O(1) 点查，性能最优。

- **缺点**：业务字段值变更或 Compaction 时，需更新所有相关条目（无缓冲层）。

#### 4.3.2 全量构建示例

```rust
fn build_index_c(table: &Table, index_store: &mut KvStore) -> Result<()> {
    let scan = table.scan().build()?;
    let tasks = scan.plan_files()?;
    for task in tasks {
        let reader = task.open()?;
        for (row_pos, row) in reader.enumerate() {
            let key = row.get("name")?.to_string();
            let file_path = task.file().path();
            index_store.append(key, (file_path, row_pos));  // 追加到列表
        }
    }
    Ok(())
}
```

#### 4.3.3 增量维护（详见第 5 节）

同样利用增量扫描，但更新时需处理列表的合并/删除，较为繁琐。

---

### 4.4 方案 D：文件级索引（Puffin / Manifest）

利用 Iceberg 的 Puffin 文件或 Manifest 文件存储文件级统计信息（如 min/max、Bloom Filter），用于查询时剪枝。

- 与快照绑定，增量更新天然支持。

- 精度为文件级，无法精确到行。

- 适合过滤性强的查询，不适合点查。

---

### 4.5 方案 E：纯元数据剪枝

完全依赖 Iceberg 元数据（快照 `first_row_id`、文件 `first_row_id` 和 `row_count`）在查询时跳过文件。

- 零侵入，开箱即用。

- 需扫描 Manifest，可能定位到多个文件，点查性能有限。

---

## 5. 增量索引构建：通过快照对比实现（核心优化）

此方法适用于方案 B、C、D，可避免全量重建。

### 5.1 原理：Iceberg 增量扫描（Incremental Scan）

Iceberg 原生支持通过对比两个快照，仅获取新增或重写的数据文件（即 `ADDED` 状态的 Manifest 条目）。

- Java SDK：`IncrementalDataTableScan`

- Rust SDK（待发布）：PR #2153 已合入，即将提供 `appends_after(snapshot_id)` API。

**Rust API 预览**：

```rust
let scan = table.scan()
    .appends_after(last_snapshot_id)?   // 从该快照之后的所有新增
    .build()?;
let tasks = scan.plan_files()?;         // 仅返回新增或重写的文件
```

### 5.2 增量维护流程

```textile
记录 last_snapshot_id（上次索引构建到的快照）
         ↓
获取 current_snapshot_id（最新快照）
         ↓
调用 appends_after(last_snapshot_id) 获得增量文件列表
         ↓
仅扫描这些文件，更新索引：
   - 对于新增 INSERT 文件：插入新条目
   - 对于 Compaction 新文件：删除旧文件条目，插入新条目
         ↓
更新 last_snapshot_id = current_snapshot_id
```

### 5.3 适用场景与限制

| 场景              | 适用性     | 说明                  |
| --------------- | ------- | ------------------- |
| INSERT（追加）      | ✅ 完全适用  | 新文件状态为 `ADDED`，精准获取 |
| Compaction（重写）  | ✅ 适用    | 新文件被识别，但需处理旧条目失效    |
| UPDATE / DELETE | ⚠️ 有限适用 | 会产生删除文件，需额外解析标记删除   |
| OVERWRITE 分区    | ⚠️ 部分适用 | 新文件识别，但需清除被替换的旧文件条目 |

---

## 6. 索引维护：如何处理 INSERT / UPDATE / DELETE / Compaction

下表总结各 DML/DDL 对索引的影响及应对策略（以方案 C 为例，方案 B 类似，但分层处理）。

| 操作             | 数据变化                        | 旧索引是否仍可用？                      | 处理策略                            |
| -------------- | --------------------------- | ------------------------------ | ------------------------------- |
| **INSERT**     | 新增数据写入新文件，产生新行              | 旧索引不包含新数据                      | 增量扫描新文件，追加新条目                   |
| **UPDATE**     | 旧行标记删除，新行插入新文件（新 `_row_id`） | 指向旧行的条目失效；新行未索引                | 需清理旧条目，并插入新条目（可结合增量扫描 + 解析删除文件） |
| **DELETE**     | 行被标记删除                      | 索引条目失效（指向已删除行）                 | 需清理对应条目（需解析删除文件）                |
| **Compaction** | 文件重写，`_row_id` 不变但物理位置变     | 索引条目失效（file_path / row_pos 已变） | 增量扫描新文件，删除旧文件所有条目，插入新条目         |

**实施建议**：

- 对于 DELETE/UPDATE，需解析删除文件（Delete Files）获取被删除的 `_row_id` 或物理位置，然后从索引中删除。

- 若无法解析删除文件，可定期全量重建索引以保证一致性。

---

## 7. 查询策略：“旧数据扫索引，新数据扫表”

为应对索引滞后于最新快照的情况（增量索引尚未构建完成），查询时可采取分治策略：

1. 维护一个“已索引快照 ID”检查点 `last_indexed_snapshot`。

2. 查询时：
   
   - 对于 `last_indexed_snapshot` 之前的数据：使用索引定位。
   
   - 对于 `last_indexed_snapshot` 之后的新数据：通过增量扫描实时读取并动态计算（不依赖索引）。

3. 合并两部分结果。

此策略保证查询结果的正确性，且新数据扫描量通常较小（取决于增量更新频率）。后台可异步推进 `last_indexed_snapshot` 以缩小扫描范围。

---

## 8. 方案优劣与风险评估

| 方案  | 查询性能    | 写入/维护开销 | 一致性   | 实现复杂度 | 社区兼容性 |
| --- | ------- | ------- | ----- | ----- | ----- |
| A   | 高       | 极高      | 需自行保证 | 极高    | 低     |
| B   | 高（两步查）  | 中       | 索引可滞后 | 中高    | 高     |
| C   | 最高（一步）  | 中       | 索引可滞后 | 中     | 高     |
| D   | 中（文件剪枝） | 低       | 天然一致  | 中     | 高     |
| E   | 低       | 无       | 完全一致  | 低     | 最高    |

---

## 9. 方案选择建议

| 您的场景                       | 推荐方案             | 理由                        |
| -------------------------- | ---------------- | ------------------------- |
| 业务字段稳定，追求极致点查性能            | **方案 C**         | 无中间层，查询最快，实现相对简单          |
| 业务字段频繁变更，或需要与 CDC / 行级血缘结合 | **方案 B**         | `_row_id` 中间层减少字段变更时的更新开销 |
| 仅需文件级过滤，希望与社区对齐            | **方案 D（Puffin）** | 官方方向，维护简单                 |
| 快速验证，无性能要求                 | **方案 E**         | 零侵入，即刻可用                  |

**最终建议**：优先进行方案 C 的概念验证。若后续业务字段变更成为痛点，再考虑迁移到方案 B。

---

## 10. 附录：关键 API 参考（Rust SDK）

```rust
// 获取当前快照
let snapshot = table.current_snapshot();

// 获取快照的 first_row_id
let first_row_id = snapshot.first_row_id();

// 增量扫描（PR #2153，即将发布）
let scan = table.scan()
    .appends_after(last_snapshot_id)?
    .build()?;

// 获取增量数据文件
let tasks = scan.plan_files()?;

// 从 FileScanTask 中获取数据文件的 first_row_id
let first_row_id = task.file().first_row_id();

// 读取数据文件（需自行实现读取逻辑）
let reader = task.open()?;
```
