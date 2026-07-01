# Rust Iceberg SDK 自定义索引方案汇总

## 一、背景与核心挑战

### 1.1 当前 Rust SDK 对 `_row_id` 的支持现状

| 层面        | 支持状态  | 说明                                            |
| --------- | ----- | --------------------------------------------- |
| **元数据读取** | ✅ 已支持 | 可解析 V3 表的 `firstRowId`、快照元数据等                 |
| **数据写入**  | ❌ 未实现 | Writer 不会自动将 `_row_id` 写入 Parquet 文件          |
| **数据读取**  | ❌ 未实现 | Reader 不会计算 `firstRowId + row_position` 返回给用户 |

### 1.2 核心挑战

要在 Rust 生态中构建基于 `_row_id` 的自定义索引，需要解决以下三个核心问题：

1. **如何获取 `_row_id` 的值？** —— 写入时计算并存储，或读取时动态计算。

2. **如何建立 `_row_id` 到物理位置的映射？** —— 索引中存储 `(_row_id, file_path, row_position)`。

3. **如何保证 `_row_id` 的稳定性？** —— `_row_id` 在数据重写（Compaction）后必须保持不变。

### 1.3 术语约定

| 术语                 | 说明                                  |
| ------------------ | ----------------------------------- |
| **`_row_id`**      | Iceberg V3 规范中的系统元数据列，由引擎管理，全局唯一且稳定 |
| **`row_position`** | 数据行在文件内部的物理偏移量（从 0 开始）              |
| **`first_row_id`** | 数据文件或快照中第一条数据的 `_row_id`            |
| **`file_path`**    | 数据文件在存储系统中的路径                       |

---

## 二、方案总览

以下是所有可行方案的分类汇总：

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                        自定义索引实现方案全景图                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─ 方案一：物理存储 + 外部索引（推荐）                                    │
│  │   └─ 写入时物理存储 _row_id → 外部 KV 存储映射                         │
│  │                                                                         │
│  ├─ 方案二：物理存储 + Manifest 级索引                                    │
│  │   └─ 写入时物理存储 _row_id → Manifest 中记录 min/max _row_id         │
│  │                                                                         │
│  ├─ 方案三：物理存储 + Parquet Row Group 索引                             │
│  │   └─ 写入时物理存储 _row_id → Row Group 级别过滤                       │
│  │                                                                         │
│  ├─ 方案四：物理存储 + Parquet Footer 内嵌索引                            │
│  │   └─ 写入时物理存储 _row_id → Footer 中存储索引结构                    │
│  │                                                                         │
│  ├─ 方案五：动态计算 + 外部索引                                            │
│  │   └─ 写入时不存储 _row_id → 读取时计算并索引                           │
│  │                                                                         │
│  └─ 方案六：纯元数据剪枝（无索引）                                         │
│      └─ 利用快照和 Manifest 的 _row_id 范围进行文件级别过滤               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、方案详细说明

### 方案一：物理存储 + 外部索引（推荐⭐⭐⭐⭐⭐）

#### 3.1.1 架构图

```textile
┌─────────────────────────────────────────────────────────────────────────┐
│                            写入流程                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────┐                  │
│  │  业务数据 │───▶│ 扩展Writer  │───▶│ Parquet文件  │                  │
│  └──────────┘    │ 计算_row_id │    │ (含_row_id列) │                  │
│                  └──────┬──────┘    └──────────────┘                  │
│                         │                                              │
│                         ▼                                              │
│                  ┌─────────────┐                                       │
│                  │ 外部索引存储 │  ← (_row_id, file_path, row_pos)    │
│                  │ (RocksDB)   │                                       │
│                  └─────────────┘                                       │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                            查询流程                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────┐                  │
│  │ 查询条件  │───▶│ 查询索引    │───▶│ _row_id列表  │                  │
│  └──────────┘    └─────────────┘    └──────┬───────┘                  │
│                                             │                          │
│                                             ▼                          │
│  ┌──────────┐    ┌─────────────┐    ┌──────────────┐                  │
│  │ 返回结果  │◀───│ 读取数据    │◀───│ 定位文件+行  │                  │
│  └──────────┘    └─────────────┘    └──────────────┘                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.1.2 核心实现步骤

**步骤 1：扩展写入器，物理存储 `_row_id`**

```rust
// 伪代码：在 ParquetWriter 中计算并写入 _row_id
impl ParquetWriter {
    fn write_with_row_id(&mut self, batch: RecordBatch, first_row_id: i64) -> Result<()> {
        let row_count = batch.num_rows();
        // 生成 _row_id 列
        let row_ids: Vec<i64> = (0..row_count)
            .map(|i| first_row_id + i as i64)
            .collect();
        // 将 _row_id 作为普通列追加到 batch
        let batch_with_row_id = add_column(batch, "_row_id", row_ids);
        // 写入 Parquet（使用普通 field_id，避免与保留 ID 冲突）
        self.write_batch(batch_with_row_id)?;
        Ok(())
    }
}
```

**步骤 2：构建外部索引**

```rust
// 伪代码：写入时同步构建索引
fn write_and_index(
    table: &mut Table,
    data: RecordBatch,
    index_store: &mut IndexStore,
) -> Result<()> {
    // 1. 获取当前全局 next_row_id
    let next_row_id = table.metadata().next_row_id();
    
    // 2. 写入数据（扩展 Writer 会写入 _row_id 列）
    let file_path = table.append(data)?;
    
    // 3. 构建索引条目
    let row_count = data.num_rows();
    for i in 0..row_count {
        let row_id = next_row_id + i as i64;
        index_store.put(row_id, (file_path.clone(), i as u64))?;
    }
    
    // 4. 更新 next_row_id（表元数据）
    table.metadata_mut().set_next_row_id(next_row_id + row_count);
    
    Ok(())
}
```

**步骤 3：查询回表**

```rust
// 伪代码：通过索引查询数据
fn query_by_index(
    table: &Table,
    index_store: &IndexStore,
    key: &str,
) -> Result<Vec<RecordBatch>> {
    // 1. 查询索引，获取 row_id 列表
    let row_ids = index_store.get(key)?;
    
    // 2. 将 row_id 转换为物理位置
    let mut locations = Vec::new();
    for row_id in row_ids {
        let (file_path, row_pos) = index_store.get_location(row_id)?;
        locations.push((file_path, row_pos));
    }
    
    // 3. 批量读取数据
    let mut results = Vec::new();
    for (file_path, row_pos) in locations {
        let batch = table.read_row_at(file_path, row_pos)?;
        results.push(batch);
    }
    
    Ok(results)
}
```

#### 3.1.3 优缺点分析

| 优点                           | 缺点                           |
| ---------------------------- | ---------------------------- |
| ✅ 索引查询性能极高（O(1) 点查）          | ❌ 需要维护外部索引存储，系统复杂度高          |
| ✅ 支持多种索引类型（B-Tree、Hash、全文检索） | ❌ 索引与数据的一致性需要应用层保证           |
| ✅ 与 Rust SDK 解耦，升级风险低        | ❌ 存储成本增加（额外索引 + `_row_id` 列） |
| ✅ 支持增量索引更新                   | ❌ 写入性能有额外开销                  |

---

### 方案二：物理存储 + Manifest 级索引

#### 3.2.1 架构图

```textile
┌─────────────────────────────────────────────────────────────────────────┐
│                           Manifest 级索引                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Manifest File                               │   │
│  ├─────────────────────────────────────────────────────────────────┤   │
│  │  data_file: {                                                 │   │
│  │    file_path: "s3://bucket/data.parquet",                     │   │
│  │    first_row_id: 1000,                                        │   │
│  │    row_count: 500,                                            │   │
│  │    min_row_id: 1000,   ← 新增                                 │   │
│  │    max_row_id: 1499,   ← 新增                                 │   │
│  │    ...                                                        │   │
│  │  }                                                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.2.2 核心实现

扩展 Iceberg 的 Manifest 条目，为每个数据文件记录其包含的 `_row_id` 范围（min/max）。

**实现方式**：

1. 修改 `DataFile` 结构，添加 `min_row_id` 和 `max_row_id` 字段。

2. 写入数据文件后，扫描其中的 `_row_id` 列，获取最小值和最大值。

3. 在生成 Manifest 条目时，将这些值写入。

4. 查询时，通过 Manifest 文件快速过滤掉不包含目标 `_row_id` 的文件。

#### 3.2.3 优缺点分析

| 优点                 | 缺点                                 |
| ------------------ | ---------------------------------- |
| ✅ 与 Iceberg 生态整合紧密 | ❌ 需要扩展 Iceberg 的 Manifest 格式（侵入性强） |
| ✅ 无需外部存储，减少组件依赖    | ❌ 与社区版本可能不兼容                       |
| ✅ 文件级别过滤，减少扫描量     | ❌ 粒度是文件级，无法精确定位行                   |
| ✅ 元数据内置，一致性有保障     | ❌ 需要修改 Rust SDK 核心代码               |

---

### 方案三：物理存储 + Parquet Row Group 索引

#### 3.3.1 原理说明

Parquet 文件本身包含 Row Group 元数据。如果我们将 `_row_id` 作为物理列存储，可以借助 Parquet 的列统计信息（min/max）实现 Row Group 级别的过滤。

#### 3.3.2 核心实现

```rust
// 读取时，利用 Parquet 的 Row Group 统计信息过滤
fn read_with_row_group_pruning(
    file_path: &str,
    target_row_id: i64,
) -> Result<RecordBatch> {
    let parquet_reader = ParquetReader::open(file_path)?;
    let metadata = parquet_reader.metadata();
    
    // 遍历所有 Row Group，根据 _row_id 列的统计信息过滤
    for row_group in metadata.row_groups() {
        let col_meta = row_group.column(column_index_of("_row_id"))?;
        let (min, max) = (col_meta.min(), col_meta.max());
        if target_row_id >= min && target_row_id <= max {
            // 读取该 Row Group
            let batch = parquet_reader.read_row_group(row_group.id())?;
            // 在内存中进一步过滤到精确行
            let filtered = batch.filter(|row| row.get("_row_id") == target_row_id)?;
            return Ok(filtered);
        }
    }
    Ok(RecordBatch::empty())
}
```

#### 3.3.3 优缺点分析

| 优点                       | 缺点                                    |
| ------------------------ | ------------------------------------- |
| ✅ 利用 Parquet 原生能力，无需额外存储 | ❌ 仅支持 Row Group 级别过滤，仍需读取整个 Row Group |
| ✅ 实现相对简单                 | ❌ 如果 Row Group 很大，过滤效果有限              |
| ✅ 与 Parquet 生态兼容性好       | ❌ 需要确保 `_row_id` 列有统计信息               |

---

### 方案四：物理存储 + Parquet Footer 内嵌索引

#### 3.4.1 原理说明

在 Parquet 文件的 Footer 中写入自定义元数据（key-value），存储 `(_row_id → row_position)` 的映射索引。

#### 3.4.2 核心实现

```rust
// 写入时：构建索引并写入 Footer
fn write_with_footer_index(
    writer: &mut ParquetWriter,
    batch: RecordBatch,
    first_row_id: i64,
) -> Result<()> {
    // 1. 正常写入数据
    writer.write_batch(batch)?;
    
    // 2. 构建索引
    let mut index = HashMap::new();
    for (i, row) in batch.rows().enumerate() {
        let row_id = first_row_id + i as i64;
        index.insert(row_id, i as u64);
    }
    
    // 3. 序列化索引并写入 Footer
    let serialized = serde_json::to_string(&index)?;
    writer.add_key_value_metadata("row_id_index", serialized)?;
    
    Ok(())
}

// 读取时：从 Footer 加载索引
fn read_with_footer_index(file_path: &str, target_row_id: i64) -> Result<RecordBatch> {
    let reader = ParquetReader::open(file_path)?;
    let metadata = reader.metadata();
    
    // 从 Footer 读取索引
    let index_str = metadata.key_value_metadata()
        .get("row_id_index")
        .ok_or("Index not found")?;
    let index: HashMap<i64, u64> = serde_json::from_str(index_str)?;
    
    // 查找行位置
    let row_pos = index.get(&target_row_id)
        .ok_or("Row not found")?;
    
    // 读取指定行
    let batch = reader.read_row_at(*row_pos)?;
    Ok(batch)
}
```

#### 3.4.3 优缺点分析

| 优点                | 缺点                  |
| ----------------- | ------------------- |
| ✅ 数据和索引一体化，一致性有保障 | ❌ 索引随文件一起加载，无法跨文件查询 |
| ✅ 无外部依赖           | ❌ 索引过大时会影响文件读取效率    |
| ✅ 实现相对简单          | ❌ 不适合跨文件的全局索引查询     |

---

### 方案五：动态计算 + 外部索引

#### 3.5.1 原理说明

与方案一类似，但**不在写入时物理存储 `_row_id`**，而是在读取时动态计算。

#### 3.5.2 核心实现

```rust
// 读取时动态计算 _row_id 并构建索引
fn build_index_dynamically(
    table: &Table,
    index_store: &mut IndexStore,
) -> Result<()> {
    // 1. 扫描所有数据文件
    let scan = table.scan().build()?;
    let tasks = scan.plan_files()?;
    
    for task in tasks {
        let first_row_id = task.file().first_row_id();
        let reader = task.open()?;
        
        // 2. 逐行读取，动态计算 _row_id
        for (row_position, row) in reader.enumerate() {
            let row_id = first_row_id + row_position as i64;
            // 3. 构建索引
            index_store.put(row_id, (task.file().path(), row_position))?;
        }
    }
    Ok(())
}
```

#### 3.5.3 优缺点分析

| 优点                     | 缺点                   |
| ---------------------- | -------------------- |
| ✅ 写入侧无需改造，无额外存储        | ❌ 索引构建需要全表扫描，成本高     |
| ✅ 利用 Rust SDK 已有的元数据能力 | ❌ 动态计算有 CPU 开销       |
| ✅ 对现有写入流程无影响           | ❌ 索引构建与数据写入异步，一致性难保证 |

---

### 方案六：纯元数据剪枝（无索引）

#### 3.6.1 原理说明

不构建任何索引，完全依赖 Iceberg 的元数据进行文件级别过滤。

#### 3.6.2 核心实现

利用快照的 `first_row_id` + `added_rows` 范围，快速定位目标 `_row_id` 所属的快照，然后只扫描该快照下的 Manifest 文件。

```rust
fn prune_by_snapshot_metadata(
    table: &Table,
    target_row_id: i64,
) -> Result<Vec<DataFile>> {
    let snapshots = table.snapshots()?;
    let mut candidate_files = Vec::new();
    
    for snapshot in snapshots {
        let first = snapshot.first_row_id();
        let count = snapshot.added_rows();
        if target_row_id >= first && target_row_id < first + count {
            // 该快照可能包含目标行
            let manifest = table.read_manifest(snapshot.manifest_list())?;
            for entry in manifest.entries() {
                // 进一步用文件级 first_row_id 过滤
                let file_first = entry.first_row_id();
                let file_count = entry.row_count();
                if target_row_id >= file_first && target_row_id < file_first + file_count {
                    candidate_files.push(entry.data_file());
                }
            }
        }
    }
    Ok(candidate_files)
}
```

#### 3.6.3 优缺点分析

| 优点                | 缺点               |
| ----------------- | ---------------- |
| ✅ 无需任何额外存储或改造     | ❌ 定位不精准，可能扫描多个文件 |
| ✅ 利用 Iceberg 原生能力 | ❌ 点查性能无法保证       |
| ✅ 实现最简单           | ❌ 不适合高频点查场景      |

---

## 四、方案对比矩阵

| 方案                      | 实现复杂度 | 查询性能  | 写入开销 | 存储成本 | 一致性   | 侵入性 | 推荐度   |
| ----------------------- | ----- | ----- | ---- | ---- | ----- | --- | ----- |
| **一、物理+外部索引**           | 中     | ⭐⭐⭐⭐⭐ | 中    | 高    | 需应用保证 | 低   | ⭐⭐⭐⭐⭐ |
| **二、Manifest 级索引**      | 高     | ⭐⭐⭐   | 低    | 低    | 好     | 高   | ⭐⭐    |
| **三、Parquet Row Group** | 低     | ⭐⭐⭐   | 低    | 低    | 好     | 低   | ⭐⭐⭐   |
| **四、Parquet Footer 索引** | 中     | ⭐⭐⭐⭐  | 中    | 中    | 好     | 中   | ⭐⭐⭐   |
| **五、动态计算+外部索引**         | 中     | ⭐⭐⭐⭐  | 低    | 高    | 差     | 低   | ⭐⭐    |
| **六、纯元数据剪枝**            | 低     | ⭐⭐    | 无    | 无    | 好     | 无   | ⭐⭐    |

---

## 五、方案选型建议

### 5.1 根据场景选择

| 场景                | 推荐方案      | 理由                     |
| ----------------- | --------- | ---------------------- |
| **高频点查，对性能要求极高**  | 方案一       | 外部索引查询性能最佳，可实现 O(1) 点查 |
| **不想引入额外组件**      | 方案三 或 方案四 | 利用 Parquet 原生能力，无需外部存储 |
| **数据一致性是首要目标**    | 方案二 或 方案四 | 索引随数据原子写入，一致性好         |
| **快速验证，不想大改**     | 方案六       | 纯元数据剪枝，无需任何改造          |
| **查询模式多样（点查+范围）** | 方案一       | 外部索引可支持多种索引类型          |

### 5.2 综合推荐

**首选方案：方案一（物理存储 + 外部索引）**

- 性能最优，扩展性最强

- 与 Rust SDK 解耦，升级风险低

- 支持增量索引更新，适合大数据场景

**备选方案：方案三（物理存储 + Parquet Row Group 索引）**

- 实现最简单，适合轻量级场景

- 无需额外存储，利用 Parquet 原生能力

### 5.3 实施路线图

```textile
阶段 1：元数据准备
├── 确认表格式为 V3
├── 验证 Rust SDK 能读取 first_row_id
└── 评估现有数据量和增长速度

阶段 2：写入侧改造
├── 扩展 ParquetWriter，写入 _row_id 列
├── 确保 _row_id 作为普通列存储（避免保留 ID）
└── 维护全局 next_row_id（表元数据）

阶段 3：索引构建
├── 选择外部索引存储（RocksDB/Sled）
├── 实现索引写入逻辑（同步/异步）
└── 实现索引查询逻辑

阶段 4：查询回表
├── 实现 _row_id → 物理位置的映射
├── 实现精确行读取
└── 性能测试与优化
```

---

## 六、注意事项与风险

### 6.1 `_row_id` 稳定性

- **风险**：数据重写（Compaction）后，`_row_id` 必须保持不变。

- **应对**：写入侧必须物理存储 `_row_id`，并在重写时保留该列的值。

### 6.2 索引与数据一致性

- **风险**：外部索引与数据写入不是原子操作。

- **应对**：
  
  - 采用 WAL 机制，先写索引再提交数据。
  
  - 或采用异步索引构建，容忍短暂的索引不一致。

### 6.3 元数据膨胀

- **风险**：Manifest 级索引或 Footer 索引可能导致元数据过大。

- **应对**：定期进行元数据清理和优化。

### 6.4 社区兼容性

- **风险**：自实现方案可能与未来社区版本不兼容。

- **应对**：将自实现逻辑与 SDK 核心解耦，便于后续迁移。

---

## 七、附录

### 7.1 Rust SDK 关键 API 参考

```rust
// 获取数据文件的 first_row_id
let first_row_id = data_file.first_row_id();

// 获取快照的 first_row_id
let first_row_id = snapshot.first_row_id();

// 读取 Manifest 文件
let manifest = table.read_manifest(manifest_list_path)?;

// 扫描表
let scan = table.scan().build()?;
let tasks = scan.plan_files()?;
```

### 7.2 相关社区 Issue

- [iceberg-rust #2153](https://github.com/apache/iceberg-rust/pull/2153) - Incremental scan support

- [iceberg-rust #2150](https://github.com/apache/iceberg-rust/pull/2150) - V3 metadata support

### 7.3 参考文献

1. [Apache Iceberg V3 Spec - Row Lineage](https://iceberg.apache.org/spec/#row-lineage)

2. [Iceberg Rust SDK Documentation](https://docs.rs/iceberg/latest/iceberg/)

3. [Parquet Row Group Indexing](https://parquet.apache.org/docs/file-format/metadata/#row-group-metadata)


