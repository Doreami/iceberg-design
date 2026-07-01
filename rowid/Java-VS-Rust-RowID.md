# Java vs Rust Iceberg SDK — RowID 能力对比

基于 Apache Iceberg Row Lineage (RowID) API，对比 Java SDK 1.11.0 与 Rust SDK 0.9.1 的实现差异。

> 示例标记：🔴 RowID 直接相关　🟡 基础设施（前置条件）　⚪ Iceberg 通用功能

## 一、运行环境

|         | Java                 | Rust                     |
| ------- | -------------------- | ------------------------ |
| SDK 版本  | iceberg 1.11.0       | iceberg 0.9.1            |
| 语言版本    | JDK 21               | Rust 1.96 (edition 2024) |
| 运行方式    | `mvn exec:java`      | `cargo run`              |
| Catalog | HadoopCatalog (本地文件) | MemoryCatalog (本地文件)     |

## 二、13 个示例实现对比

### 示例 1: 创建 V3 表 🟡 基础设施

|     | Java                                                      | Rust                                                  |
| --- | --------------------------------------------------------- | ----------------------------------------------------- |
| API | `buildTable().withProperty(FORMAT_VERSION, "3").create()` | `TableCreation::builder().format_version(V3).build()` |
| 步骤  | 1 步                                                       | 2 步 (先建 namespace, 再建表)                               |
| 状态  | ✅                                                         | ✅                                                     |

**Java:**
```java
Map<String, String> props = new HashMap<>();
props.put(TableProperties.FORMAT_VERSION, "3");
Table table = catalog.buildTable(tableId, schema)
    .withPartitionSpec(spec)
    .withProperties(props)
    .create();
```

**Rust:**
```rust
let ns = NamespaceIdent::new("mydb".to_string());
catalog.create_namespace(&ns, HashMap::new()).await?;
let creation = TableCreation::builder()
    .name("table".to_string())
    .schema(schema)
    .format_version(FormatVersion::V3)
    .build();
catalog.create_table(&ns, creation).await?;
```

> **SDK 自动维护的 RowID 字段**: `next_row_id`（表级）、快照 `first_row_id`/`added_rows`、文件 `first_row_id`（manifest entry）均由 SDK V3 commit 自动计算和持久化，无需手动干预。用户只需正常 `fast_append().commit()`，SDK 自动处理行号递增和分配。

---

### 示例 2: 遍历所有快照 firstRowId 🔴 RowID

|      | Java                 | Rust                                 |
| ---- | -------------------- | ------------------------------------ |
| API  | `table.snapshots()`  | `table.metadata().snapshots()`       |
| 返回类型 | `Iterable<Snapshot>` | `impl Iterator<Item = &SnapshotRef>` |
| 步骤   | 1 步                  | 1 步 (多一层 metadata())                 |
| 状态   | ✅                    | ✅                                    |

**Java:**
```java
for (Snapshot snap : table.snapshots()) {
    System.out.printf("id=%d, firstRowId=%s, addedRows=%s%n",
        snap.snapshotId(), snap.firstRowId(), snap.addedRows());
}
```

**Rust:**
```rust
for snap in table.metadata().snapshots() {
    println!("id={}, firstRowId={:?}, addedRows={:?}",
        snap.snapshot_id(), snap.first_row_id(), snap.added_rows_count());
}
```

---

### 示例 3: 当前快照 firstRowId 🔴 RowID

|     | Java                                            | Rust                                                          |
| --- | ----------------------------------------------- | ------------------------------------------------------------- |
| API | `table.currentSnapshot()` → `snapshot.firstRowId()` | `table.metadata().current_snapshot()` → `snap.first_row_id()` |
| 步骤  | 1 步 (链式调用)                                      | 2 步 (metadata() + Option 解包)                                  |
| 状态  | ✅                                               | ✅                                                             |

**Java:**
```java
Snapshot snap = table.currentSnapshot();
System.out.printf("firstRowId=%s, addedRows=%s, range=[%s, %s]%n",
    snap.firstRowId(), snap.addedRows(),
    snap.firstRowId(),
    snap.firstRowId() + snap.addedRows() - 1);
```

**Rust:**
```rust
if let Some(snap) = table.metadata().current_snapshot() {
    if let (Some(first), Some(added)) = (snap.first_row_id(), snap.added_rows_count()) {
        println!("firstRowId={}, addedRows={}, range=[{}, {}]",
            first, added, first, first + added - 1);
    }
}
```

---

### 示例 4: 数据文件级 firstRowId 🔴 RowID

|     | Java                    | Rust                                                    |
| --- | ----------------------- | ------------------------------------------------------- |
| API | `DataFile.firstRowId()` | `FileScanTask` 未暴露，需从 `ManifestEntry.first_row_id` 获取 |
| 步骤  | 1 步                     | 2-3 步 (scan plan_files → 加载 manifest → 读 entry)           |
| 状态  | ✅                       | ⚠️ 需绕行                                                  |

**Java:**
```java
for (FileScanTask task : table.newScan().planFiles()) {
    System.out.println(task.file().firstRowId());
}
```

**Rust (绕行方案 — 通过 manifest entry 读取):**
```rust
// 方案: 加载 snapshot 的 manifest list, 遍历 manifest entry 读取 first_row_id
let snap = table.metadata().current_snapshot().unwrap();
let manifest_list = snap.load_manifest_list(
    table.file_io(), table.metadata()
).await?;

for entry in manifest_list.entries() {
    // ManifestEntry 包含 first_row_id 字段
    println!("file={}, firstRowId={:?}",
        entry.data_file().file_path(),
        entry.data_file().first_row_id()); // DataFile.first_row_id() 是 pub 方法
}
```

---

### 示例 5: 读取 _row_id 元数据列 🔴 RowID

|     | Java                                           | Rust                                                        |
| --- | ---------------------------------------------- | ----------------------------------------------------------- |
| API | `MetadataColumns.schemaWithRowLineage(schema)` | `table.scan().select(["id", "_row_id"]).build()`          |
| 步骤  | 1 步（自动追加 `_row_id` + `_last_updated_sequence_number`） | 手动指定列名，Rust scan 原生支持 metadata column 解析                |
| 实测  | ✅ V3 writer 写入, reader 可读                         | ❌ 已确认 Parquet 中无此列 (`iceberg::writer` 不写, reader 不算)   |
| 状态  | ✅                                              | ❌ (Rust 0.9.1 数据层完全不支持)                                 |

**Java:**
```java
Schema s = MetadataColumns.schemaWithRowLineage(table.schema());
try (CloseableIterable<Record> result = IcebergGenerics.read(table)
        .project(s).build()) {
    for (Record rec : result) {
        System.out.println(rec.getField("_row_id"));
    }
}
```

**Rust (需绕过 iceberg::writer):**
```rust
// iceberg::writer 只写 schema 中的列, _row_id 会被丢弃!
// 必须用 ArrowWriter 直写 Parquet:
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
const ROW_ID_FID: i32 = i32::MAX - 107;

let next = table.metadata().next_row_id() as i64;
let row_ids: Vec<i64> = (next..next + n).collect();
let batch = RecordBatch::try_new(..., vec![
    ...,  // 业务列
    Arc::new(Int64Array::from(row_ids)),  // _row_id 手动计算
]).unwrap();
let mut buf = Vec::new();
ArrowWriter::try_new(&mut buf, arrow_schema, Some(props)).unwrap()
    .write(&batch).unwrap();
// 再通过 Transaction::fast_append 提交
```
> **实测验证** (Rust 0.9.1):
> - `_file`：✅ reader 计算填充的虚拟列，始终可用。
> - `_row_id`：❌ `iceberg::writer` **不会**嵌入 `_row_id`（不像 Java V3 writer 自动嵌入）。
> - 即使手动在 Arrow batch 中加 `_row_id` 列 + `PARQUET_FIELD_ID_META_KEY`，**仍被 `iceberg::writer` 丢弃**（只写 Iceberg schema 中声明的列）。
> - **Workaround**: 绕过 `iceberg::writer`，用 `ArrowWriter` 直写 Parquet + 手动算值（从 `next_row_id()` 取起始值）+ 手动加 `PARQUET_FIELD_ID_META_KEY` 元数据。
>
> **V3 Spec 约定与 Rust 0.9.1 实现差距**:
> - 按 Iceberg V3 规范，`_row_id` 是**元数据列**（类似 `_file`），理论上 reader 应自动计算：`_row_id = DataFile.firstRowId() + 文件内行位置`。
> - `_file` 能读是因为 Rust reader **实现了**这个计算（取当前文件路径）。
> - `_row_id` 不能读是因为 Rust reader **未实现** `firstRowId + position` 的计算逻辑。
> - Java V3 writer "自动写入 `_row_id` 到 Parquet" 实际是一种**优化/前置计算**，把 reader 的活提前干了。
> - **Rust SDK 0.9.1 的状态**：writer 不写，reader 也不算——两边都缺。补其中一边即可：要么 writer 写入（Java 做法），要么 reader 计算（Spec 本意）。
> - **我们的 Workaround 走的是 writer 侧（Java 做法）**：reader 的 `_file`/`_row_id` 计算在 SDK 内部 `record_batch_transformer` 中，外部无法扩展。writer 侧可以用 `ArrowWriter` 完全控制 Parquet 输出内容，注入 `_row_id` 列。所以只能走这边。
>
> **Rust 0.9.1 元数据列实测结果**:
> | 列 | `select` 可用? | 说明 |
> |---|---|---|
> | `_file` | ✅ | reader 自动填充文件路径 |
> | `_pos` | ❌ | 同 `_row_id`，reader 未计算 |
> | `_row_id` | ❌ | writer 不写，reader 不算 |
> | `_last_updated_sequence_number` | ❌ | 同上 |
> | `_spec_id` | ⚠️ | 未测 |
> | `_deleted` | ⚠️ | 未测 |
>
> **元数据列的通用限制**（Java / Rust / Spark 均适用）：
> | 操作 | 表字段 | 元数据列 (`_row_id`, `_file`, `_pos`...) |
> |------|:------:|:----------------------------------------:|
> | `project` / `select` | ✅ | ✅ 可投影到输出 |
> | `Expressions` / `filter` | ✅ | ❌ 无法过滤（不在 table schema 中） |
>
> 元数据列只在投影层存在，谓词下推不认它们。因此 `WHERE _row_id = 0` 无论 Java 还是 Rust 都只能走应用层过滤。
>
> **已知的成熟替代方案**:
> | 方案 | 做法 | 使用方 |
> |------|------|--------|
> | 物理标识 `(file, pos)` | `pack_row_id = (file_index << 32) \| row_position`，不用全局 `_row_id` | iceberg-index 项目 |
> | Writer 侧注入 | `ArrowWriter` 直写 + 手动算 `_row_id` + `PARQUET_FIELD_ID_META_KEY` | 本文 Workaround |
> | `_file` + 应用层算 | `_file` 定位文件 → manifest `first_row_id` → 应用层 `+ 行号` | 间接但可行 |
> | 等 SDK 更新 | reader 补上 `_row_id` 计算（同 `_file` 机制），或 writer 自动嵌入 | 暂无 PR |

---

### 示例 6: 精确查询 + _row_id 🔴 RowID

|     | Java                                                        | Rust                                                        |
| --- | ----------------------------------------------------------- | ----------------------------------------------------------- |
| API | `IcebergGenerics.read().where(Expressions.in("id", ids)).project(schemaWithLineage)` | `table.scan().select([...]).build()?.to_arrow()`，应用层过滤 |
| 步骤  | 1 步 (过滤 + 投影一体)                                                | 2 步 (scan 返回全量，Arrow 层手动过滤)                                  |
| 状态  | ✅                                                           | ⚠️ 需 Arrow 层过滤 (0.9.1 scan 无 filter API)                     |

**Java:**
```java
Schema s = MetadataColumns.schemaWithRowLineage(table.schema());
try (CloseableIterable<Record> result = IcebergGenerics.read(table)
        .where(Expressions.in("id", List.of(1L, 50L, 100L)))
        .project(s).build()) {
    for (Record rec : result) {
        System.out.printf("id=%s, _row_id=%s, _last_updated_sequence_number=%s%n",
            rec.getField("id"), rec.getField("_row_id"),
            rec.getField("_last_updated_sequence_number"));
    }
}
```

**Rust:**
```rust
// 0.9.1 的 scan builder 只有 select(), 无 filter()
// 需要 scan 后 Arrow 层手动过滤
let stream = table.scan()
    .select(["id", "name", "_row_id", "_last_updated_sequence_number"])
    .build()?
    .to_arrow().await?;
let batches: Vec<_> = stream.try_collect().await?;
// 应用层过滤: 遍历 RecordBatch, 过滤 id in [1, 50, 100]
let target_ids: HashSet<i64> = vec![1, 50, 100].into_iter().collect();
for batch in batches {
    let id_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    for row in 0..batch.num_rows() {
        if target_ids.contains(&id_col.value(row)) {
            println!("id={}, row={}", id_col.value(row), row);
        }
    }
}
```

---

### 示例 6+: 直接用 _row_id 值查数据 🔴 RowID

|     | Java                                            | Rust |
| --- | ----------------------------------------------- | ---- |
| API | `Expressions.in("_row_id", rowIds)`             | ❌    |
| 前提 | V3 writer 把 `_row_id` 写入了 Parquet                   | Parquet 中无此列 |
| 状态 | ✅ (数据在, 但 `Expressions` 不认元数据列, 需应用层过滤)           | ❌    |

**Java:**
```java
// _row_id 是元数据列, Expressions 无法直接过滤, 需应用层过滤
var targetIds = Set.of(0L, 49L, 99L);
for (Record rec : IcebergGenerics.read(table)
        .project(MetadataColumns.schemaWithRowLineage(table.schema()))
        .build()) {
    Long rid = (Long) rec.getField("_row_id");
    if (targetIds.contains(rid)) {
        // 命中: _row_id=0 → id=1, _row_id=49 → id=50, _row_id=99 → id=100
    }
}
```

**Rust**: 不可用——`_row_id` 不在 Parquet 文件中。

---

### 示例 7: 增量扫描 (两个快照之间) ⚪ 通用

|     | Java                                                              | Rust                 |
| --- | ----------------------------------------------------------------- | -------------------- |
| API | `newIncrementalAppendScan().fromSnapshotExclusive().toSnapshot()` | ❌ 无原生 API            |
| 步骤  | 1 步                                                               | 需手动过滤 manifest entry |
| 状态  | ✅                                                                 | ❌ (社区 PR #2153 开发中)  |

**Java:**
```java
TableScan scan = table.newIncrementalAppendScan()
    .fromSnapshotExclusive(fromId)
    .toSnapshot(toId)
    .build();
for (FileScanTask task : scan.planFiles()) {
    DataFile df = task.file();
    // df 只包含两个快照之间新增的文件
}
```

**Rust (手动实现):**
```rust
// 社区 PR #2153 已实现 from_snapshot_exclusive/to_snapshot,
// 预计在 0.10.0 或更高版本发布。
// 当前版本需手动实现：
let scan = table.scan().snapshot_id(to).build()?;
let tasks: Vec<_> = scan.plan_files().await?.try_collect().await?;
// 然后加载 manifest entry, 过滤 status==ADDED && snapshot_id in (from, to]
```

**社区状态**: [PR #2153](https://github.com/apache/iceberg-rust/pull/2153) 已合并到 main 分支，提供 `from_snapshot_exclusive/inclusive`、`to_snapshot`、`appends_after` 等 API，预计在 0.10.0 发布。

---

### 示例 8: CDC 持续增量扫描 ⚪ 通用

|     | Java                                               | Rust              |
| --- | -------------------------------------------------- | ----------------- |
| API | `newIncrementalAppendScan().fromSnapshotExclusive(checkpoint)` | ❌ 同示例 7，依赖增量扫描 |
| 步骤  | 1 步                                                | 同示例 7 绕行方案        |
| 状态  | ✅                                                  | ❌ (PR #2153 开发中)  |

**Java:**
```java
// CDC 管道, 从上次检查点之后读取所有新增数据
long checkpoint = lastProcessedSnapshotId;
TableScan cdcScan = table.newIncrementalAppendScan()
    .fromSnapshotExclusive(checkpoint)
    .build();
// 循环轮询: 每次拿到新快照后更新 checkpoint
```

**Rust:**
```rust
// 同示例 7，手动实现后包一层循环
// 0.10.0 起可使用 appends_after(checkpoint).build()
```

---

### 示例 9: 追加后 Row-ID 分布 🔴 RowID

|     | Java                                                                            | Rust                                                           |
| --- | ------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| API | `table.newScan().planFiles()` + `DataFile.firstRowId()`                       | `table.scan().build()?.plan_files()` + `DataFile.first_row_id()` |
| 步骤  | 1 步                                                                             | 2 步 (见示例 4 绕行方案)                                                 |
| 状态  | ✅                                                                               | ⚠️ 需绕行 (同示例 4)                                                  |

**Java:**
```java
for (FileScanTask task : table.newScan().planFiles()) {
    DataFile df = task.file();
    System.out.printf("file=%s, rows=%d, firstRowId=%s%n",
        df.path(), df.recordCount(), df.firstRowId());
}
// 输出: file1 → firstRowId=0 (100 rows), file2 → firstRowId=100 (10 rows)
// → row-id 跨快照全局递增
```

**Rust:**
```rust
let tasks: Vec<_> = table.scan().build()?.plan_files().await?.try_collect().await?;
// ⚠️ FileScanTask 无 first_row_id, 需用示例 4 的绕行方案
for task in tasks {
    println!("file={}, rows={:?}", task.data_file_path(), task.record_count);
}
// 绕行: 通过 manifest entry 获取每个文件的 first_row_id
```

---

### 示例 10: Changelog 变更扫描 ⚪ 通用

|     | Java                            | Rust      |
| --- | ------------------------------- | --------- |
| API | `newIncrementalChangelogScan()` | ❌ SDK 不支持 |
| 状态  | ✅                               | ❌         |

**社区状态**: Rust SDK 目前无 Changelog Scan 相关 issue/PR。Java SDK 原生支持 `IncrementalChangelogScan`（返回 `ChangelogScanTask`，包含 `operation=INSERT/DELETE`、`changeOrdinal()`、`commitSnapshotId()`）。Rust 侧需等待社区规划。

**手动实现思路**: 加载两个快照的 manifest，对比文件差异。新增文件 = INSERT，被替换的文件 = DELETE。

---

### 示例 11: 快照祖先链 🔴 RowID

|     | Java                  | Rust                                          |
| --- | --------------------- | --------------------------------------------- |
| API | `snapshot.parentId()` | `snap.parent_snapshot_id()` + `snapshot_by_id()` |
| 步骤  | 1 步 (链式)              | 2 步 (parent 返回 Option<i64>，需手动查找)               |
| 状态  | ✅                     | ✅                                             |

**Java:**
```java
Long pid = snapshot.parentId();
if (pid != null) {
    Snapshot parent = table.snapshot(pid);
}
```

**Rust:**
```rust
if let Some(pid) = snap.parent_snapshot_id() {
    let parent = table.metadata().snapshot_by_id(pid);
}
```

---

### 示例 12: Overwrite + Row ID 重新分配 ⚪ 通用

|     | Java                                                       | Rust      |
| --- | ---------------------------------------------------------- | --------- |
| API | `newOverwrite().overwriteByRowFilter().addFile().commit()` | ❌ SDK 不支持 |
| 状态  | ✅                                                          | ❌         |

**社区状态**: Rust SDK 目前无 Overwrite API。对应 Java 的 `OverwriteFiles` 接口（支持 `overwriteByRowFilter` 表达式匹配）。Rust transaction 模块有 `fast_append`、`update_table_properties` 等基础操作，但无 `overwrite` 语义。

**手动实现思路**: 通过 `fast_append` + 手动标记旧文件删除逻辑模拟。

---

### 示例 13: _last_updated_sequence_number 🔴 RowID

|     | Java                                                                     | Rust                                                               |
| --- | ------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| API | `MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER`                           | `metadata_columns::RESERVED_FIELD_ID_LAST_UPDATED_SEQUENCE_NUMBER` |
| 读取  | `schemaWithRowLineage()` 自动追加                                           | 同示例 5: `select(["id", "_last_updated_sequence_number"])`           |
| 状态  | ✅                                                                        | ✅ 常量 + 读取均已支持                                                      |

**Java:**
```java
Schema s = MetadataColumns.schemaWithRowLineage(table.schema());
// s 自动包含 _row_id + _last_updated_sequence_number
// 覆盖后: 被覆盖行 sequence_number 递增到新快照的 sequence number
// 未覆盖行: sequence_number 保持不变
```

**Rust:**
```rust
let stream = table.scan()
    .select(["id", "_row_id", "_last_updated_sequence_number"])
    .build()?
    .to_arrow().await?;
// 从 Arrow RecordBatch 读取两列元数据
// _last_updated_sequence_number 行为与 Java 一致:
//   覆盖后递增到新快照 sequence number, 未覆盖则不变
```

---

## 三、数据写入对比

| | Java | Rust |
|---|---|---|
| 代码量 | ~5 行 | ~30 行 |
| 写入方式 | `GenericParquetWriter::create` → 按行写 | `iceberg::writer` (Arrow RecordBatch) |
| 提交流程 | `table.newAppend().appendFile().commit()` | `Transaction::fast_append() → apply() → commit()` |
| _row_id 写入 | ✅ V3 writer 自动嵌入 | ❌ `iceberg::writer` **不嵌入, 且手动加列也被丢弃** |
| _row_id workaround | 不需要 | 绕过 iceberg::writer → `ArrowWriter` 直写 → 手动算值 + `PARQUET_FIELD_ID_META_KEY` |
| field ID | ✅ 自动处理 | ⚠️ 需在 Arrow schema 中手动加 `PARQUET_FIELD_ID_META_KEY` |

**Java:**
```java
DataWriter<Record> writer = Parquet.writeData(outputFile)
    .schema(schema).createWriterFunc(GenericParquetWriter::create).build();
for (Record r : records) writer.write(r);
writer.close();
table.newAppend().appendFile(writer.toDataFile()).commit();
```

**Rust:**
```rust
// 1. 构造 Arrow RecordBatch
let batch = RecordBatch::try_new(schema, vec![col_id, col_name, col_score]).unwrap();
// 2. 写 Parquet 到 Vec<u8>
let mut buf = Vec::new();
let mut writer = ArrowWriter::try_new(&mut buf, arrow_schema, Some(props)).unwrap();
writer.write(&batch).unwrap();
writer.close().unwrap();
// 3. 通过 FileIO 写入文件系统
let output = file_io.new_output(&path)?;
output.write(Bytes::from(buf)).await?;
// 4. 构造 DataFile + fast_append + commit
let data_file = DataFileBuilder::default()
    .content(DataContentType::Data).file_path(path)
    .file_format(DataFileFormat::Parquet).record_count(count)
    .partition_spec_id(spec_id).build().unwrap();
let tx = Transaction::new(table);
let action = tx.fast_append().add_data_files(vec![data_file]);
let tx = action.apply(tx).unwrap();
tx.commit(&catalog).await.unwrap();
```

## 四、汇总

| 示例 | 功能 | Java 1.11.0 | Rust 0.9.1 | 差距 |
|:----:|------|:-----------:|:----------:|------|
| 1 | V3 建表 | ✅ | ✅ | 多一步建 namespace |
| 2 | 快照遍历 firstRowId | ✅ | ✅ | 多一层 metadata() |
| 3 | 当前快照 firstRowId | ✅ | ✅ | Option 解包 |
| 4 | 文件级 firstRowId | ✅ | ⚠️ | 需 manifest entry 绕行 |
| 5 | 读取 _row_id 列 | ✅ | ❌ | writer不写 reader不算 |
| 6 | 精确查询 + _row_id | ✅ | ⚠️ | 按id过滤✅, _row_id 不可用 |
| 6+ | 直接用 _row_id 查 | ✅ | ❌ | Java数据在但需应用层过滤, Rust无_row_id列 |
| 7 | 增量扫描 | ✅ | ❌→0.10.0 | PR #2153 已合并 |
| 8 | CDC 持续增量 | ✅ | ❌→0.10.0 | 同示例 7 |
| 9 | Row-ID 分布 | ✅ | ⚠️ | 同示例 4 绕行 |
| 10 | Changelog 扫描 | ✅ | ❌ | 无社区计划 |
| 11 | 快照祖先链 | ✅ | ✅ | 多一步 snapshot_by_id |
| 12 | Overwrite | ✅ | ❌ | 无社区计划 |
| 13 | _last_updated_sequence_number | ✅ | ❌ | 同示例 5, writer不写 reader不算 |

## 五、社区演进计划

| 功能 | 状态 | 社区跟踪 |
|------|------|---------|
| 增量扫描 (from_snapshot_exclusive) | ✅ 已合并 main | [PR #2153](https://github.com/apache/iceberg-rust/pull/2153) → 预计 0.10.0 |
| Changelog 扫描 | ❌ 暂无计划 | 无相关 issue/PR |
| Overwrite / RowDelta | ❌ 暂无计划 | 无相关 issue/PR |
| schemaWithRowLineage 便捷方法 | ❌ 暂无计划 | 可通过 select 手动实现 |
| DataFile.firstRowId() 暴露到 FileScanTask | ❌ 暂无计划 | 需从 manifest entry 绕行 |

## 六、关键结论

1. **RowID 核心查询** (firstRowId, addedRows, 快照遍历, 祖先链) — Rust 0.9.1 已完整支持
2. **`_row_id` 基础原理** — V3 Spec 约定 `_row_id` 是元数据列，应由 reader 计算 = `DataFile.firstRowId() + 行位置`（同 `_file` 机制）。Java 的做法是 writer 提前写入（优化）；Rust 的现状是 **writer 不写，reader 也不算**，两边都缺
3. **`_row_id` Workaround** — 绕过 `iceberg::writer`，`ArrowWriter` 直写 Parquet + 手动算值（`next_row_id()`）+ `PARQUET_FIELD_ID_META_KEY`
4. **增量扫描** — Java 原生，Rust 需等 0.10.0 (PR #2153 已合并)
5. **数据写入** — Rust 比 Java 繁琐约 6 倍（Arrow 底层 API vs GenericRecord 高层封装）
6. **Changelog / Overwrite** — Rust 暂无计划，需手动实现
