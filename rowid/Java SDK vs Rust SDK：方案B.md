# Java SDK vs Rust SDK：方案B（动态计算 + `_row_id` 中间层）实现对比

## 1. 方案B概述

方案B是一种基于 **`_row_id` 作为逻辑指针** 的二级索引方案，旨在加速对 Iceberg 表的点查（如 `WHERE name = 'Alice'`）。其核心结构为两层索引：

- **第一层（倒排索引）**：`业务字段值 → _row_id 列表`  
  例如：`"Alice" → [1001, 1005, 1023]`

- **第二层（位置映射）**：`_row_id → (file_path, row_position)`  
  例如：`1001 → ("s3://bucket/part-0000.parquet", 42)`

**查询流程**：

1. 根据业务条件（如 `name='Alice'`）查找第一层索引，得到一组 `_row_id`。

2. 对于每个 `_row_id`，通过第二层索引获取物理位置 `(file_path, row_position)`。

3. 直接读取对应文件中的指定行，返回数据。

该方案的核心挑战在于：

- 如何获取 `_row_id`（因为 Iceberg V3 中 `_row_id` 属于元数据列，可能未物理存储）。

- 如何高效构建和维护两层索引，尤其是在数据变更（INSERT/UPDATE/DELETE/Compaction）时。

---

## 2. Java SDK 实现方案B

### 2.1 获取 `_row_id`

Java SDK 对 Iceberg V3 表格式的支持非常成熟。在读取数据文件时，可以直接获取每行的 `_row_id`：

```java
for (FileScanTask task : tasks) {
    Long firstRowId = task.file().firstRowId();   // 获取文件起始 row_id
    String filePath = task.file().path().toString();
    int position = 0;
    try (CloseableIterable<Record> records = task.open()) {
        for (Record record : records) {
            long rowId = firstRowId + position;    // 计算行 row_id
            // 此时 rowId 即为该行的 _row_id
            position++;
        }
    }
}
```

- **`DataFile.firstRowId()`** 返回该数据文件的起始 `_row_id`。

- 每行的 `_row_id` 通过 `firstRowId + 行内偏移` 计算得出，无需读取数据内容。

- Java SDK 在读取时已自动处理该逻辑，开发者无需关心底层细节。

### 2.2 增量更新：利用 `IncrementalDataTableScan`

Java SDK 提供了原生的增量扫描 API，可以高效获取两个快照之间新增的数据文件：

```java
TableScan incrementalScan = table
    .newIncrementalDataTableScan()
    .fromSnapshotExclusive(lastSnapshotId)   // 不包含起始快照
    .toSnapshot(currentSnapshotId)           // 包含结束快照
    .build();
```

- 该扫描仅返回在指定快照范围内状态为 `ADDED` 的数据文件，即**新增或重写**的文件。

- 利用这一能力，索引构建任务可以定期执行，只处理变更的文件，从而大幅减少扫描量。

### 2.3 索引构建完整流程（Java）

```java
public class JavaIndexBuilder {
    public void buildFullIndex(Table table, ExternalIndexStore store) {
        TableScan scan = table.newScan().build();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                processFile(task, store);
            }
        }
    }

    public void buildIncrementalIndex(Table table, ExternalIndexStore store,
                                      long fromSnapshotId, long toSnapshotId) {
        TableScan scan = table.newIncrementalDataTableScan()
                .fromSnapshotExclusive(fromSnapshotId)
                .toSnapshot(toSnapshotId)
                .build();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                processFile(task, store);
            }
        }
    }

    private void processFile(FileScanTask task, ExternalIndexStore store) {
        Long firstRowId = task.file().firstRowId();
        String filePath = task.file().path().toString();
        int position = 0;
        try (CloseableIterable<Record> records = task.open()) {
            for (Record record : records) {
                long rowId = firstRowId + position;
                String name = record.getField("name").toString();
                store.addInvertedIndex(name, rowId);          // 第一层
                store.putRowIdLocation(rowId, filePath, position); // 第二层
                position++;
            }
        }
    }
}
```

### 2.4 处理 Compaction 与 DELETE

- **Compaction**：会生成新文件并替换旧文件。增量扫描会识别出这些新文件，因此只需在新文件中重建索引条目，并删除对应旧文件的所有条目即可。

- **DELETE**：被删除的行在 Manifest 中会被标记为 `DELETED`。Java SDK 的增量扫描本身不处理删除，但你可以通过快照的 `addedRows` 和 `deletedRows` 统计信息判断，或额外解析删除文件（Delete Files）来清理索引。

---

## 3. Rust SDK 实现方案B

### 3.1 获取 `_row_id`

Rust SDK 当前（截至 0.9.1）**不支持**在读取数据时自动提供 `_row_id`。开发者需要自行计算：

```rust
let first_row_id = task.file().first_row_id().unwrap(); // 从元数据获取
let reader = task.open()?;
for (row_pos, row) in reader.enumerate() {
    let row_id = first_row_id + row_pos as i64;  // 手动计算
    // 构建索引...
}
```

- 虽然计算本身简单，但需要开发者显式处理，并且必须确保 `first_row_id` 可用（V3 表中一般会有）。

### 3.2 增量更新：功能待完善

Rust SDK 目前**尚未正式发布**增量扫描 API。社区 PR [#2153](https://github.com/apache/iceberg-rust/pull/2153) 已添加了 `appends_after(snapshot_id)` 方法，但可能尚未合并到稳定版本。

因此，在稳定版本可用之前，Rust 实现增量构建需要：

- **自行追踪变更的快照**：通过定期比较快照列表，找出新增的快照。

- **自行识别变更文件**：读取新增快照的 Manifest，提取 `ADDED` 状态的文件。

- 这增加了实现的复杂性和出错风险。

### 3.3 索引构建完整流程（Rust）

```rust
fn build_index(table: &Table, store: &mut IndexStore) -> Result<()> {
    let scan = table.scan().build()?;
    let tasks = scan.plan_files()?;
    for task in tasks {
        let first_row_id = task.file().first_row_id().unwrap();
        let file_path = task.file().path();
        let reader = task.open()?;
        for (row_pos, row) in reader.enumerate() {
            let row_id = first_row_id + row_pos as i64;
            let name = row.get("name").unwrap().to_string();
            store.add_inverted_index(&name, row_id);
            store.put_location(row_id, &file_path, row_pos);
        }
    }
    Ok(())
}
```

- 增量构建需要额外实现快照对比和文件列表获取逻辑，无法直接复用简单的 API。

### 3.4 处理 Compaction 与 DELETE

- 同样需要自行检测 Compaction 产生的快照（`operation == "replace"` 或 `"overwrite"`），识别新旧文件，更新索引。

- 对于 DELETE，Rust SDK 目前对删除文件的解析支持也有限，需要额外开发。

---

## 4. 对比总览

| 对比维度                     | Java SDK                           | Rust SDK                           |
| ------------------------ | ---------------------------------- | ---------------------------------- |
| **获取 `_row_id`**         | 自动计算，API 直接提供 `firstRowId`         | 需手动计算 `first_row_id + row_pos`     |
| **增量扫描**                 | 原生 `IncrementalDataTableScan`，使用简单 | 社区 PR 中，尚未正式发布；需自行实现快照比较           |
| **文件读取**                 | 通过 `FileScanTask.open()`，迭代器友好     | 通过 `task.open()`，同样需要迭代，但 API 相对低级 |
| **索引构建代码量**              | 较少，集成了 Iceberg 高级 API              | 较多，需自行处理更多元数据细节                    |
| **生态集成**                 | 与 Spark、Flink 等无缝集成                | 主要与 DataFusion 集成，生态相对年轻           |
| **性能（语言层面）**             | JVM GC 可能引入停顿，但吞吐量高                | 无 GC，内存控制精准，适合低延迟场景                |
| **维护成本**                 | 低，依赖成熟 API，社区支持丰富                  | 高，需跟进 SDK 演变，自行补齐缺失功能              |
| **Compaction/Delete 处理** | 可通过增量扫描 + 删除文件解析实现                 | 需自行实现类似逻辑，复杂度更高                    |

---

## 5. 性能与可维护性分析

### 5.1 查询性能

两种语言的方案在查询路径上几乎一致：

- 查询第一层索引（外部存储）→ 获取 `_row_id` → 查询第二层索引 → 读取文件。

- 语言层面的差异主要在于外部索引存储的访问延迟和文件读取效率，而方案本身的逻辑相同。

### 5.2 构建性能

- **全量构建**：两者都需要扫描所有数据文件，I/O 是主要瓶颈，语言差异影响不大。

- **增量构建**：Java 可以借助原生增量扫描，精准处理变更文件，显著减少扫描量；Rust 在官方功能就绪前，可能需要更复杂的手段，甚至不得不定期全量重建。

### 5.3 维护与升级

- **Java**：API 稳定，向后兼容性好，升级 SDK 通常只需调整少量 API 调用。

- **Rust**：SDK 仍在快速演进，API 可能变动；自实现的增量逻辑可能随版本变化需要重新适配。

---

## 6. 结论与建议

- **如果项目主要使用 Java 生态**（如 Spark、Flink），且对查询性能要求高，**方案B在 Java 中实现是成熟且高效的选择**，开发周期短，维护成本低。

- **如果必须使用 Rust**（例如为了低延迟、无 GC 的原生服务），且能够接受较高的开发投入和持续跟进社区变化的成本，方案B同样可行。建议密切关注 Rust SDK 的增量扫描进展，待其稳定后可以大幅简化实现。

- **通用建议**：无论哪种语言，外部索引存储的选择（如 RocksDB、TiKV）和 Compaction 监听机制的设计，对整体性能和稳定性影响巨大，应作为重点评估。

---

## 7. 附录：关键代码片段对比（Java vs Rust）

| 操作                | Java                                                                                     | Rust                                            |
| ----------------- | ---------------------------------------------------------------------------------------- | ----------------------------------------------- |
| 获取文件 `firstRowId` | `task.file().firstRowId()`                                                               | `task.file().first_row_id().unwrap()`           |
| 计算行 `_row_id`     | `long rowId = firstRowId + position;`                                                    | `let row_id = first_row_id + row_pos as i64;`   |
| 增量扫描              | `table.newIncrementalDataTableScan().fromSnapshotExclusive(...).toSnapshot(...).build()` | `table.scan().appends_after(...).build()` (待发布) |
| 外部索引写入            | 调用自定义 `store` 方法                                                                         | 调用自定义 `store` 方法                                |
