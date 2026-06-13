# Apache Iceberg Puffin 自定义索引 — Rust 统一设计文档 v3

> **版本**: 3.2 | **日期**: 2026-06-13
>
> **v3.2 新增**: 方案 A — statistics[] 仅存放单个指针（index_catalog.puffin），索引元数据独立管理。解决了平铺式 statistics[] 随 segment 线性膨胀的问题。详见 §3.7–3.8。
>
> **v3.3 简化**: 移除 file_index / RoaringBitmap 中间层，文件覆盖直接使用路径字符串（`HashSet<String>`），消除 file_index_map / hash_to_index 映射层。详见 §8。
>
> **本文档合并了以下设计，形成唯一的权威参考：**
>
> | 源文档 | 合并内容 |
> |--------|---------|
> | Puffin_BTree索引_完整生命周期_v2.1 | 状态机、时间线 T0-T8、异步窗口处理 |
> | Apache_Iceberg_Puffin_自定义索引_实现分析 | Puffin 格式、Reader/Writer、Bloom 示例 |
> | Puffin_异步索引模块_设计影响分析与修改方案 | stale_orphan 保护、check_index_usability |
> | Puffin_索引映射_文件结构设计_LanceDB借鉴 | RowAddress、filelist、Rewrite Map、三层粒度 |
> | Puffin_meta-index与filemap设计方案分析 | 平铺式 statistics[]（方案 B）、filemap 决策 |
> | Puffin_文件拆分策略_每索引组一个Puffin | 一索引一文件、写放大对比、index-group-uuid 职责 |
> | Puffin索引与Iceberg分区关联设计方案 | partition-bitmap、分区演进、查询决策流程 |
> | Puffin_自定义索引_创建与扫描流程深度分析 | 构建循环、扫描流程、缓存系统 |
> | Puffin_自定义索引_Rust实现_v3 | 增量构建追加模型、部分失效、Parquet 版本无关回表、Java 桥接 |

---

## 目录

- [第一部分：基础](#第一部分基础)
  - [1. 概述与设计原则](#1-概述与设计原则)
  - [2. 架构全景图](#2-架构全景图)
  - [3. Puffin 文件格式规范](#3-puffin-文件格式规范)
- [第二部分：索引组织](#第二部分索引组织)
  - [4. 索引组与索引段](#4-索引组与索引段)
  - [5. Blob 类型体系](#5-blob-类型体系)
  - [6. 索引状态机](#6-索引状态机)
- [第三部分：生命周期时间线](#第三部分生命周期时间线)
  - [7. T0-T8 完整时间线](#7-t0-t8-完整时间线)
- [第四部分：文件覆盖与部分失效](#第四部分文件覆盖与部分失效)
  - [8. 文件覆盖与文件列表](#8-文件覆盖与文件列表)
  - [9. 部分失效机制](#9-部分失效机制)
  - [10. Rewrite Map 设计](#10-rewrite-map-设计)
- [第五部分：分区感知与回表](#第五部分分区感知与回表)
  - [11. 分区感知索引](#11-分区感知索引)
  - [12. 统一回表设计](#12-统一回表设计)
- [第六部分：接口设计](#第六部分接口设计)
  - [13. Java SDK 桥接层](#13-java-sdk-桥接层)
  - [14. Rust 核心接口](#14-rust-核心接口)
  - [15. PuffinReader / PuffinWriter](#15-puffinreaderpuffinwriter)
  - [16. 自定义索引插件框架](#16-自定义索引插件框架)
- [第七部分：流程](#第七部分流程)
  - [17. 全量构建流程](#17-全量构建流程)
  - [18. 增量构建流程](#18-增量构建流程)
  - [19. 查询流程](#19-查询流程)
  - [20. 索引维护流程](#20-索引维护流程)
- [第八部分：附录](#第八部分附录)
  - [A. 数据结构速查](#a-数据结构速查)
  - [B. 性能基线](#b-性能基线)
  - [C. 规范速查表](#c-规范速查表)

---

# 第一部分：基础

## 1. 概述与设计原则

### 1.1 什么是 Puffin

Puffin 是 Apache Iceberg 的配套统计与索引文件格式，用于存储无法放入 Manifest 中的辅助信息。

```
                   Iceberg Table Metadata
                  ┌──────────────────────────┐
                  │  Snapshots               │
                  │  ├── s0                  │
                  │  ├── s1                  │
                  │  │   ├── manifest-list   │──→ ManifestList (Avro)
                  │  │   ├── manifest        │──→ ManifestFile (Avro) ──→ DataFile (Parquet)
                  │  │   └── statistics[]    │──→ Puffin ←── 本文核心
                  │  └── s2                  │
                  └──────────────────────────┘
```

核心设计原则：分离关注点（Manifest 管操作元数据，Puffin 管优化元数据）；可选消费（Reader 忽略 Puffin 不影响正确性）；可扩展（通过 type 字符串标识 Blob 类型）。

### 1.2 三大核心设计目标（v3）

```
目标 1: 增量构建 = 追加文件，不移改旧文件
  Snapshot S1: idx1.puffin           ← 覆盖 file 0-499
  Snapshot S3: idx1_delta.puffin     ← 覆盖 file 500-999 (新文件)
  Snapshot S5: idx1_delta2.puffin    ← 覆盖 file 1000-1499 (新文件)
  → 旧 Puffin immutable → 无写放大 → 可永久缓存

目标 2: 部分失效 vs 全失效
  CoW UPDATE 重写 file 0 → file 2000
  → Index-A: 仅 file 0 失效, 其余 499 个文件继续有效
  → 不是整个 Index-A 失效!
  → 渐进: Rewrite Map 转译 → 累积到阈值再异步重建

目标 3: Parquet 版本无关回表
  RowAddress 不依赖 Parquet 版本; Page 定位自动适配 v1/v2
```

### 1.3 核心原则速查

| # | 原则 | 说明 |
|---|------|------|
| 1 | 一个 IndexGroup = 一个逻辑索引 | `index_group_uuid` 唯一标识 |
| 2 | 一个 Puffin 文件 = 一个 IndexSegment | 物理隔离, 独立生命周期 |
| 3 | 旧 Puffin 文件不可变 | 增量构建只追加新文件 |
| 4 | delta-from 链 | 同逻辑索引的多 segment 通过 uuid 链接 |
| 5 | 部分失效优于全失效 | 优先 try Rewrite Map, 超阈值才重建 |
| 6 | 索引评估在分区裁剪之后 | Manifest stats 先裁剪, 索引处理剩余 |
| 7 | 索引不要求全覆盖 | 无索引覆盖的文件自动回退全扫描 |
| 8 | MoR DELETE 不影响索引 | Merge-on-Read 对索引透明 |

### 1.4 语言与集成策略

- **Puffin 文件 I/O**：纯 Rust 实现（Reader/Writer/压缩/JSON 解析）
- **索引构建与评估**：纯 Rust 实现（计算密集型, 零拷贝, 安全并发）
- **TableMetadata 操作**：通过 JNI 桥接调用 Java SDK（唯一可靠实现），使用 `jni` crate
- **Manifest 解析**：通过 JNI 桥接（复用 Java SDK）

---

## 2. 架构全景图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       Puffin Rust 索引系统 v3                              │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌─────────────┐    JNI / C-ABI     ┌─────────────────────────────────┐  │
│  │  Java SDK   │◄──────────────────►│     Rust Bridge Layer            │  │
│  │  (Iceberg)  │                    │  ├── TableMetadataBridge          │  │
│  │  • Catalog  │                    │  ├── SnapshotBridge               │  │
│  │  • TableMeta│                    │  └── ManifestBridge               │  │
│  │  • Commit   │                    └──────────────┬────────────────────┘  │
│  └─────────────┘                                   │                      │
│                                                    ▼                      │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                     Rust Index Manager                                │ │
│  │  ┌─────────────────────┐  ┌──────────────────┐  ┌────────────────┐  │ │
│  │  │ IndexSegmentManager  │  │ IndexCatalog     │  │IndexMaintainer  │  │ │
│  │  │ • build_full()      │  │ • 索引发现        │  │ • on_rewrite() │  │ │
│  │  │ • build_incremental │  │ • group/segment   │  │ • rebuild()    │  │ │
│  │  │ • mark_files_stale  │  │   目录管理         │  │ • GC           │  │ │
│  │  │ • get_usable_segs   │  │ • Footer 缓存     │  │ • validate()   │  │ │
│  │  └─────────────────────┘  └──────────────────┘  └────────────────┘  │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                      Puffin Core (纯 Rust)                             │ │
│  │  ┌────────────┐  ┌────────────┐  ┌──────────────┐  ┌─────────────┐  │ │
│  │  │PuffinReader│  │PuffinWriter│  │CompressionMgr│  │FooterParser │  │ │
│  │  └────────────┘  └────────────┘  │ LZ4 / Zstd   │  │ serde_json  │  │ │
│  │                                  └──────────────┘  └─────────────┘  │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                     IndexPlugin trait 体系                             │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │ │
│  │  │BTreeIndex│  │BloomIndex│  │BitmapIdx │  │  自定义索引            │ │ │
│  │  │(Range)   │  │(EQ/IN)   │  │(低基数列) │  │  impl IndexPlugin    │ │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────────────┘ │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │                     行回表 (Row Lookup)                                │ │
│  │  ┌────────────────────┐  ┌────────────────────┐  ┌───────────────┐  │ │
│  │  │ RowAddress (128bit)│  │ FileRegistry        │  │ParquetPage    │  │ │
│  │  │ file_hash|rg|row   │  │ hash→path 映射       │  │Locator        │  │ │
│  │  └────────────────────┘  └────────────────────┘  │ v1/v2 通用     │  │ │
│  │                                                   └───────────────┘  │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.1 文件级全景图：catalog.puffin 与各索引文件的关系

```
                        TableMetadata JSON
                   ┌────────────────────────────┐
                   │  statistics[]:              │
                   │    └── 唯一一条 ──────────────┐
                   │        type: registry-v1    │ │
                   │        path: index_catalog  │ │
                   └────────────────────────────┘ │
                                                  │
                    ┌─────────────────────────────┘
                    ▼
         ┌──────────────────────────────────────────────────────┐
         │              index_catalog.puffin                    │
         │              (中枢注册表, ~3-12KB)                    │
         │                                                      │
         │  ┌────────────────────────────────────────────────┐  │
         │  │  registry Blob:                                 │  │
         │  │                                                  │  │
         │  │  Group "a1b2c3d4" (btree, field=1)              │  │
         │  │  ├── Segment s0: path=snap-1001-2-idx1.puffin ──┼──┐
         │  │  │     status=active, files=0-499                │  │ │
         │  │  ├── Segment s1: path=snap-1003-3-idx1delta ────┼──┼─┐
         │  │  │     status=active, files=500-999, delta=s0   │  │ │ │
         │  │  └── Segment s2: path=snap-1011-6-idx1delta2 ───┼──┼─┼─┐
         │  │        status=active, spec_id=1                  │  │ │ │
         │  │                                                  │  │ │ │
         │  │  Group "b2c3d4e5" (btree, field=2)              │  │ │ │
         │  │  └── Segment s3: path=snap-1003-3-idx2.puffin ──┼──┼─┼─┼─┐
         │  │        status=remapping, files=0-999             │  │ │ │ │
         │  │                                                  │  │ │ │ │
         │  │  Group "d4e5f6a7" (rewrite-map)                 │  │ │ │ │
         │  │  └── Segment s4: path=snap-1007-4-rewrite ──────┼──┼─┼─┼─┼─┐
         │  │        affected-groups: a1b2c3d4, b2c3d4e5      │  │ │ │ │ │
         │  │                                                  │  │ │ │ │ │
         │  │  Group "e5f6a7b8" (ivf, field=3)               │  │ │ │ │ │
         │  │  └── Segment s5: path=snap-1015-7-ivf.puffin ───┼──┼─┼─┼─┼─┼─┐
         │  │        status=active, files=0-1999               │  │ │ │ │ │ │
         │  └────────────────────────────────────────────────┘  │ │ │ │ │ │ │
         └─────────────────┬────────────────┬─────────────────┘  │ │ │ │ │ │
                           │                │  (路径引用)         │ │ │ │ │ │
         ┌─────────────────▼────────────────▼─────────────────┐  │ │ │ │ │ │
         │          物理 Puffin 文件 (metadata/ 目录下)         │  │ │ │ │ │ │
         │                                                    │  │ │ │ │ │ │
         │  ┌─────────────────────────────────────────────┐   │  │ │ │ │ │ │
         │  │ snap-1001-2-idx1.puffin    (Index-A seg 0)  │◄──┘  │ │ │ │ │
         │  │ ├── btree-v1 Blob     (16MB, BTree 数据)    │      │ │ │ │ │
         │  │ ├── filelist-v1     (覆盖 500 个文件)     │      │ │ │ │ │
         │  │ └── partition-bitmap  (spec_id=0, 10 分区)   │      │ │ │ │ │
         │  └─────────────────────────────────────────────┘      │ │ │ │ │
         │                                                       │ │ │ │ │
         │  ┌─────────────────────────────────────────────┐      │ │ │ │ │
         │  │ snap-1003-3-idx1delta.puffin (Index-A seg 1)│◄─────┘ │ │ │ │
         │  │ ├── btree-v1 Blob     (3.3MB, 增量数据)     │        │ │ │ │
         │  │ ├── filelist-v1     (覆盖 500 个文件)        │        │ │ │ │
         │  │ └── partition-bitmap  (spec_id=0, 10 分区)   │        │ │ │ │
         │  └─────────────────────────────────────────────┘        │ │ │ │
         │                                                         │ │ │ │
         │  ┌─────────────────────────────────────────────┐        │ │ │ │
         │  │ snap-1011-6-idx1delta2.puffin (Index-A s2)  │◄───────┘ │ │ │
         │  │ ├── btree-v1 Blob     (5MB, spec_id=1 数据) │          │ │ │
         │  │ └── partition-bitmap  (spec_id=1, 1 分区)    │          │ │ │
         │  └─────────────────────────────────────────────┘          │ │ │
         │                                                           │ │ │
         │  ┌─────────────────────────────────────────────┐          │ │ │
         │  │ snap-1003-3-idx2.puffin    (Index-B)        │◄─────────┘ │ │
         │  │ ├── btree-v1 Blob     (32MB, col=cust_id)   │            │ │
         │  │ └── filelist-v1     (覆盖 1000 个文件)     │            │ │
         │  └─────────────────────────────────────────────┘            │ │
         │                                                             │ │
         │  ┌─────────────────────────────────────────────┐            │ │
         │  │ snap-1007-4-rewrite.puffin  (Rewrite Map)   │◄───────────┘ │
         │  │ └── rewritemap-v1 Blob  (file 0→1000 映射)  │              │
         │  └─────────────────────────────────────────────┘              │
         │                                                               │
         │  ┌─────────────────────────────────────────────┐              │
         │  │ snap-1015-7-ivf.puffin     (IVF 向量索引)    │◄─────────────┘
         │  │ ├── ivf-flat-v1 Blob   (150MB, 向量聚类)    │
         │  │ └── filelist-v1      (覆盖 2000 个文件)    │
         │  └─────────────────────────────────────────────┘
         │
         └──────────────────────────────────────────────────────────────┐
                                                                        │
         ┌──────────────────────────────────────────────────────────────┘
         ▼
    ┌─────────────────────────────────────────────────────────┐
    │              实际数据 (data/ 目录下)                      │
    │  part-00001.parquet  part-00002.parquet  ...  part-02000 │
    └─────────────────────────────────────────────────────────┘

关键关系:
  • TableMetadata → index_catalog.puffin       (1:1, 一个指针)
  • index_catalog.puffin → N 个 index .puffin  (1:N, 路径引用)
  • 每个 index .puffin → M 个 .parquet 文件    (1:M, 通过 filelist)
  • 查询只需读 catalog.puffin 就能发现所有索引
  • 索引变更只重写 catalog.puffin (~3-12KB), 不动各索引 .puffin
```

### 2.2 索引注册表结构图：catalog.puffin 如何描述 N 个索引

上图从文件级别展示了 catalog.puffin 和各索引文件的连接关系。下面从**数据结构**级别展示 catalog.puffin 内部如何组织——哪些字段描述索引、哪些字段描述增量段、哪些字段描述文件覆盖。

```
                      TableMetadata.statistics[]
                     ┌─────────────────────────────────────────────────────┐
                     │ [0]: type="puffin.idx.registry-v1"           │
                     │      path="s3://.../metadata/index_catalog.puffin"  │
                     │      ← statistics[] 仅此一条指向自定义索引           │
                     └──────────────────────────┬──────────────────────────┘
                                                │
                     ┌──────────────────────────▼──────────────────────────┐
                     │          index_catalog.puffin 内部结构               │
                     │  (标准 Puffin 文件: Magic + Blob₀ + Blob₁ + Footer) │
                     ├─────────────────────────────────────────────────────┤
                     │                                                     │
                     │  ┌─────────────────────────────────────────────┐   │
                     │  │  Blob₀: registry-v1 (索引注册表, Zstd压缩)  │   │
                     │  │                                             │   │
                     │  │  registry_version = 17                      │   │
                     │  │  num_groups = 3                             │   │
                     │  │                                             │   │
                     │  │  ┌─────────────────────────────────────┐   │   │
                     │  │  │ Group[0]: uuid="a1b2c3d4..."        │   │   │
                     │  │  │   name      = "btree_order_id"      │   │   │
                     │  │  │   type      = "btree-v1"            │   │   │
                     │  │  │   field_id  = 1                     │   │   │
                     │  │  │   partition_spec_id = 0             │   │   │
                     │  │  │   num_segments = 3                  │   │   │
                     │  │  │                                     │   │   │
                     │  │  │   Segments:                         │   │   │
                     │  │  │   ┌─────────────────────────────┐  │   │   │
                     │  │  │   │ seg[0]: uuid="s0-xxx"       │  │   │   │
                     │  │  │   │   path="snap-1001-2-idx1"   │──┼───┼───┼──→ .puffin 文件
                     │  │  │   │   status=active ratio=0.0   │  │   │   │
                     │  │  │   │   built_at_snapshot=1001    │  │   │   │
                     │  │  │   │   file_coverage: {          │  │   │   │
                     │  │  │   │     format: "range"         │  │   │   │  ← 摘要: 覆盖哪些文件
                     │  │  │   │     value: "part-00001~part-00500"  │   │   │  (完整文件路径列表
                     │  │  │   │   }                         │  │   │   │      在 .puffin 内)
                     │  │  │   └─────────────────────────────┘  │   │   │
                     │  │  │                                     │   │   │
                     │  │  │   ┌─────────────────────────────┐  │   │   │
                     │  │  │   │ seg[1]: uuid="s1-yyy"       │  │   │   │
                     │  │  │   │   path="snap-1003-idx1delta"│──┼───┼───┼──→ .puffin 文件
                     │  │  │   │   status=active ratio=0.0   │  │   │   │
                     │  │  │   │   delta_from_seg = "s0-xxx" │  │   │   │  ← 增量链!
                     │  │  │   │   file_coverage: {          │  │   │   │     指向基础段
                     │  │  │   │     format: "range"         │  │   │   │
                     │  │  │   │     value: "500-999"        │  │   │   │
                     │  │  │   │   }                         │  │   │   │
                     │  │  │   └─────────────────────────────┘  │   │   │
                     │  │  │                                     │   │   │
                     │  │  │   ┌─────────────────────────────┐  │   │   │
                     │  │  │   │ seg[2]: uuid="s2-zzz"       │  │   │   │
                     │  │  │   │   path="snap-1011-idx1delta2│──┼───┼───┼──→ .puffin 文件
                     │  │  │   │   status=active ratio=0.0   │  │   │   │
                     │  │  │   │   delta_from_seg = "s1-yyy" │  │   │   │
                     │  │  │   │   file_coverage: {          │  │   │   │
                     │  │  │   │     format: "range"         │  │   │   │
                     │  │  │   │     value: "1000-1499"      │  │   │   │
                     │  │  │   │   }                         │  │   │   │
                     │  │  │   └─────────────────────────────┘  │   │   │
                     │  │  └─────────────────────────────────────┘   │   │
                     │  │                                             │   │
                     │  │  ┌─────────────────────────────────────┐   │   │
                     │  │  │ Group[1]: uuid="b2c3d4e5..."        │   │   │
                     │  │  │   name      = "btree_customer_id"   │   │   │
                     │  │  │   type      = "btree-v1"            │   │   │
                     │  │  │   field_id  = 2                     │   │   │
                     │  │  │   num_segments = 1                  │   │   │
                     │  │  │   Segments:                         │   │   │
                     │  │  │   ┌─────────────────────────────┐  │   │   │
                     │  │  │   │ seg[0]: uuid="s3-aaa"       │  │   │   │
                     │  │  │   │   path="snap-1003-3-idx2"   │──┼───┼───┼──→ .puffin 文件
                     │  │  │   │   status=remapping ratio=0.1│  │   │   │
                     │  │  │   │   file_coverage: {          │  │   │   │
                     │  │  │   │     format: "range"         │  │   │   │
                     │  │  │   │     value: "0-999"          │  │   │   │
                     │  │  │   │   }                         │  │   │   │
                     │  │  │   └─────────────────────────────┘  │   │   │
                     │  │  └─────────────────────────────────────┘   │   │
                     │  │                                             │   │
                     │  │  ┌─────────────────────────────────────┐   │   │
                     │  │  │ Group[2]: uuid="e5f6a7b8..."        │   │   │
                     │  │  │   name      = "ivf_embedding"       │   │   │
                     │  │  │   type      = "ivf-flat-v1"         │   │   │
                     │  │  │   field_id  = 5                     │   │   │
                     │  │  │   num_segments = 1                  │   │   │
                     │  │  │   Segments:                         │   │   │
                     │  │  │   ┌─────────────────────────────┐  │   │   │
                     │  │  │   │ seg[0]: path="snap-1015-ivf"│──┼───┼───┼──→ .puffin 文件
                     │  │  │   │   ...                       │  │   │   │
                     │  │  │   └─────────────────────────────┘  │   │   │
                     │  │  └─────────────────────────────────────┘   │   │
                     │  └─────────────────────────────────────────────┘   │
                     │                                                     │
                     │  ┌─────────────────────────────────────────────┐   │
                     │  │  Blob₁: audit-log-v1 (审计日志, Zstd压缩)   │   │
                     │  │  num_entries=127, max_entries=500           │   │
                     │  │  [最近 500 条状态变更记录, 用于运维排查]     │   │
                     │  └─────────────────────────────────────────────┘   │
                     │                                                     │
                     │  Footer JSON:                                       │
                     │    blobs[0]: type="registry-v1", offset=4, ...      │
                     │    blobs[1]: type="audit-log-v1", offset=..., ...   │
                     │    properties: { "puffin-type": "index-catalog" }   │
                     └─────────────────────────────────────────────────────┘
```

#### Group 与 Segment 的层级关系

```
查询 "field=1 有哪些索引可用?"
  → 遍历 registry 中的 Group[]:
      Group[0]: field_id=1, name="btree_order_id"
        → seg[0]: active, covers files 0-499   (基础段, ~16MB)
        → seg[1]: active, covers files 500-999   (增量, ~3.3MB, delta_from=seg[0])
        → seg[2]: active, covers files 1000-1499 (增量, ~5MB,  delta_from=seg[1])
      Group[1]: field_id=2 → 跳过 (不是查询列)
      Group[2]: field_id=5 → 跳过

  结果: 1 个逻辑索引 = Group[0] = 3 个物理 segment 链
        查询时需搜索全部 3 个 segment 的 BTree
```

#### 文件覆盖与文件路径映射

```
file_coverage 在 registry 中只存摘要 (如 "part-00001~part-00500"), 用于索引发现
完整路径列表在各自 .puffin 文件的 filelist-v1 Blob 中:

  snap-1001-2-idx1.puffin 的 filelist-v1 Blob:
  ┌──────────────────────────────────────────────────────────────┐
  │  file_paths:           (构建时按 Manifest 顺序记录)           │
  │    { hash: 0xA3F2109B5678CDEF,                               │
  │      path: "created_at_day=2026-01-01/part-00001.parquet",   │
  │      rows: 2048 }                                            │
  │    { hash: 0x7E123456ABCD9012,                               │
  │      path: "created_at_day=2026-01-01/part-00002.parquet",   │
  │      rows: 2048 }                                            │
  │    ...                                                       │
  │    { hash: 0xFEDC..., path: "...part-00500", rows: 2048 }    │
  │                                                              │
  │  original_files (HashSet<String>):                           │
  │    {"created_at_day=2026-01-01/part-00001.parquet",          │
  │     "created_at_day=2026-01-01/part-00002.parquet", ...}     │
  │    ↑ 500 个路径, Zstd 压缩后 ~15KB                            │
  │  stale_files (HashSet<String>):                              │
  │    {"created_at_day=2026-01-01/part-00001.parquet",          │
  │     "created_at_day=2026-01-03/part-00050.parquet"}          │
  │    ↑  file_0 和 file_50 已被 CoW 重写                          │
  │                                                              │
  │  hash_to_path (HashMap<u64, String>, 查询时构建):             │
  │    0xA3F2109B5678CDEF → "created_at_day=2026-01-01/          │
  │                          part-00001.parquet"                  │
  │    0x7E123456ABCD9012 → "created_at_day=2026-01-01/          │
  │                          part-00002.parquet"                  │
  │    ...                                                       │
  └──────────────────────────────────────────────────────────────┘

  file_path (String) 在 segment 内部用于覆盖和失效判定
  file_path_hash (XXH64) 用于 RowAddress → 跨 segment/snapshot 稳定
  查询时: BTree 返回 (hash, rg, row)
          → hash_to_path[hash] → file_path
          → stale_files.contains(file_path) → 过滤失效文件
          → file_path → 读 Parquet
```

#### 多版本索引存储全景

```
snapshot 1001:     snap-1001-2-idx1.puffin (Index-A seg0, 16MB)
                   index_catalog.puffin v1 (registry_version=1)

snapshot 1003:     snap-1001-2-idx1.puffin (不变!)
                 + snap-1003-3-idx2.puffin (Index-B, 32MB)
                 + snap-1003-3-idx1delta.puffin (Index-A seg1, 3.3MB)
                   index_catalog.puffin v3 (registry_version=3)
                   ↑ 重写 ~10KB, 仅改 registry, 索引文件不碰

snapshot 1007:     snap-1001-2-idx1.puffin (不变)
                   snap-1003-3-idx2.puffin (不变)
                   snap-1003-3-idx1delta.puffin (不变)
                 + snap-1007-4-rewrite.puffin (Rewrite Map, ~1KB)
                   index_catalog.puffin v5 (registry_version=5)
                   ↑ Index-A seg0: active→stale_orphan→remapping

snapshot 1011:     (同上 3 个文件不变)
                 + snap-1011-6-idx1delta2.puffin (Index-A seg2, 5MB)
                   index_catalog.puffin v7 (registry_version=7)

snapshot 1015:     (同上)
                 + snap-1015-7-ivf.puffin (IVF向量索引, 150MB)
                   index_catalog.puffin v8 (registry_version=8)

关键:
  • 索引 Puffin 文件一旦写入, 永不被修改 (immutable)
  • index_catalog.puffin 每次重写 (~10-16KB), 记录状态变更
  • TableMetadata.statistics[] 只有 1 条指针, 从不膨胀
  • 旧 snapshot 的 catalog.puffin 保留完整历史 (Iceberg snapshot 过期时 GC)
```

---

## 3. Puffin 文件格式规范

### 3.1 文件布局

```
Offset 0    ┌──────────────────┐
            │  Magic (4 bytes) │  0x50 0x46 0x41 0x31  ("PFA1")
Offset 4    ├──────────────────┤
            │  Blob₁           │  原始索引/统计二进制数据 (变长, 可压缩)
            ├──────────────────┤
            │  Blob₂           │
            ├──────────────────┤
            │  ...             │
            ├──────────────────┤
            │  Blobₙ           │
            ├──────────────────┤ ← Footer 开始
            │  Magic (4B)      │
            ├──────────────────┤
            │  FooterPayload   │  UTF-8 JSON, 可选 LZ4 压缩
            ├──────────────────┤
            │  FooterPayloadSize│ i32 LE (已压缩后长度)
            ├──────────────────┤
            │  Flags (4 bytes) │  bit 0 = Footer LZ4 压缩标志
            ├──────────────────┤
            │  Magic (4B)      │
            └──────────────────┘
```

**整数编码**：全部有符号小端序（signed, two's complement, little-endian）。

### 3.2 Magic Number

```
字节:  0x50 0x46 0x41 0x31
u32 LE: 0x31414650
含义:  "Puffin Fratercula arctica, version 1"
```

### 3.3 Footer Payload JSON Schema

```json
{
  "blobs": [
    {
      "type": "puffin.idx.btree-v1",
      "fields": [1],
      "snapshot-id": 8027658604211071520,
      "sequence-number": 1,
      "offset": 4,
      "length": 16384016,
      "compression-codec": "zstd",
      "properties": {
        "index-group-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        "blob-role": "primary",
        "status": "active",
        "num-pages": "250",
        "total-rows": "1024000",
        "partition-spec-id": "0"
      }
    },
    {
      "type": "puffin.idx.filelist-v1",
      "fields": [1],
      "snapshot-id": 8027658604211071520,
      "sequence-number": 1,
      "offset": 16384020,
      "length": 16,
      "compression-codec": "none",
      "properties": {
        "blob-role": "metadata",
        "file-ids": "0-499"
      }
    }
  ],
  "properties": {
    "created-by": "puffin-rs/3.0",
    "index-group-uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "index-name": "btree_order_id_snap1001",
    "status": "active"
  }
}
```

### 3.4 BlobMetadata 字段

| 字段 | JSON 类型 | 必需 | 说明 |
|------|----------|------|------|
| `type` | string | ✅ | Blob 类型标识符 |
| `fields` | int[] | ✅ | Iceberg 字段 ID 列表 |
| `snapshot-id` | long | ✅ | 源快照 ID |
| `sequence-number` | long | ✅ | 源快照序列号 |
| `offset` | long | ✅ | Blob 数据在文件中的起始字节偏移 |
| `length` | long | ✅ | Blob 数据字节长度（压缩后） |
| `compression-codec` | string | ❌ | `"lz4"` / `"zstd"`；省略 = 不压缩 |
| `properties` | map | ❌ | 任意 key-value 元数据 |

### 3.5 压缩规范

| Codec | 使用场景 | 规范 |
|-------|----------|------|
| LZ4 | FooterPayload / Blob 数据 | 单帧，必须有 content size present |
| Zstd | Blob 数据 | 单帧，必须有 content size present |
| (省略) | Blob 数据不压缩 | — |

FooterPayload **只能用 LZ4**；Blob 可用 LZ4 或 Zstd。

### 3.6 Footer 读取算法

```
1. 读文件最后 4 字节 → 验证尾部 Magic
2. 回退 4+4=8 字节 → 读 Flags, 检查 bit 0 决定是否 LZ4 解压
3. 回退 4 字节 → 读 FooterPayloadSize (i32 LE)
4. 计算 footer_payload_start = file_size - 12 - payload_size
5. 读取 FooterPayload 原始字节 → 如果 Flags&1 则 LZ4 解压
6. JSON 解析 → FileMetadata
7. 遍历 blobs[] → 按 offset/length/compression 读取每个 Blob
```

**Rust 实现要点**：使用 `std::io::Read::seek` + `byteorder` crate（LE 读取），`serde_json` 替代 simdjson，`lz4` / `zstd` crate 解压。

### 3.7 TableMetadata.statistics[] 中的存储策略（方案 A）

**设计决策**：statistics[] **不直接存放各索引 segment 的条目**。改用一个轻量指针指向独立的 `index_catalog.puffin`。

```
问题: 平铺式的 statistics[] 随 segment 数量线性膨胀
  5 个索引组 × 10 次增量构建 = 50 条 statistics[] + ~10 条 Rewrite Map
  → TableMetadata JSON 轻松突破 100KB
  → 每次 commit 都全量读写

方案 A: statistics[] 只放一个指针
  → O(1) 固定大小, 永不超过 1KB
  → 索引元数据自包含在 index_catalog.puffin 中
```

```json
// TableMetadata.statistics[] — v3.2 之后:
{
  "statistics": [
    // (可选) 内置 Puffin 统计:
    {
      "snapshot-id": 8027658604211071520,
      "statistics-path": "s3://.../metadata/snap-xxx-theta.puffin",
      "file-size-in-bytes": 16384,
      "file-footer-size-in-bytes": 512,
      "blob-metadata": [{
        "type": "apache-datasketches-theta-v1",
        "source-snapshot-id": 8027658604211071520,
        "source-snapshot-sequence-number": 1,
        "fields": [1],
        "properties": { "ndv": "5000000" }
      }]
    },

    // 自定义索引注册表 — 唯一的一行:
    {
      "snapshot-id": 1011,
      "statistics-path": "s3://.../metadata/index_catalog.puffin",
      "file-size-in-bytes": 12288,
      "file-footer-size-in-bytes": 512,
      "blob-metadata": [{
        "type": "puffin.idx.registry-v1",
        "source-snapshot-id": 1011,
        "source-snapshot-sequence-number": 11,
        "fields": [],
        "properties": {
          "num-groups": "5",
          "num-segments": "52",
          "registry-version": "17",
          "registry-checksum": "sha256:abc123..."
        }
      }]
    }
  ]
}
```

**statistics[] 大小对比**：

| 场景 | 平铺式 (旧) | 方案 A (新) |
|------|-----------|-----------|
| 3 个索引, 各 2 segments | 6 条, ~2KB | 1 条, ~300B |
| 5 个索引, 各 10 segments | 50 条, ~20KB | 1 条, ~300B |
| 10 个索引, 各 20 segments | 200 条, ~80KB | 1 条, ~300B |

**关键区别**：statistics[] 中的 blob-metadata 仅作**发现用途** — 告诉你 `index_catalog.puffin` 在哪儿以及大概有多新。真正的索引目录完整信息在 `index_catalog.puffin` 内部。读取 Blob 数据始终以 Puffin Footer 为准。

### 3.8 index_catalog.puffin — 索引注册表

所有自定义索引的元数据（group、segment、状态、文件覆盖摘要）都存放在 `index_catalog.puffin` 的 registry Blob 中。

```
index_catalog.puffin
┌──────────────────────────────────────────────────────────────┐
│  Blob[0]: puffin.idx.registry-v1                      │
│                                                              │
│  二进制布局:                                                  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  Header (16 bytes)                                     │  │
│  │  ├── magic:     0x5247 ("RG")  (2 bytes)              │  │
│  │  ├── version:   2              (2 bytes)               │  │
│  │  ├── num_groups: 5            (4 bytes LE)             │  │
│  │  ├── registry_version: 17     (4 bytes LE)             │  │
│  │  └── updated_at_snapshot: 1011 (4 bytes LE)            │  │
│  ├────────────────────────────────────────────────────────┤  │
│  │  Group Directory (常驻内存, 每个 group 定长 120B)          │  │
│  │  └── for each group (共 num_groups 个):                    │  │
│  │       ├── group_uuid:       36 bytes (UUID 字符串)         │  │
│  │       ├── index_name_len:   u16                            │  │
│  │       ├── index_name:       变长 (如 "btree_order_id")     │  │
│  │       ├── blob_type_len:    u16                            │  │
│  │       ├── blob_type:        变长                           │  │
│  │       ├── field_id:         i32                            │  │
│  │       ├── partition_spec_id: i32                           │  │
│  │       ├── created_at_snapshot: i64                         │  │
│  │       ├── num_segments:     u16     ← 该组的 segment 数量  │  │
│  │       └── segment_index_start: u32  ← Segment List 中的    │  │
│  │                                      起始索引 (0-based)    │  │
│  │                                                             │  │
│  │  Segment List (变长条目顺序排列, 无索引结构)                  │  │
│  │  └── 共 num_segments_total 个条目, 按 group 连续排列:        │  │
│  │                                                             │  │
│  │       Group[0] 的 segments:  segment[seg0_start]            │  │
│  │                               segment[seg0_start+1]         │  │
│  │                               ...共 num_segments[0] 个      │  │
│  │       Group[1] 的 segments:  segment[seg1_start]            │  │
│  │                               ...共 num_segments[1] 个      │  │
│  │                                                             │  │
│  │       每个 segment 条目 (变长, 含 4 个变长字符串):            │  │
│  │       ┌───────────────────────────────────────────────┐    │  │
│  │       │  FIXED HEADER (111 bytes)                     │    │  │
│  │       │  ├── segment_uuid:        36 bytes            │    │  │
│  │       │  ├── delta_from_seg_uuid: 36 bytes (或全零)   │    │  │
│  │       │  ├── status:              u8                  │    │  │
│  │       │  ├── staleness_ratio:     f32                 │    │  │
│  │       │  ├── built_at_snapshot:   i64                 │    │  │
│  │       │  ├── status_updated_at:   i64 (epoch ms)      │    │  │
│  │       │  ├── total_rows:          i64                 │    │  │
│  │       │  ├── num_pages:           i32                 │    │  │
│  │       │  ├── puffin_file_size:    i64                 │    │  │
│  │       │  ├── puffin_footer_size:  i32                 │    │  │
│  │       │  └── file_coverage_format: u8                 │    │  │
│  │       ├───────────────────────────────────────────────┤    │  │
│  │       │  VARIABLE STRINGS (len-prefixed, 顺序读取)     │    │  │
│  │       │  ├── puffin_path_len:     u16                 │    │  │
│  │       │  ├── puffin_path:         变长 (相对路径)      │    │  │
│  │       │  ├── deprecates_len:      u16                 │    │  │
│  │       │  ├── deprecates_uuids:    变长 (逗号分隔)      │    │  │
│  │       │  ├── file_coverage_len:   u16                 │    │  │
│  │       │  ├── file_coverage:       变长                │    │  │
│  │       │  │   format=0: "0-499" (range string)        │    │  │
│  │       │  │   format=1: Roaring Bitmap serialized     │    │  │
│  │       │  ├── partition_summary_len: u16               │    │  │
│  │       │  └── partition_summary:   变长                │    │  │
│  │       └───────────────────────────────────────────────┘    │  │
│  │                                                             │  │
│  │ 解析方式: 每个 segment 条目 = 111B 定长头 + 4 个变长字符串   │  │
│  │   先读 111B 确定头 → 从 file_coverage_format 知格式          │  │
│  │   → 依次读 puffin_path_len → puffin_path                    │  │
│  │   → 依次读 deprecates_len → deprecates_uuids                │  │
│  │   → 依次读 file_coverage_len → file_coverage                │  │
│  │   → 依次读 partition_summary_len → partition_summary        │  │
│  │   → 条目总长度 = 111 + Σ(len_i + 2) for the 4 strings       │  │
│  │   → buffer 指针前进, 继续读下一条 (重复 num_segments[g] 次)   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  Blob 大小估算:                                                   │
│    5 groups × ~120B + 50 segments × ~200B = 10.6KB               │
│    Zstd 压缩后: ~3KB                                             │
└──────────────────────────────────────────────────────────────────┘
```

#### Registry 中的 UUID 体系

registry 中共有三种 UUID，各自有不同的引用方和被引用方：

```
segment_uuid (每个 segment 一个, 36 bytes)
  │
  ├── 被 delta_from_seg_uuid 引用 ──→ 形成增量链
  │     seg[1].delta_from_seg_uuid = seg[0].segment_uuid
  │     seg[2].delta_from_seg_uuid = seg[1].segment_uuid
  │     查询时: 沿链找到所有同属一个逻辑索引的 segment
  │
  ├── 被 deprecates_uuids 引用 ──→ 标识被替代的旧 segment
  │     seg_new.deprecates_uuids = "s0-xxx,s1-yyy"
  │     → 旧 segment status 变为 deprecated → 最终 GC
  │
  ├── 被 audit-log 引用 ──→ 记录变更事件
  │     audit_entry.segment_uuid = "s0-xxx"
  │     → 运维排查时定位受影响的 segment
  │
  └── 被 Rewrite Map 间接引用 ──→ 通过 group_uuid 找到 group,
        再遍历该 group 的 segments 找到需要转译的 segment
        (Rewrite Map 的 affected-index-groups 存的是 group_uuid,
         不直接存 segment_uuid, 因为一个 CoW 影响的是整个 group 下的所有
         覆盖了重写文件的 segment)

group_uuid (每个 group 一个, 36 bytes)
  │
  ├── 被 Rewrite Map 的 affected-index-groups 引用
  │     → CoW UPDATE 后标记: "a1b2c3d4,b2c3d4e5"
  │     → 后台任务据此找到受影响的 segment
  │
  ├── 被 delta-from-group-uuid 引用 (v2 兼容, 在 Puffin Footer properties 中)
  │     → 增量索引 Puffin 文件自描述其所属的逻辑索引
  │
  └── 被 audit-log 引用 ──→ 记录变更事件

为什么不用位置索引 (如 "segment #3 in group X")?
  • 位置索引在删除/重排后立即失效
  • 当 seg[0] 被 GC 删除后, seg[1] 变成位置 0, seg[2] 变成位置 1
    → 所有引用 "segment #2" 的地方都坏了
  • UUID 永久稳定, 删除不影响其他条目的引用
```

**index_catalog.puffin 的更新模式**：每次索引状态变更时，重写整个文件（~3-12KB）。因为体积小，重写开销可忽略。原子性由 TableMetadata commit 保证——先写新的 catalog.puffin，再更新 statistics[] 指向，最后 commit TableMetadata。

**与各索引 Puffin Footer 的关系**：registry Blob 持有索引发现所需的全部摘要（状态、文件覆盖、构建统计）。各索引 Puffin 文件的 Footer 仍然自包含完整的物理信息（offset/length/compression），但状态以 registry 为准（因为它是原子更新的）。

#### registry_version 的作用

`registry_version` 是一个**单调递增计数器**（从 1 开始，每次状态变更 +1）。它不是时间戳，不编码任何语义——唯一的作用是**版本比较**。

```
三个使用场景:

1. 缓存失效 (查询引擎)
   ┌─────────────────────────────────────────────────────────────┐
   │ 查询 S1011:                                                 │
   │   TableMetadata 中 registry-version = 17                    │
   │   内存缓存中 registry_version_cached = 17 → 命中, no-op     │
   │                                                             │
   │ 查询 S1015 (CoW UPDATE 后):                                  │
   │   TableMetadata 中 registry-version = 18                    │
   │   内存缓存 = 17                                                │
   │   18 ≠ 17 → 缓存失效 → 重新读 index_catalog.puffin            │
   └─────────────────────────────────────────────────────────────┘

2. 快速判断 "索引状态是否变更" (无需比较整个 registry 内容)
   ┌─────────────────────────────────────────────────────────────┐
   │ Snapshot S1011:  registry-version = 17                      │
   │ Snapshot S1015:  registry-version = 18                      │
   │ → 17 ≠ 18 → S1015 有索引状态变更                              │
   │    (如果 17 = 17 → 两个 snapshot 的索引状态完全一致)           │
   └─────────────────────────────────────────────────────────────┘

3. 并发冲突检测 (可选)
   ┌─────────────────────────────────────────────────────────────┐
   │ 写入方读到 version=17, 修改后写入 version=18                  │
   │ 如果在写入时发现远程 version 已经变成 18 (被其他人改了)         │
   │   → 检测到冲突 → 重试                                        │
   └─────────────────────────────────────────────────────────────┘

version 17 意味着什么?
  这是该表创建自定义索引以来, registry 经历了 17 次变更。
  例如: 3 次全量构建 + 8 次增量构建 + 4 次状态变更 + 2 次 GC = 17

每次变更触发 +1:
  • 新 segment 构建完成 (building → active)
  • 状态变更 (active → stale_partial / remapping / stale_orphan)
  • Rewrite Map 生成完成
  • segment 删除 (GC)
  • group 创建 / 删除

注意: 17 不代表 "当前有 17 个 segment" — 它只是变更计数。
      如果有 52 个 segment, 那是 num_segments_total 的值。
```

### 3.9 index_catalog.puffin — 完整 Puffin 文件格式

`index_catalog.puffin` 本身也是一个标准 Puffin 文件。下面是它的完整文件布局：

```
┌──────────────────────────────────────────────────────────────────┐
│              index_catalog.puffin 完整文件布局                     │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Offset 0    ┌──────────────────┐                                │
│              │  Magic (4 bytes) │  0x50 0x46 0x41 0x31 ("PFA1")  │
│  Offset 4    ├──────────────────┤                                │
│              │                  │                                │
│              │  Blob₀            │  ← 核心: 索引注册表             │
│              │  (变长, ~3-12KB)   │     type = puffin.idx          │
│              │                   │            .registry-v1        │
│              │  [Registry 二进制] │     Zstd 压缩                  │
│              │                   │                                │
│              ├──────────────────┤                                │
│              │  Blob₁ (可选)      │  ← 审计日志 / 变更历史         │
│              │  type=audit-log   │     (未来扩展)                  │
│              ├──────────────────┤ ← Footer 开始                   │
│              │  Magic (4B)      │  0x50 0x46 0x41 0x31            │
│              ├──────────────────┤                                │
│              │  FooterPayload    │  UTF-8 JSON, 可选 LZ4 压缩     │
│              │  (变长, ~500B)     │  FileMetadata 对象              │
│              ├──────────────────┤                                │
│              │  FooterPayloadSize│  i32 LE                        │
│              ├──────────────────┤                                │
│              │  Flags (4 bytes)  │  bit 0 = LZ4 压缩标志          │
│              ├──────────────────┤                                │
│              │  Magic (4B)      │  0x50 0x46 0x41 0x31            │
│              └──────────────────┘                                │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

#### Blob₀: 索引注册表 (puffin.idx.registry-v1)

Blob 数据区（Zstd 压缩前）的二进制格式在 §3.8 已详细描述。此处关注 **Puffin 层面的元数据**。

**Blob 的 Footer JSON 条目**：

```json
{
  "type": "puffin.idx.registry-v1",
  "fields": [],
  "snapshot-id": 8027658604211071520,
  "sequence-number": 11,
  "offset": 4,
  "length": 3072,
  "compression-codec": "zstd",
  "properties": {
    "num-groups": "5",
    "num-segments": "52",
    "registry-version": "17",
    "registry-format-version": "2",
    "total-puffin-files": "52",
    "total-index-bytes": "2684354560",
    "created-by": "puffin-rs/3.2"
  }
}
```

#### Blob₁: 审计日志 (puffin.idx.audit-log-v1)

记录 registry 的变更历史，用于运维排查、监控告警和跨 snapshot 状态 diff。这是**正确性非关键路径**——即使 audit-log 丢失，索引系统仍完全正常工作。

**设计原则**：在 catalog.puffin 内与 registry 共存，写入时截断保留最近 N 条，避免无限增长。

##### 操作类型

```rust
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOp {
    SegmentCreated      = 0,  // 新构建完成 (全量/增量)
    StatusChanged       = 1,  // active→stale_partial / stale_partial→remapping / etc.
    SegmentDeprecated   = 2,  // 被新 segment 取代 (重建/defragment)
    SegmentDeleted      = 3,  // GC 清理
    GroupCreated        = 4,  // 新索引组
    GroupDropped        = 5,  // 删除索引组
    RewriteMapGenerated = 6,  // Rewrite Map 生成完成
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditTrigger {
    CowUpdate         = 0,   // CoW UPDATE 触发的 stale 标记
    Compaction        = 1,   // Compaction 触发的 stale 标记
    AnalyzeTable      = 2,   // ANALYZE TABLE 触发的构建/增量
    BackgroundRebuild = 3,   // 后台 staleness 超阈值触发
    DropIndex         = 4,   // DROP INDEX 用户操作
    Gc                = 5,   // 垃圾回收
    Defragment        = 6,   // 段合并
}
```

##### 单条记录格式

```
audit-log-v1 entry (变长, 每条约 100-150 bytes):
┌──────────────────────────────────────────────────────────┐
│  timestamp_ms:     i64  (8 bytes, epoch ms)             │
│  op:               u8   (AuditOp 枚举)                   │
│  trigger:          u8   (AuditTrigger 枚举)              │
│  group_uuid:       36 bytes (NULL-terminated)            │
│  segment_uuid:     36 bytes (NULL-terminated, 或全零)    │
│                                                          │
│  // 以下字段取决于 op 类型:                               │
│  // STATUS_CHANGED 时:                                   │
│  old_status:       u8                                    │
│  new_status:       u8                                    │
│  staleness_ratio:  f32  (4 bytes, 仅当 new_status 涉及    │
│                           stale/remap 时有效)             │
│  // SEGMENT_CREATED 时:                                  │
│  segment_type:     u8   (0=full, 1=incremental)          │
│  puffin_path_len:  u16                                   │
│  puffin_path:      变长 (相对路径)                        │
│  // SEGMENT_DEPRECATED 时:                                │
│  replaced_by_seg_uuid: 36 bytes                          │
│                                                          │
│  // 所有 op 都有:                                        │
│  snapshot_id:      i64  (8 bytes, 当前操作的 snapshot)    │
│  reason_len:       u16  (2 bytes)                        │
│  reason:           变长 (人类可读, UTF-8)                  │
│    示例: "CoW UPDATE rewrote file_0,file_50 → file_1000, │
│            file_1001; staleness_ratio=0.004"              │
│    示例: "Compaction merged files 1-49 → file_1001;       │
│            staleness_ratio accumulated to 0.10"           │
│    示例: "ANALYZE TABLE: incremental build for field=1    │
│            on 500 new files"                              │
└──────────────────────────────────────────────────────────┘
```

##### Blob 整体格式

```
audit-log-v1 Blob 二进制 (Zstd 压缩前):
┌──────────────────────────────────────────────────────────┐
│  Header (12 bytes)                                       │
│  ├── magic:          0x4155 ("AU")    (2 bytes)         │
│  ├── version:        1                (2 bytes)         │
│  ├── num_entries:    127              (4 bytes LE)      │
│  ├── max_entries:    500              (4 bytes LE)      │
│  │   ↑ 超过此值时, 写入时从头部截断最旧的 entries         │
│  ├── oldest_ts_ms:   1718000000000    (8 bytes, i64)    │
│  └── newest_ts_ms:   1718200000000    (8 bytes, i64)    │
├──────────────────────────────────────────────────────────┤
│  Entries[] (按时间升序排列, 最旧在前, 最新在后)            │
│  └── [entry₀] [entry₁] ... [entry₁₂₆]                   │
│                                                          │
│  127 条 × ~120B ≈ 15KB (Zstd 压缩后 ~4KB)                │
└──────────────────────────────────────────────────────────┘
```

##### 使用场景

```
场景 1: 运维排查 "查询为什么突然慢了?"
  → 读 audit-log, 过滤 STATUS_CHANGED 事件
  → 最近一条: timestamp=17:23:15, group=order_id_btree,
    trigger=COW_UPDATE, reason="...rewrote file_0..."
  → 直接定位根因 (无需翻历史 snapshots)

场景 2: 监控告警 "索引重建频率异常"
  → 读 audit-log, 统计最近 24h 的 SEGMENT_DEPRECATED 事件
  → group=order_id_btree: 3 次/24h → 正常
  → group=customer_id_btree: 12 次/24h → 异常 → 告警

场景 3: 跨 snapshot 索引状态 diff
  → snapshot 1005 的 catalog.puffin 中 audit-log newest_ts_ms = t1
  → snapshot 1007 的 catalog.puffin 中:
      过滤 timestamp_ms > t1 的 entries → 这段时间内所有变更

场景 4: GC 安全确认
  → 读 audit-log, 过滤 SEGMENT_DELETED
  → 确认 segment s0-xxx 已记录删除事件 → 可以物理删除 Puffin 文件
```

##### 生命周期与截断

```
每次写 catalog.puffin 时:
  1. 将本次变更追加到 audit-log (1 条或多条)
  2. 如果 num_entries > max_entries (500):
       截断最旧的 entries, 始终保留最近 500 条
       oldest_ts_ms 更新为截断后最旧条目的时间戳
  3. 与 registry Blob 一起写入 catalog.puffin

截断后的数据永久丢失 — audit-log 不保留完整历史
(如果需要完整历史, 从 Iceberg snapshot 的时间线
 逐个读取历史 catalog.puffin 文件来重建)
```

#### Footer Payload JSON (FileMetadata)

```json
{
  "blobs": [
    {
      "type": "puffin.idx.registry-v1",
      "fields": [],
      "snapshot-id": 8027658604211071520,
      "sequence-number": 11,
      "offset": 4,
      "length": 3072,
      "compression-codec": "zstd",
      "properties": {
        "num-groups": "5",
        "num-segments": "52",
        "registry-version": "17",
        "registry-format-version": "2",
        "blob-role": "primary",
        "total-puffin-files": "52",
        "total-index-bytes": "2684354560"
      }
    },
    {
      "type": "puffin.idx.audit-log-v1",
      "fields": [],
      "snapshot-id": 8027658604211071520,
      "sequence-number": 11,
      "offset": 3076,
      "length": 4096,
      "compression-codec": "zstd",
      "properties": {
        "blob-role": "metadata",
        "num-entries": "127",
        "max-entries": "500",
        "oldest-entry-ms": "1718000000000",
        "newest-entry-ms": "1718200000000",
        "oldest-snapshot-id": "1001",
        "newest-snapshot-id": "1011"
      }
    }
  ],
  "properties": {
    "created-by": "puffin-rs/3.2",
    "puffin-type": "index-catalog",
    "index-catalog-version": "2",
    "description": "自定义索引注册表 — TableMetadata.statistics[] 的唯一指针目标"
  }
}
```

#### TableMetadata.statistics[] 中的对应条目

```json
{
  "statistics": [
    {
      "snapshot-id": 8027658604211071520,
      "statistics-path": "s3://bucket/db/orders/metadata/index_catalog.puffin",
      "file-size-in-bytes": 6656,
      "file-footer-size-in-bytes": 512,
      "blob-metadata": [
        {
          "type": "puffin.idx.registry-v1",
          "source-snapshot-id": 8027658604211071520,
          "source-snapshot-sequence-number": 11,
          "fields": [],
          "properties": {
            "num-groups": "5",
            "num-segments": "52",
            "registry-version": "17",
            "registry-checksum": "sha256:abc123..."
          }
        }
      ]
    }
  ]
}
```

#### 文件整体大小估算

| 组件 | 大小 (压缩前/后) |
|------|-----------------|
| Magic (头 + Footer ×2) | 12 bytes |
| Blob₀: registry 二进制 | ~10KB / ~3KB (Zstd) |
| Blob₁: audit-log (最近 500 条) | ~50KB / ~12KB (Zstd) |
| FooterPayload JSON | ~1KB / ~400B (LZ4) |
| FooterPayloadSize + Flags | 8 bytes |
| **总计** | **~16KB** |

> 即使 10 个索引组 × 每个 20 个 segment = 200 segment，registry 也仅 ~40KB（压缩后 ~12KB），audit-log 上限控制在 ~50KB（压缩后 ~12KB）。catalog.puffin 整体 < 30KB，每次 commit 重写开销可忽略。

---

# 第二部分：索引组织

## 4. 索引组与索引段

### 4.1 核心概念

```
一个逻辑索引 (IndexGroup) = 1 个或多个物理索引段 (IndexSegment)

IndexGroup (uuid="a1b2c3d4-...", field=1, type="btree")
│
├── IndexSegment[0] (uuid="s0-xxx", built_at_snapshot=1001)
│   Puffin: snap-1001-2-idx1.puffin
│   覆盖: file_id ∈ {0..499}
│   状态: active
│
├── IndexSegment[1] (uuid="s1-yyy", built_at_snapshot=1003)
│   Puffin: snap-1003-3-idx1delta.puffin
│   覆盖: file_id ∈ {500..999}
│   delta-from-segment: "s0-xxx"
│   状态: active
│
└── IndexSegment[2] (uuid="s2-zzz", built_at_snapshot=1007)
    Puffin: snap-1007-5-idx1delta2.puffin
    覆盖: file_id ∈ {1000..1499}
    delta-from-segment: "s1-yyy"
    状态: active
```

### 4.2 index-group-uuid 的职责边界

```
同一个 Puffin 文件内:
  不需要 UUID 来分组! 整个文件就是一个 segment
  用 blob-role (primary/metadata) 区分角色

跨 Puffin 文件:
  UUID 用于链接:
    delta-from-group-uuid: 增量链 — 新 segment → 旧 segment
    affected-index-groups: Rewrite Map 影响范围
    deprecates-group-uuids: 全量重建替代旧索引
```

使用位置：
| 位置 | 用途 |
|------|------|
| Puffin Footer properties | 标识"这个文件属于哪个索引组" |
| index_catalog.puffin registry | **唯一权威的索引发现目录 + 状态存储** |
| TableMetadata.statistics[] | 仅存放指向 index_catalog.puffin 的指针 |
| delta-from-group-uuid | 增量链: 新索引→旧索引 |
| affected-index-groups | Rewrite Map 影响范围 (逗号分隔 UUID) |
| deprecates-group-uuids | 全量重建取代旧索引 |

### 4.3 一索引一文件策略

**核心结论**：一个 Puffin 文件 = 恰好一个 IndexSegment。大索引（BTree/Bitmap/Bloom >1MB）必须独立文件。

**原因**：
1. **文件膨胀**：共享 Puffin → 98MB；独立 → 每个 16-32MB
2. **写放大**：增量只需写 3.3MB（vs 重写 98MB）；写放大 1/30
3. **生命周期解耦**：DROP INDEX → 直接删除对应文件（vs 重写/标记垃圾）

**例外**：NDV 统计（Theta Sketch + HLL Sketch）可以合并，需满足：完全相同的文件覆盖范围 + 完全相同的生命周期 + 总大小 < 1MB。

### 4.4 目录结构

```
s3://bucket/warehouse/db/orders/metadata/
├── v1.metadata.json
├── index_catalog.puffin               ← 索引注册表 (中枢, ~3-12KB)
├── snap-1001-1-abc.avro              ← Manifest List
├── snap-1001-2-idx1.puffin           ← Index-A Segment[0] (col=1, files 0-499, 16MB)
├── snap-1003-3-idx2.puffin           ← Index-B (col=2, files 0-999, 32MB)
├── snap-1003-3-idx1delta.puffin      ← Index-A Segment[1] (col=1, files 500-999, 3.3MB)
├── snap-1007-4-rewrite.puffin        ← Rewrite Map
├── snap-1011-6-idx1delta2.puffin     ← Index-A Segment[2] (col=1, spec_id=1, 5MB)
└── m1-xxxx.avro                      ← Manifest File
```

### 4.5 文件名约定

```
格式: {prefix}-{snapshotId}-{seqNum}-{type}{suffix}.puffin

示例:
  snap-1001-2-idx1.puffin       ← Index-A, snapshot 1001
  snap-1003-3-idx1delta.puffin  ← Index-A delta, snapshot 1003
  snap-1007-4-rewrite.puffin    ← Rewrite Map, snapshot 1007
```

---

## 5. Blob 类型体系

### 5.1 命名约定

```
格式: <org>.<project>-<index-type>-v<version>
示例: puffin.idx.btree-v1
```

### 5.2 完整类型列表

| Blob Type | 粒度 | 角色 | 用途 |
|-----------|------|------|------|
| `apache-datasketches-theta-v1` | 表/分区级 | primary | NDV 估算（内置） |
| `deletion-vector-v1` | 文件内行级 | primary | 行级删除向量（内置） |
| `puffin.idx.btree-v1` | Row Group 级 | primary | Range / EQ 过滤 + 行定位 |
| `puffin.idx.bloom-v1` | 文件组级 | primary | EQ / IN 快速排除 |
| `puffin.idx.bitmap-v1` | 文件内行级 | primary | 低基数列精确行定位 |
| `puffin.idx.filelist-v1` | — | metadata | 该 segment 覆盖的文件路径列表（含 hash 和行数） |
| `puffin.idx.partition-bitmap-v1` | — | metadata | 分区值 → Page 范围映射 |
| `puffin.idx.rewritemap-v1` | — | primary | 文件重映射 (old_addr → new_addr) |
| `puffin.idx.registry-v1` | — | primary | **索引注册表** — 所有 group/segment 的目录和状态 |
| `puffin.idx.audit-log-v1` | — | metadata | **审计日志** — 索引状态变更历史（最近 500 条，上限截断） |

### 5.3 索引能力矩阵

| Blob Type | 过滤能力 | 回表粒度 | 适用列基数 | 索引大小估算 |
|-----------|---------|----------|-----------|-------------|
| `bloom-v1` | EQ, IN | 文件级 | >1000 | N × 1 byte |
| `bitmap-v1` | EQ, IN, IS_NULL | 行级(精确) | <10000 | N × distinct × 1bit |
| `btree-v1` | Range, EQ | Row Group 级 | 任意 | N × log(N) × 12B |
| `theta-v1` | NDV 估算 | — | 任意 | ~16KB (固定) |

---

## 6. 索引状态机

### 6.1 完整状态机

```
                    ┌─────────────┐
                    │  BUILDING   │  ← 构建中, 查询不可用
                    └──────┬──────┘
                           │ 构建完成
                           ▼
                    ┌─────────────┐
         ┌──────────│   ACTIVE    │──────────┐
         │          │ (ratio=0)   │          │
         │          └──────┬──────┘          │
         │                 │                  │
         │   文件变更        │   文件变更        │
         │   ratio>0        │   ratio>=0.1     │
         │   but<0.1        │                  │
         ▼                 ▼                  ▼
┌──────────────────┐  ┌──────────────┐  ┌──────────────┐
│  STALE_PARTIAL   │  │  REMAPPING   │  │STALE_ORPHAN  │
│ (跳过stale文件)   │  │ (Rewrite Map │  │ (需Rebuild或  │
│ 查询可用 ✓        │  │  转译)       │  │ Rewrite Map) │
│                  │  │ 查询可用 ✓    │  │ 查询不可用 ✗   │
└────────┬─────────┘  └──────┬───────┘  └──────┬───────┘
         │                   │                  │
         │  如果ratio>=0.1    │  ratio>=0.3      │  生成Rewrite Map
         ▼                   ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  REMAPPING   │   │  REBUILDING  │   │  REMAPPING   │
└──────────────┘   └──────┬───────┘   └──────────────┘
                          │ 重建完成
                          ▼
                   ┌──────────────┐
                   │   ACTIVE     │
                   │ (新segment)  │
                   │ 旧→DEPRECATED│
                   └──────────────┘

  被新 segment 取代 → DEPRECATED → DELETED (GC)
```

### 6.2 状态速查

| 状态 | 查询可用? | 性能 | 说明 |
|------|----------|------|------|
| `building` | ❌ | — | 构建中，数据不完整 |
| `active` | ✅ | 全速 | staleness_ratio = 0 |
| `stale_partial` | ✅ | 略慢 | ratio < 0.1，跳过少量 stale 文件即可 |
| `remapping` | ✅ | 稍慢 | 有 Rewrite Map，需查表转译 |
| `stale_orphan` | ❌ | — | ratio ≥ 0.3 或无 Rewrite Map |
| `rebuilding` | ❌ | — | 正在重建 |
| `deprecated` | ❌ | — | 已被新 segment 替代 |
| `deleted` | ❌ | — | 可 GC |

### 6.3 异步现实

所有索引操作都是异步的。关键的危险窗口处理：

```
CoW UPDATE 完成 → 文件已重写 → 索引已失效
     │
     ├── Rewrite Map 生成中… (可能几秒到几分钟)
     │   ← [危险窗口] segment 被标记为 stale_orphan
     │     查询跳过此 segment, 回退全扫描
     │     数据不丢失! 仅性能暂时下降
     │
     └── Rewrite Map 就绪 → segment 状态 → remapping
         ← [窗口结束] 索引恢复可用 (通过查表)
```

**安全保证**：当 status ≠ active/remapping/stale_partial 时，引擎回退到 Manifest Stats + 全扫描。**不会丢数据，只影响性能。**

### 6.4 状态存储位置

| 存储位置 | 权威性 | 用途 |
|---------|--------|------|
| index_catalog.puffin registry Blob | **权威** | 查询时判断索引可用性；原子更新 |
| TableMetadata.statistics[] | 指针 | 指向 index_catalog.puffin 路径 |
| 各索引 Puffin Footer properties | 写入快照 | 交叉验证（可能过时） |

状态变更流程：重写 index_catalog.puffin → 更新 TableMetadata 中的 registry 指针（version/file-size） → 原子 commit。各索引 Puffin 文件不重写。

### 6.5 状态变更的临界点

```rust
/// staleness_ratio 阈值
/// < 0.1: ACTIVE 或 STALE_PARTIAL → 直接跳过 stale 文件
/// ≥ 0.1: 触发 Rewrite Map 生成 → REMAPPING
/// ≥ 0.3: 触发异步重建 → REBUILDING
pub const STALENESS_REMAP_THRESHOLD: f64   = 0.1;
pub const STALENESS_REBUILD_THRESHOLD: f64 = 0.3;
```

---

# 第三部分：生命周期时间线

## 7. T0-T8 完整时间线

以下使用一个具体的分区表来演示索引的完整生命周期：

```
表 Schema:
  field-id 1: order_id    (long)
  field-id 2: customer_id (long)
  field-id 3: status      (string)
  field-id 5: created_at  (timestamp)

分区: day(created_at), partition_spec_id=0, partition_field_id=1000

文件命名: file_id 稳定递增, 每个 Parquet 约 2048 行, 2 个 Row Group
索引命名: index-group-uuid = UUID, 贯穿整个生命周期
```

### T0: 创建分区表

```sql
CREATE TABLE orders (
  order_id    BIGINT,
  customer_id BIGINT,
  status      STRING,
  created_at  TIMESTAMP
) PARTITIONED BY (day(created_at));
```

TableMetadata (v1): current-snapshot-id=-1, snapshots=[], statistics=[]

### T1: INSERT 第一批数据

```
INSERT INTO orders ... → 2026-01-01 ~ 2026-01-10
→ file_id: 0~499 (每天 50 文件), 共 ~1,024,000 行
```

目录新增 `data/created_at_day=2026-01-01/` … `2026-01-10/`。TableMetadata (v2): current-snapshot-id=1001。

### T2: CREATE INDEX (异步构建)

```sql
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS order_id;
→ 触发异步构建 Index-A (col=1, files 0-499)
→ Index Group UUID: a1b2c3d4-...
```

**T2a: 构建中 (status=building)**

TableMetadata 的 statistics[] 指针 + index_catalog.puffin 新增 building 条目。此时任何查询都不会使用该索引。

```json
// TableMetadata (v3) — statistics[]:
{
  "statistics": [
    {
      "snapshot-id": 1001,
      "statistics-path": "s3://.../metadata/index_catalog.puffin",
      "file-size-in-bytes": 2048,
      "blob-metadata": [{
        "type": "puffin.idx.registry-v1",
        "properties": { "num-groups": "1", "num-segments": "1",
                        "registry-version": "1" }
      }]
    }
  ]
}

// index_catalog.puffin — registry Blob 内容:
//   Group a1b2c3d4:
//     Segment s0-xxx: path="snap-1001-2-idx1.puffin", status="building",
//                     file-range="0-499", built-at-snapshot=1001
```

**T2b: 构建完成 (status=active)**

索引 Puffin 文件 (`snap-1001-2-idx1.puffin`, 16MB) + 新的 index_catalog.puffin 写入完成 → commit。segment.status → active。

### T3: INSERT 第二批数据

```
INSERT INTO orders ... → 2026-01-11 ~ 2026-01-20
→ file_id: 500~999 (500 个新文件)
```

TableMetadata (v4): current-snapshot-id=1003。**Puffin 文件不变**。Index-A 的 filelist 仍然只含第一批 500 个文件路径。

查询时：catalog 从 index_catalog.puffin 发现 Index-A Segment[0] 覆盖 files 0-499（active），files 500-999 无索引 → 回退 Manifest Stats。索引不要求全覆盖。

### T4: 第二个索引 + 增量索引 (异步)

```sql
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, order_id;
```

**T4a: 构建中**

index_catalog.puffin 新增两条 building segment 条目：
- Index-B (col=2, 全量, building): group `b2c3d4e5-...`
- Index-A-delta (col=1, 增量, building): group `c3d4e5f6-...`, `delta-from-seg: s0-xxx`
- Index-A (group `a1b2c3d4-...`): 不变 (segment s0-xxx 保持 active)

TableMetadata.statistics[] 始终只有 1 条 registry 指针，`registry-version` 递增。

**T4b: 构建完成**

目录新增：
- `snap-1003-3-idx2.puffin` (Index-B, 32MB)
- `snap-1003-3-idx1delta.puffin` (Index-A-delta, 3.3MB)
- `index_catalog.puffin` (更新, ~4KB, registry-version=3)

三个索引组的逻辑结构：
```
field=1 (order_id):
  Index-A (a1b2c3d4):        files 0-499,   status=active
    ↓ delta-from
  Index-A-delta (c3d4e5f6):  files 500-999, status=active

field=2 (customer_id):
  Index-B (b2c3d4e5):        files 0-999,   status=active
```

### T5: DELETE (Merge-on-Read)

```sql
DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2026-01-05';
→ 影响 files 0-199 (写入 positional delete files)
```

**索引完全不受影响。** MoR DELETE 写入 delete file，查询引擎在读取 DataFile 时检查 delete file → 索引无需感知。

### T6: UPDATE (Copy-on-Write) — 危险窗口

```sql
UPDATE orders SET status = 'priority' WHERE order_id IN (1, 2, 3);
→ 这 3 行在 file_id=0 → CoW 重写为 file_id=1000
```

**T6a: UPDATE COMMIT（同步标记 stale_orphan）**

在 UPDATE 提交的同一事务中，更新 index_catalog.puffin，将受影响 segment 标记为 `stale_orphan`：

```
// index_catalog.puffin registry 更新:
//   Group a1b2c3d4 Segment s0-xxx:
//     status: active → stale_orphan
//     pre-stale-status: active
//     stale-file-paths: "part-00001.parquet"
//     file-staleness-ratio: 0.002
//   Group b2c3d4e5 Segment s?-xxx:
//     同样受影响（覆盖了 file 0）
//   Group c3d4e5f6 Segment s1-yyy:
//     不受影响（覆盖 files 500+）
```

危险窗口内的查询：
- Index-A Segment[0]: status=stale_orphan → INDEX_SKIP ✗
- Index-A-delta Segment[1]: status=active → INDEX_SAFE_TO_USE ✓
- **数据不丢失，仅性能暂时下降** ✓

**T6b: Rewrite Map 异步生成完成**

后台任务生成 Rewrite Map → `snap-1007-4-rewrite.puffin`。更新 index_catalog.puffin：

```
// index_catalog.puffin registry 更新:
//   Group a1b2c3d4 Segment s0-xxx:
//     status: stale_orphan → remapping
//     rewrite-map-path: "snap-1007-4-rewrite.puffin"
//   (新增 Rewrite Map segment 条目)
```

危险窗口结束后的查询：
- BTree 返回 row_addr = (file_id=0, rg=0, row=41)
- Rewrite Map: file_id=0 → file_id=1000
- 读取 file 1000, row 41 ✓

**T6c: staleness 评估**

staleness_ratio = 0.002（仅 1 个文件失效）< 0.3 → **不触发重建**。索引保持在 remapping 状态。

### T7: Compaction

```
Compaction: files 1-49 → file 1001

T7a: Compaction COMMIT (同步)
  → Index-A status: "remapping" → "stale_orphan"
    stale-file-ids: "0,1-49", file-staleness-ratio: 0.10
  → Index-B 同样

T7b: 新的累积 Rewrite Map 生成 (异步)
  → Index-A status: "stale_orphan" → "remapping"

T7c: staleness 评估
  ratio=0.10 (< 0.3) → 不触发重建
```

### T8: 分区演进

```sql
ALTER TABLE orders SET PARTITION SPEC (month(created_at));
→ partition_spec_id = 1
INSERT INTO orders ... (2026-01-21 ~ 2026-01-31, spec_id=1)
→ file_id: 1002~1551
```

增量索引 `snap-1011-6-idx1delta2.puffin` (spec_id=1) + 更新 `index_catalog.puffin`：
- partition-bitmap Blob 标记 `partition-spec-id: 1`
- 查询时通过 `partition_spec_id` 自动匹配正确的 segment

---

# 第四部分：文件覆盖与部分失效

## 8. 文件覆盖与文件列表

### 8.1 设计：直接存储文件路径

v3.3 起，文件覆盖信息**直接存储文件路径**，不再使用中间层（file_index / RoaringBitmap）。

```
设计动机:
  • 简化数据模型: 路径 → 路径, 无需 file_index 中间映射
  • 消除映射层: 不再需要 file_index_map / hash_to_index 翻译
  • 足够高效: 500 个路径 (~120B each) ≈ 60KB, Zstd 压缩后 ~15KB
  • RowAddress 仍用 file_path_hash (u64): BTree key 需要定长整数
```

### 8.2 单层文件标识设计

```
┌──────────────────────────────────────────────────────────────┐
│                   文件标识的单层设计                            │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  文件追踪层 (segment 内): file_path (String) — 直接存储       │
│  ═══════════════════════════════════════════════════════════  │
│                                                              │
│    构建时记录覆盖的文件路径:                                   │
│      original_files: {"part-00001.parquet",                  │
│                       "part-00002.parquet", ...}             │
│      stale_files:    {"part-00001.parquet"}  // 已失效       │
│                                                              │
│    集合运算: HashSet<String> — 足够高效                       │
│      contains / insert / remove — 均摊 O(1)                  │
│      500 个文件的交/并/差 在微秒级                             │
│                                                              │
│  RowAddress 层: file_path_hash (u64) — 跨快照稳定             │
│  ═══════════════════════════════════════════════════════════  │
│                                                              │
│    BTree entry 中: row_addr = (file_path_hash, rg, row)      │
│    不随文件重写/重排而变化                                     │
│    hash → path 通过 FileRegistry / hash_to_path 查询          │
│                                                              │
│  映射层 (segment 内): hash_to_path (HashMap<u64, String>)    │
│  ═══════════════════════════════════════════════════════════  │
│                                                              │
│    RowAddress 返回 (hash, rg, row)                            │
│    → hash_to_path[hash] → file_path                          │
│    → original_files.contains(file_path) → 覆盖检查            │
│    → stale_files.contains(file_path) → 失效检查               │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

| | file_path (String) | file_path_hash (u64) |
|---|---|---|
| 作用域 | segment 内部 (文件追踪) | BTree index entry / RowAddress |
| 数据结构 | `HashSet<String>` | u64 标量 |
| 产生方式 | Manifest 文件路径 | XXH64(file_path) |
| 稳定性 | 跨快照永久稳定 | 跨快照永久稳定 |
| 用途 | 覆盖检查、失效标记、比率计算 | 行定位、回表、Rewrite Map 入口 |

### 8.3 filelist Blob 格式（v3.3 — 直接路径存储）

```
puffin.idx.filelist-v1 二进制:
┌──────────────────────────────────────────────────────────┐
│  Header                                                   │
│  ├── magic:           0x464C ("FL")    (2 bytes)         │
│  ├── version:         1                (2 bytes)         │
│  ├── num_files:       500              (4 bytes LE)      │
│  └── total_rows:      1024000          (8 bytes LE)      │
├──────────────────────────────────────────────────────────┤
│  File Path List (路径列表 — 构建时写入, 查询时加载)        │
│  └── for each file (0..num_files-1):                     │
│       ├── path_hash:      u64 (8 bytes, XXH64)           │
│       ├── row_count:      i64 (8 bytes)                  │
│       ├── path_len:       u16 (2 bytes)                  │
│       └── file_path:      (变长, UTF-8, 不含路径前缀)      │
│                                                          │
│   500 files × ~130 bytes = ~65KB (Zstd 压缩至 ~15KB)     │
│                                                          │
│  加载后内存结构:                                           │
│    file_paths:  Vec<String>            (按序)             │
│    hash_to_path: HashMap<u64, String>  (RowAddress→路径)  │
│    original_files: HashSet<String>     (覆盖/失效判定)     │
└──────────────────────────────────────────────────────────┘
```

### 8.4 IndexSegment 中的文件覆盖数据结构

```rust
use std::collections::{HashMap, HashSet};

pub struct IndexSegment {
    // ... 其他字段 ...

    // ===== 文件覆盖 (直接用文件路径) =====
    pub original_files: HashSet<String>,   // 构建时覆盖的文件路径集合
    pub stale_files: HashSet<String>,      // 已失效的文件路径子集
    pub staleness_ratio: f64,              // stale_files.len() / original_files.len()

    // ===== hash ↔ path 映射 =====
    pub file_paths: Vec<FileListEntry>,               // 按写入顺序, 含行数
    pub hash_to_path: HashMap<u64, String>,           // RowAddress file_hash → file_path
}

pub struct FileListEntry {
    pub path_hash: u64,     // XXH64(file_path)
    pub file_path: String,  // 完整路径 (相对路径)
    pub row_count: i64,     // 文件行数
}

impl IndexSegment {
    pub fn file_count(&self) -> usize {
        self.file_paths.len()
    }

    pub fn hash_of_path(&self, path: &str) -> u64 {
        xxhash64(path.as_bytes())
    }

    pub fn resolve_path(&self, path_hash: u64) -> Option<&str> {
        self.hash_to_path.get(&path_hash).map(|s| s.as_str())
    }

    pub fn covers_file(&self, path: &str) -> bool {
        self.original_files.contains(path)
    }

    pub fn covers_file_hash(&self, path_hash: u64) -> bool {
        self.hash_to_path.get(&path_hash)
            .map_or(false, |path| self.original_files.contains(path))
    }
}
```

### 8.5 filemap 决策（不变）

**不需要每个 snapshot 独立的 filemap.json。** Manifest 已经是权威文件列表。每个 Puffin 文件自带的 filelist Blob 内含文件路径列表，自包含、自描述。

---

## 9. 部分失效机制

### 9.1 核心数据结构

文件覆盖和失效追踪直接使用文件路径（`HashSet<String>`）：

```rust
use std::collections::HashSet;

// 失效状态寄存在 IndexSegment 内部
// original_files 和 stale_files 直接存文件路径

impl IndexSegment {
    /// 标记失效 — 输入是文件路径, 直接匹配
    pub fn mark_stale_by_paths(&mut self, paths: &[String]) {
        for path in paths {
            if self.original_files.contains(path) {
                self.stale_files.insert(path.clone());
            }
        }
        self.recalc_ratio();
    }

    /// 检查 RowAddress 的结果是否来自 stale 文件
    /// addr 中存的是 file_path_hash, 通过 hash_to_path 找到路径再判断
    pub fn is_stale_addr(&self, addr: &RowAddress) -> bool {
        self.hash_to_path.get(&addr.file_hash())
            .map_or(false, |path| self.stale_files.contains(path))
    }

    /// 检查指定文件路径是否已失效
    pub fn is_path_stale(&self, path: &str) -> bool {
        self.stale_files.contains(path)
    }

    fn recalc_ratio(&mut self) {
        let total = self.original_files.len() as f64;
        self.staleness_ratio = if total > 0.0 {
            self.stale_files.len() as f64 / total
        } else {
            0.0
        };
    }
}
```

### 9.2 失效决策流程

```
CoW UPDATE / Compaction 重写文件 → mark_files_stale()

对每个 IndexSegment:
  1. 输入: rewritten_file_paths (外部文件路径列表)
  2. 直接匹配: 对每个 path, 检查 original_files.contains(path)
  3. 标记: stale_files.insert(path.clone())
  4. 重新计算: staleness_ratio = stale_files.len() / original_files.len()
  5. 如果 ratio >= rebuild_threshold (0.3) → status = stale_orphan
  6. 如果 ratio > 0 且 < remap_threshold (0.1) → status = stale_partial
  7. 通过 JavaBridge 原子更新 TableMetadata
```

### 9.3 查询时的安全处理

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUsability {
    SafeToUse,          // 直接使用
    SafeWithStaleSkip,  // 跳过 stale 文件
    UseWithRemap,       // 需要 Rewrite Map 转译
    Skip,               // 不安全, 跳过
}

/// check_index_usability 的核心逻辑
pub fn check_index_usability(seg: &IndexSegment) -> IndexUsability {
    if seg.status == "active" && seg.staleness_ratio == 0.0 {
        return IndexUsability::SafeToUse;
    }

    if seg.status == "stale_partial" {
        return IndexUsability::SafeWithStaleSkip;
        // BTree 返回结果后用 is_stale_addr() 过滤
    }

    if seg.status == "remapping" && seg.rewrite_map_available {
        return IndexUsability::UseWithRemap;
    }

    // stale_orphan, rebuilding, building, deprecated → 回退全扫描
    IndexUsability::Skip
}

// -------------------------------------------------------
// 查询结果过滤 (SafeWithStaleSkip 模式):
//   BTree.search(42) → {row_addr_1, row_addr_2, row_addr_3}
//   row_addr_1: seg.is_stale_addr(&addr) → true  → 丢弃
//   row_addr_2: seg.is_stale_addr(&addr) → false → 保留, 回表
//   row_addr_3: seg.is_stale_addr(&addr) → false → 保留, 回表
//
// is_stale_addr 内部:
//   addr.file_hash() → hash_to_path[&hash] → file_path
//   → stale_files.contains(file_path) ? → 决策
// -------------------------------------------------------
```

**关键安全保证**：当 `Skip` 时，引擎回退到 Manifest Stats + 全扫描。不会丢数据。

---

## 10. Rewrite Map 设计

### 10.1 格式

```
puffin.idx.rewritemap-v1 Blob 二进制布局:
┌──────────────────────────────────────────────────────────┐
│  Header                                                   │
│  ├── magic:           0x524D ("RM")    (2 bytes)         │
│  ├── version:         1                (2 bytes)         │
│  ├── num_mappings:    N                (4 bytes LE)      │
│  └── flags:           0                (4 bytes)         │
├──────────────────────────────────────────────────────────┤
│  Mapping Entries (区间映射)                                │
│  └── for each mapping:                                    │
│       ├── old_file_hash:     u64       (8 bytes)         │
│       ├── old_start_row:     u32       (4 bytes)         │
│       ├── old_end_row:       u32       (4 bytes)         │
│       ├── new_file_hash:     u64       (8 bytes)         │
│       ├── new_start_row:     u32       (4 bytes)         │
│       ├── old_file_path_len: u16                          │
│       ├── old_file_path:     (变长)                       │
│       ├── new_file_path_len: u16                          │
│       └── new_file_path:     (变长)                       │
└──────────────────────────────────────────────────────────┘
```

使用区间映射而非逐行映射：典型 Compaction 场景中映射是连续区间，一行区间比逐行映射紧凑数千倍。

**注意**：Rewrite Map 的 key/value 使用 `file_path_hash`（跨 segment 稳定），路径解析通过 `hash_to_path` 完成。

### 10.2 RewriteMap 数据结构

```rust
use std::collections::HashMap;

pub struct RewriteMap {
    /// 区间映射列表
    pub mappings: Vec<RewriteMapping>,
    /// old_file_hash → mapping index 加速查找
    hash_index: HashMap<u64, Vec<usize>>,
}

pub struct RewriteMapping {
    pub old_file_hash: u64,
    pub old_start_row: u32,
    pub old_end_row: u32,
    pub new_file_hash: u64,
    pub new_start_row: u32,
    /// adjusted row = new_start_row + (query_row - old_start_row)
}

impl RewriteMap {
    /// 尝试转译 RowAddress — 如果命中映射, 返回新地址
    pub fn remap(&self, addr: &RowAddress) -> Option<RowAddress> {
        let indices = self.hash_index.get(&addr.file_hash())?;
        for &i in indices {
            let m = &self.mappings[i];
            let row = addr.row_in_rg() as u32;
            if row >= m.old_start_row && row <= m.old_end_row {
                let adjusted_row = m.new_start_row + (row - m.old_start_row);
                return Some(RowAddress::create(
                    m.new_file_hash,
                    addr.rg_index(),
                    adjusted_row as u16,
                ));
            }
        }
        None
    }
}
```

### 10.3 使用方式

```
查询时 (REMAPPING 模式):
  旧索引返回 row_addr = (old_file_hash, rg, row)
  → Rewrite Map: old_file_hash → new_file_hash
  → 转换: (new_file_hash, rg, adjusted_row)
  → 读取新文件

异步重建时 (后台):
  遍历旧 segment 的 file_paths:
    for each entry in file_paths:
      if stale_files.contains(&entry.file_path):  // 该文件已失效
        查找 Rewrite Map: entry.path_hash → new_hash, row_range
        重新构建索引条目
  → 写入新索引文件
  → 旧 Rewrite Map 标记 deprecated
```

### 10.4 完整翻译路径

```
从 "文件被重写" 到 "索引结果被正确转译" 的完整链路:

═══════════════════════════════════════════════════════════════

Step 1: 标记失效 (segment 内部, 直接路径匹配)
  CoW UPDATE 重写 part-00001.parquet → part-01000.parquet
  → seg.original_files.contains("part-00001.parquet") → true
  → seg.stale_files.insert("part-00001.parquet".into())

Step 2: 生成 Rewrite Map (跨 segment, file_path_hash)
  读 Manifest: 发现 part-00001 被 part-01000 替代
  → entry: {old_hash: 0xA3F2..., new_hash: 0xBEEF...,
            old_range: [0, 2047], new_range: [0, 2047]}

Step 3: 查询转译 (RowAddress 层)
  BTree 返回 row_addr = (0xA3F2..., rg=0, row=41)
  → Rewrite Map 查表: 0xA3F2... → 0xBEEF..., row 不变
  → 新地址: (0xBEEF..., rg=0, row=41)
  → FileRegistry::resolve(0xBEEF...) → "part-01000.parquet"
  → 读 Parquet ✓

═══════════════════════════════════════════════════════════════

关键: 文件追踪在 segment 内部直接用路径 (HashSet<String>),
      file_path_hash 在跨 segment 场景使用 (RowAddress, Rewrite Map),
      hash_to_path (HashMap<u64, String>) 完成 hash→path 翻译
```

---

# 第五部分：分区感知与回表

## 11. 分区感知索引

### 11.1 执行顺序

索引评估在分区裁剪**之后**执行（不可颠倒）：

```
Partition Pruning (Manifest List)   → O(manifest_count), 过滤 ~99%
  ↓
File Pruning (Manifest Entry stats) → O(candidate_files), 过滤 ~99%
  ↓
Index Evaluation (Puffin)           → 仅对少量剩余文件
  ↓
RowGroup Pruning (Parquet Footer)
  ↓
实际读取 Parquet 数据
```

### 11.2 partition-bitmap 设计

每个分区感知的 Puffin 文件包含一个 `partition-bitmap-v1` Blob：

```
puffin.idx.partition-bitmap-v1 二进制:
┌──────────────────────────────────────────────────────────┐
│  Header (32 bytes)                                       │
│  ├── magic: 0x5042 ("PB")                                │
│  ├── version: 1, partition_spec_id, num_partition_fields │
│  └── partition_field_id, transform, value_type,          │
│      num_partitions, total_rows, flags                    │
├──────────────────────────────────────────────────────────┤
│  Partition Directory (常驻内存, 每项 24B)                  │
│  ┌──────────────┬──────────┬──────────┬───────────────┐ │
│  │ partition_   │page_start│page_end  │file_list       │ │
│  │ value (编码)  │(u32)     │(u32)     │offset (u32)   │ │
│  ├──────────────┼──────────┼──────────┼───────────────┤ │
│  │ 19723(01-01) │ 0        │ 249      │ 4096          │ │
│  │ 19724(01-02) │ 250      │ 499      │ 4108          │ │
│  │ ...          │ ...      │ ...      │ ...           │ │
│  └──────────────┴──────────┴──────────┴───────────────┘ │
│  365 分区 × 24B = 8,760 bytes                          │
├──────────────────────────────────────────────────────────┤
│  File List Section (按需加载)                              │
│  └── 每个分区下辖的文件路径列表                             │
└──────────────────────────────────────────────────────────┘
```

### 11.3 分区演进

Iceberg 表可同时有多个 partition_spec_id 的文件。索引通过为每个 spec 创建独立的 partition-bitmap Blob 来支持：

```
Puffin 文件中:
  Blob[1]: partition-bitmap for spec_id=0 (DAY partitions)
  Blob[2]: partition-bitmap for spec_id=1 (MONTH partitions, delta-from Blob[1])
```

查询时根据 Manifest 中每个文件的 `partition_spec_id` 匹配对应的 bitmap。

---

## 12. 统一回表设计

### 12.1 RowAddress (128-bit)

```rust
/// 不依赖 Parquet 版本, 不依赖全局 file_id 序列
pub struct RowAddress {
    high: u64,  // [file_path_hash:48 | rg_index:16]
    low: u64,   // [row_in_rg:16 | reserved:48]
}

impl RowAddress {
    const FILE_HASH_BITS: u8 = 48;
    const RG_INDEX_BITS: u8  = 16;
    const ROW_OFF_BITS: u8   = 16;

    pub fn create(file_hash: u64, rg_index: u16, row_in_rg: u16) -> Self {
        RowAddress {
            high: ((file_hash & 0xFFFF_FFFF_FFFF) << 16) | (rg_index as u64),
            low:  row_in_rg as u64,
        }
    }

    pub fn file_hash(&self) -> u64 { (self.high >> 16) & 0xFFFF_FFFF_FFFF }
    pub fn rg_index(&self) -> u16  { self.high as u16 }
    pub fn row_in_rg(&self) -> u16 { self.low as u16 }
}
```

**选择 file_path_hash 而非 file_id 的原因**：跨快照稳定，同一文件路径 hash 不变；不需要外部维护 file_id 序列；与 Manifest 中的文件路径直接对应。

### 12.2 Parquet 版本无关的 Page 定位

```rust
use std::sync::Arc;

pub struct ParquetPageLocator {
    meta: Arc<ParquetFileMetadata>,  // version, RGs, column_indices
}

pub struct PageLocation {
    pub page_offset: u64,
    pub compressed_bytes: u32,
    pub first_row_in_page: u32,
    pub num_rows: u32,
}

impl ParquetPageLocator {
    /// 自动选择最优策略: V2 用 ColumnIndex 二分, V1 回退顺序扫描
    pub fn locate(
        &self,
        rg_index: u32,
        col_index: u32,
        target_row: u32,
    ) -> Vec<PageLocation> {
        if self.meta.has_column_index() {
            self.locate_via_index(rg_index, col_index, target_row)
        } else {
            self.locate_via_scan(rg_index, col_index, target_row)
        }
    }

    /// V2: O(log pages) 二分 ColumnIndex
    fn locate_via_index(&self, rg: u32, col: u32, target_row: u32)
        -> Vec<PageLocation>;

    /// V1 fallback: O(pages) 顺序扫描 Page headers
    fn locate_via_scan(&self, rg: u32, col: u32, target_row: u32)
        -> Vec<PageLocation>;
}
```

### 12.3 FileRegistry

```rust
use std::collections::HashMap;
use std::sync::Arc;

/// file_path_hash ↔ file_path 双向映射
pub struct FileRegistry {
    hash_to_entry: HashMap<u64, FileEntry>,
    path_to_hash: HashMap<String, u64>,
}

struct FileEntry {
    path: String,
    size: i64,
    record_count: i64,
    exists_in_current_snapshot: bool,
    parquet_meta: Option<Arc<ParquetFileMetadata>>,
}

impl FileRegistry {
    pub fn build_from_manifest(&mut self, files: &[DataFileInfo]) {
        for f in files {
            let h = xxhash64(f.file_path.as_bytes());
            self.hash_to_entry.insert(h, FileEntry {
                path: f.file_path.clone(),
                size: f.file_size,
                record_count: f.record_count,
                exists_in_current_snapshot: true,
                parquet_meta: None,  // 延迟加载
            });
            self.path_to_hash.insert(f.file_path.clone(), h);
        }
    }

    pub fn resolve_path(&self, path_hash: u64) -> Option<&str> {
        self.hash_to_entry.get(&path_hash).map(|e| e.path.as_str())
    }

    pub fn hash_of(&self, path: &str) -> Option<u64> {
        self.path_to_hash.get(path).copied()
    }

    pub fn file_exists(&self, path_hash: u64) -> bool {
        self.hash_to_entry.get(&path_hash)
            .map_or(false, |e| e.exists_in_current_snapshot)
    }

    /// 延迟加载 Parquet 元数据 (首次调用时读取 Footer)
    pub fn get_parquet_meta(&mut self, path_hash: u64)
        -> Option<Arc<ParquetFileMetadata>>;
}
```

---

# 第六部分：接口设计

## 13. Java SDK 桥接层

设计原则：**Java 管元数据（TableMetadata 读写 commit），Rust 管索引计算（构建、序列化、查询）**。小数据量通过 JNI，大数据量在 Rust 内部零拷贝。

### 13.1 Java 侧接口

```java
package org.apache.iceberg.puffin.bridge;

public interface PuffinBridge {
    /** 加载表元数据摘要 (不含完整 snapshot 历史) */
    String loadTableMetadata(String tablePath);

    /** 提交统计文件注册, 原子 commit */
    long commitStatisticsFile(String tablePath,
                              String statisticsFileJson,
                              String updateMode); // "append"/"replace"/"update_status"

    /** 原子更新多个索引组的状态 (CoW UPDATE / Compaction 时) */
    long updateIndexStatuses(String tablePath, String statusUpdatesJson);

    /** 获取 snapshot 的数据文件列表 */
    String getDataFiles(String tablePath, long snapshotId);

    /** 比较两个 snapshot 的差异 */
    String diffSnapshots(String tablePath, long fromSnapshotId, long toSnapshotId);

    /** 获取统计文件列表 */
    String getStatisticsFiles(String tablePath, long snapshotId);
}
```

### 13.2 Rust 侧 JNI 桥接

```rust
use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::jlong;
use serde::{Deserialize, Serialize};

pub struct JavaBridge {
    vm: jni::JavaVM,
}

impl JavaBridge {
    /// 创建 JVM 实例并加载 Iceberg SDK
    pub fn create(classpath: &str) -> Result<Self, BridgeError> {
        let vm = jni::InitArgsBuilder::new()
            .option(format!("-Djava.class.path={}", classpath))
            .build()?;
        Ok(JavaBridge { vm: jni::JavaVM::new(vm)? })
    }

    /// 加载表元数据摘要
    pub fn load_table_meta(&self, table_path: &str)
        -> Result<Option<TableMeta>, BridgeError>;

    /// 获取 snapshot 的数据文件列表
    pub fn get_data_files(&self, table_path: &str, snapshot_id: i64)
        -> Result<Vec<DataFileInfo>, BridgeError>;

    /// 比较两个 snapshot 的差异
    pub fn diff_snapshots(&self, table_path: &str, from: i64, to: i64)
        -> Result<SnapshotDiff, BridgeError>;

    /// 提交统计文件 (原子 commit)
    pub fn commit_statistics_file(&self, table_path: &str,
                                   meta: &StatisticsFileMeta,
                                   mode: &str) -> Result<i64, BridgeError>;

    /// 原子更新索引状态
    pub fn update_index_statuses(&self, table_path: &str,
                                  updates: &[IndexStatusUpdate])
        -> Result<i64, BridgeError>;
}

// ===== 数据交换类型 (serde 序列化为 JSON 通过 JNI 传递) =====

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TableMeta {
    pub current_snapshot_id: i64,
    pub last_sequence_number: i64,
    pub table_uuid: String,
    pub location: String,
    pub format_version: i32,
    pub statistics: Vec<StatisticsFileMeta>,
    pub partition_specs: Vec<PartitionSpec>,
}

#[derive(Serialize, Deserialize)]
pub struct DataFileInfo {
    pub file_path: String,
    pub file_size: i64,
    pub record_count: i64,
    pub file_format: i32,
    pub partition_spec_id: u32,
    pub column_stats: Vec<ColumnStats>,
}

#[derive(Serialize, Deserialize)]
pub struct ColumnStats {
    pub field_id: i32,
    pub min_i64: Option<i64>,
    pub max_i64: Option<i64>,
    pub null_count: i64,
}

#[derive(Deserialize)]
pub struct SnapshotDiff {
    pub added: Vec<DataFileInfo>,
    pub removed_paths: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct StatisticsFileMeta {
    pub snapshot_id: i64,
    pub path: String,
    pub file_size: i64,
    pub footer_size: i64,
    pub blob_summaries: Vec<BlobMetaSummary>,
}

#[derive(Serialize, Deserialize)]
pub struct IndexStatusUpdate {
    pub segment_uuid: String,
    pub new_status: String,
    pub new_staleness_ratio: f64,
}
```

---

## 14. Rust 核心接口

### 14.1 IndexCatalog — 元数据管理

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// 管"有什么索引、覆盖哪些文件、什么状态"
/// 数据来源: index_catalog.puffin 的 registry Blob (非 TableMetadata.statistics[])
pub struct IndexCatalog {
    groups: HashMap<String, Arc<IndexGroup>>,
    field_index: HashMap<i32, Vec<String>>,     // field_id → group_uuids
    segments: HashMap<String, Arc<RwLock<IndexSegment>>>,
    registry_version_cached: i32,
}

impl IndexCatalog {
    /// 从 index_catalog.puffin 加载全部索引目录
    /// 这是查询时的唯一入口 — 读一次 Footer + registry Blob 即可获得
    /// 所有 group 和 segment 的摘要
    pub fn load_from_catalog_puffin(&mut self, catalog_puffin_path: &str)
        -> Result<(), CatalogError>;

    /// 重新加载 — 检测 catalog.puffin 的 registry-version 变更
    /// 如果未变更 → 使用内存缓存 (no-op)
    pub fn refresh(&mut self, table_path: &str) -> Result<(), CatalogError>;

    /// 获取指定 field_id 的所有索引组
    pub fn get_groups_for_field(&self, field_id: i32) -> Vec<Arc<IndexGroup>>;
    pub fn get_group(&self, uuid: &str) -> Option<Arc<IndexGroup>>;
    pub fn get_segment(&self, seg_uuid: &str) -> Option<Arc<RwLock<IndexSegment>>>;

    // ============ 修改 (内存) ============
    pub fn add_group(&mut self, group: IndexGroup) -> Result<(), CatalogError>;
    pub fn add_segment(&mut self, seg: IndexSegment) -> Result<(), CatalogError>;
    pub fn update_segment_status(&mut self, seg_uuid: &str,
                                  new_status: &str,
                                  new_staleness_ratio: f64);
    pub fn mark_group_deprecated(&mut self, uuid: &str);
    pub fn mark_segment_deleted(&mut self, uuid: &str);

    // ============ 持久化 ============

    /// 将当前 Catalog 状态序列化为 registry Blob 二进制
    /// → 写入 (新的) index_catalog.puffin
    /// → 返回新的 Puffin 文件路径和元数据
    pub fn to_registry_blob(&self) -> CatalogSnapshot;

    pub fn registry_version(&self) -> i32 { self.registry_version_cached }
}

pub struct CatalogSnapshot {
    pub registry_blob: Vec<u8>,    // Blob 二进制数据
    pub file_size: i64,
    pub footer_size: i64,
    pub registry_version: i32,
    pub num_groups: i32,
    pub num_segments: i32,
}
```

### 14.2 IndexSegmentManager — 构建管理

```rust
pub struct IndexSegmentManager {
    catalog: Arc<RwLock<IndexCatalog>>,
    bridge: Arc<JavaBridge>,
    plugin_registry: Arc<IndexRegistry>,
}

impl IndexSegmentManager {
    pub fn create_index_group(&self, field_id: i32,
                               index_type: &str,
                               cfg: &BuildConfig) -> Result<String, BuildError>;
    pub fn drop_index_group(&self, uuid: &str) -> Result<(), BuildError>;

    /// 全量构建 — 返回新 segment uuid
    pub fn build_full(&self, group_uuid: &str,
                      table_path: &str) -> Result<String, BuildError>;

    /// 增量构建 — 返回新 segment uuid (无新文件返回 None)
    pub fn build_incremental(&self, group_uuid: &str,
                              table_path: &str,
                              from_snapshot: i64)
        -> Result<Option<String>, BuildError>;

    /// 部分失效 — CoW UPDATE/Compaction 时调用
    pub fn mark_files_stale(&self, table_path: &str,
                             rewritten_file_paths: &[String],
                             new_file_paths: &[String],
                             row_mappings: &[FileRewriteEntry])
        -> Result<(), BuildError>;

    pub fn generate_rewrite_map(&self, table_path: &str)
        -> Result<(), BuildError>;

    /// 查询 — 收集可用 segment
    pub fn get_usable_segments(&self, field_id: i32, snapshot_id: i64)
        -> Vec<Arc<RwLock<IndexSegment>>>;
}
```

### 14.3 IndexUsabilityChecker — 安全校验

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexUsability {
    SafeToUse,          // 直接使用
    SafeWithStaleSkip,  // 跳过 stale 文件
    UseWithRemap,       // 需要 Rewrite Map 转译
    Skip,               // 不安全, 跳过
}

pub struct UsabilityResult {
    pub usability: IndexUsability,
    pub reason: String,
    pub staleness_ratio: f64,
    pub rewrite_map: Option<Arc<RewriteMap>>,  // 如果 UseWithRemap
}

pub struct IndexUsabilityChecker;

impl IndexUsabilityChecker {
    pub fn check(&self, seg: &IndexSegment) -> UsabilityResult;
}
```

### 14.4 IndexMaintainer — 维护操作

```rust
pub struct IndexMaintainer {
    catalog: Arc<RwLock<IndexCatalog>>,
    seg_manager: Arc<IndexSegmentManager>,
    bridge: Arc<JavaBridge>,
}

impl IndexMaintainer {
    // ===== 被动 (数据变更触发) =====
    pub fn on_files_rewritten(&self, table_path: &str,
                               old_paths: &[String],
                               new_paths: &[String]) -> Result<(), MaintainError>;
    pub fn on_files_appended(&self, _table: &str, _paths: &[String]) {}
    pub fn on_delete_files_added(&self, _table: &str, _paths: &[String]) {}

    // ===== 主动 (后台任务) =====
    pub fn generate_rewrite_maps(&self, table_path: &str)
        -> Result<(), MaintainError>;
    pub fn rebuild_stale_indexes(&self, table_path: &str)
        -> Result<(), MaintainError>;
    pub fn defragment_index_group(&self, group_uuid: &str)
        -> Result<(), MaintainError>;
    pub fn garbage_collect(&self, table_path: &str)
        -> Result<(), MaintainError>;

    // ===== 用户操作 =====
    pub fn drop_index(&self, uuid: &str, table_path: &str)
        -> Result<(), MaintainError>;
    pub fn rebuild_index(&self, uuid: &str, table_path: &str)
        -> Result<String, MaintainError>;

    // ===== 校验 =====
    pub fn validate_index(&self, uuid: &str, table_path: &str)
        -> Result<Vec<ValidationReport>, MaintainError>;
}

pub struct ValidationReport {
    pub segment_uuid: String,
    pub status: String,
    pub staleness_ratio: f64,
    pub total_files: usize,
    pub stale_files: usize,
    pub active_files: usize,
    pub has_rewrite_map: bool,
}
```

---

## 15. PuffinReader / PuffinWriter

### 15.1 FileInput / FileOutput 抽象

```rust
use std::io::{Read, Write, Seek};

/// 文件读取抽象 (本地 FS / S3 / HDFS)
pub trait FileInput: Read + Seek + Send + Sync {
    fn file_size(&self) -> Result<i64, io::Error>;
    fn path(&self) -> &str;
}

/// 文件写入抽象
pub trait FileOutput: Write + Seek + Send {
    fn tell(&self) -> Result<i64, io::Error>;
    fn path(&self) -> &str;
}
```

### 15.2 PuffinReader

```rust
pub struct PuffinReader<R: FileInput> {
    input: R,
    cached_meta: Option<FileMetadata>,
}

impl<R: FileInput> PuffinReader<R> {
    pub fn new(input: R) -> Self {
        PuffinReader { input, cached_meta: None }
    }

    /// 读取并解析 Footer (首次调用时, 后续缓存)
    pub fn read_footer(&mut self) -> Result<&FileMetadata, PuffinError>;

    /// 读取指定 Blob (自动解压)
    pub fn read_blob(&mut self, meta: &BlobMetadata)
        -> Result<Blob, PuffinError>;

    /// 按 type 查找并读取第一个匹配 Blob
    pub fn find_blob(&mut self, blob_type: &str)
        -> Result<Option<Blob>, PuffinError>;

    pub fn find_blob_by_fields(&mut self, blob_type: &str,
                                field_ids: &[i32])
        -> Result<Option<Blob>, PuffinError>;

    /// 已缓存的 FileMetadata (只读)
    pub fn metadata(&self) -> Option<&FileMetadata>;

    /// 批量读取多个 Blob (合并为一次 IO, 对 S3 友好)
    pub fn read_blobs_batch(&mut self, metas: &[BlobMetadata])
        -> Result<Vec<Blob>, PuffinError>;
}
```

### 15.3 PuffinWriter

```rust
pub struct PuffinWriter<W: FileOutput> {
    output: W,
    blobs: Vec<(BlobDesc, Vec<u8>)>,
    compress_footer: bool,
    bytes_written: i64,
}

pub struct BlobDesc {
    pub blob_type: String,
    pub fields: Vec<i32>,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub compression: CompressionCodec,
    pub properties: HashMap<String, String>,
}

impl<W: FileOutput> PuffinWriter<W> {
    pub fn new(output: W, compress_footer: bool) -> Self;

    /// 添加 Blob (数据自动压缩)
    pub fn add_blob(&mut self, desc: BlobDesc, data: &[u8])
        -> Result<(), PuffinError>;

    /// 完成写入 (写入 Footer + Magic)
    pub fn finish(&mut self) -> Result<(), PuffinError>;

    /// 放弃写入
    pub fn abort(&mut self) -> Result<(), PuffinError>;

    /// 已写入字节数
    pub fn bytes_written(&self) -> i64;
    pub fn footer_size(&self) -> i64;
}
```

### 15.4 核心类型

```rust
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionCodec {
    None = 0,
    Lz4  = 1,
    Zstd = 2,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobMetadata {
    #[serde(rename = "type")]
    pub blob_type: String,
    pub fields: Vec<i32>,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    pub offset: i64,
    pub length: i64,
    #[serde(rename = "compression-codec", skip_serializing_if = "Option::is_none")]
    pub compression_codec: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub blobs: Vec<BlobMetadata>,
    pub properties: HashMap<String, String>,
    pub footer_offset: i64,
}

#[derive(Debug, Clone)]
pub struct Blob {
    pub metadata: BlobMetadata,
    pub data: Vec<u8>,  // 已解压
}
```

---

## 16. 自定义索引插件框架

### 16.1 IndexPlugin trait

```rust
use std::any::Any;

/// 自定义索引插件接口 — 对标 C++ 的 IIndexPlugin
pub trait IndexPlugin: Send + Sync + Any {
    /// 标识此插件的 Blob 类型名
    fn blob_type(&self) -> &str;

    /// 序列化: Puffin blob ↔ 内存对象
    fn deserialize(&mut self, data: &[u8]) -> Result<(), PluginError>;
    fn serialize(&self) -> Result<Vec<u8>, PluginError>;

    /// 构建阶段
    fn begin_build(&mut self) {}
    fn add_row(&mut self, value: &IcebergValue, row_addr: &RowAddress)
        -> Result<(), PluginError>;
    fn end_build(&mut self) {}
    fn merge(&mut self, _other: &dyn IndexPlugin) -> Result<bool, PluginError> {
        Ok(false)
    }

    /// 查询: 根据表达式评估索引是否可跳过/匹配
    fn evaluate(&self, expr: &Expression, filter: Option<&RowFilter>)
        -> Result<IndexEvalResult, PluginError>;

    /// 元数据
    fn memory_bytes(&self) -> usize;
    fn total_rows(&self) -> i64 { 0 }
    fn estimated_serialized_bytes(&self) -> usize;

    /// 向下转型支持
    fn as_any(&self) -> &dyn Any;
}
```

### 16.2 IndexRegistry

```rust
use std::collections::HashMap;
use std::sync::Mutex;

pub type PluginFactory = Box<dyn Fn() -> Box<dyn IndexPlugin> + Send + Sync>;

pub struct IndexRegistry {
    factories: Mutex<HashMap<String, (PluginFactory, i32)>>,  // type → (factory, priority)
}

impl IndexRegistry {
    pub fn instance() -> &'static IndexRegistry {
        use std::sync::OnceLock;
        static INSTANCE: OnceLock<IndexRegistry> = OnceLock::new();
        INSTANCE.get_or_init(|| IndexRegistry {
            factories: Mutex::new(HashMap::new()),
        })
    }

    pub fn register_factory(&self, blob_type: &str,
                             factory: PluginFactory,
                             priority: i32) {
        let mut f = self.factories.lock().unwrap();
        f.insert(blob_type.to_string(), (factory, priority));
    }

    pub fn unregister(&self, blob_type: &str) {
        let mut f = self.factories.lock().unwrap();
        f.remove(blob_type);
    }

    pub fn create(&self, blob_type: &str) -> Option<Box<dyn IndexPlugin>> {
        let f = self.factories.lock().unwrap();
        f.get(blob_type).map(|(factory, _)| factory())
    }

    pub fn registered_types(&self) -> Vec<String> {
        let f = self.factories.lock().unwrap();
        f.keys().cloned().collect()
    }
}
```

### 16.3 添加自定义索引清单

```
□ 1. 定义 Blob Type 名称 — 格式: <org>.<project>-<index-type>-v<version>
□ 2. 实现 IndexPlugin trait — blob_type(), deserialize(), serialize(),
     add_row(), evaluate(), memory_bytes()
□ 3. 注册工厂 — IndexRegistry::instance().register_factory(...)
□ 4. 定义 Properties — 在 BlobMetadata.properties 中存放索引参数
□ 5. 实现构建逻辑 — 读取数据文件列数据 → 构建 → serialize → add_blob
□ 6. 集成查询规划 — 加载索引 → evaluate(expr) → 跳过/保留文件
```

---

# 第七部分：流程

## 17. 全量构建流程

```
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS order_id

Phase 1: 准备
  ├── JavaBridge::load_table_meta() → current_snapshot
  ├── JavaBridge::get_data_files() → N 个 DataFileInfo
  ├── 加载 index_catalog.puffin (如果有) → 获取已有 segments
  ├── 构建 FileRegistry (file_path → hash)
  └── 创建 IndexSegment (status = "building")
      → 先写入临时 Puffin 文件头 (status=building)
      → 更新 index_catalog.puffin registry (追加新的 building segment)
      → 通过 JavaBridge 更新 TableMetadata 指向新 catalog.puffin

Phase 2: 数据读取 + 索引构建 (Rust, 可并行 via rayon)
  for each batch_of_files:
    ├── ColumnReader → 批量读取 Page
    ├── plugin.add_row(value, row_addr) ← 热路径
    └── 进度上报

Phase 3: 序列化 + 写入 (Rust)
  ├── plugin.end_build()
  ├── plugin.serialize() → blob_data
  ├── PuffinWriter::add_blob(BTREE, blob_data)
  ├── PuffinWriter::add_blob(FILEBITMAP, file_ids)
  ├── PuffinWriter::add_blob(PARTITION_BITMAP, ...)  // 可选
  └── PuffinWriter::finish()

Phase 4: 注册 (更新 index_catalog.puffin)
  ├── catalog.update_segment_status(seg_uuid, "active", 0.0)
  ├── catalog_snapshot = catalog.to_registry_blob()
  ├── 写入新的 index_catalog.puffin (仅 ~3-12KB)
  └── JavaBridge::commit_statistics_file(table_path, catalog_puffin_meta, "replace")
      → 原子 commit: 新 catalog.puffin + TableMetadata 指针更新
```

### 17.1 并行构建优化

列间并行：每个列的索引构建独立，可完全并行（rayon scope）。列内并行：同一列的不同文件可并行读取，但构建需串行（或支持 merge）。写入必须串行（PuffinWriter 非线程安全）。

加速比 ≈ column_count（当 IO 不是瓶颈时）。

### 17.2 Bloom Filter 参数自动估算

```rust
/// n = 预估行数, p = 目标误判率
/// m = -n * ln(p) / (ln(2))^2  (bit 数量)
/// k = (m/n) * ln(2)           (hash 函数数量)
pub fn estimate_bloom_params(n: i64, p: f64) -> (usize, usize) {
    let m = -n as f64 * p.ln() / (2.0_f64.ln().powi(2));
    let bit_size = ((m.ceil() as usize + 63) / 64) * 64;  // 对齐 64 位
    let hashes = ((m / n as f64) * 2.0_f64.ln()).round() as usize;
    (bit_size, hashes.clamp(1, 20))
}
// 示例: n=10^7, p=0.01 → m≈12MB, k=7
```

---

## 18. 增量构建流程

```
Phase 1: 差异检测
  ├── 加载当前快照 DataFile 列表
  ├── JavaBridge::diff_snapshots(from_snapshot, current_snapshot)
  │     → added_files, removed_files
  ├── 收集现有 segment 已覆盖的 file_hash 集合
  │     covered = ∪(segment.original_files)
  └── new_files = added_files \ covered
      → 为空 → 无需构建, 返回 None (但仍处理 removed_files)

Phase 2: 增量构建 (仅 new_files)
  ├── 创建新 delta segment (status = "building")
  │     delta-from-segment = 链上最新 segment
  ├── 只为 new_files 构建索引 (数据量远小于全量)
  └── 写入新 Puffin 文件 (体积小, 旧文件不动)

Phase 3: 处理 removed_files (部分失效)
  ├── 对每个现有 segment, 检查交集
  ├── 更新 stale_files + staleness_ratio
  └── 通过 bridge 原子更新 TableMetadata

Phase 4: 注册 (更新 index_catalog.puffin)
  ├── 新 segment.status = "active"
  ├── 如果 Phase 3 有部分失效 → 更新受影响旧 segment 的状态
  ├── catalog.to_registry_blob() → 写入新的 index_catalog.puffin
  └── JavaBridge::commit_statistics_file(table_path, catalog_puffin_meta, "replace")
      → 原子 commit
```

**写放大优势**：增量仅构建 new_files。500 个新文件的增量索引约 3.3MB，全量重建约 48MB，写放大仅 1/15。index_catalog.puffin 重写仅 ~3-12KB，可忽略。

---

## 19. 查询流程

```
SELECT * FROM orders WHERE dt='2026-06-10' AND order_id = 42

Phase 1: 分区裁剪 (Manifest List) → 保留相关 Manifest
Phase 2: 文件裁剪 (Manifest Entry stats) → 保留候选文件
         → 构建 active_file_set (当前快照实际存在的文件)

Phase 3: 发现索引 (读 index_catalog.puffin)
  catalog.load_from_catalog_puffin(table_meta.statistics 中的 registry 路径)
  → 一次 Footer + Blob 读取 (~3-12KB)
  → 获得全部 group 和 segment 的摘要
  catalog.get_groups_for_field(1)
  → 找到 Group a1b2c3d4: 3 个 segments (active/stale_partial/active)

Phase 4: 安全校验 (每个 segment)
  checker.check(&seg):
    Segment[0]: status=active, ratio=0 → SafeToUse
    Segment[1]: status=stale_partial, ratio=0.004 → SafeWithStaleSkip
    Segment[2]: status=active, ratio=0 → SafeToUse

Phase 5: 加载索引 + 分区位图定位
  对每个可用 segment:
    ├── 加载 partition_bitmap → 根据 dt='2026-06-10' 定位 page_range
    ├── 加载 BTree (从缓存或 Puffin)
    ├── BTree.search(42, page_range)
    └── 返回 row_addr 列表 (每个 addr 携带 file_path_hash)

Phase 6: 结果验证 + 回表
  对每个 row_addr:
    ├── [SafeWithStaleSkip 模式]
    │   seg.is_stale_addr(&addr):
    │     addr.file_hash() → hash_to_path[&hash] → file_path
    │     → stale_files.contains(&file_path)? → 是 → 丢弃
    │
    ├── [UseWithRemap 模式]
    │   Rewrite Map: addr.file_hash() → new_file_hash?
    │     是 → 替换 addr 的 file_hash 部分
    │     否且 is_stale_addr → 丢弃
    │
    ├── FileRegistry::resolve(addr.file_hash()) → file_path
    ├── ParquetPageLocator::locate(rg, row_in_rg) → Page offset
    └── 读取 Page → 解码 → 返回数据

Phase 7: 合并结果
  索引命中 ∪ 全扫描回退 (未被索引覆盖的文件) → 去重 → 返回
```

### 19.1 索引发现缓存

```rust
// 查询时只需读一次 index_catalog.puffin
// 后续查询检查 registry-version 是否变更, 未变更则用内存缓存
impl IndexCatalog {
    pub fn load_from_catalog_puffin(&mut self, path: &str)
        -> Result<(), CatalogError>
    {
        let mut reader = PuffinReader::open(path)?;
        let blob = reader.find_blob("puffin.idx.registry-v1")?
            .ok_or(CatalogError::MissingRegistry)?;
        let version = parse_registry_version(&blob.data);
        if version == self.registry_version_cached {
            return Ok(());  // 缓存命中
        }
        // 解析全部 group/segment 摘要 (~10KB, <1ms)
        self.parse_registry_blob(&blob.data)?;
        self.registry_version_cached = version;
        Ok(())
    }
}
```

### 19.2 查询时文件路径的解析

```
BTree entry 存的是 RowAddress (file_path_hash, rg, row)
        │
        ▼
  RowAddress::file_hash() → u64
        │
        ├── seg.hash_to_path[&hash] → file_path (String)
        │   └── seg.stale_files.contains(&file_path) → 失效判断
        │
        ├── RewriteMap::remap(hash) → new_hash → 重映射
        │
        └── FileRegistry::resolve(hash) → file_path → 读 Parquet

文件追踪全程使用 String 路径 (HashSet)
跨 segment / 跨 snapshot 接口 (RowAddress, Rewrite Map) 使用 file_path_hash
segment 内的 hash_to_path (HashMap<u64, String>) 负责 hash→path 翻译
```

### 19.3 索引数据缓存

```rust
use std::sync::Arc;
use std::collections::HashMap;

/// LRU Cache: Key = (puffin_path, blob_type, field_id)
/// Value = Arc<dyn IndexPlugin>
/// 单次 Scan 内有效, 跨 Scan 可选保留
pub struct IndexCache {
    // 使用 lru crate 或手动实现
    entries: HashMap<CacheKey, Arc<dyn IndexPlugin>>,
    max_memory: usize,
    current_memory: usize,
}

pub fn evaluate_with_cache(
    seg: &IndexSegment,
    expr: &Expression,
    cache: &mut IndexCache,
) -> Result<IndexEvalResult, PluginError> {
    let key = CacheKey {
        puffin_path: seg.puffin_path.clone(),
        index_type: seg.index_type.clone(),
        field_id: seg.field_id,
    };
    let plugin = cache.get_or_load(&key, || {
        load_and_deserialize_index(&seg.puffin_path, &seg.index_type, seg.field_id)
    })?;

    let mut filter = RowFilter::default();
    filter.allowed_files = seg.original_files.clone();
    for stale in &seg.stale_files {
        filter.allowed_files.remove(stale);
    }
    // filter.allowed_files 再与 active_files_in_snapshot 取交集

    plugin.evaluate(expr, Some(&filter))
}
```

---

## 20. 索引维护流程

### 20.1 on_files_rewritten (CoW UPDATE / Compaction 回调)

```
UPDATE COMMIT 事务中 (同步):
  1. 写入新 DataFile + Manifest
  2. IndexMaintainer::on_files_rewritten():
     for each segment in all groups:
       // rewritten_file_paths → 直接匹配路径
       for path in rewritten_paths:
         if seg.original_files.contains(path):
           stale_paths.push(path.clone())
       if !stale_paths.is_empty():
         for p in &stale_paths:
           seg.stale_files.insert(p.clone());
         更新 seg.staleness_ratio
         if ratio >= REBUILD_THRESHOLD:
           seg.status = "stale_orphan"
         elif ratio > 0:
           seg.status = "stale_partial"
  3. catalog_snapshot = catalog.to_registry_blob()
  4. 写入新的 index_catalog.puffin
  5. bridge.commit_statistics_file(table_path, &catalog_puffin_meta, "replace")
     → 原子 commit (新 catalog.puffin + TableMetadata 指针)
```

### 20.2 后台 Rewrite Map 生成

```
后台任务:
  1. 找到所有 status = stale_orphan 的 segment
  2. 比对 segment.original_files 和当前 Manifest:
     → 找到哪些 stale 文件已重写到哪些新文件
  3. 生成区间映射:
     [old_file_hash, start_row, end_row] → [new_file_hash, start_row, end_row]
  4. 写入 Rewrite Map Puffin
  5. 更新 index_catalog.puffin:
     segment.status: stale_orphan → remapping
     新增 Rewrite Map segment 条目
  6. commit (新 catalog.puffin + TableMetadata 指针)
```

### 20.3 后台索引重建

```
后台任务:
  1. 找到 staleness_ratio >= REBUILD_THRESHOLD 的 segment
  2. 确定新覆盖范围:
       active_paths = original_files \ stale_files  (直接的路径集合差)
       remap_target_paths = Rewrite Map 中 stale 文件的新目标路径
       最终覆盖文件 = active_paths ∪ remap_target_paths
       (文件列表从 Manifest 获取)
  3. segment.status = "rebuilding" (更新 index_catalog.puffin)
  4. 全量构建新 segment (覆盖新文件范围)
  5. 更新 index_catalog.puffin:
       新 segment.deprecates = 旧 segment uuid
       新 segment.status = "active"
       旧 segment.status = "deprecated"
  6. commit (新 catalog.puffin + TableMetadata 指针)

注意: 新 segment 有自己的 file_paths 列表 (来自当前 Manifest)
      旧 segment 的文件列表随之废弃
```

### 20.4 合并索引段 (defragment)

当 delta-from 链过长（如 segments ≥ 10）时：
1. 全量构建一个新 segment（覆盖所有文件）
2. 更新 index_catalog.puffin: 新 segment.deprecates = 所有旧 segment uuid
3. 旧 segments → deprecated
4. GC 清理 deprecated segments

### 20.5 垃圾回收

```
触发: deprecated 超过 TTL, 或覆盖的所有文件都已删除
操作:
  1. 更新 index_catalog.puffin:
     segment.status = "deleted"
     从 registry 中移除该 segment 条目
  2. 删除对应的 Puffin 文件 (索引 Blob + Rewrite Map)
  3. commit (新 catalog.puffin + TableMetadata 指针)

注意: 不再需要从 TableMetadata.statistics[] 移除条目
      因为 statistics[] 只有一条 catalog 指针, 从不变化
```

---

# 第八部分：附录

## A. 数据结构速查

```rust
use std::collections::{HashMap, HashSet};

// ==================== IndexCatalog ====================
pub struct IndexGroup {
    pub uuid: String,
    pub name: String,
    pub index_type: String,
    pub field_id: i32,
    pub partition_spec_id: i32,
    pub created_at_snapshot: i64,
    pub segment_uuids: Vec<String>,    // 时间顺序
}

pub struct IndexSegment {
    pub uuid: String,
    pub index_group_uuid: String,
    pub puffin_path: String,
    pub built_at_snapshot: i64,
    pub delta_from_segment: Option<String>,
    // 文件覆盖 (直接存储文件路径)
    pub file_coverage_loaded: bool,
    pub original_files: HashSet<String>,   // 构建时覆盖的文件路径集合
    pub stale_files: HashSet<String>,      // 已失效的文件路径子集
    pub staleness_ratio: f64,
    // hash ↔ path 映射
    pub file_paths: Vec<FileListEntry>,               // 按写入顺序, 含行数
    pub hash_to_path: HashMap<u64, String>,           // RowAddress file_hash → file_path
    // 状态
    pub status: String,
    pub status_updated_at: i64,
    pub rewrite_map_available: bool,
    // 统计
    pub total_rows: i64,
    pub num_pages: i32,
    pub puffin_file_size: i64,
    pub puffin_footer_size: i64,
    pub index_type: String,
}

pub struct FileListEntry {
    pub path_hash: u64,     // XXH64(file_path)
    pub file_path: String,  // 完整路径 (相对路径)
    pub row_count: i64,     // 文件行数
}

// ==================== BuildConfig ====================
pub struct BuildConfig {
    // Bloom
    pub bloom_fpp: f64,
    pub bloom_num_hashes: usize,        // 0 = auto

    // BTree
    pub btree_page_size: usize,
    pub btree_max_entries_per_page: usize,
    pub btree_sort_by_partition: bool,

    // Bitmap
    pub bitmap_max_distinct: usize,

    // General
    pub batch_size: usize,
    pub compression: CompressionCodec,
    pub compression_level: i32,
    pub compress_footer: bool,
    pub num_build_threads: usize,         // 0 = auto
    pub created_by: String,
    pub staleness_rebuild_threshold: f64,
    pub staleness_remap_threshold: f64,
}

impl Default for BuildConfig {
    fn default() -> Self {
        BuildConfig {
            bloom_fpp: 0.01,
            bloom_num_hashes: 0,
            btree_page_size: 4096,
            btree_max_entries_per_page: 256,
            btree_sort_by_partition: true,
            bitmap_max_distinct: 100_000,
            batch_size: 65536,
            compression: CompressionCodec::Zstd,
            compression_level: 3,
            compress_footer: true,
            num_build_threads: 0,
            created_by: "puffin-rs/3.0".into(),
            staleness_rebuild_threshold: 0.3,
            staleness_remap_threshold: 0.1,
        }
    }
}

// ==================== RowAddress ====================
pub struct RowAddress {
    pub high: u64,  // [file_hash:48 | rg_index:16]
    pub low: u64,   // [row_in_rg:16 | reserved:48]
}

// ==================== Expression ====================
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprOp {
    Eq, Neq, Lt, Gt, Le, Ge, In, NotIn, IsNull, NotNull,
    And, Or, Not, StartsWith,
}

#[derive(Debug, Clone)]
pub enum IndexEvalVerdict {
    CannotDecide,
    CanSkip,
    CanMatch,
    ExactMatch(Vec<RowAddress>),
}

pub struct IndexEvalResult {
    pub verdict: IndexEvalVerdict,
    pub reason: String,
}

pub struct RowFilter {
    pub allowed_files: HashSet<String>,  // 允许搜索的文件路径
    // 查询时: 只搜索这些文件对应的数据页
}

impl Default for RowFilter {
    fn default() -> Self {
        RowFilter { allowed_files: HashSet::new() }
    }
}
```

## B. 性能基线

| 操作 | 预期延迟 | 备注 |
|------|----------|------|
| Footer 读取+解析 | 1-2ms | ~1KB JSON, serde_json |
| Blob 读取 (4KB, NVMe) | 0.1-0.5ms | 不含解压 |
| Blob 解压 LZ4 (64KB) | ~0.15ms | lz4 crate |
| Blob 解压 Zstd (64KB) | ~0.3ms | zstd crate |
| Bloom 查询 (单次) | ~0.3µs | XXH128 double-hash |
| Bloom 查询 (1000项 IN) | ~3µs | |
| BTree 搜索 (100万行) | ~100µs | Page Table 常驻内存 |
| 文件路径列表加载 (500 文件) | ~0.5ms | Zstd 解压 + String 解析 |
| Parquet Page 定位 (V2, ColumnIndex) | ~0.1ms | |
| Parquet Page 定位 (V1, 无索引) | ~1ms | 顺序扫描 ~100 page headers |

## C. 规范速查表

| 项目 | 值 |
|------|-----|
| Magic (bytes) | `0x50 0x46 0x41 0x31` |
| Magic (u32 LE) | `0x31414650` |
| 版本 | 1 |
| 整数编码 | 有符号, 补码, 小端序 |
| FooterPayload 编码 | UTF-8 JSON |
| FooterPayload 压缩 | LZ4 (flags bit 0) |
| Blob 压缩 | LZ4 / Zstd / 无 |
| 文件后缀 | `.puffin` |

---

> **文档版本**: 3.2 | **日期**: 2026-06-13
>
> **v3.2 新增**: 方案 A — statistics[] 单个指针（index_catalog.puffin），索引元数据独立管理。详见 §3.7–3.8。
>
> **v3.3 简化**: 移除 file_index / RoaringBitmap 中间层，文件覆盖直接使用路径字符串（`HashSet<String>`），消除映射层。
>
> **本文档合并了全部有效设计，是 Puffin 自定义索引 Rust 实现的唯一权威参考。**
> 各源文档的详细分析和讨论过程仍可查阅，但设计决策以本文档为准。

---

## Rust vs C++ 设计差异摘要

| 方面 | C++ 原版 | Rust 版本 |
|------|---------|-----------|
| 命名空间 | `namespace puffin::bridge` | `pub mod bridge` |
| 虚函数/多态 | `class IIndexPlugin` (virtual) | `trait IndexPlugin` |
| 智能指针 | `std::unique_ptr<T>` | `Box<T>` |
| 共享所有权 | `std::shared_ptr<T>` | `Arc<T>` |
| 可选值 | `std::optional<T>` | `Option<T>` |
| 动态数组 | `std::vector<T>` | `Vec<T>` |
| 哈希表 | `std::unordered_map<K,V>` | `HashMap<K,V>` |
| 字符串 | `std::string` / `const char*` | `String` / `&str` |
| 字节切片 | `std::span<const uint8_t>` | `&[u8]` |
| 枚举 | `enum class` | `#[repr(u8)] enum` |
| 常量 | `constexpr` | `const` |
| 单例 | `static X& instance()` | `OnceLock<X>` |
| 函数对象 | `std::function<F>` | `Box<dyn Fn() -> ...>` |
| 互斥锁 | `std::mutex` / `std::shared_mutex` | `Mutex<T>` / `RwLock<T>` |
| JNI 桥接 | 原生 JNI | `jni` crate |
| JSON 解析 | simdjson | `serde_json` |
| 哈希 | XXH64 (xxhash) | `xxhash-rust` crate |
| 压缩 | lz4 / zstd (C lib) | `lz4` / `zstd` crate |
| 文件追踪 | RoaringBitmap (file_index) | `HashSet<String>` (直接路径) |
| 并发构建 | std::thread | `rayon` crate |
| 缓存 | 自定义 LRU | `lru` crate 或自定义 |
| 错误处理 | 异常 / std::optional | `Result<T, E>` |
| 生命周期 | 手动 delete / RAII | 所有权系统 + Drop trait |
| 线程安全 | 文档/约定 | 编译期 Send + Sync trait |
