# 索引结构

Lance将索引视为分层在表行标识符之上的独立冗余数据结构。这使文件格式不受内置搜索结构的限制，并使索引格式独立于表布局而发展。

Lance支持三种主要类型的索引来加速数据访问：标量索引、向量索引和系统索引。

标量索引加速了对整数、时间戳和字符串等标量数据类型的查询。这包括主要的跳过结构，如区域图，以及次要结构，如B树、位图索引和全文搜索索引。它们通常接受等式、范围、集合成员资格或令牌匹配等谓词，并返回匹配的行标识符。

向量索引专门用于高维嵌入上的近似最近邻搜索。示例包括基于IVF的布局和HNSW图。向量索引接收查询向量并返回行标识符和距离分数，而不是标量谓词。

系统索引是支持内部表维护和行标识符解析的辅助结构。最终用户不会直接查询它们。示例包括片段重用索引，它支持压缩后的高效重映射。



## 设计

Lance索引的设计考虑了以下设计选择：

1. 索引是按需加载的：数据集可以在不加载任何索引的情况下加载和读取。只有当查询可以从索引中受益时，才会加载索引。这种设计最大限度地减少了内存使用，加快了数据集打开时间。

2. 索引可以逐步加载：索引的设计是为了在查询执行期间只将必要的部分加载到内存中。例如，在查询B树索引时，它会加载一个小的页表，以确定为给定查询加载索引的哪些页，然后只加载这些页来执行索引搜索。这分摊了冷索引查询的成本，因为每个查询只需要加载索引的一小部分。

3. 索引可以合并成比碎片更大的单位。索引比数据文件小得多，因此合并索引段以覆盖多个片段是有效的。这减少了在查询执行期间需要打开的索引文件的数量，然后减少了需要查询的唯一索引数据结构的数量。

4. 索引文件在写入后是不可变的，类似于数据文件。只能通过创建新文件来修改它们。这意味着它们可以安全地缓存在内存或磁盘上，而不必担心一致性问题。



## 基础概念

Lance中的索引是在数据集的特定列（或多列）上定义的。它是通过它的名字来识别的。

1. 索引由多个索引段组成，由其唯一的UUID标识。每个段都是一个独立的、自包含的索引，覆盖了数据的一个子集。

2. 每个索引段覆盖数据集中不相交的片段子集。这些段必须覆盖它们所覆盖的片段中的所有行，但有一个例外：如果片段在创建索引时有删除标记，则允许索引段不包含已删除的行。索引覆盖的片段是记录在fragment_bitmap字段中的片段。

3. 索引段不需要覆盖所有片段。这意味着索引不需要完全更新。当这种情况发生时，引擎可以将查询拆分为索引和未索引的子计划，并合并结果。



## 索引存储

每个索引的内容都存储在基路径下的_indices/{UUID}目录中。我们称此位置为索引目录。存储在索引目录中的实际内容取决于索引类型。这些可以是索引实现定义的任意文件。然而，它们通常由包含索引数据结构的Lance文件组成。这允许重用现有的Lance文件格式代码来读取和写入索引数据。





## 创建和更新索引段

索引段是通过事务处理过程创建和更新的：

1. 构建索引数据：从要索引的片段中读取相关列数据，并构建索引数据结构。将这些写入新_indices/{UUID}目录中的文件，其中{UUID}是新生成的唯一标识符。

2. 准备元数据：使用以下内容创建IndexMetadata消息：

3. uuid：新生成的uuid

4. name:索引名称（如果添加到现有索引，则必须与现有段匹配）

5. fields：索引所依赖的列：搜索它的键控列，后面是covering_fields中命名的任何仅携带的列。fields[0]始终是键控列。

6. covering_fields：字段的尾随子集，其值由索引携带但未被键入，让只投影这些列的查询得到回答，而不需要片段。对于不包含额外列的索引，为空。在这里声明一个列本身并不能使其可服务——请参阅为携带的列提供服务。

7. fragment_bitmap：此片段所覆盖的片段ID集

8. index_details：特定于索引的配置和参数

9. version：此索引类型的格式版本

10. 请参阅table.proto中的完整protobuf定义。

11. 提交事务：编写一个新的清单，在其IndexSection中包含新的索引段。这是使用与数据写入相同的事务机制原子地完成的。

在原地更新列（不删除行）时，引擎必须从任何字段包含该列的索引段的fragment_bitmap字段中删除受影响的片段ID，无论索引是键控还是仅携带该列。这将这些片段标记为需要重新索引，而不会使整个段无效，并防止从索引中读取无效数据。



## 索引兼容性

在使用索引段之前，引擎必须验证它们是否支持它：

1. 检查索引类型：index_details字段包含一个protobuf任何消息，其类型URL标识索引类型（例如，B-tree、IVF、HNSW）。如果引擎无法识别类型，则应跳过此索引段。

2. 检查版本：IndexMetadata中的version字段指示索引段的格式版本。如果引擎不支持此版本，则应跳过此索引段。这允许索引格式随着时间的推移而发展，同时保持向后兼容性。

当引擎无法使用索引段时，它应该回退到扫描该段所覆盖的片段。





## 为携带的列提供服务

IndexMetadata.covering_fields记录索引段声明它携带的列。它并不能确定该段的存储是否保存了它们的值。

该段的存储模式是权威的。在回答来自携带列的查询之前，引擎必须确认该列存在于它打开的存储中，如果不存在，则退回到基表。声明命名其存储不包含的列的段是合法状态，而不是损坏：允许无法通过重建携带有效负载的维护操作将其撤回并保持声明不变。

> 目前还没有索引构建器写入进位值，因此今天每个声明都先于其存储。因此，读取covering_fields的引擎必须将其纯粹视为声明，并为基表中的每一列提供服务，直到它们自己验证了存储为止。这是过渡性的；上述规则并非如此。





## 索引加载

加载索引时：

1. 从清单中的index_section字段获取索引部分的偏移量。

2. 从清单文件中读取索引部分。这是一个IndexSection类型的protobuf消息，其中包含一个IndexMetadata消息列表，每个消息描述一个索引段。

3. 从数据集目录下的_indices/{UUID}目录读取索引文件，其中{UUID}是索引段的UUID。

> 优化清单加载
> 
> 当清单文件很小时，您可以急切地读取和缓存索引部分。这避免了加载索引时读取额外的文件。



IndexMetadata消息包含有关索引段的重要信息：

- uuid：索引段的唯一标识符。

- fields：索引所依赖的列：搜索索引的键控列，后面跟着它仅携带的任何列，如covering_fields中所述。fields[0]始终是键控列。

- covering_fields：字段的尾随子集，其值由索引与自己的数据一起携带，但未被键入。对于不携带额外列的索引，为空。此声明对该段可以提供的内容没有权威性——请参阅服务携带的列。

- fragment_bitmap：此索引段覆盖的片段ID集。

- index_details：protobuf任何包含索引特定详细信息的消息，如索引类型、参数和存储格式。这允许不同的索引类型存储自己的元数据。



## 处理已删除和无效的行

由于索引段是不可变的，因此它们可能包含对已删除或更新的行的引用。这些应该在查询执行期间过滤掉。

有四种情况需要考虑：

1. 索引段中有一些已删除的行。索引段中的一些行已被标记为已删除，但其中一些行仍然存在。删除文件中的行地址应用于从索引中筛选出结果。

2. 一个索引段已被完全删除。这可以通过检查数据集中是否缺少索引段位图中存在的片段ID来检测。应过滤掉此索引段中的任何行地址。

3. 一个索引段已将索引的一列更新到位。仅通过检查元数据无法检测到这一点。为了防止读取无效数据，引擎应该过滤掉不在索引当前fragment_bitmap中的任何行地址。该列不必是索引键控的列：字段中的每一列都有计数，包括covering_fields中仅包含的列。当键控列保持不变时，可以更新进位列，而覆盖该索引段的剩余段将从过时的进位值中应答。

4. 索引段在覆盖文件中具有更新的值。这可以通过检查索引的fragment_bitmap中的任何片段是否有覆盖文件来检测。对于commitd_version大于索引段dataset_version的每个覆盖层，覆盖层都携带未反映在索引中的更新值，因此其覆盖的行必须从索引结果中排除。被排除的行将根据其在平坦路径上的当前（叠加）值进行重新评估——如果不重新评估就丢弃它们，将默默地丢失与新值匹配的行。排除是字段感知的：只有覆盖索引字段中某一列的覆盖层才重要——键入或仅携带。将其限制在键控列中会在覆盖层更新进位值后覆盖一个片段，然后索引将提供一个过时的进位值。您可以仅排除受影响的行或整个片段；后者更简单、更安全，但重新评估的行数比必要的多。有关排除集、重新评估和正确性不变性，请参见数据覆盖文件。



## Compaction和remapping

压缩片段时，片段中行的行地址会发生变化。这意味着引用这些片段的任何索引段将不再指向现有的行地址。有三种方法可以处理这个问题：

1. 什么都不做，让索引段不再覆盖这些片段。这种方法简单有效，但这意味着压缩会立即使索引过时。这是查询性能最差的选项。

2. 立即用重新映射的行地址重写索引段。这种方法确保索引保持最新，但在压缩过程中会产生明显的写入放大。

3. 创建一个索引段重用索引，将旧行地址映射到新行地址。这允许读取器在读取索引段时重新映射内存中的行地址。这种方法在查询执行过程中增加了一些IO和计算开销，但避免了压缩过程中的写入放大。



## 索引的稳定行ID

索引可以选择使用稳定的行ID而不是行地址。稳定的行ID是一个逻辑标识符，即使在压缩过程中移动行，它也保持不变。

**优点：**

- 压实后无需重新映射

- 只有当索引的某个字段（键控列或covering_fields中命名的任何列）中的数据发生变化时，更新才会使索引无效

**权衡：**

- 需要额外的查找，以便在查询时将稳定的行ID转换为物理行地址。该功能目前处于实验阶段。性能评估正在进行中，以确定何时值得权衡。







# 标量索引

## Btree

BTree索引是一个两级结构，提供高效的范围查询和排序访问。它在包含所有值的昂贵内存结构和无法有效搜索的昂贵磁盘结构之间取得了平衡。

BTree的上层被设计为缓存在内存中并存储在BTree结构（page_lookup.lance）中，而叶子则使用子索引（page_data.lance，目前只是一个平面文件）进行搜索。这种设计实现了高效的内存使用——例如，对于10亿个值，索引可以存储256K个大小为4K的叶子，只需要几MiB的内存（取决于数据类型）用于BTree元数据，同时将任何搜索缩小到仅4K值。



### 存储布局

BTree索引由两个文件组成：

1. page_lookup.lance-B树结构映射值范围到页码

2. page_data.lance-包含排序值和行ID的实际子索引（平面文件）



#### Page Lookup File Schema (BTree Structure)

| Column       | Type       | Nullable | Description                                              |
| ------------ | ---------- | -------- | -------------------------------------------------------- |
| `min`        | {DataType} | true     | Minimum value in the page (forms BTree keys)             |
| `max`        | {DataType} | true     | Maximum value in the page (for range pruning)            |
| `null_count` | UInt32     | false    | Number of null values in the page                        |
| `page_idx`   | UInt32     | false    | Page number pointing to the sub-index in page_data.lance |

#### Schema Metadata

| Key          | Type   | Description                               |
| ------------ | ------ | ----------------------------------------- |
| `batch_size` | String | Number of rows per page (default: "4096") |

#### Page Data File Schema (Sub-indices)

| Column   | Type       | Nullable | Description                                       |
| -------- | ---------- | -------- | ------------------------------------------------- |
| `values` | {DataType} | true     | Sorted values from the indexed column (flat file) |
| `ids`    | UInt64     | false    | Row IDs corresponding to each value               |





### 加速查询

The BTree index provides exact results for the following query types:

| Query Type | Description               | Operation                                                                   |
| ---------- | ------------------------- | --------------------------------------------------------------------------- |
| **Equals** | `column = value`          | BTree lookup to find relevant pages, then search within sub-indices         |
| **Range**  | `column BETWEEN a AND b`  | BTree traversal for pages overlapping the range, then search each sub-index |
| **IsIn**   | `column IN (v1, v2, ...)` | Multiple BTree lookups, union results from all matching sub-indices         |
| **IsNull** | `column IS NULL`          | Returns rows from all pages where null_count > 0                            |



## Bitmap



## Label List



## Zone Map



## Bloom Filter



## Full Text Search



## N-gram



## FM-Index



## RTree



# 向量索引

Lance为高效的向量相似性搜索提供了一个强大且可扩展的二级索引系统。所有矢量索引都存储为常规Lance文件，使其易于移植和管理。它旨在跨大规模矢量数据集进行高效的相似性搜索。



## 概念

Lance将每个向量索引分为3个部分——聚类、子索引和量化。



## 聚类

ClusteringClustering将所有向量划分为不同的不相交簇（也称为分区）。Lance目前支持使用反向文件（IVF）作为主要的聚类机制。IVF使用k-means聚类算法将向量划分为簇。每个簇包含与簇质心相似的向量。在搜索过程中，只检查最相关的聚类，大大减少了搜索时间。IVF可以与任何子索引类型和量化方法相结合。



## 子索引

子索引子索引决定了如何组织向量进行搜索。Lance目前支持：

- FLAT：无近似的精确搜索-扫描所有向量

- HNSW：用于快速近似搜索的分层导航小世界图



## 量化

量化方法决定了矢量的存储和压缩方式。Lance目前支持：

- 产品量化（PQ）：通过将向量拆分为更小的子向量并独立量化每个子向量来压缩向量

- 标量量化（SQ）：独立地对向量的每个维度应用标量量化

- RabitQ（RQ）：使用随机旋转和二进制量化进行极端压缩

- FLAT：无量化，保留原始矢量以进行精确搜索



## 常见组合

当我们引用索引类型时，它通常是{clustering}_{sub_index}_{量化}。如果子索引只是FLAT，我们通常会省略它，只通过以下方式引用它{clustering}_{量化}。以下是常用的组合：

| Index Type      | Name                                            | Description                                                                              |
| --------------- | ----------------------------------------------- | ---------------------------------------------------------------------------------------- |
| **IVF_PQ**      | Inverted File with Product Quantization         | Combines IVF clustering with PQ compression for efficient storage and search             |
| **IVF_HNSW_SQ** | Inverted File with HNSW and Scalar Quantization | Uses IVF for coarse clustering and HNSW for fine-grained search with scalar quantization |
| **IVF_SQ**      | Inverted File with Scalar Quantization          | Combines IVF clustering with scalar quantization for balanced compression                |
| **IVF_RQ**      | Inverted File with RabitQ                       | Combines IVF clustering with RabitQ for extreme compression using binary quantization    |
| **IVF_FLAT**    | Inverted File without quantization              | Uses IVF clustering with exact vector storage for precise search within clusters         |



## 版本控制

到目前为止，Lance矢量索引格式已经经历了3个版本。本文档目前仅记录最新版本3。向量索引的特定版本记录在通用索引元数据的index_version字段中。



## 存储布局（V3）

每个矢量索引存储为2个常规Lance文件-索引文件和辅助文件。

### 索引文件

包含具有索引特定模式的搜索图/结构的索引结构文件。它存储在索引目录中名为index.idx的Lance文件中。

#### Arrow Schema

索引文件以图形或平面组织存储搜索结构。Lance文件的箭头模式因使用的子索引类型而异。

> 所有分区都存储在同一个文件中，分区必须按顺序写入。

#### Flat

FLAT索引执行精确搜索，没有近似值。这本质上是一个具有最小模式的空文件：

| Column          | Type   | Nullable | Description                                  |
| --------------- | ------ | -------- | -------------------------------------------- |
| `__flat_marker` | uint64 | false    | Marker field for FLAT index (no actual data) |



#### HNSW

HNSW（分层导航小世界）索引通过多级图结构提供快速近似搜索。这将使用以下模式存储HNSW图：

| Column        | Type   | Nullable | Description            |
| ------------- | ------ | -------- | ---------------------- |
| `__vector_id` | uint32 | true     | Vector identifier      |
| `__neighbors` | list   | true     | Neighbor node IDs      |
| `_distance`   | list   | true     | Distances to neighbors |

> HNSW由多个级别组成，所有级别必须从级别0开始按顺序编写。



#### Arrow Schema Metadata

索引文件在其箭头模式元数据中包含元数据，用于描述索引配置和结构。以下是元数据键及其对应值：

##### "lance:index"

Contains basic index configuration information in JSON:

| JSON Key        | Type   | Expected Values                                           |
| --------------- | ------ | --------------------------------------------------------- |
| `type`          | String | Index type (e.g., "IVF_PQ", "IVF_RQ", "IVF_HNSW", "FLAT") |
| `distance_type` | String | Distance metric (e.g., "l2", "cosine", "dot")             |

##### "lance:ivf"

引用存储在Lance文件全局缓冲区中的IVF元数据。此值记录全局缓冲区索引，目前始终为“1”。

> Lance文件中的全局缓冲区索引是从1开始的，因此在通过代码访问它们时需要减去1。

##### "lance:flat"

包含FLAT子索引结构的分区特定元数据。这是一个空字符串，因为FLAT索引目前不需要额外的元数据。

##### "lance:hnsw"

包含每个分区的HNSW特定JSON元数据，包括图形结构信息：

| JSON Key        | Type   | Expected Values                          |
| --------------- | ------ | ---------------------------------------- |
| `entry_point`   | u32    | Starting node for graph traversal        |
| `params`        | Object | HNSW construction parameters (see below) |
| `level_offsets` | Array  | Offset for each level in the graph       |

params对象包含以下HNSW构造参数：

| JSON Key            | Type   | Description                                                    | Default |
| ------------------- | ------ | -------------------------------------------------------------- | ------- |
| `max_level`         | u16    | Maximum level of the HNSW graph                                | 7       |
| `m`                 | usize  | Number of connections to establish while inserting new element | 20      |
| `ef_construction`   | usize  | Size of the dynamic list for candidates                        | 150     |
| `prefetch_distance` | Option | Number of vectors ahead to prefetch while building             | Some(2) |

### Lance File Global Buffer

。。。待补充TODO



# 系统索引


