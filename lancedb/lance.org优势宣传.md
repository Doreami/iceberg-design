# 简介

Lance是一种用于多模式人工智能的现代开源lakehouse格式。它通过快速随机访问和扫描为lakehouse带来了高性能的矢量和全文搜索、特征工程和模型训练，同时保留了SQL分析、ACID事务、时间旅行以及与开放引擎（Apache Spark、Ray、PyTorch、Trino、DuckDB）和开放目录（Apache Polaris、Unity Catalog、Apache Gravitino、Hive Metastore）的集成。



# 核心优势

- **表达丰富的混合检索 (Expressive hybrid search)**：支持在同一个数据集上，结合**向量相似性搜索、全文搜索（BM25）和 SQL 分析**进行查询。所有查询类型都能通过 Lance 规范中的二级索引加速。

```python
import lance

ds = lance.dataset("s3://my-bucket/docs")

# Full text search
ds.to_table(full_text_query="machine learning")

# Hybrid search
ds.to_table(
    nearest={
        "column": "embedding", "q": query_vec, "k": 10
    },
    filter="year > 2020",
)
```

- **极速的随机访问 (Lightning-fast random access)**：官方宣称其随机访问速度比 Parquet 或 Iceberg **快 100 倍**。通过优化的文件格式、行寻址和二级索引，可以**瞬时获取**跨文件的单条记录，非常适合机器学习服务、数据采样和交互式应用。

```python
import lance

ds = lance.dataset("s3://my-bucket/embeddings.lance")

# Access the 2nd & 51st rows
ds.take([2, 51], columns=["id", "vec_gemma3"])

# Take 1000 random samples
ds.sample(1000, columns=["id", "vec_llama"])
```

- **原生多模态数据支持 (Native multimodal data)**：可以在一种格式中同时存储**图像、视频、音频、文本和嵌入向量**以及表格数据。其 blob 编码能高效处理大型二进制对象并支持懒加载，优化的向量存储则能加速相似性搜索。

```python
import lance
import av

ds = lance.dataset("s3://my-bucket/videos.lance")

# Get blobs from the 2nd and 51st rows
blobs = ds.take_blobs("video", ids=[2, 51])

for blob in blobs:
    with av.open(blob) as container:
        stream = container.streams.video[0]
        container.seek(start_time=500, stream=stream)
```

- **高效的数据演进 (Data evolution > schema evolution)**：传统上，回填列值通常需要重写整个表，但 Lance 支持**高效的 schema 演进与回填**——添加带数据的列只需向表写入新的 Lance 文件即可。

```python
import lance

dataset = lance.dataset("my_data.lance")

@lance.batch_udf()
def add_embeddings(batch):
    vectors = model.encode(batch["text"])
    return {"embedding": vectors}

dataset.add_columns(add_embeddings)
```

- **丰富的生态系统集成 (Rich ecosystem integrations)**：能与 **Pandas、Polars、Ray、PyTorch** 等工具集成，用于处理和机器学习；同时连接到 **Apache DataFusion、DuckDB、Apache Spark、Trino** 等，用于 SQL 分析和分布式处理。


