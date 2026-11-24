# **Partitioning Basics in PySpark — Databricks Complete Notes & Hands-on Labs**

## **1. What is partitioning in Apache Spark?**

**Partitioning is the process of dividing a large dataset into smaller logical chunks called partitions so that Apache Spark can process data in parallel across multiple executors.**

Partitioning is one of the most important concepts in PySpark because it directly affects:

- Parallel processing and resource utilization
- Job execution time
- Data shuffling between executors
- Memory consumption
- Join and aggregation performance
- Data storage and query performance

### **Real-world example**

Suppose an e-commerce company has **10 million customer orders** stored in a dataset.

Without sufficient parallelism, processing the data can become slow.

Spark can divide those records into multiple partitions and process them concurrently.

Conceptual illustration: four tasks run concurrently if sufficient executor resources are available. Actual partition sizes and scheduling vary.

For example, assuming evenly distributed data:

| Partitions | Records per partition |
| ---------- | --------------------- |
| 1          | 10,000,000            |
| 2          | 5,000,000             |
| 4          | 2,500,000             |
| 10         | 1,000,000             |
| 100        | 100,000               |

**Important:** Increasing partitions does not always improve performance. Too many partitions can increase scheduling and shuffle overhead.

## **2. Types of partitioning in Databricks**

There are two distinct concepts that you must understand.

| Feature       | Spark execution partitioning      | Storage partitioning               |
| ------------- | --------------------------------- | ---------------------------------- |
| Purpose       | Parallel data processing          | Organizing data on storage         |
| Works on      | DataFrame execution               | Files or tables                    |
| Main APIs     | `repartition()`, `coalesce()`     | `write.partitionBy()`              |
| Data movement | May involve shuffle               | Organizes records by column values |
| Benefit       | Parallelism and task distribution | Partition pruning                  |
| Example       | Divide orders into 8 tasks        | Store orders by country            |

### **A. Spark execution partitions**

These represent logical portions of a DataFrame processed by Spark tasks.

```
df = spark.range(0, 100)df = df.repartition(4)
```

Spark redistributes the records into four execution partitions.

### **B. Storage partitions**

These organize data into directories based on column values.

```
df.write \    .mode("overwrite") \    .partitionBy("country") \    .parquet("/Volumes/main/default/labs/orders")
```

The resulting folder layout resembles:

```
orders/
├── country=India/
│   └── part-00000-....snappy.parquet
├── country=USA/
│   └── part-00001-....snappy.parquet
└── country=UK/
    └── part-00002-....snappy.parquet
```

One directory can contain multiple files, and the directory count does not tell you the number of Spark execution partitions.

## **3. Creating a DataFrame for partitioning practice**

The following examples work in a Databricks Python notebook, including serverless compute. They avoid direct `SparkContext` access.

### **Notebook Cell 1 — Create sample e-commerce data**

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as Torders_data = [    (1, "India", "Electronics", 50000, "2026-01-01"),    (2, "India", "Clothing", 2000, "2026-01-01"),    (3, "USA", "Electronics", 70000, "2026-01-02"),    (4, "UK", "Furniture", 15000, "2026-01-02"),    (5, "India", "Furniture", 25000, "2026-01-03"),    (6, "USA", "Clothing", 3000, "2026-01-03"),    (7, "UK", "Electronics", 45000, "2026-01-04"),    (8, "India", "Electronics", 60000, "2026-01-04"),    (9, "USA", "Furniture", 40000, "2026-01-05"),    (10, "India", "Clothing", 5000, "2026-01-05")]schema = T.StructType([    T.StructField("order_id", T.IntegerType(), False),    T.StructField("country", T.StringType(), True),    T.StructField("category", T.StringType(), True),    T.StructField("amount", T.IntegerType(), True),    T.StructField("order_date", T.StringType(), True)])df = spark.createDataFrame(orders_data, schema)df = df.withColumn(    "order_date",    F.to_date("order_date"))display(df)
```

SAMPLE OUTPUT

| order_id | country | category    | amount | order_date |
| -------- | ------- | ----------- | ------ | ---------- |
| 1        | India   | Electronics | 50000  | 2026-01-01 |
| 2        | India   | Clothing    | 2000   | 2026-01-01 |
| 3        | USA     | Electronics | 70000  | 2026-01-02 |
| 4        | UK      | Furniture   | 15000  | 2026-01-02 |
| 5        | India   | Furniture   | 25000  | 2026-01-03 |

The remaining five rows are also available in `df`.

## **4. repartition() — Increase or decrease partitions**

`repartition()` creates a new DataFrame whose records are redistributed across partitions.

It generally causes a **full shuffle**, meaning records move between tasks and potentially between executors.

### **Syntax**

```
df.repartition(numPartitions)df.repartition(numPartitions, "column_name")
```

### **Notebook Cell 2 — Repartition into four partitions**

```
df_repartitioned = df.repartition(4)df_repartitioned.select(    "order_id",    "country",    F.spark_partition_id().alias("partition_id")).orderBy("partition_id", "order_id").show(20)
```

Use the following to inspect how many rows went into each partition:

```
df_repartitioned \    .groupBy(F.spark_partition_id().alias("partition_id")) \    .agg(F.count("*").alias("row_count")) \    .orderBy("partition_id") \    .show()
```

The counts can vary by Spark version and execution plan.

### **Notebook Cell 3 — Repartition using a column**

```
df_country = df.repartition(4, "country")df_country.select(    "order_id",    "country",    F.spark_partition_id().alias("partition_id")).orderBy("country", "order_id").show()
```

All rows with the same `country` value will be assigned to the same hash partition for this repartition operation.

However, because there are only three countries, some of the four partitions might be empty.

**Use case:** Redistributing records by a join key or grouping key before subsequent operations, when that layout is beneficial.

### **Understanding shuffling**

A shuffle is relatively expensive because it may involve network transfer, disk I/O, serialization, and sorting or hashing overhead.

## **5. coalesce() — Reduce partitions efficiently**

`coalesce()` is commonly used to reduce the number of partitions without a full shuffle.

### **Syntax**

```
df.coalesce(numPartitions)
```

### **Notebook Cell 4 — Reduce partitions**

```
df_large = spark.range(0, 1000, 1, 8)df_reduced = df_large.coalesce(2)df_reduced.select(    F.spark_partition_id().alias("partition_id")).groupBy("partition_id").count().show()
```

This typically reduces eight source partitions to two without a full redistribution.

### **repartition() vs coalesce()**

| Feature             | repartition()              | coalesce()          |
| ------------------- | -------------------------- | ------------------- |
| Increase partitions | Yes                        | No, not normally    |
| Decrease partitions | Yes                        | Yes                 |
| Full shuffle        | Typically yes              | Normally no         |
| Redistribute data   | Yes                        | Limited             |
| Best for            | Balancing/reshuffling data | Reducing partitions |
| Data skew handling  | Can improve distribution   | May preserve skew   |

According to Spark's documentation, `coalesce()` uses a narrow dependency and avoids a full shuffle in normal cases.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

PySpark 4.3.0 documentation+1



**Example:** If you have 200 partitions and want 20 partitions for output, `coalesce(20)` may reduce overhead. If partitions are severely unbalanced, `repartition(20)` may perform better despite the shuffle.

## **6. partitionBy() — Physical storage partitioning**

`partitionBy()` is used when writing data to storage.

Unlike `repartition()`, which controls Spark execution partitions, `partitionBy()` organizes output data according to column values.

### **Real-world use case**

Suppose you have 500 million sales transactions from multiple countries.

Users frequently run queries such as:

```
SELECT *
FROM sales
WHERE country = 'India';
```

With traditional directory-based partitioning on `country`, Spark can skip directories belonging to other countries. This optimization is called **partition pruning**.

### **Notebook Cell 5 — Create a partitioned Delta table**

This example works with a schema where you have permission to create tables.

```
# Check the current catalog and schema.spark.sql("""SELECT current_catalog(), current_schema()""").show()# Save as a managed Delta table partitioned by country.df.write \    .format("delta") \    .mode("overwrite") \    .partitionBy("country") \    .saveAsTable("partitioning_demo_orders")
```

The resulting physical layout conceptually resembles:

```
partitioning_demo_orders/
│
├── _delta_log/
│
├── country=India/
│   └── part-....snappy.parquet
│
├── country=USA/
│   └── part-....snappy.parquet
│
└── country=UK/
    └── part-....snappy.parquet
```

Delta Lake also maintains a transaction log, making this different from a simple Parquet directory.

### **Notebook Cell 6 — Read the partitioned table**

```
partitioned_df = spark.table("partitioning_demo_orders")display(partitioned_df)
```

Now filter for India:

```
india_orders = partitioned_df.filter(    F.col("country") == "India")display(india_orders)
```

Expected records:

| order_id | country | category    | amount |
| -------- | ------- | ----------- | ------ |
| 1        | India   | Electronics | 50000  |
| 2        | India   | Clothing    | 2000   |
| 5        | India   | Furniture   | 25000  |
| 8        | India   | Electronics | 60000  |
| 10       | India   | Clothing    | 5000   |

### **Notebook Cell 7 — Partition pruning**

```
spark.sql("""SELECT *FROM partitioning_demo_ordersWHERE country = 'India'""").explain("formatted")
```

Look for partition filters in the physical plan, for example:

```
PartitionFilters:
  [isnotnull(country), (country = India)]
```

The exact execution plan depends on Databricks Runtime and compute configuration.

**Benefit:** Spark can avoid reading irrelevant country partitions rather than scanning the entire dataset.

## **7. Partitioning by multiple columns**

You can partition files by more than one column.

### **Example: Country and order date**

```
df.write \    .format("delta") \    .mode("overwrite") \    .partitionBy("country", "order_date") \    .saveAsTable("partitioning_demo_multi")
```

Conceptual storage structure:

```
partitioning_demo_multi/
│
├── country=India/
│   ├── order_date=2026-01-01/
│   ├── order_date=2026-01-03/
│   ├── order_date=2026-01-04/
│   └── order_date=2026-01-05/
│
├── country=USA/
│   ├── order_date=2026-01-02/
│   ├── order_date=2026-01-03/
│   └── order_date=2026-01-05/
│
└── country=UK/
    ├── order_date=2026-01-02/
    └── order_date=2026-01-04/
```

Query:

```
spark.sql("""SELECT *FROM partitioning_demo_multiWHERE country = 'India'AND order_date = DATE '2026-01-01'""").show()
```

This is useful when a large legacy table has predictable query patterns involving the same partition columns.

However, too many partition combinations can create numerous small directories and files.

## **8. Data skew and partitioning**

**Data skew occurs when some partitions contain significantly more data than others.**

Suppose your application processes 1 million transactions:

| Country | Transactions |
| ------- | ------------ |
| India   | 800,000      |
| USA     | 100,000      |
| UK      | 60,000       |
| Canada  | 40,000       |

Hash partitioning by `country` can cause one task to handle most records.

Uneven partition workload

0250K500K750K1MIndiaUSAUKCanada

Illustrative distribution of 1 million records

The task processing India's records might take much longer than the others.

### **Notebook Cell 8 — Create skewed data**

```
india_df = spark.range(0, 80000).select(    F.col("id").alias("order_id"),    F.lit("India").alias("country"))usa_df = spark.range(80000, 90000).select(    F.col("id").alias("order_id"),    F.lit("USA").alias("country"))uk_df = spark.range(90000, 100000).select(    F.col("id").alias("order_id"),    F.lit("UK").alias("country"))skewed_df = india_df.unionByName(usa_df).unionByName(uk_df)skewed_partitioned = skewed_df.repartition(4, "country")skewed_partitioned \    .groupBy(F.spark_partition_id().alias("partition_id")) \    .count() \    .orderBy("partition_id") \    .show()
```

Observe that partition sizes may be unequal.

### **Notebook Cell 9 — Improve overall distribution**

```
balanced_df = skewed_df.repartition(4)balanced_df \    .groupBy(F.spark_partition_id().alias("partition_id")) \    .count() \    .orderBy("partition_id") \    .show()
```

Repartitioning without a key generally distributes records more evenly.

**Important distinction:** This does not solve every skewed join or aggregation problem. If all records with the same key must be processed together, Spark may still experience skew. Techniques such as salting, adaptive query execution, and skew-join optimization can be necessary.

## **9. How many partitions should you create?**

There is no universal ideal number.

The optimal partition count depends on data volume, executor resources, shuffle requirements, file sizes, and Spark's adaptive query execution.

Partition sizing calculator

Illustrative sizing estimate for execution partitions, not an automatic recommendation.

Data volume

10 GB

Estimated target size

128 MB

256 MB

512 MB

Estimated partitions

# **80**

Approx. size per partition

# **128 MB**

Formula: partitions ≈ total data size ÷ target partition size. Real Spark partitions depend on input splits, compression, processing, and AQE.

In practice, check the Spark UI for task duration, skew, shuffle read/write, and spill metrics before changing partition counts.

## **10. Partitioning in modern Databricks: Liquid clustering**

An important Databricks recommendation is that **new Delta Lake tables should generally use liquid clustering instead of traditional static partitions**.

Databricks recommends avoiding static partitioning for tables smaller than approximately 1 TB and choosing partition columns only when each partition is expected to contain at least approximately 1 GB. Liquid clustering can organize data without fixed partition-directory boundaries.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+2



For example, on a Databricks environment that supports liquid clustering:

```
CREATE TABLE IF NOT EXISTS orders_clustered (
    order_id BIGINT,
    country STRING,
    amount DOUBLE,
    order_date DATE
)
USING DELTA
CLUSTER BY (country, order_date);
```

This is a separate modern table-layout technique. It does not replace execution-level `repartition()` or `coalesce()`.

## **11. Quick comparison**

| Method                      | Purpose                                      | Shuffle?                              |
| --------------------------- | -------------------------------------------- | ------------------------------------- |
| `repartition(8)`            | Create 8 execution partitions                | Yes, typically                        |
| `repartition(8, "country")` | Hash distribute by country                   | Yes                                   |
| `coalesce(2)`               | Reduce execution partitions                  | Usually no full shuffle               |
| `partitionBy("country")`    | Create physical data partitions when writing | Writer may shuffle or reorganize data |
| `CLUSTER BY`                | Optimize Delta table data layout             | Managed by Databricks optimization    |

## **12. Interview questions and answers**

**Q1. What is the difference between&#x20;**`repartition()`**&#x20;and&#x20;**`partitionBy()`**?**

`repartition()` controls Spark execution partitions. `partitionBy()` controls the physical directory layout when writing data.

**Q2. What is the difference between&#x20;**`repartition()`**&#x20;and&#x20;**`coalesce()`**?**

`repartition()` generally performs a shuffle and can increase or decrease the partition count. `coalesce()` normally avoids a full shuffle and is used to reduce partitions.

**Q3. What is partition pruning?**

Partition pruning is an optimization where Spark avoids reading partitions that cannot satisfy a query's filters.

**Q4. Does increasing partitions always improve performance?**

No. Too many partitions cause task-scheduling overhead and potentially small-file problems. Too few partitions reduce parallelism and may lead to large tasks.

**Q5. Does&#x20;**`repartition(4, "country")`**&#x20;create four country folders?**

No. It creates four execution partitions using a hash-based distribution. Physical country directories are created using `write.partitionBy("country")`.

**Q6. What is the difference between&#x20;**`groupBy()`**&#x20;and partitioning?**

`groupBy()` aggregates rows sharing the same key, whereas partitioning divides records into logical processing units or organizes stored files. A `groupBy()` commonly triggers a shuffle.

**Q7. Why does Spark sometimes show fewer or more tasks than expected?**

Spark's Adaptive Query Execution (AQE), source partition counts, shuffle partition settings, and query stages can affect the actual number of tasks.

## **13. Final Databricks practice exercise**

Execute this notebook cell to create a large synthetic dataset, repartition it, and calculate totals by partition.

```
from pyspark.sql import functions as F# Create 100,000 recordslarge_df = spark.range(1, 100001)# Add country and amountlarge_df = (    large_df    .withColumn(        "country",        F.when(F.col("id") % 3 == 0, "India")         .when(F.col("id") % 3 == 1, "USA")         .otherwise("UK")    )    .withColumn(        "amount",        (F.col("id") % 5000).cast("double")    ))# Repartition into 6 partitionsrepartitioned_df = large_df.repartition(6)# Inspect distributionpartition_stats = (    repartitioned_df    .withColumn("partition_id", F.spark_partition_id())    .groupBy("partition_id")    .agg(        F.count("*").alias("total_records"),        F.sum("amount").alias("total_amount")    )    .orderBy("partition_id"))display(partition_stats)
```

The complete example is executable without manually uploading files or accessing `spark.sparkContext`.

**Key takeaway:** Use Spark execution partitions to control parallel processing, use storage partitioning for suitable large traditional datasets, and prefer liquid clustering for new Databricks Delta tables when supported.
