# **Caching & Persistence in Apache Spark and Databricks**

## **Detailed theory, practical examples, notebook outputs and explanations**

Caching and persistence are among the most important performance optimization techniques in Apache Spark. They are particularly useful in data engineering pipelines, ETL processing, machine learning, exploratory data analysis and large-scale reporting.

In this module, we will cover how caching works internally, how Spark manages executor memory, different storage levels, performance benchmarking, execution plans, practical business use cases and common mistakes.

All practical examples use self-generated datasets so that you do not need to upload external files. Each notebook cell includes its expected output and an explanation of what that output signifies.

# **1. Why do we need caching in Spark?**

To understand caching, we first need to understand three fundamental concepts:

- Lazy evaluation
- DataFrame lineage
- Transformations and actions

## **1.1 Lazy evaluation**

Apache Spark does not immediately execute transformations when they are defined. Instead, it creates a logical execution plan describing the operations that need to be performed.

Consider the following code:

```
df = spark.range(1, 1000001)filtered_df = df.filter("id > 500000")result_df = filtered_df.withColumn(    "value",    filtered_df.id * 10)
```

Spark has defined three operations, but it has not yet computed all the resulting rows.

When we execute:

```
result_df.count()
```

Spark executes the operations necessary to produce the count.

This is called lazy evaluation.

## **1.2 What is DataFrame lineage?**

Lineage represents the sequence of operations used to construct a DataFrame from its original data source.

For example, an e-commerce pipeline might perform the following transformations.

DataFrame lineage records the transformations required to produce a result.

If you execute multiple actions on the same transformed DataFrame without caching, Spark may execute the required parts of that lineage repeatedly.

For large datasets, this could mean repeatedly performing:

- Reading millions of records from storage.
- Decompressing and decoding data.
- Filtering and cleaning records.
- Performing expensive joins.
- Shuffling data between executors.
- Calculating aggregations.

Caching helps prevent unnecessary repetition of previously computed intermediate results.

## **1.3 Without caching vs. with caching**

Without caching

With caching

Subsequent actions can reuse cached intermediate results instead of rebuilding them from the source.

Caching does not eliminate all processing. Each subsequent action still needs to perform its own downstream operations, such as aggregation or sorting.

# **2. How caching works internally in Apache Spark**

Spark is a distributed processing framework. A DataFrame is divided into multiple partitions that can be processed by different executors.

When you cache a DataFrame on classic Spark compute, Spark stores computed partitions using the executors' storage resources. It does not simply store the entire DataFrame in the driver's RAM.

Suppose you have a 6 GB DataFrame divided into six equally sized partitions, running on three executors.

| Executor   | Cached partitions | Illustrative data size |
| ---------- | ----------------- | ---------------------- |
| Executor 1 | P1, P2            | 2 GB                   |
| Executor 2 | P3, P4            | 2 GB                   |
| Executor 3 | P5, P6            | 2 GB                   |
| Total      | 6 partitions      | 6 GB                   |

Actual partition placement and cached size depend on task scheduling, compression, data distribution and available resources.

## **2.1 Internal components responsible for caching**

| Component          | Responsibility                                     |
| ------------------ | -------------------------------------------------- |
| Driver             | Coordinates execution and maintains query plans    |
| Executor           | Computes and stores DataFrame partitions           |
| CacheManager       | Registers cached Spark SQL queries                 |
| BlockManager       | Manages the storage and retrieval of cached blocks |
| MemoryStore        | Manages blocks held in memory                      |
| DiskStore          | Manages blocks stored on local disk                |
| Catalyst Optimizer | Identifies query plans that can reuse cached data  |

## **2.2 Life cycle of a cached DataFrame**

Consider:

```
df = spark.range(1000000)transformed_df = df.withColumn(    "square",    df.id * df.id)transformed_df.cache()transformed_df.count()transformed_df.show(5)transformed_df.unpersist()
```

On classic Spark, the following happens:

| Step | Operation       | Internal behavior                     |
| ---- | --------------- | ------------------------------------- |
| 1    | `spark.range()` | Defines source plan                   |
| 2    | `withColumn()`  | Adds transformation to the plan       |
| 3    | `cache()`       | Registers the plan for caching        |
| 4    | `count()`       | Computes and caches the partitions    |
| 5    | `show()`        | Can read previously cached partitions |
| 6    | `unpersist()`   | Removes cached blocks                 |

If Spark loses some cached partitions, it can generally recompute them using their original lineage.

# **3. Understanding Spark memory management**

Spark executor memory serves multiple purposes. Two particularly important categories are **execution memory** and **storage memory**.

Conceptual memory layout; execution and storage memory can dynamically share space.

### **Execution memory**

Used for computations such as joins, sorting, shuffling, aggregations and window functions.

### **Storage memory**

Used for retaining cached DataFrame and RDD blocks.

### **What happens when storage memory becomes full?**

Suppose a dataset requires 10 GB of cached storage, but only 6 GB of storage memory is currently available.

The behavior depends on the selected storage level.

| Storage level     | Possible behavior                                                         |
| ----------------- | ------------------------------------------------------------------------- |
| `MEMORY_ONLY`     | Retain available blocks in memory; recompute missing blocks when required |
| `MEMORY_AND_DISK` | Store blocks in memory or on disk                                         |
| `DISK_ONLY`       | Store cached blocks on local disk                                         |
| No persistence    | Recompute required transformations                                        |

Caching too many DataFrames can cause memory pressure, increase disk activity and negatively affect joins and aggregations. This is why indiscriminate caching is not recommended.

# **4. Deep dive into&#x20;**`cache()`

## **Definition**

The `cache()` method persists a DataFrame using Spark's default storage level.

### **Syntax**

```
dataframe.cache()
```

Internally, for modern classic PySpark DataFrames, it is equivalent to:

```
from pyspark import StorageLeveldataframe.persist(    StorageLevel.MEMORY_AND_DISK_DESER)
```

This has been the default DataFrame storage level since Spark 3.0.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

PySpark 4.3.0 documentation+1



## **4.1 What is serialization and deserialization?**

**Serialization** converts an in-memory data representation into bytes suitable for storing or transferring.

**Deserialization** reconstructs a usable in-memory representation from serialized data.

| Property           | Serialized           | Deserialized              |
| ------------------ | -------------------- | ------------------------- |
| Representation     | Encoded bytes        | In-memory representation  |
| Memory requirement | Generally lower      | Potentially higher        |
| CPU overhead       | May require decoding | May require less decoding |
| Access speed       | Workload dependent   | Workload dependent        |

Spark SQL also uses optimized columnar caching and compression. Therefore, a cached DataFrame should not be confused with a collection of ordinary Python objects stored in RAM.

# **5. Deep dive into&#x20;**`persist()`

## **Definition**

The `persist()` method allows developers to specify how computed data should be retained.

### **Syntax**

```
dataframe.persist(storageLevel)
```

### **Examples**

```
from pyspark import StorageLeveldf.persist(StorageLevel.MEMORY_ONLY)
```

```
df.persist(StorageLevel.MEMORY_AND_DISK)
```

```
df.persist(StorageLevel.DISK_ONLY)
```

If you do not provide a storage level:

```
df.persist()
```

Spark uses the same default storage level as `cache()` on classic compute.

## **5.1 Available storage levels**

| Storage level           | RAM | Disk | Deserialized flag | Replication |
| ----------------------- | --- | ---- | ----------------- | ----------- |
| `MEMORY_ONLY`           | Yes | No   | False             | 1           |
| `MEMORY_ONLY_2`         | Yes | No   | False             | 2           |
| `MEMORY_AND_DISK`       | Yes | Yes  | False             | 1           |
| `MEMORY_AND_DISK_2`     | Yes | Yes  | False             | 2           |
| `MEMORY_AND_DISK_DESER` | Yes | Yes  | True              | 1           |
| `DISK_ONLY`             | No  | Yes  | False             | 1           |
| `DISK_ONLY_2`           | No  | Yes  | False             | 2           |
| `DISK_ONLY_3`           | No  | Yes  | False             | 3           |
| `OFF_HEAP`              | Yes | Yes  | False             | 1           |
| `NONE`                  | No  | No   | False             | 1           |

These are PySpark `StorageLevel` flags. Replication of 2 means Spark attempts to maintain two copies of each persisted block, not two copies of the entire DataFrame on every executor.

## **5.2 Understanding storage level parameters**

A storage level can be defined using five parameters.

```
StorageLevel(    useDisk,    useMemory,    useOffHeap,    deserialized,    replication)
```

For example:

```
custom_level = StorageLevel(    True,    True,    False,    False,    1)print(custom_level)
```

**Expected output:**

```
Disk Memory Serialized 1x Replicated
```

This output signifies:

| Parameter      | Value | Meaning                       |
| -------------- | ----- | ----------------------------- |
| `useDisk`      | True  | Disk storage is allowed       |
| `useMemory`    | True  | Memory storage is allowed     |
| `useOffHeap`   | False | Off-heap storage is disabled  |
| `deserialized` | False | Serialized storage flag       |
| `replication`  | 1     | One copy of each cached block |

It is equivalent to `StorageLevel.MEMORY_AND_DISK`.

# **6. Important: Databricks Free Edition and serverless compute**

Before performing the practical labs, you should understand that caching behavior differs between classic Spark compute and newer Databricks serverless compute.

Databricks currently documents support for DataFrame caching on serverless environment version 5 and above when the relevant preview is enabled.

Unlike classic Spark, serverless DataFrame caching eagerly materializes a session-specific snapshot when `cache()` is called. On this environment, `persist()` behaves like `cache()` and ignores the requested storage level.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+2



| Feature              | Classic Spark               | Supported serverless caching |
| -------------------- | --------------------------- | ---------------------------- |
| `cache()`            | Lazy                        | Eager materialization        |
| `persist()`          | Configurable storage levels | Same behavior as `cache()`   |
| Custom storage level | Honored                     | Ignored                      |
| Storage management   | Spark executors             | Databricks-managed           |
| SQL `CACHE TABLE`    | Supported                   | Not supported                |
| `unpersist()`        | Supported                   | Supported                    |

The following labs identify which examples depend on classic compute. The data preparation and reporting examples work without needing `SparkContext`.

If caching APIs are unavailable in your Databricks workspace, use the Delta table alternative in Lab 9.

# **7. Practical Databricks notebook labs**

Execute the following cells sequentially in a Python notebook.

The data outputs below are calculated expected results. Execution times, physical plans, partition counts and storage metrics may vary depending on your Databricks compute environment.

## **Lab 1: Create an e-commerce sales dataset**

### **Notebook Cell 1 — Import libraries**

```
from pyspark.sql import functions as Ffrom pyspark import StorageLevelimport timeprint("Libraries imported successfully")
```

**Expected output:**

```
Libraries imported successfully
```

**Output significance:** All required PySpark libraries are available. No additional package installation is required.

### **Notebook Cell 2 — Create the DataFrame**

```
data = [    (101, "Amit",   "North", "Electronics", 25000),    (102, "Rahul",  "South", "Clothing",     3000),    (103, "Priya",  "North", "Electronics", 18000),    (104, "Neha",   "East",  "Grocery",      1500),    (105, "Rohit",  "West",  "Electronics", 32000),    (106, "Anjali", "South", "Clothing",     7000),    (107, "Karan",  "North", "Grocery",      2500),    (108, "Pooja",  "East",  "Electronics", 45000),    (109, "Vikas",  "West",  "Clothing",    12000),    (110, "Simran", "South", "Electronics", 22000)]schema = """    order_id INT,    customer_name STRING,    region STRING,    category STRING,    amount INT"""sales_df = spark.createDataFrame(data, schema)sales_df.orderBy("order_id").show()
```

**Expected output:**

| order_id | customer_name | region | category    | amount |
| -------- | ------------- | ------ | ----------- | ------ |
| 101      | Amit          | North  | Electronics | 25000  |
| 102      | Rahul         | South  | Clothing    | 3000   |
| 103      | Priya         | North  | Electronics | 18000  |
| 104      | Neha          | East   | Grocery     | 1500   |
| 105      | Rohit         | West   | Electronics | 32000  |
| 106      | Anjali        | South  | Clothing    | 7000   |
| 107      | Karan         | North  | Grocery     | 2500   |
| 108      | Pooja         | East   | Electronics | 45000  |
| 109      | Vikas         | West   | Clothing    | 12000  |
| 110      | Simran        | South  | Electronics | 22000  |

**Output significance:** We have created a DataFrame containing ten transactions across four geographical regions and three product categories.

### **Notebook Cell 3 — Inspect the schema**

```
sales_df.printSchema()
```

**Expected output:**

```
root
 |-- order_id: integer (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- region: string (nullable = true)
 |-- category: string (nullable = true)
 |-- amount: integer (nullable = true)
```

**Output significance:** The DataFrame has five columns with explicitly defined data types. Caching does not change these data types or the schema.

### **Notebook Cell 4 — Calculate basic statistics**

```
sales_df.agg(    F.count("*").alias("total_orders"),    F.sum("amount").alias("total_revenue"),    F.avg("amount").alias("average_order"),    F.min("amount").alias("minimum_order"),    F.max("amount").alias("maximum_order")).show()
```

**Expected output:**

| total_orders | total_revenue | average_order | minimum_order | maximum_order |
| ------------ | ------------- | ------------- | ------------- | ------------- |
| 10           | 168000        | 16800.0       | 1500          | 45000         |

**Output significance:** The company has ten orders generating ₹1,68,000 in total revenue. These statistics establish a baseline for subsequent examples.

## **Lab 2: Execute multiple actions without caching**

### **Notebook Cell 5 — Create a transformed DataFrame**

```
high_value_df = (    sales_df    .filter(F.col("amount") > 10000)    .withColumn("tax", F.col("amount") * 0.18)    .withColumn(        "final_amount",        F.col("amount") + F.col("tax")    ))high_value_df.orderBy("order_id").show()
```

**Expected output:**

| order_id | customer_name | region | category    | amount | tax    | final_amount |
| -------- | ------------- | ------ | ----------- | ------ | ------ | ------------ |
| 101      | Amit          | North  | Electronics | 25000  | 4500.0 | 29500.0      |
| 103      | Priya         | North  | Electronics | 18000  | 3240.0 | 21240.0      |
| 105      | Rohit         | West   | Electronics | 32000  | 5760.0 | 37760.0      |
| 108      | Pooja         | East   | Electronics | 45000  | 8100.0 | 53100.0      |
| 109      | Vikas         | West   | Clothing    | 12000  | 2160.0 | 14160.0      |
| 110      | Simran        | South  | Electronics | 22000  | 3960.0 | 25960.0      |

**Output significance:**

Spark filters orders above ₹10,000, calculates 18% tax and adds the final transaction amount. Six transactions satisfy the condition.

The resulting DataFrame has a lineage containing filtering and column transformations.

### **Notebook Cell 6 — Check whether it is cached**

```
print("Storage level:", high_value_df.storageLevel)print(    "Is persisted:",    high_value_df.storageLevel != StorageLevel.NONE)
```

**Expected output:**

```
Storage level: Serialized 1x Replicated
Is persisted: False
```

The exact text representation can vary.

**Output significance:** The DataFrame has not been persisted. Despite the storage-level string containing `1x Replicated`, no memory or disk caching is enabled.

### **Notebook Cell 7 — Execute three actions**

```
print("Total high-value orders:", high_value_df.count())high_value_df.groupBy("region").agg(    F.sum("amount").alias("revenue")).orderBy("region").show()high_value_df.groupBy("category").agg(    F.count("*").alias("orders")).orderBy("category").show()
```

**Expected output 1:**

```
Total high-value orders: 6
```

**Expected output 2:**

| region | revenue |
| ------ | ------- |
| East   | 45000   |
| North  | 43000   |
| South  | 22000   |
| West   | 44000   |

**Expected output 3:**

| category    | orders |
| ----------- | ------ |
| Clothing    | 1      |
| Electronics | 5      |

**What do these outputs signify?**

We have generated three reports using the same transformed DataFrame.

Although `high_value_df` is a Python variable, Spark has not automatically stored its complete computed result for reuse.

For each action, Spark can execute the necessary upstream transformations again. However, Catalyst may eliminate operations that are unnecessary for a particular result. For example, `count()` may not need to calculate the tax column at all.

## **Lab 3: Using&#x20;**`cache()`**&#x20;and inspecting storage**

### **Notebook Cell 8 — Cache the DataFrame**

```
high_value_df.cache()print("Storage level:", high_value_df.storageLevel)
```

**Expected output on classic compute:**

```
Storage level: Disk Memory Deserialized 1x Replicated
```

**Expected output on supported serverless compute:**

```
Storage level: Disk Serialized 1x Replicated
```

**Output significance:**

On classic compute, Spark has registered the DataFrame with its default memory-and-disk storage level. The cached partitions are populated by an action.

On supported serverless compute, the result has already been materialized, and the reported level is `DISK_ONLY`.

### **Notebook Cell 9 — Materialize and reuse cached data**

```
print("Total rows:", high_value_df.count())high_value_df.groupBy("region").agg(    F.sum("final_amount").alias("total_revenue")).orderBy("region").show()high_value_df.agg(    F.sum("tax").alias("total_tax")).show()
```

**Expected output 1:**

```
Total rows: 6
```

**Expected output 2:**

| region | total_revenue |
| ------ | ------------- |
| East   | 53100.0       |
| North  | 50740.0       |
| South  | 25960.0       |
| West   | 51920.0       |

**Expected output 3:**

| total_tax |
| --------- |
| 27720.0   |

**Output significance:** All three operations can reuse the cached high-value transaction data. The tax and final amount calculations do not need to be rebuilt from the original source for each report when the required cached results are available.

### **Notebook Cell 10 — Verify individual storage flags**

```
level = high_value_df.storageLevelprint("Use Disk:", level.useDisk)print("Use Memory:", level.useMemory)print("Use OffHeap:", level.useOffHeap)print("Deserialized:", level.deserialized)print("Replication:", level.replication)
```

**Expected output on classic compute:**

```
Use Disk: True
Use Memory: True
Use OffHeap: False
Deserialized: True
Replication: 1
```

**Expected output on supported serverless compute:**

```
Use Disk: True
Use Memory: False
Use OffHeap: False
Deserialized: False
Replication: 1
```

**Output significance:** These properties describe the configured storage policy. They do not tell you exactly how many bytes are currently cached in RAM.

For that information, inspect the Spark UI storage metrics on classic compute.

## **Lab 4: Verify caching through the execution plan**

An execution plan is one of the most useful ways to understand what Spark is actually doing internally.

### **Notebook Cell 11 — Inspect the execution plan**

```
high_value_df.explain(mode="formatted")
```

**Representative classic Spark output:**

```
== Physical Plan ==

InMemoryTableScan
   +- InMemoryRelation
         +- Project
               +- Filter
                     +- Scan ExistingRDD
```

The actual output will contain additional attributes, expressions, operator IDs and potentially Databricks-specific operators.

### **Understanding each execution plan operator**

| Operator            | Meaning                            |
| ------------------- | ---------------------------------- |
| `Scan ExistingRDD`  | Reads the underlying source data   |
| `Filter`            | Retains orders above ₹10,000       |
| `Project`           | Calculates and selects columns     |
| `InMemoryRelation`  | Represents the cached query result |
| `InMemoryTableScan` | Scans the cached result            |

**Important observation:** The original transformations may still appear underneath the cached relation in the execution plan. That does not necessarily mean Spark is recomputing them on every action.

On serverless compute, the plan can instead reflect a scan of the materialized snapshot.

### **Notebook Cell 12 — Remove the cache**

```
high_value_df.unpersist(blocking=True)print(    "Cache released:",    high_value_df.storageLevel == StorageLevel.NONE)
```

**Expected output:**

```
Cache released: True
```

**Output significance:** Spark no longer retains the DataFrame under its previous storage policy.

On classic compute, you can continue using the same DataFrame, and Spark can recompute it.

On supported serverless compute, `unpersist()` invalidates the cached DataFrame and its dependent DataFrames. You must recreate the DataFrame from its original source before using it again.

## **Lab 5: Experiment with different persistence levels**

This lab is particularly useful for understanding how `persist()` differs from `cache()`.

### **Notebook Cell 13 — MEMORY_ONLY**

```
memory_df = sales_df.filter(    F.col("amount") > 10000)memory_df.persist(StorageLevel.MEMORY_ONLY)print("Row count:", memory_df.count())print("Storage level:", memory_df.storageLevel)memory_df.unpersist(blocking=True)
```

**Expected classic compute output:**

```
Row count: 6
Storage level: Memory Serialized 1x Replicated
```

**Output significance:** The six records are eligible for memory-only caching. Missing or evicted blocks may need to be recomputed.

**Use case:** Small reference datasets, frequently accessed lookup data and intermediate datasets that comfortably fit into executor memory.

### **Notebook Cell 14 — MEMORY_AND_DISK**

```
memory_disk_df = sales_df.filter(    F.col("amount") > 10000)memory_disk_df.persist(    StorageLevel.MEMORY_AND_DISK)print("Row count:", memory_disk_df.count())print("Storage level:", memory_disk_df.storageLevel)memory_disk_df.unpersist(blocking=True)
```

**Expected classic compute output:**

```
Row count: 6
Storage level: Disk Memory Serialized 1x Replicated
```

**Output significance:** Spark can retain blocks in memory and use disk when necessary. The output does not necessarily mean that some data has actually been written to disk.

**Use case:** Large reusable datasets that might exceed the available storage memory.

### **Notebook Cell 15 — DISK_ONLY**

```
disk_df = sales_df.filter(    F.col("amount") > 10000)disk_df.persist(StorageLevel.DISK_ONLY)print("Row count:", disk_df.count())print("Storage level:", disk_df.storageLevel)disk_df.unpersist(blocking=True)
```

**Expected classic compute output:**

```
Row count: 6
Storage level: Disk Serialized 1x Replicated
```

**Output significance:** Spark retains computed blocks on local disk rather than using memory for their cached storage.

Although disk access is generally slower than memory access, reading already computed data from disk can still be faster than repeating an expensive join or aggregation.

### **Notebook Cell 16 — MEMORY_AND_DISK_DESER**

```
deser_df = sales_df.filter(    F.col("amount") > 10000)deser_df.persist(    StorageLevel.MEMORY_AND_DISK_DESER)print("Row count:", deser_df.count())print("Storage level:", deser_df.storageLevel)deser_df.unpersist(blocking=True)
```

**Expected classic compute output:**

```
Row count: 6
Storage level: Disk Memory Deserialized 1x Replicated
```

**Output significance:** This is the default storage policy used by modern classic Spark DataFrame caching.

If you execute all four examples on supported serverless compute, the row counts will remain the same, but all four storage-level outputs will report `DISK_ONLY`, because the explicit storage-level arguments are ignored.

# **8. Lab 6: Performance comparison using a large dataset**

The previous dataset contained only ten rows. It is useful for learning syntax but unsuitable for demonstrating meaningful performance differences.

We will now generate one million transactions and apply more computationally expensive transformations.

### **Notebook Cell 17 — Generate one million records**

```
from pyspark.sql import functions as Flarge_df = (    spark.range(0, 1000000)    .withColumn(        "customer_id",        F.col("id") % 10000    )    .withColumn(        "region_id",        F.col("id") % 5    )    .withColumn(        "amount",        ((F.col("id") % 1000) + 1) * 10    )    .withColumn(        "transaction_hash",        F.sha2(F.col("id").cast("string"), 256)    )    .withColumn(        "tax",        F.col("amount") * 0.18    )    .withColumn(        "final_amount",        F.col("amount") + F.col("tax")    ))print("Total records:", large_df.count())large_df.select(    "id",    "customer_id",    "region_id",
```

**Expected output:**

```
Total records: 1000000
```

| id | customer_id | region_id | amount | tax | final_amount |
| -- | ----------- | --------- | ------ | --- | ------------ |
| 0  | 0           | 0         | 10     | 1.8 | 11.8         |
| 1  | 1           | 1         | 20     | 3.6 | 23.6         |
| 2  | 2           | 2         | 30     | 5.4 | 35.4         |
| 3  | 3           | 3         | 40     | 7.2 | 47.2         |
| 4  | 4           | 4         | 50     | 9.0 | 59.0         |

**Output significance:** We now have one million transactions distributed among five region IDs. The dataset includes computed values and a SHA-256 hash for each transaction.

### **Notebook Cell 18 — Define an expensive intermediate computation**

```
def build_processed_data():    return (        large_df        .filter(F.col("amount") > 5000)        .groupBy("customer_id", "region_id")        .agg(            F.sum("final_amount").alias("total_amount"),            F.count("*").alias("total_orders"),            F.max("transaction_hash").alias("max_hash")        )    )processed_df = build_processed_data()processed_df.select(    "customer_id",    "region_id",    "total_amount",    "total_orders").orderBy("customer_id").show(5)
```

**Expected output:**

| customer_id | region_id | total_amount | total_orders |
| ----------- | --------- | ------------ | ------------ |
| 500         | 0         | 591180.0     | 100          |
| 501         | 1         | 592360.0     | 100          |
| 502         | 2         | 593540.0     | 100          |
| 503         | 3         | 594720.0     | 100          |
| 504         | 4         | 595900.0     | 100          |

Minor floating-point differences may occur.

**Output significance:** Spark has filtered high-value transactions and aggregated them by customer and region. This intermediate computation involves filtering, hashing and grouped aggregation, making it a stronger caching candidate.

### **Notebook Cell 19 — Define a reusable benchmark function**

```
def execute_reports(df):    result1 = df.agg(        F.sum("total_orders").alias("orders")    ).collect()    result2 = df.groupBy("region_id").agg(        F.sum("total_amount").alias("revenue")    ).collect()    result3 = df.agg(        F.max("max_hash").alias("max_hash")    ).collect()    return result1, result2, result3print("Benchmark function created")
```

**Expected output:**

```
Benchmark function created
```

**Output significance:** The function performs three independent actions against the same processed DataFrame. Each returns only a small result to the driver.

Using the same function for both experiments helps make the benchmark comparable.

### **Notebook Cell 20 — Measure execution without caching**

```
uncached_df = build_processed_data()start = time.perf_counter()uncached_results = execute_reports(uncached_df)without_cache_time = time.perf_counter() - startprint(    f"Without cache: {without_cache_time:.2f} seconds")print(    "Total filtered orders:",    uncached_results[0][0]["orders"])
```

**Illustrative output:**

```
Without cache: 12.00 seconds
Total filtered orders: 500000
```

**Output significance:** Half of the original one million records satisfy the condition `amount > 5000`.

The execution time represents the combined time for the three reporting actions without explicitly caching their shared intermediate data.

The 12-second value is illustrative, not a measured Databricks result.

### **Notebook Cell 21 — Measure execution with caching**

```
cached_df = build_processed_data()start_cache = time.perf_counter()cached_df.cache()cached_rows = cached_df.count()cache_build_time = time.perf_counter() - start_cachestart_reports = time.perf_counter()cached_results = execute_reports(cached_df)cached_report_time = (    time.perf_counter() - start_reports)total_cached_time = (    cache_build_time + cached_report_time)print("Cached rows:", cached_rows)print(f"Cache preparation: {cache_build_time:.2f}s")print(f"Cached reports: {cached_report_time:.2f}s")print(f"Total cached workflow: {total_cached_time:.2f}s")print(    "Total filtered orders:",    cached_results[0][0]["orders"])
```

**Illustrative output:**

```
Cached rows: 5000
Cache preparation: 4.00s
Cached reports: 2.00s
Total cached workflow: 6.00s
Total filtered orders: 500000
```

**Output significance:**

The filtered 500,000 transactions have been aggregated into 5,000 customer-region groups.

Rather than caching all one million raw records, we are caching the much smaller reusable intermediate result.

There are two separate costs:

- Cache preparation cost: time required to compute and retain the data.
- Cache reuse cost: time required to execute the reports against the cached data.

### **Notebook Cell 22 — Calculate performance improvement**

```
if total_cached_time > 0:    speedup = without_cache_time / total_cached_time    reduction = (        (without_cache_time - total_cached_time)        / without_cache_time    ) * 100    print(f"Speedup: {speedup:.2f}x")    print(f"Time reduction: {reduction:.2f}%")
```

**Illustrative output:**

```
Speedup: 2.00x
Time reduction: 50.00%
```

0 seconds4 seconds8 seconds12 seconds16 secondsWithout cacheCache preparationCached reportsTotal with cache

Illustrative benchmark — actual performance depends on cluster size, runtime, optimization and workload.

**Output significance:** In this hypothetical benchmark, caching reduces the total workflow execution time by 50%, including the cost of building the cache.

A fair comparison must include the initial caching cost. Comparing only the uncached workflow against already-warmed cached queries can exaggerate the improvement.

### **Notebook Cell 23 — Cleanup**

```
cached_df.unpersist(blocking=True)print("Benchmark cache released")
```

**Expected output:**

```
Benchmark cache released
```

For more reliable benchmarking, run each experiment several times, alternate execution order and compare median execution times.

# **9. Understanding the caching break-even point**

Caching has a cost. It is beneficial only when the savings from repeated reuse exceed the initial caching overhead.

Suppose:

| Operation                     | Cost       |
| ----------------------------- | ---------- |
| Recompute transformed dataset | 10 seconds |
| Compute and materialize cache | 12 seconds |
| Read cached dataset           | 2 seconds  |

Without caching:

\\[ T\_{\text{uncached}}=N \times 10 \\]

With caching:

\\[ T\_{\text{cached}}=12+N \times 2 \\]

Here, N represents the number of subsequent uses.

Caching becomes beneficial when:

\\[ N > \frac{12}{10-2} \\]

\\[ N > 1.5 \\]

Therefore, caching becomes beneficial from the second reuse in this simplified example.

## **Interactive caching performance simulator**

Recomputation cost per use

10s

Cache preparation cost

12s

Cached read cost per use

2s

Number of DataFrame uses

5

Without caching

# **50s**

With caching

# **22s**

Without caching

With caching

Time saved

# **28s**

Speedup

# **2.27x**

Caching becomes beneficial at 2 uses in this model.

This simulator illustrates why caching a DataFrame used only once is frequently a poor optimization decision.

# **10. Lab 7: Real-world ETL pipeline using joins and caching**

## **Business scenario**

An e-commerce organization maintains two datasets:

1. Customer master data.
2. Customer order transactions.

The organization wants to generate regional revenue, customer spending and tax reports.

All reports require an expensive shared join.

### **Notebook Cell 24 — Create customer and order datasets**

```
customers = [    (1, "Amit", "Delhi"),    (2, "Rahul", "Mumbai"),    (3, "Priya", "Bangalore"),    (4, "Neha", "Delhi")]orders = [    (1001, 1, 15000),    (1002, 2, 20000),    (1003, 1, 10000),    (1004, 3, 30000),    (1005, 4, 5000),    (1006, 3, 12000)]customers_df = spark.createDataFrame(    customers,    "customer_id INT, name STRING, city STRING")orders_df = spark.createDataFrame(    orders,    "order_id INT, customer_id INT, amount INT")customers_df.show()orders_df.show()
```

**Expected customer output:**

| customer_id | name  | city      |
| ----------- | ----- | --------- |
| 1           | Amit  | Delhi     |
| 2           | Rahul | Mumbai    |
| 3           | Priya | Bangalore |
| 4           | Neha  | Delhi     |

**Expected orders output:**

| order_id | customer_id | amount |
| -------- | ----------- | ------ |
| 1001     | 1           | 15000  |
| 1002     | 2           | 20000  |
| 1003     | 1           | 10000  |
| 1004     | 3           | 30000  |
| 1005     | 4           | 5000   |
| 1006     | 3           | 12000  |

**Output significance:** The two datasets can be combined using `customer_id` as the join key.

### **Notebook Cell 25 — Join and cache the transformed dataset**

```
enriched_df = (    orders_df.join(        customers_df,        on="customer_id",        how="inner"    )    .withColumn(        "tax",        F.col("amount") * 0.18    ))enriched_df.cache()print("Joined records:", enriched_df.count())enriched_df.orderBy("order_id").show()
```

**Expected output:**

```
Joined records: 6
```

| customer_id | order_id | amount | name  | city      | tax    |
| ----------- | -------- | ------ | ----- | --------- | ------ |
| 1           | 1001     | 15000  | Amit  | Delhi     | 2700.0 |
| 2           | 1002     | 20000  | Rahul | Mumbai    | 3600.0 |
| 1           | 1003     | 10000  | Amit  | Delhi     | 1800.0 |
| 3           | 1004     | 30000  | Priya | Bangalore | 5400.0 |
| 4           | 1005     | 5000   | Neha  | Delhi     | 900.0  |
| 3           | 1006     | 12000  | Priya | Bangalore | 2160.0 |

**Output significance:** The transaction data has been enriched with customer attributes. This is the ideal shared caching point because all subsequent reports depend on the joined data.

### **Notebook Cell 26 — City-wise revenue report**

```
city_report = enriched_df.groupBy("city").agg(    F.sum("amount").alias("total_revenue"),    F.count("*").alias("total_orders"))city_report.orderBy(    F.desc("total_revenue")).show()
```

**Expected output:**

| city      | total_revenue | total_orders |
| --------- | ------------- | ------------ |
| Bangalore | 42000         | 2            |
| Delhi     | 30000         | 3            |
| Mumbai    | 20000         | 1            |

₹0₹15K₹30K₹45KBangaloreDelhiMumbai

**Output significance:** Bangalore generates the highest revenue, even though Delhi has the highest order count.

The report can reuse the previously cached join result.

### **Notebook Cell 27 — Customer spending report**

```
customer_report = enriched_df.groupBy(    "customer_id",    "name").agg(    F.sum("amount").alias("total_spending"))customer_report.orderBy(    F.desc("total_spending")).show()
```

**Expected output:**

| customer_id | name  | total_spending |
| ----------- | ----- | -------------- |
| 3           | Priya | 42000          |
| 1           | Amit  | 25000          |
| 2           | Rahul | 20000          |
| 4           | Neha  | 5000           |

**Output significance:** Priya is the highest-spending customer. The original join does not need to be repeated if the cached result remains available.

### **Notebook Cell 28 — Tax report**

```
enriched_df.agg(    F.sum("amount").alias("total_sales"),    F.sum("tax").alias("total_tax"),    F.sum(        F.col("amount") + F.col("tax")    ).alias("gross_revenue")).show()
```

**Expected output:**

| total_sales | total_tax | gross_revenue |
| ----------- | --------- | ------------- |
| 92000       | 16560.0   | 108560.0      |

**Output significance:** The organization has generated its financial report from the same shared enriched DataFrame.

### **Notebook Cell 29 — Release cache**

```
enriched_df.unpersist(blocking=True)print("All reports completed. Cache released.")
```

**Expected output:**

```
All reports completed. Cache released.
```

### **Complete ETL architecture**

# **11. Lab 8: Caching, partitions and repartitioning**

Caching works at the partition level on classic Spark. Therefore, partitioning decisions can influence the efficiency of cached workloads.

### **Notebook Cell 30 — Create a partitioned DataFrame**

```
partitioned_df = (    spark.range(0, 100)    .repartition(4)    .withColumn(        "partition_id",        F.spark_partition_id()    ))partitioned_df.groupBy("partition_id").count() \    .orderBy("partition_id").show()
```

**Representative output:**

| partition_id | count |
| ------------ | ----- |
| 0            | 25    |
| 1            | 25    |
| 2            | 25    |
| 3            | 25    |

Exact distribution may vary.

**Output significance:** Spark distributes the 100 rows across four partitions. Each partition is a unit of parallel processing.

### **Notebook Cell 31 — Cache the partitioned DataFrame**

```
partitioned_df.cache()print("Total records:", partitioned_df.count())partitioned_df.groupBy("partition_id").agg(    F.count("*").alias("rows"),    F.sum("id").alias("sum_of_ids")).orderBy("partition_id").show()partitioned_df.unpersist(blocking=True)
```

**Expected output structure:**

```
Total records: 100
```

```
+------------+----+----------+
|partition_id|rows|sum_of_ids|
+------------+----+----------+
|           0| ...|       ...|
|           1| ...|       ...|
|           2| ...|       ...|
|           3| ...|       ...|
+------------+----+----------+
```

The sum of all partition row counts will be 100, and the sum of all `sum_of_ids` values will be 4,950.

**Output significance:** The DataFrame contains four logical partitions before caching. On classic compute, Spark can retain the computed partitions as distributed cached blocks.

### **Should you repartition before caching?**

Consider:

```
df.repartition(20).cache()
```

This can be useful when the original dataset has poorly distributed partitions and the new partitioning scheme benefits repeated downstream work.

However, `repartition()` generally introduces a shuffle. Do not repartition merely because you are caching.

# **12. Lab 9: Delta table as an alternative to caching**

Sometimes you want intermediate data to remain available across notebook sessions, compute restarts or independent jobs.

In those scenarios, a Delta table may be more appropriate than a temporary DataFrame cache.

This example requires a current catalog and schema where you have permission to create tables.

### **Notebook Cell 32 — Create reusable intermediate data**

```
reusable_sales_df = (    sales_df    .filter(F.col("amount") > 10000)    .withColumn(        "tax",        F.col("amount") * 0.18    ))reusable_sales_df.write \    .format("delta") \    .mode("errorifexists") \    .saveAsTable("cache_training_sales_demo")print("Delta table created successfully")
```

**Expected output:**

```
Delta table created successfully
```

**Output significance:** The processed data has been saved to durable Delta storage.

Unlike a cached DataFrame, the table can survive the termination of the current compute session.

### **Notebook Cell 33 — Read the Delta table**

```
saved_df = spark.table("cache_training_sales_demo")saved_df.groupBy("region").agg(    F.sum("amount").alias("revenue")).orderBy("region").show()
```

**Expected output:**

| region | revenue |
| ------ | ------- |
| East   | 45000   |
| North  | 43000   |
| South  | 22000   |
| West   | 44000   |

**Output significance:** The processed data can be reused without repeating the original filtering and tax calculation pipeline.

### **Notebook Cell 34 — Verify table format**

```
spark.sql("""    DESCRIBE DETAIL cache_training_sales_demo""").select(    "format",    "numFiles",    "sizeInBytes").show(truncate=False)
```

**Representative output:**

```
+------+--------+-----------+
|format|numFiles|sizeInBytes|
+------+--------+-----------+
|delta |       1|       ... |
+------+--------+-----------+
```

**Output significance:**

- `format`: Confirms that the table is using Delta Lake.
- `numFiles`: Number of active data files.
- `sizeInBytes`: Size of the active data files.

File count and physical size depend on runtime and write optimizations.

### **Notebook Cell 35 — Cleanup training table**

```
spark.sql("""    DROP TABLE IF EXISTS cache_training_sales_demo""")print("Training table removed")
```

**Expected output:**

```
Training table removed
```

Only execute this cleanup against the demonstration table.

# **13. Lab 10: SQL-based caching in Databricks**

**Compute requirement:** Classic Spark compute that supports SQL cache commands. These commands are not supported on Databricks serverless compute.

### **Notebook Cell 36 — Create a temporary view**

```
sales_df.createOrReplaceTempView(    "sales_cache_demo_view")print("Temporary view created")
```

**Expected output:**

```
Temporary view created
```

Creating a temporary view does not automatically cache its computed data.

### **Notebook Cell 37 — Cache using SQL**

```
%sql

CACHE TABLE sales_cache_demo_view;
```

**Expected output:** Successful execution, generally without a meaningful result table.

Unlike DataFrame `cache()` on classic Spark, SQL `CACHE TABLE` is eager by default. You can use `CACHE LAZY TABLE` if you want lazy materialization.

### **Notebook Cell 38 — Query the cached view**

```
%sql

SELECT
    region,
    SUM(amount) AS revenue,
    COUNT(*) AS total_orders
FROM sales_cache_demo_view
GROUP BY region
ORDER BY region;
```

**Expected output:**

| region | revenue | total_orders |
| ------ | ------- | ------------ |
| East   | 46500   | 2            |
| North  | 45500   | 3            |
| South  | 32000   | 3            |
| West   | 44000   | 2            |

**Output significance:** SQL users can reuse the cached view for repeated queries without explicitly using the Python DataFrame caching methods.

### **Notebook Cell 39 — Verify cache status**

```
print(    spark.catalog.isCached("sales_cache_demo_view"))
```

**Expected output:**

```
True
```

**Output significance:** Spark recognizes the view as cached.

### **Notebook Cell 40 — Remove SQL cache**

```
%sql

UNCACHE TABLE sales_cache_demo_view;
```

Verify:

```
print(    spark.catalog.isCached("sales_cache_demo_view"))
```

**Expected output:**

```
False
```

**Output significance:** The view still exists, but its cached results have been removed.

# **14. How to monitor caching in Spark UI**

On supported classic Databricks compute, open the Spark UI and navigate to the Storage tab after materializing a cached DataFrame.

**Spark UI — Storage**

Illustrative view

Cached partitions

# **8 / 8**

Memory size

# **240 MB**

Disk size

# **0 B**

Fraction cached

100% — All eight partitions materialized

| Storage level | Memory + Disk |
| ------------- | ------------- |
| Replication   | 1             |
| Executor 1    | 120 MB        |
| Executor 2    | 120 MB        |

The illustration above represents what cached storage metrics might look like; it is not an actual measurement from your cluster.

### **Understanding important metrics**

| Metric                | What it signifies                         |
| --------------------- | ----------------------------------------- |
| Cached partitions     | Number of partitions currently retained   |
| Fraction cached       | Percentage of partitions currently cached |
| Size in memory        | Actual memory occupied by cached blocks   |
| Size on disk          | Cached data occupying local disk          |
| Storage level         | Configured persistence policy             |
| Executor distribution | Location of cached blocks                 |

### **Different storage scenarios**

Scenario 1 — Entire dataset in memory

100%

All cached partitions are available from memory.

Scenario 2 — Memory and disk

60%

40%

Some cached partitions reside on disk.

Scenario 3 — Partial MEMORY_ONLY cache

60%

40%

Missing partitions may need to be recomputed.

Memory

Disk

Not cached

The percentages above illustrate partition distribution rather than exact memory measurements.

# **15. Different types of caching in Databricks**

One of the most common misconceptions is that every form of caching in Databricks is the same.

Databricks has multiple mechanisms for avoiding repeated processing or reading.

| Feature         | Spark DataFrame cache                       | Databricks disk cache              | SQL result cache                 |
| --------------- | ------------------------------------------- | ---------------------------------- | -------------------------------- |
| What is cached? | Computed DataFrame results                  | Supported underlying data files    | Query result                     |
| Activation      | Explicit                                    | Automatic on supported compute     | Managed automatically            |
| Typical purpose | Avoid repeated transformations              | Avoid remote file reads            | Avoid repeated query computation |
| Storage         | Executor memory/disk or serverless snapshot | Local storage on supported compute | Managed cache                    |
| Main use case   | PySpark pipelines                           | Delta and Parquet reads            | SQL analytics                    |

### **Example scenario**

```
df = spark.table("sales")transformed_df = (    df.filter("amount > 10000")      .groupBy("region")      .sum("amount"))
```

Databricks disk caching may accelerate reading the underlying table files.

However, that does not necessarily mean the filtering and aggregation results are cached.

When you execute:

```
transformed_df.cache()
```

You explicitly request reuse of the computed DataFrame result.

# **16. Cache vs. persist vs. checkpoint vs. Delta table**

These four techniques solve related but different problems.

| Feature                             | `cache()`         | `persist()`       | `checkpoint()`     | Delta table           |
| ----------------------------------- | ----------------- | ----------------- | ------------------ | --------------------- |
| Reuse computed data                 | Yes               | Yes               | Yes                | Yes                   |
| Custom storage policy               | No                | Classic compute   | No                 | Storage configuration |
| Truncates lineage                   | No, classic Spark | No, classic Spark | Yes                | New table read        |
| Survives cluster termination        | No                | No                | Files may survive  | Yes                   |
| Suitable for permanent data sharing | No                | No                | Not primarily      | Yes                   |
| Main purpose                        | Performance       | Storage control   | Lineage management | Durable storage       |

### **Understanding checkpointing**

Consider a machine learning algorithm that executes hundreds of transformations iteratively.

```
Source
  → Transformation 1
  → Transformation 2
  → Transformation 3
  → ...
  → Transformation 100
```

The logical plan can become very large.

Caching on classic Spark retains intermediate results, but it does not fundamentally remove the original lineage.

Checkpointing creates a materialized boundary that truncates the lineage, making subsequent computation depend on the checkpointed data rather than the entire original plan.

Reliable checkpointing requires supported compute and a configured checkpoint location.

**Remember:** Caching is mainly a performance optimization. Checkpointing is mainly used for managing lineage and computation recovery. Delta tables provide durable, reusable data.

# **17. Common caching mistakes in production**

## **Mistake 1: Caching a DataFrame that is used only once**

Incorrect optimization:

```
df.cache()df.show()
```

Spark incurs caching overhead without meaningful reuse.

Better:

```
df.show()
```

## **Mistake 2: Caching the raw dataset instead of the filtered dataset**

Suppose the source dataset contains 100 million records, but your workflow only needs 2 million records.

Less efficient:

```
raw_df.cache()filtered_df = raw_df.filter(    "amount > 10000")
```

Potentially better:

```
filtered_df = raw_df.filter(    "amount > 10000")filtered_df.cache()
```

This can significantly reduce the cached working set when only the filtered data is reused.

## **Mistake 3: Caching unnecessary columns**

Less efficient:

```
df.cache()
```

If the dataset contains 100 columns but reports require only three, consider:

```
selected_df = df.select(    "customer_id",    "region",    "amount")selected_df.cache()
```

Reducing the number of columns can reduce storage and cache preparation costs.

## **Mistake 4: Forgetting to call&#x20;**`unpersist()`

```
df1.cache()df2.cache()df3.cache()df4.cache()
```

Caching numerous datasets without releasing them can create unnecessary resource pressure.

Better:

```
df1.cache()# Complete all required processingdf1.unpersist()
```

## **Mistake 5: Assuming&#x20;**`show()`**&#x20;materializes all partitions**

On classic Spark:

```
df.cache()df.show(5)
```

Spark may need to compute only enough partitions to produce five rows.

If you want to materialize all partitions, use:

```
df.cache()df.count()
```

Note that this is different from supported serverless compute, where caching is eager.

## **Mistake 6: Assuming cache guarantees fault tolerance**

A cached block can be lost when an executor fails or is removed.

Classic Spark can generally recover missing blocks using lineage. Replication can reduce the likelihood of recomputation, but caching is not a replacement for durable storage.

## **Mistake 7: Caching stale data**

A cached DataFrame is not a general guarantee that subsequent queries will reflect the latest source changes.

For reliable freshness, recreate the DataFrame from its source and cache the new result if required.

On supported serverless compute, the cached DataFrame represents a fixed snapshot of the data at the time of caching.

# **18. Production-grade decision framework**

Use the following workflow to decide whether caching is appropriate.

# **19. Advanced interview questions and answers**

| Question                                                                 | Answer                                                                                                                                    |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| What is the default DataFrame storage level?                             | `MEMORY_AND_DISK_DESER` on modern classic Spark.                                                                                          |
| Is `cache()` lazy or eager?                                              | Lazy on classic Spark; eager on the documented Databricks serverless snapshot implementation.                                             |
| Does `persist()` trigger an action?                                      | Not on classic Spark.                                                                                                                     |
| Where is data cached?                                                    | Across executor storage resources on classic Spark, not solely on the driver.                                                             |
| Does caching eliminate shuffles?                                         | It can avoid recomputing upstream shuffles but does not eliminate new downstream shuffles.                                                |
| Does caching remove lineage?                                             | Not on classic Spark.                                                                                                                     |
| Can a cached DataFrame be larger than RAM?                               | Yes, with a suitable memory-and-disk storage level.                                                                                       |
| Is `DISK_ONLY` always slower than recomputation?                         | No. Disk reads may be cheaper than repeating expensive computations.                                                                      |
| Can storage levels be changed?                                           | On classic Spark, unpersist first and then persist with the new level.                                                                    |
| Can cached partitions be evicted?                                        | Yes, depending on memory pressure and the storage policy.                                                                                 |
| Does cache survive compute termination?                                  | No, not as durable user-accessible data.                                                                                                  |
| Can different DataFrames share the same cache?                           | Equivalent logical plans can reuse a cache on classic Spark; serverless reuse follows DataFrame derivation.                               |
| Is `cache()` always faster?                                              | No. It introduces preparation and storage overhead.                                                                                       |
| Does caching preserve row order?                                         | No. Use `orderBy()` when deterministic output ordering is required.                                                                       |
| Does caching change the original source data?                            | No.                                                                                                                                       |
| What is the difference between `unpersist(True)` and `unpersist(False)`? | On classic Spark, `True` waits for block removal; `False` does not. The parameter is ignored by the documented serverless implementation. |

# **20. Final revision cheat sheet**

```
from pyspark import StorageLevel# Default cachingdf.cache()# Materialize all partitions on classic Sparkdf.count()# Check storage policyprint(df.storageLevel)# Inspect execution plandf.explain("formatted")# Release cached datadf.unpersist(blocking=True)
```

Custom storage examples:

```
# Memory onlydf.persist(StorageLevel.MEMORY_ONLY)# Memory and diskdf.persist(StorageLevel.MEMORY_AND_DISK)# Disk onlydf.persist(StorageLevel.DISK_ONLY)# Default DataFrame storage leveldf.persist(StorageLevel.MEMORY_AND_DISK_DESER)
```

These are alternative storage choices, not four commands to execute sequentially against an already persisted DataFrame.

## **Official documentation**

- [Apache Spark — DataFrame.cache()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.cache.html?utm_source=chatgpt.com)
- [Apache Spark — DataFrame.persist()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html?utm_source=chatgpt.com)
- [Apache Spark — Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html?utm_source=chatgpt.com)
- [Apache Spark — Memory Tuning](https://spark.apache.org/docs/latest/tuning.html?utm_source=chatgpt.com)
- [Databricks — Cache DataFrames on Serverless Compute](https://docs.databricks.com/aws/en/compute/serverless/dataframe-cache?utm_source=chatgpt.com)

**The most important principle to remember:** Do not cache data simply because it is large. Cache data because it is expensive to compute, will be reused multiple times, and can be retained at a lower total cost than repeated computation.
