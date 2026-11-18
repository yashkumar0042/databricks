# Module 2 — Apache Spark & PySpark Mastery (Databricks)

**Hands-on, instructor-ready notes and notebook** · Updated November 2026 · PySpark / SQL · Fundamentals through intermediate/advanced ETL

**Goal:** Build and analyze an e-commerce data pipeline using one consistent dataset. Every section includes a definition, example, practical use case, and notes on Databricks behavior. The core notebook operates from in-memory DataFrames. File I/O labs run **only if** you supply an existing, writable Unity Catalog Volume path.

**Recommended compute:** Databricks serverless notebook or classic standard compute for the core labs. Legacy RDD, SparkContext broadcast variables, and accumulators require compatible **classic dedicated** compute.

**Run instructions:** Import the `.ipynb` file into Databricks as a notebook; attach compute; run from top to bottom. No separate packages are required for the core labs. `pandas_udf` demonstration is optional.

**Learning map:** Spark motivation → architecture → RDDs → DataFrames/Datasets → sessions → lazy execution → formats/schema → transformations/aggregations/joins/windows → UDFs → broadcast/accumulators → write modes → partitioning → caching → capstone → exercises.

---

## 1. Why Apache Spark exists

**Definition:** Apache Spark is a distributed processing engine that executes data-intensive operations across a cluster rather than being limited to one machine.

**Problem it solves:** Single-host memory/CPU constraints, slow serial ETL, and historical MapReduce-style pipelines requiring repeated disk-materialization steps. Spark supports parallel computation, SQL/DataFrame optimization, caching, and fault recovery using lineage.

**Example:** An online marketplace processes billions of order events to calculate revenue by region and prepare reporting tables.

**Use cases:** ETL/ELT, batch and streaming analytics, feature engineering, log analysis, data quality, and lakehouse transformations.

**Databricks context:** Spark is the distributed processing engine; Databricks adds managed compute, Unity Catalog governance, Delta Lake tables, collaborative notebooks, and workflows. Delta Lake tables are usually the right persisted output for production.

---

## 2. Architecture: Driver, Executor, Cluster Manager, Jobs, Stages, Tasks

**Driver:** Coordinates the Spark application, builds and schedules the execution plan, and receives results. `collect()` and `toPandas()` can exhaust driver memory if misused.

**Executor:** Executes tasks for partitions, participates in shuffles, and may cache intermediate data. Multiple tasks may run concurrently on an executor.

**Cluster manager / compute layer:** Allocates compute resources; Databricks handles infrastructure allocation and management. Spark supports Kubernetes, YARN, and Standalone in other deployments.

**Job:** Execution triggered by an action such as `count()` or `show()`; a job consists of stages.

**Stage:** A set of tasks Spark can execute without a data-exchange boundary; shuffles often separate stages.

**Task:** Work performed on a partition.

**Shuffle:** Network/data redistribution for operations such as `groupBy`, many joins, and `repartition`. Shuffles can be expensive.

**Use case:** In the Spark UI, inspect long-running stages, skewed tasks, and shuffle-read/write volumes. `explain("formatted")` displays the physical execution plan.

---

```python
from pyspark.sql import functions as F, types as T, Window

big_demo = (spark.range(1, 100_001)
            .withColumn("region_id", F.col("id") % 4))
by_region = big_demo.groupBy("region_id").agg(F.count("*").alias("transactions"))
by_region.orderBy("region_id").show()
print("Explain the physical plan (look for Exchange / shuffle):")
by_region.explain("formatted")
```

---

## 3. Reusable lab dataset — e-commerce orders, customers, employees

**Definition:** `spark.createDataFrame` converts a small local Python collection into a distributed Spark DataFrame. For production use, ingest from governed tables or storage rather than constructing large Python lists.

**Data model:** `orders` is the fact table; `customers` is a dimension; `employees` demonstrates ranking windows. Deliberate missing join keys show join behavior. Values use `double` for teaching convenience; production money should usually use `DecimalType`.

**Use case:** Practice day-to-day Spark API operations on consistent, easy-to-verify data.

---

```python
orders_schema = T.StructType([
    T.StructField("order_id", T.IntegerType(), False),
    T.StructField("customer_id", T.IntegerType(), False),
    T.StructField("order_date", T.StringType(), False),
    T.StructField("category", T.StringType(), False),
    T.StructField("amount", T.DoubleType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("region", T.StringType(), False),
])

orders_data = [
    (1001, 1, "2026-11-01", "electronics", 1200.0, "paid", "north"),
    (1002, 1, "2026-11-03", "books",        200.0, "paid", "north"),
    (1003, 2, "2026-11-01", "electronics",  700.0, "paid", "south"),
    (1004, 2, "2026-11-05", "books",        150.0, "cancelled", "south"),
    (1005, 3, "2026-11-02", "home",         450.0, "paid", "west"),
    (1006, 4, "2026-11-02", "books",        300.0, "paid", "north"),
    (1007, 3, "2026-11-06", "electronics",  900.0, "paid", "west"),
    (1008, 1, "2026-11-06", "home",         250.0, "paid", "north"),
]
orders = spark.createDataFrame(orders_data, schema=orders_schema)

customers = spark.createDataFrame([
    (1, "Asha", "north"), (2, "Bharat", "south"),
    (3, "Charu", "west"), (5, "Dev", "east")
], "customer_id int, customer_name string, home_region string")

employees = spark.createDataFrame([
    ("E1", "Data", 100000, "2026-11-01"),
    ("E2", "Data",  90000, "2026-11-02"),
    ("E3", "Data",  90000, "2026-11-03"),
    ("E4", "Data",  80000, "2026-11-04"),
    ("E5", "Platform", 120000, "2026-11-01"),
    ("E6", "Platform", 110000, "2026-11-02"),
    ("E7", "Platform", 110000, "2026-11-03"),
    ("E8", "Platform",  95000, "2026-11-04"),
], "employee_id string, department string, salary int, effective_date string")

print("Orders:"); orders.orderBy("order_id").show(truncate=False)
print("Customers:"); customers.orderBy("customer_id").show()
print("Order schema:"); orders.printSchema()
```

---

## 4. RDDs — original Spark abstraction (historical / classic-only)

**Definition:** A *Resilient Distributed Dataset* is an immutable, distributed collection of records with lineage-based fault tolerance. Basic operators include `map`, `filter`, `flatMap`, and `reduceByKey`.

**Benefits:** Flexible low-level operations and explicit partition-oriented processing.

**Limitations:** No relational schema and fewer automatic SQL optimizations; not the recommended API for most ETL.

**Use case:** Understanding legacy Spark code and low-level partition control. **Databricks support:** RDD APIs are not supported on serverless and standard compute; only run the guarded code after selecting a compatible classic dedicated cluster.

---

```python
RUN_CLASSIC_RDD_LABS = False  # Change to True ONLY on compatible classic dedicated compute.

if RUN_CLASSIC_RDD_LABS:
    sc = spark.sparkContext
    rdd = sc.parallelize([1, 2, 3, 4, 5])
    print("RDD squares:", rdd.map(lambda n: n * n).collect())
    words = sc.parallelize(["spark sql", "spark fast"]).flatMap(lambda s: s.split())
    print("RDD word counts:", words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b).collect())
else:
    print("RDD lab skipped: not available on Databricks serverless or standard compute.")

# Recommended DataFrame replacement for the same operation:
df_numbers = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["number"])
df_numbers.select("number", (F.col("number") ** 2).alias("square")).show()
```

---

## 5. RDD vs DataFrame vs Dataset

| Item | RDD | DataFrame | Typed Dataset |
|---|---|---|---|
| Structure | Distributed objects | Named columns and schema | Typed records with schema |
| Optimization | Mostly developer-controlled | Catalyst and Spark SQL planner | Catalyst and type-aware JVM API |
| API support | Scala, Java, Python (classic) | Python, Scala, Java, SQL | Mainly Scala and Java |
| Databricks choice | Legacy/low-level | **Default choice for ETL** | Specialized JVM use cases |

**Definition:** A DataFrame is an immutable, distributed tabular dataset with a schema. A typed Dataset offers compile-time typing in Scala/Java; Python does not expose a separate typed Dataset API.

**Example:** `orders.filter(...).groupBy(...).agg(...)` lets Spark optimize the relational plan.

**Use case:** Bronze-to-silver cleansing, silver-to-gold reporting, warehouse queries, and feature engineering. Prefer DataFrames for PySpark.

---

## 6. SparkSession and SparkContext

**SparkSession:** Primary entry point for DataFrames and SQL; in Databricks the `spark` variable is provided automatically. Avoid manually creating or stopping a session in a notebook.

**SparkContext:** Historical low-level Spark entry point for RDD APIs and broadcast/accumulator variables. Unsupported in serverless; restricted in standard compute.

**Example and use case:** `spark.sql` can query notebook temporary views; `spark.table` loads governed tables; `spark.read` loads file formats.

---

```python
print("Spark version:", spark.version)
orders.createOrReplaceTempView("demo_orders")
customers.createOrReplaceTempView("demo_customers")

spark.sql("""
SELECT region, COUNT(*) AS paid_orders, ROUND(SUM(amount), 2) AS revenue
FROM demo_orders
WHERE status = 'paid'
GROUP BY region
ORDER BY revenue DESC
""").show()
```

---

## 7. Transformations vs actions

**Transformation:** An operation that creates a new logical DataFrame/RDD without normally computing all results immediately. Examples: `select`, `filter`, `withColumn`, `join`, `groupBy`, `orderBy`, `repartition`.

**Action:** Requests an actual result or write, triggering execution. Examples: `count`, `show`, `collect`, `first`, `take`, writes. Some inspection/metadata operations may cause planning work without processing all rows.

**Use case:** Build a multi-step ETL chain before paying the cost of materializing it; inspect `explain()` to understand optimization.

**Important:** `collect()` transfers all rows to the driver: use only for small outputs. `display` is a Databricks UI helper, not a data engineering storage format.

---

```python
transformed = (orders
    .filter(F.col("status") == "paid")
    .select("customer_id", "region", "amount")
    .withColumn("tax_amount", F.round(F.col("amount") * 0.18, 2)))
print("Transformations defined; the dataset hasn't been fully materialized here.")
print("Action count:", transformed.count())
transformed.orderBy("customer_id").show()
```

---

## 8. Lazy evaluation and Catalyst optimization

**Definition:** Spark generally builds a logical plan for transformations and executes it when an action requires the data. Catalyst analyzes and optimizes relational queries; the physical planner selects execution strategies. Adaptive Query Execution (AQE), when enabled, may revise physical decisions at runtime.

**Example:** Spark can push filters/projections toward a source and combine compatible expressions.

**Use case:** Avoid unnecessary repeated actions and diagnose shuffle-heavy joins. `explain('formatted')` shows the plan, but it is not itself equivalent to executing the whole query.

**Nuance:** On newer Databricks serverless compute, `df.cache()` / `df.persist()` eagerly compute a snapshot rather than following classic lazy cache materialization.

---

```python
plan_demo = (orders
    .select("region", "status", "amount")
    .filter(F.col("status") == "paid")
    .groupBy("region").agg(F.sum("amount").alias("revenue")))
plan_demo.explain("formatted")
plan_demo.orderBy(F.desc("revenue")).show()
```

---

## 9. File formats: CSV, JSON, Parquet, Avro (optional Volume lab)

**CSV:** Plain-text row/column format. Human-readable and common for external uploads, but lacks embedded rich schema and typically requires explicit parsing options.

**JSON:** Flexible nested semi-structured text. Common for APIs, event logs and nested payloads; parsing and schema evolution need care.

**Parquet:** Compressed, columnar file format with schema information. Effective for analytic scans and selective column access.

**Avro:** Row-oriented binary serialization with schema support. Common in messaging/event ecosystems and cross-service data exchange.

**Databricks production choice:** Persist managed analytical tables in **Delta Lake**, which builds a transactional table format over Parquet data files; don't confuse Delta with raw Parquet. Use governed Unity Catalog Volumes for non-table files.

**Requirements for the lab:** Create or select an existing writable Volume in Catalog Explorer (such as `/Volumes/main/default/training_volume`) and set `VOLUME_ROOT` below to a new subdirectory under it. Leave blank to skip all filesystem writes safely. Because writes are illustrative, use only a dedicated scratch path.

**Use cases:** CSV from vendors; JSON from APIs; Parquet exports; Avro from change-data-capture systems; Delta for durable governed gold tables.

---

```python
# EDIT ONLY IF YOU HAVE AN EXISTING UNITY CATALOG VOLUME WITH WRITE PERMISSION.
# Example: VOLUME_ROOT = "/Volumes/main/default/training_volume/module2_demo_yourname"
VOLUME_ROOT = ""
ENABLE_FILE_IO = bool(VOLUME_ROOT.strip())
print("Volume I/O enabled:", ENABLE_FILE_IO)

if ENABLE_FILE_IO:
    if not VOLUME_ROOT.startswith("/Volumes/"):
        raise ValueError("Use a governed Unity Catalog /Volumes/<catalog>/<schema>/<volume>/... path")
    io_paths = {fmt: f"{VOLUME_ROOT.rstrip('/')}/{fmt}_orders" for fmt in ("csv", "json", "parquet", "avro")}
    orders.write.format("csv").option("header", "true").mode("overwrite").save(io_paths["csv"])
    orders.write.format("json").mode("overwrite").save(io_paths["json"])
    orders.write.format("parquet").mode("overwrite").save(io_paths["parquet"])
    orders.write.format("avro").mode("overwrite").save(io_paths["avro"])

    csv_df = (spark.read.format("csv").option("header", "true")
              .schema(orders_schema).load(io_paths["csv"]))
    json_df = spark.read.format("json").load(io_paths["json"])
    parquet_df = spark.read.format("parquet").load(io_paths["parquet"])
    avro_df = spark.read.format("avro").load(io_paths["avro"])
    for name, df in [("csv", csv_df), ("json", json_df), ("parquet", parquet_df), ("avro", avro_df)]:
        print(name, "rows:", df.count())
else:
    print("Skipping file formats lab. Set VOLUME_ROOT to enable it.")
```

---

## 10. Schema inference vs explicit schema

**Schema inference:** Spark inspects sample data to decide column types (e.g. `inferSchema=True` for CSV). Useful for exploration, but adds parsing work and can produce unexpected types when new or malformed rows arrive.

**Explicit schema:** Developer supplies field names, data types, and nullability via `StructType`, such as `order_id: int`, `amount: double`.

**Use cases:** Prefer explicit schemas for production ingestion, stable contracts, predictable casts, and efficient schema-on-read processing. Investigate invalid records separately instead of silently treating malformed IDs as strings.

**Example:** Run both CSV readers below after enabling the Volume lab and compare `printSchema()`; JSON and Parquet have different inference characteristics.

---

```python
if ENABLE_FILE_IO:
    inferred = (spark.read.option("header", True)
                .option("inferSchema", True).csv(io_paths["csv"]))
    explicit = (spark.read.option("header", True)
                .schema(orders_schema).csv(io_paths["csv"]))
    print("Inferred CSV schema:"); inferred.printSchema()
    print("Explicit CSV schema:"); explicit.printSchema()
else:
    print("Current DataFrame uses a supplied StructType:")
    orders.printSchema()

# An explicit production-safe schema for financial amounts can use DecimalType:
decimal_schema = T.StructType([
    T.StructField("order_id", T.IntegerType(), False),
    T.StructField("amount", T.DecimalType(18, 2), True)
])
print(decimal_schema.simpleString())
```

---

## 11. `select`, `filter`, `withColumn`, `drop`, `alias`

- **`select`**: Choose/compute columns; use for projection pruning.
- **`filter` / `where`**: Keep rows matching a condition; use for data cleansing and incremental slices.
- **`withColumn`**: Add/replace a column using a Spark expression; useful for derived fields. Avoid long loops repeatedly invoking it when `select`/`withColumns` can express work more clearly.
- **`drop`**: Remove unneeded columns.
- **`alias`**: Rename columns for clarity, or alias DataFrames to disambiguate joins.

**Use case:** Clean bronze transactions into silver-ready data: parse event dates, keep valid paid orders, standardize categories, calculate taxes, and drop sensitive/unneeded fields.

---

```python
clean_orders = (
    orders.filter(F.col("status") == "paid")
    .select("order_id", "customer_id", "order_date", "category", "amount", "region")
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("category", F.upper(F.col("category")))
    .withColumn("tax", F.round(F.col("amount") * 0.18, 2))
    .withColumn("total_with_tax", F.round(F.col("amount") + F.col("tax"), 2))
    .drop("tax")
)
clean_orders.select(
    F.col("order_id").alias("transaction_id"), "category", "total_with_tax"
).orderBy("transaction_id").show()
```

---

## 12. `groupBy` and aggregation: sum, avg, count, min, max

**Definition:** An aggregation combines multiple input rows into one summary row per grouping key. `groupBy` groups by columns; `agg` applies reductions such as `sum`, `avg`, `count`, `min`, and `max`.

**Example:** Compute paid order metrics for each region.

**Use cases:** Financial summaries, daily sales KPIs, order volumes, anomaly thresholds, revenue per product category.

**Caution:** Aggregations typically create a shuffle. `count('*')` counts rows while `count('amount')` counts non-null amounts; `countDistinct` is different from counting rows.

---

```python
paid_orders = orders.filter(F.col("status") == "paid")
region_kpis = (paid_orders.groupBy("region")
    .agg(
        F.count("*").alias("orders"),
        F.sum("amount").alias("revenue"),
        F.round(F.avg("amount"), 2).alias("average_order_value"),
        F.min("amount").alias("min_order"),
        F.max("amount").alias("max_order"),
    ).orderBy(F.desc("revenue")))
region_kpis.show()
# Expected revenue: north=1950, south=700, west=1350; total paid revenue=4000.
assert paid_orders.agg(F.sum("amount")).first()[0] == 4000.0
```

---

## 13. Joins: inner, left, right, full, semi, anti

**Definition:** A join combines rows from two datasets according to a key or predicate.

**Inner:** only matching keys (orders for customers 1,2,3).

**Left outer:** all left rows plus matching right columns (order for customer 4 retains NULL customer attributes).

**Right outer:** all right rows, even where the left has no match (customer 5 appears without an order).

**Full outer:** all keys on both sides, matching where possible.

**Left semi:** only left rows whose keys exist on the right; returns **left columns only**. Useful for existence filtering and whitelist checks.

**Left anti:** only left rows without a matching right key; returns **left columns only**. Useful for finding orphan records and data quality gaps.

**Use cases:** Enrich facts with dimensions, detect missing dimension keys, identify customers without orders, implement CDC comparisons.

**Caution:** Joins can multiply rows when the right key has duplicates. Standard SQL null keys do not match; use null-safe equality only when required. Skew and shuffles deserve attention on large tables.

---

```python
join_types = ["inner", "left", "right", "full", "left_semi", "left_anti"]
for how in join_types:
    joined = orders.join(customers, on="customer_id", how=how)
    print(f"{how:10s}: {joined.count()} rows")

print("Left anti: orders with no matching customer")
orders.join(customers, on="customer_id", how="left_anti").show()
print("Customers without orders")
customers.join(orders, on="customer_id", how="left_anti").show()

# Disambiguate same-named columns using aliases:
o, c = orders.alias("o"), customers.alias("c")
(o.join(c, F.col("o.customer_id") == F.col("c.customer_id"), "left")
 .select(F.col("o.order_id"), F.col("c.customer_name"))
 .orderBy("order_id")).show()
```

---

## 14. Window functions: row_number, rank, dense_rank, lag, lead

**Window:** Performs calculations across related rows while preserving individual rows (in contrast to `groupBy`, which collapses groups).

- **`row_number()`**: Unique consecutive number within a partition given an ordering. Add deterministic tie-breaker columns.
- **`rank()`**: Same rank for equal sort keys, then skips rank positions (1,2,2,4).
- **`dense_rank()`**: Same rank for ties, no rank gaps (1,2,2,3).
- **`lag()`**: Fetch a value from a previous row in the window ordering.
- **`lead()`**: Fetch a value from a following row.

**Use cases:** Top-N employees by salary, latest-record deduplication, previous transaction comparison, churn/activity sequences, SCD Type 2 change tracking.

**Caution:** Ordering is needed for meaningful windows; ties can produce unstable `row_number` unless additional ordering is specified.

---

```python
salary_groups = Window.partitionBy("department").orderBy(F.desc("salary"))
stable_order = Window.partitionBy("department").orderBy(F.desc("salary"), F.asc("employee_id"))

ranked = (employees
    .withColumn("row_number", F.row_number().over(stable_order))
    .withColumn("rank", F.rank().over(salary_groups))
    .withColumn("dense_rank", F.dense_rank().over(salary_groups)))
ranked.orderBy("department", "row_number").show()

# Latest record per order/customer ID: stable tie-breaker makes dedup deterministic.
latest_event_window = Window.partitionBy("customer_id").orderBy(
    F.to_date("order_date").desc(), F.col("order_id").desc())
latest_order_per_customer = (orders
    .withColumn("rn", F.row_number().over(latest_event_window))
    .filter(F.col("rn") == 1)
    .drop("rn"))
latest_order_per_customer.orderBy("customer_id").show()
```

---

```python
customer_timeline = Window.partitionBy("customer_id").orderBy("order_date", "order_id")
sequenced_orders = (orders
    .withColumn("previous_amount", F.lag("amount").over(customer_timeline))
    .withColumn("next_amount", F.lead("amount").over(customer_timeline))
    .withColumn("change_from_previous", F.col("amount") - F.col("previous_amount")))
sequenced_orders.orderBy("customer_id", "order_date", "order_id").show()
```

---

## 15. Python UDFs and their performance cost

**UDF definition:** A User-Defined Function runs custom logic not already covered by Spark built-in functions.

**Python UDF example:** Convert a product category to uppercase. The built-in `upper()` expression generally performs better because it is optimized inside Spark's SQL engine, while a standard Python UDF adds Python execution and serialization overhead, and limits optimizer visibility.

**Pandas UDF:** Vectorized Python UDF using Arrow/pandas batches, often better than row-at-a-time Python UDFs, but still not automatically superior to native Spark functions.

**Use case:** Apply specialized parsing/business functions when no comparable built-in or SQL expression exists.

**Rule:** First consider built-ins (`upper`, `when`, `regexp_replace`, `transform`, `coalesce`, date functions), then pandas/Arrow UDFs as appropriate, and regular Python UDFs only when necessary. Benchmark on real data.

---

```python
@F.udf(returnType=T.StringType())
def upper_category_python(value):
    return value.upper() if value is not None else None

print("Native Spark function:")
orders.select("category", F.upper("category").alias("native_upper")).show(3)
print("Python UDF:")
orders.select("category", upper_category_python("category").alias("python_upper")).show(3)
print("Physical plan for the Python UDF (often includes BatchEvalPython):")
orders.select(upper_category_python("category").alias("x")).explain("formatted")
```

---

```python
RUN_PANDAS_UDF = False  # Set True only if pandas + pyarrow are installed on your compute.
if RUN_PANDAS_UDF:
    import pandas as pd

    @F.pandas_udf("double")
    def add_gst_vectorized(amounts: pd.Series) -> pd.Series:
        return amounts * 1.18

    orders.select("amount", add_gst_vectorized("amount").alias("with_tax")).show()
else:
    print("Optional pandas UDF skipped; compare with native F.col('amount') * 1.18 first.")
```

---

## 16. Broadcast variables, broadcast joins, and accumulators

**SparkContext broadcast variable:** A read-only value distributed efficiently to executors instead of being shipped with every task. `sc.broadcast` is legacy API requiring compatible classic dedicated compute.

**Broadcast JOIN hint:** `F.broadcast(small_dimension)` asks Spark to distribute a small lookup table to avoid a large shuffle join. This **is not the same thing** as `sc.broadcast` and is supported in DataFrame workflows where broadcast joins are available. The hinted side must actually be small enough to fit in execution memory.

**Accumulator:** Legacy write-mostly distributed variable for counters/metrics. Executor tasks add to it; driver reads it. Retries can affect exact-once counting semantics. For reliable business metrics, prefer DataFrame aggregations.

**Use cases:** Product lookup joins; small mapping dimensions; historical task debugging counters. A large broadcast can exhaust executor memory, so measure lookup sizes.

---

```python
from pyspark.sql.functions import broadcast

# Practical, modern replacement: broadcast the SMALL customer dimension.
broadcast_joined = orders.join(broadcast(customers), "customer_id", "left")
broadcast_joined.select("order_id", "customer_id", "customer_name").orderBy("order_id").show()
broadcast_joined.explain("formatted")

# Reliable distributed count and sum rather than an accumulator for KPIs:
orders.agg(F.count("*").alias("records"), F.sum("amount").alias("all_status_amount")).show()

if RUN_CLASSIC_RDD_LABS:
    sc = spark.sparkContext
    category_lookup = sc.broadcast({"books": "Literature", "home": "Home & Living"})
    print("Broadcast lookup:", category_lookup.value.get("books"))
    counter = sc.accumulator(0)
    sc.parallelize([1, 2, 3]).foreach(lambda _: counter.add(1))
    print("Legacy accumulator count:", counter.value)
else:
    print("Legacy sc.broadcast and sc.accumulator skipped on this compute.")
```

---

## 17. Writing data: overwrite, append, error, ignore

**`overwrite`:** Replace existing output at a target (potentially destructive; validate target!).

**`append`:** Add rows to existing output without deduplication; if you append the same batch twice, you normally produce duplicates unless you use separate idempotency controls.

**`error` / `errorifexists`:** Fail if output exists (default for many DataFrameWriter operations).

**`ignore`:** Do nothing when the destination already exists; not an upsert.

**Use cases:** Rebuild scratch tables with overwrite; add new partitions/events with append; protect one-time exports with error; avoid accidental rewriting with ignore.

**Databricks practice:** Prefer governed Delta tables, and for CDC / upserts use Delta `MERGE`, not simple append. Example below writes only into the configured demonstration Volume directory.

---

```python
if ENABLE_FILE_IO:
    mode_path = f"{VOLUME_ROOT.rstrip('/')}/write_modes_demo"
    orders.write.format("parquet").mode("overwrite").save(mode_path)
    print("Overwrite count:", spark.read.parquet(mode_path).count())
    orders.filter(F.col("order_id") == 1001).write.format("parquet").mode("append").save(mode_path)
    print("Append count (includes duplicate):", spark.read.parquet(mode_path).count())
    orders.write.format("parquet").mode("ignore").save(mode_path)
    print("Ignore count (unchanged):", spark.read.parquet(mode_path).count())
    try:
        orders.write.format("parquet").mode("errorifexists").save(mode_path)
    except Exception as exc:
        print("Expected errorifexists failure:", type(exc).__name__)
else:
    print("Skipped file-write modes. Configure a unique VOLUME_ROOT first.")
```

---

## 18. Partitioning basics — execution partition vs partitioned files

**Spark execution partition:** A logical slice of data worked on by a task. `repartition(n, key)` redistributes rows (typically shuffle); `coalesce(n)` usually reduces partition count without full redistribution.

**Storage partition (`partitionBy`)**: Organizes file output into directory paths by a column (e.g., `region=north/`). Helps selective reads in suitable file/table layouts but can create many tiny files when columns have high cardinality.

**Use cases:** Avoid data skew in large workloads, limit output file counts, create manageable historical tables by low/medium cardinality fields.

**Databricks note:** For modern Delta managed tables, liquid clustering or automatic optimization may be better than manually partitioning small datasets. Don't indiscriminately choose a partition key or `repartition(1)` in production.

---

```python
partitioned = orders.repartition(4, "region")
print("Target shuffle partitions:", partitioned.rdd.getNumPartitions() if RUN_CLASSIC_RDD_LABS else "4 requested (RDD inspection unavailable in standard/serverless)")

if ENABLE_FILE_IO:
    partition_path = f"{VOLUME_ROOT.rstrip('/')}/region_partitioned_parquet"
    (orders.write.mode("overwrite").format("parquet")
     .partitionBy("region").save(partition_path))
    print("Rows after loading storage partitions:", spark.read.parquet(partition_path).count())
    spark.read.parquet(partition_path).filter(F.col("region") == "north").explain("formatted")
else:
    print("Partitioned file-output lab skipped; no Volume path configured.")
```

---

## 19. Caching & persistence (`cache` vs `persist`)

**Caching:** Reuse an expensive intermediate result instead of recomputing it for multiple downstream operations.

**Classic compute:** `cache()` chooses a default storage level; `persist(StorageLevel...)` allows the user to specify a level. Caches are typically materialized on an action.

**Serverless current behavior:** In supported serverless environments, **both** `cache()` and `persist()` eagerly compute a session-local snapshot. `persist(storageLevel)` accepts but ignores the requested storage level; only transformations derived from the cached DataFrame reuse that snapshot. `unpersist()` invalidates that cached DataFrame and derived views, so don't continue to use them after releasing the snapshot. The feature requires supported serverless environment/client versions.

**Use case:** A cleaned/joined base reused for multiple reports in one session; avoid caching a DataFrame read only once.

**Reminder:** `cache()` is not equivalent to Databricks disk/IO caching or a persistent Delta table.

---

```python
from pyspark import StorageLevel

expensive_base = (orders.filter(F.col("status") == "paid")
    .join(customers, "customer_id", "left")
    .select("order_id", "region", "amount", "customer_name"))

cached_base = expensive_base.cache()  # Serverless: eagerly snapshots; classic: typically lazy cache.
print("Revenue by region from cached base:")
cached_base.groupBy("region").agg(F.sum("amount").alias("revenue")).show()
print("Highest customer spend from cached base:")
cached_base.groupBy("customer_name").agg(F.sum("amount").alias("spend")).orderBy(F.desc("spend")).show()

cached_base.unpersist()
# On serverless, do NOT query cached_base or its derived DataFrames after unpersist().

# persist(storageLevel) illustrated with a different DataFrame:
from_persist = orders.filter(F.col("amount") > 200).persist(StorageLevel.MEMORY_AND_DISK)
from_persist.groupBy("category").count().show()
from_persist.unpersist()
```

---

## 20. Capstone: Bronze → Silver → Gold sales reporting in PySpark

**Bronze:** Preserve raw source facts with minimal changes.

**Silver:** Validate data types, remove invalid records and duplicates, standardize status, enrich with customers.

**Gold:** Produce business-ready reports (revenue per region/category, customer lifetime value, daily sales).

**Use case:** E-commerce analytics, finance dashboards, and recurring data lakehouse ETL.

**Production improvements:** Deduplicate with a deterministic latest-record rule, enforce idempotence, apply permissions, use incremental ingestion/Lakeflow/Auto Loader when appropriate, handle schema evolution, choose Delta `MERGE` for upserts, and add data quality tests.

---

```python
bronze = orders  # In production: raw governed Delta table or landing data source.

silver = (bronze
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
    .withColumn("category", F.upper(F.trim(F.col("category"))))
    .filter(F.col("order_id").isNotNull() & (F.col("amount") >= 0))
    .dropDuplicates(["order_id"])
    .join(F.broadcast(customers), "customer_id", "left"))

gold_region_sales = (silver.filter(F.col("status") == "paid")
    .groupBy("region")
    .agg(F.round(F.sum("amount"), 2).alias("revenue"),
         F.countDistinct("order_id").alias("order_count"))
    .orderBy(F.desc("revenue")))

print("Silver sample:"); silver.orderBy("order_id").show(truncate=False)
print("Gold sales:"); gold_region_sales.show()
assert gold_region_sales.agg(F.sum("revenue")).first()[0] == 4000.0

if ENABLE_FILE_IO:
    gold_path = f"{VOLUME_ROOT.rstrip('/')}/gold_region_sales_delta"
    gold_region_sales.write.format("delta").mode("overwrite").save(gold_path)
    print("Delta gold table-like path:", gold_path)
    spark.read.format("delta").load(gold_path).show()
else:
    print("Gold report was computed in memory. Configure Volume to save a Delta path.")
```

---

## 21. Exercises with reference solutions

1. **Customer leaderboard:** Find the top 2 customers by paid revenue.
2. **Data quality:** Find orders with no matching customer dimension row.
3. **Time series:** Calculate cumulative paid spend per customer by order date.
4. **Window ranking:** Find employees with `dense_rank` ≤ 2 within each department.
5. **Business report:** Report `paid` revenue by `category` and `region`, excluding cancelled orders.

**Challenge for production readiness:** Explain why `dropDuplicates(["order_id"])` alone is not a safe way to select the newest record when duplicates contain conflicting values. Replace it with `row_number` using event time + an ingestion tie-breaker.

---

```python
# Solution 1: top 2 customers by paid spend.
(paid_orders.groupBy("customer_id").agg(F.sum("amount").alias("spend"))
 .orderBy(F.desc("spend"), "customer_id").limit(2)).show()

# Solution 2: orphan fact rows.
orders.join(customers, "customer_id", "left_anti").show()

# Solution 3: running paid amount; explicit ROWS frame.
paid_sequence = paid_orders.withColumn("order_day", F.to_date("order_date"))
running_w = (Window.partitionBy("customer_id")
             .orderBy("order_day", "order_id")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow))
(paid_sequence.withColumn("cumulative_amount", F.sum("amount").over(running_w))
 .orderBy("customer_id", "order_day", "order_id")).show()

# Solution 4: top two salary bands (ties kept).
ranked.filter(F.col("dense_rank") <= 2).orderBy("department", "dense_rank").show()

# Solution 5: business revenue cube slice.
(paid_orders.groupBy("category", "region")
 .agg(F.sum("amount").alias("revenue"))
 .orderBy("category", "region")).show()
```

---

## 22. Interview revision + common mistakes

| Interview question | Core answer |
|---|---|
| Transformation vs action? | Transformations build plans; actions execute/write. |
| Why use DataFrame over RDD? | Schema plus Catalyst/SQL optimization, maintainability, governance support. |
| What causes a shuffle? | Key-based redistribution in aggregation, many joins, and repartition. |
| `rank` vs `dense_rank`? | Both keep ties; rank skips numbers after ties, dense rank doesn't. |
| `left_semi` vs `left_anti`? | Matched left records vs unmatched left records; return only left columns. |
| Why explicit schema? | Stable types, predictable parsing, no CSV inference scan, clear contracts. |
| `partitionBy` vs `repartition`? | Storage layout directories vs Spark execution partition redistribution. |
| Why avoid Python UDF? | Python execution/serialization and less optimization visibility; prefer native Spark. |
| Broadcast variable vs broadcast join? | Legacy shared object via SparkContext vs optimized DataFrame join strategy. |
| `cache` vs `persist`? | Classic: default vs configurable storage level; serverless: both snapshot eagerly, persist ignores storage levels. |
| Delta vs Parquet? | Parquet is file format; Delta adds transaction log/table semantics around Parquet files. |
| Which compute for legacy RDDs? | Classic dedicated, not standard/serverless. |

**Key mistakes to avoid:** Calling `collect()` on large tables; trusting `inferSchema` for unpredictable feeds; joining non-unique dimensions without checking cardinality; using high-cardinality storage partitions; treating `append` as idempotent; caching one-time transforms; relying on Python UDF for functions Spark already supports.

## Authoritative documentation

- Apache Spark: https://spark.apache.org/docs/latest/sql-programming-guide.html
- Databricks PySpark basics: https://docs.databricks.com/aws/en/pyspark/basics
- Databricks standard compute limitations: https://docs.databricks.com/aws/en/compute/standard-limitations
- Databricks serverless migration: https://docs.databricks.com/aws/en/compute/serverless/migration
- Databricks Unity Catalog Volumes: https://docs.databricks.com/aws/en/volumes/volume-files
- Databricks Avro: https://docs.databricks.com/aws/en/query/formats/avro
- Databricks DataFrame cache on serverless: https://docs.databricks.com/aws/en/compute/serverless/dataframe-cache
- Databricks UDF guidance: https://docs.databricks.com/aws/en/udf
- Databricks DataFrameWriter modes: https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframewriter/mode
