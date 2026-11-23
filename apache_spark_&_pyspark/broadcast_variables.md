# **Broadcast Variables & Accumulators in PySpark — Databricks Complete Notes**

In Apache Spark, **Broadcast Variables** and **Accumulators** are two special types of shared variables used in distributed processing.

They solve different problems:

- **Broadcast Variables:** Efficiently distribute read-only data from the driver to executors.
- **Accumulators:** Collect counters, sums, and metrics from distributed tasks back to the driver.

Both are useful for understanding Spark architecture, performance optimization, and monitoring data processing jobs.

## **1. Broadcast Variables**

### **Definition**

A broadcast variable is a read-only variable that Spark distributes efficiently across executors so that multiple tasks can reuse the same data without repeatedly transferring it from the driver.

**Real-world use case:** An e-commerce company processes millions of orders and needs to convert currency codes into currency names or exchange rates using a small lookup dictionary.

Instead of sending that dictionary with every task, Spark can broadcast it to executors.

How a broadcast variable works

**Spark Driver**

Creates a read-only lookup dictionary

Efficiently broadcasts data

Executor 1

Shared lookup data

Executor 2

Shared lookup data

Executor 3

Shared lookup data

Each executor can reuse the broadcast data across its tasks.

### **Why broadcast variables are needed**

Suppose you have:

- 10 million customer transactions.
- A Python dictionary containing 100 currency conversion rates.
- Hundreds of tasks executing across Spark workers.

Without explicit broadcasting, a dictionary captured by a Python task function may be serialized and transferred with task closures repeatedly. With broadcasting, Spark distributes it efficiently for reuse.

Broadcasting is especially useful when many tasks reference the same relatively small, read-only dataset.

## **2. Broadcast variable example — classic PySpark**

This example demonstrates the traditional `SparkContext.broadcast()` API.

Compute requirement: Databricks Dedicated/classic compute with SparkContext access, or local PySpark. Not Databricks serverless.

```
# Databricks Notebook - Broadcast Variables# Requires SparkContext-compatible computesc = spark.sparkContext# Step 1: Create lookup dictionarycurrency_rates = {    "USD": 83.0,    "EUR": 90.0,    "GBP": 105.0,    "INR": 1.0}# Step 2: Broadcast dictionarybroadcast_rates = sc.broadcast(currency_rates)# Step 3: Create transactions RDDtransactions = [    (101, "USD", 100),    (102, "EUR", 200),    (103, "GBP", 50),    (104, "INR", 5000)]rdd = sc.parallelize(transactions)# Step 4: Use broadcast variabledef convert_to_inr(transaction):    order_id, currency, amount = transaction    rate = broadcast_rates.value[currency]    return (        order_id,        currency,        amount,        amount * rate    )# Step 5: Execute distributed processingresult = rdd.map(convert_to_inr)for row in result.collect():    print(row)# Optional: Release broadcast resources after usebroadcast_rates.unpersist()
```

**Expected output**

```
(101, 'USD', 100, 8300.0)
(102, 'EUR', 200, 18000.0)
(103, 'GBP', 50, 5250.0)
(104, 'INR', 5000, 5000.0)
```

The conversion rates are illustrative, not live exchange rates.

### **Important functions**

| Function                      | Purpose                               |
| ----------------------------- | ------------------------------------- |
| `sc.broadcast(data)`          | Creates a broadcast variable          |
| `broadcast_rates.value`       | Accesses broadcast data               |
| `broadcast_rates.unpersist()` | Releases cached executor copies       |
| `broadcast_rates.destroy()`   | Destroys broadcast variable resources |

Broadcast variables should be treated as immutable. If lookup data changes, create a new broadcast variable rather than modifying an existing one.

## **3. Broadcast joins — recommended for Databricks DataFrames**

In modern Databricks workloads, **broadcast joins** are more common than manually broadcasting Python dictionaries.

A broadcast join sends the smaller DataFrame to the executors to avoid shuffling both sides of the join.

This can significantly reduce network transfer and execution cost.

### **Example: Orders joined with product categories**

Normal join versus broadcast join

Normal shuffle join

Large Orders

Small Products

Shuffle data across workers

Broadcast join

Large Orders

Broadcast Products

Local lookup on executors

### **Databricks notebook — ready to execute on serverless**

```
from pyspark.sql import functions as Ffrom pyspark.sql.functions import broadcast# Step 1: Create orders DataFrameorders_data = [    (1, 101, 2),    (2, 102, 1),    (3, 103, 5),    (4, 101, 3),    (5, 102, 2),    (6, 104, 1)]orders_df = spark.createDataFrame(    orders_data,    ["order_id", "product_id", "quantity"])# Step 2: Create small product lookup DataFrameproducts_data = [    (101, "Laptop", 50000),    (102, "Mouse", 500),    (103, "Keyboard", 1500),    (104, "Monitor", 12000)]products_df = spark.createDataFrame(    products_data,    ["product_id", "product_name", "price"])# Step 3: Use broadcast joinresult_df = orders_df.join(    broadcast(products_df),    on="product_id",
```

**Expected result**

| order_id | product_id | quantity | product_name | price | total_amount |
| -------- | ---------- | -------- | ------------ | ----- | ------------ |
| 1        | 101        | 2        | Laptop       | 50000 | 100000       |
| 2        | 102        | 1        | Mouse        | 500   | 500          |
| 3        | 103        | 5        | Keyboard     | 1500  | 7500         |
| 4        | 101        | 3        | Laptop       | 50000 | 150000       |
| 5        | 102        | 2        | Mouse        | 500   | 1000         |
| 6        | 104        | 1        | Monitor      | 12000 | 12000        |

When you inspect the physical plan, look for:

```
BroadcastHashJoin
BroadcastExchange
```

These indicate the broadcast join strategy.

Spark may automatically broadcast a small DataFrame depending on its statistics and configuration. Explicit `broadcast()` provides a hint to the optimizer.

### **When to use a broadcast join**

Use it when one DataFrame is relatively small compared with another, such as orders joined with a small product category, region, or currency lookup table.

Avoid forcing broadcast when the small side is actually large enough to cause executor memory pressure or broadcast timeouts.

# **4. Accumulators in PySpark**

### **Definition**

An accumulator is a special shared variable used to aggregate values from distributed Spark tasks into a result that the driver can read.

Think of an accumulator as a distributed counter.

For example, suppose a pipeline processes one million customer records. You want to count how many records contain invalid email addresses.

Each executor processes its own records and increments the accumulator. Spark merges these updates into a counter accessible to the driver.

Accumulator architecture

Executor 1

+5

Executor 2

+8

Executor 3

+3

Task updates merged by Spark

**Driver**

# **16**

Total accumulated count

An accumulator works differently from a normal Python variable. Updates performed by distributed tasks are merged through Spark's execution machinery rather than directly modifying a Python variable on the driver.

## **5. Accumulator example — counting invalid records**

Requires Dedicated/classic compute with SparkContext access, or local PySpark.

```
# Step 1: Get SparkContextsc = spark.sparkContext# Step 2: Create accumulatorinvalid_counter = sc.accumulator(0)# Step 3: Sample customer recordscustomers = [    (1, "Amit", 25),    (2, "Neha", -5),    (3, "Rahul", 30),    (4, "Priya", -10),    (5, "Ravi", 22),    (6, "Karan", -1)]# Step 4: Create RDDcustomers_rdd = sc.parallelize(customers, 2)# Step 5: Validate recordsdef validate_customer(record):    customer_id, name, age = record    if age < 0:        invalid_counter.add(1)        return None    return record# Step 6: Execute Spark actionvalidated_rdd = customers_rdd.map(validate_customer)# Single action that evaluates every recordresults = validated_rdd.collect()
```

**Expected output**

```
Valid Customers: [
    (1, 'Amit', 25),
    (3, 'Rahul', 30),
    (5, 'Ravi', 22)
]

Invalid Record Count: 3
```

### **Explanation**

1. `sc.accumulator(0)` initializes the accumulator to zero.
2. `sc.parallelize()` distributes the records across Spark partitions.
3. Each task checks its assigned records.
4. If `age` is negative, the task increments the accumulator using `.add(1)`.
5. Once `collect()` executes, the driver reads the accumulated value using `.value`.

**Important:** Accumulators do not execute independently. Their updates happen when Spark actions trigger the transformations containing them.

## **6. Databricks Serverless alternative — count invalid records using DataFrames**

Databricks serverless does not expose the traditional `spark.sparkContext` API, so `sc.accumulator()` is unavailable there. Databricks recommends DataFrame aggregations as the replacement for this type of counter.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



The following example runs without SparkContext.

```
from pyspark.sql import functions as F# Step 1: Prepare datasetcustomers_data = [    (1, "Amit", 25),    (2, "Neha", -5),    (3, "Rahul", 30),    (4, "Priya", -10),    (5, "Ravi", 22),    (6, "Karan", -1)]df = spark.createDataFrame(    customers_data,    ["customer_id", "name", "age"])# Step 2: Create validation columnvalidated_df = df.withColumn(    "is_valid",    F.col("age") >= 0)# Step 3: Calculate quality metricsmetrics_df = validated_df.agg(    F.count("*").alias("total_records"),    F.sum(        F.when(F.col("is_valid"), 1).otherwise(0)    ).alias("valid_records"),    F.sum(        F.when(~F.col("is_valid"), 1).otherwise(0)    ).alias("invalid_records"))display(metrics_df)
```

Expected metrics:

| total_records | valid_records | invalid_records |
| ------------- | ------------- | --------------- |
| 6             | 3             | 3               |

This is generally a better approach for production data quality pipelines because the counters are derived directly from the records.

# **7. Real-world Databricks use case — ETL data quality validation**

Suppose you are developing a banking ETL pipeline.

Your source data contains customer transactions. You need to:

- Detect negative transaction amounts.
- Detect missing customer IDs.
- Count invalid transactions.
- Produce a clean DataFrame.
- Generate data quality metrics.

### **Complete Databricks notebook example**

```
from pyspark.sql import functions as F# Step 1: Create sample transactionstransactions = [    (1, 101, 5000.0),    (2, 102, -200.0),    (3, None, 1500.0),    (4, 104, 2500.0),    (5, 105, -100.0),    (6, 106, 7000.0),    (7, None, -300.0)]df = spark.createDataFrame(    transactions,    ["transaction_id", "customer_id", "amount"])print("Original Transactions")display(df)# Step 2: Add validation flagsvalidated_df = (    df    .withColumn(        "invalid_amount",        F.col("amount") < 0    )    .withColumn(        "missing_customer",        F.col("customer_id").isNull()    )    .withColumn(        "is_valid",        (F.col("amount") >= 0) &
```

**Expected quality metrics**

| Metric                 | Value |
| ---------------------- | ----- |
| Total transactions     | 7     |
| Invalid amount count   | 3     |
| Missing customer count | 2     |
| Total invalid records  | 4     |
| Valid records          | 3     |

Notice that transaction 7 has both a negative amount and a missing customer ID. It contributes to two error categories but is counted as only one invalid record.

This distinction is important when developing production-grade data quality reports.

# **8. Broadcast Variables vs. Accumulators**

| Feature                           | Broadcast variable             | Accumulator                    |
| --------------------------------- | ------------------------------ | ------------------------------ |
| Purpose                           | Share lookup data              | Aggregate updates              |
| Data flow                         | Driver → Executors             | Executors → Driver             |
| Access by tasks                   | Read-only                      | Add/update                     |
| Driver access                     | Reads broadcast value          | Reads final accumulated value  |
| Common use                        | Dictionaries, lookup tables    | Error counters, metrics        |
| Classic PySpark API               | `sc.broadcast()`               | `sc.accumulator()`             |
| Databricks serverless alternative | DataFrame broadcast joins      | DataFrame aggregations         |
| Performance benefit               | Reduces repeated data transfer | Distributed metric aggregation |

Quick memory trick

**Broadcast:** Send the same information outward to workers.

**Accumulator:** Combine updates from workers into a total.

# **9. Performance considerations and limitations**

### **Broadcast variables**

Broadcasting saves repeated serialization and transfer of shared data, but it is not free. Spark still needs memory and network resources to distribute and retain the broadcast value.

Do not broadcast huge lookup dictionaries without checking memory usage. If your data is naturally tabular, prefer DataFrame joins over Python UDFs and dictionary lookups whenever possible.

### **Accumulators**

Accumulators are useful for debugging and operational metrics, but they require care because Spark can retry tasks or recompute transformations.

An accumulator incremented inside a transformation can be counted more than once if its work is re-executed. Spark provides stronger guarantees for accumulator updates performed directly inside actions, but you should not rely on transformation-based accumulators for exact financial totals or business-critical counters.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Spark 4.2.0 Documentation+1



For production record counts, use DataFrame aggregations such as `count()`, `sum()` and conditional aggregations instead.

# **10. Interview questions**

1\. What is a broadcast variable in Spark?

A read-only shared variable efficiently distributed to executors, allowing multiple tasks to reuse the same lookup data without repeatedly transferring it.

2\. What is the difference between a broadcast variable and a broadcast join?

3\. Can broadcast variables be modified inside executors?

4\. When does a broadcast join improve performance?

5\. What is an accumulator?

6\. Why shouldn't accumulators be used for exact business totals?

7\. Can I use sc.broadcast() or sc.accumulator() in Databricks serverless?

## **11. Which approach should you use in Databricks?**

| Requirement                                 | Recommended approach                          |
| ------------------------------------------- | --------------------------------------------- |
| Share a Python dictionary across RDD tasks  | `sc.broadcast()` on supported classic compute |
| Join large orders with small product lookup | `broadcast(small_df)`                         |
| Count invalid records                       | DataFrame `sum(when(...))`                    |
| Calculate total revenue                     | DataFrame `sum()`                             |
| Count missing values                        | DataFrame aggregations                        |
| Build production ETL pipelines              | DataFrames, Spark SQL, Delta tables           |

**Key takeaway:** Broadcast variables optimize how data is shared with tasks, while accumulators aggregate information produced by tasks. For most modern Databricks DataFrame pipelines, use broadcast joins and SQL aggregations rather than the older SparkContext-based APIs.

Official references: [Apache Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html?utm_source=chatgpt.com) and [Databricks Serverless Migration Guide](https://docs.databricks.com/aws/en/compute/serverless/migration?utm_source=chatgpt.com).
