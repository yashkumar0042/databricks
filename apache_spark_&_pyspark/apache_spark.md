# **Apache Spark — What It Is and Why It Was Created**

Module 2: Apache Spark & PySpark Mastery | Databricks

## **1. What is Apache Spark?**

**Definition:** Apache Spark is an open-source, distributed data processing engine designed to process and analyze massive datasets efficiently by distributing computations across multiple computers (a cluster).

In simple terms, **Apache Spark allows us to process millions or billions of records in parallel instead of relying on a single machine.**

It supports Python, SQL, Scala, Java, and R and is widely used for data engineering, analytics, machine learning, and streaming applications.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Unified Engine for large-scale data analytics+1



### **Example: Why do we need Spark?**

Imagine an e-commerce company that generates 1 billion order records daily.

The company wants to calculate total revenue per country.

Traditional single-machine processing

Distributed processing with Spark

Spark divides the dataset into partitions and processes them across workers simultaneously. This can significantly improve throughput for large workloads.

## **2. Why was Apache Spark created?**

Apache Spark started at UC Berkeley's AMPLab in **2009** and was open-sourced in 2010.

It was developed to overcome limitations of Hadoop MapReduce, particularly for iterative machine learning algorithms and interactive data analysis.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Apache Spark+1



### **The problem with Hadoop MapReduce**

Traditional Hadoop MapReduce often required intermediate results to be written to distributed storage between separate jobs.

For example, consider a machine learning algorithm that must process the same dataset 20 times.

Hadoop MapReduce workflow (simplified)

Spark workflow (simplified)

Spark can reuse cached intermediate data, reducing repeated reads and writes. It does not automatically keep all data in memory.

### **Problems Spark was designed to solve**

| Problem                              | How Spark addresses it                                          |
| ------------------------------------ | --------------------------------------------------------------- |
| Expensive intermediate disk I/O      | Can cache and reuse data across computations                    |
| Slow iterative algorithms            | Supports repeated computation on reusable datasets              |
| Complex MapReduce programs           | Offers higher-level DataFrame, SQL and other APIs               |
| Limited interactive analytics        | Supports efficient distributed querying                         |
| Separate processing tools            | Provides a unified engine for batch, streaming and ML workflows |
| Large datasets exceeding one machine | Distributes computation across a cluster                        |

**Important:** Spark is not always faster than Hadoop or single-machine tools. Its performance depends on data size, partitioning, shuffles, caching, and cluster resources.

## **3. Key features of Apache Spark**

- **Distributed processing:** Processes data across multiple worker machines.
- **In-memory caching:** Reuses datasets in memory when advantageous.
- **Fault tolerance:** Recomputes lost data partitions using lineage information.
- **Lazy evaluation:** Delays execution of transformations until an action requires results.
- **Query optimization:** Uses Spark SQL's optimizer to improve execution plans.
- **Multiple workloads:** Supports batch ETL, Structured Streaming, SQL analytics and machine learning.
- **Scalability:** Can run locally or on large compute clusters.

## **4. Apache Spark example in Databricks**

Let's understand Spark with a practical PySpark notebook example.

**Business requirement:** An e-commerce company wants to calculate total sales revenue by country.

Databricks notebook — Python

```
from pyspark.sql import functions as F# Sample order recordsorders = [    (1, "India", 5000),    (2, "USA", 8000),    (3, "India", 3000),    (4, "UK", 4000),    (5, "USA", 6000),    (6, "India", 2000)]# Create distributed DataFramedf = spark.createDataFrame(    orders,    ["order_id", "country", "amount"])# Calculate total revenue per countryrevenue_df = (    df.groupBy("country")      .agg(F.sum("amount").alias("total_revenue"))      .orderBy("country"))display(revenue_df)
```

**Expected output**

| country | total_revenue |
| ------- | ------------- |
| India   | 10,000        |
| UK      | 4,000         |
| USA     | 14,000        |

### **How Spark executes this code**

1. `spark.createDataFrame()` creates a Spark DataFrame using the sample data.
2. `groupBy()` defines the grouping operation.
3. `agg()` defines the revenue calculation.
4. `orderBy()` defines the sorting operation.
5. `display()` triggers execution and displays results in Databricks.

Spark constructs an execution plan for these operations and can process the dataset in partitions across available compute resources.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://www.databricks.com\&sz=32)

Databricks+1



For this tiny dataset, distributed computing is unnecessary. But the same logic can scale to much larger data volumes.

### **Real-world Databricks example: Processing millions of orders**

In production, data may be stored as a Delta table in cloud storage rather than being created from a Python list.

```
from pyspark.sql import functions as F# Read orders from a Delta tableorders_df = spark.table("main.sales.orders")# Apply transformationsdaily_revenue = (    orders_df    .filter(F.col("order_status") == "COMPLETED")    .groupBy("order_date", "country")    .agg(F.sum("amount").alias("total_revenue")))# Save aggregated results as a managed Delta table(    daily_revenue.write    .format("delta")    .mode("overwrite")    .saveAsTable("main.sales.daily_revenue"))
```

Assumes Unity Catalog tables and columns already exist and the notebook has permission to read and write them. `overwrite` replaces the target table's data.

This is an example of an **ETL pipeline**, where Spark reads data, filters and aggregates it, then writes the processed results.

## **5. Apache Spark vs. Hadoop MapReduce vs. Pandas**

| Feature             | Spark                        | Hadoop MapReduce                  | Pandas                     |
| ------------------- | ---------------------------- | --------------------------------- | -------------------------- |
| Processing          | Distributed                  | Distributed                       | Primarily single-machine   |
| Typical workloads   | ETL, SQL, ML, streaming      | Batch processing                  | Local data analysis        |
| Data reuse          | Optional caching             | Usually separate disk-backed jobs | In-memory objects          |
| Programming         | Python, SQL, Scala, Java, R  | Java and other interfaces         | Python                     |
| Very large datasets | Strong fit                   | Strong fit for batch              | Limited by local resources |
| Ideal scenario      | Large-scale data engineering | Traditional large batch jobs      | Small and medium datasets  |

## **6. What is the relationship between Apache Spark and Databricks?**

**Databricks Platform**

Notebooks · Workflows · Governance · Data Management

**Apache Spark**

Distributed data processing engine

Cloud compute resources

Processing data from tables, object storage and other sources

Apache Spark is the underlying distributed processing engine, while Databricks provides a managed environment and additional capabilities for developing, running, securing, and monitoring data workloads.

For example, in a Databricks notebook, the `spark` session is normally provided automatically, so you can begin writing PySpark code without manually creating a SparkSession.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://www.databricks.com\&sz=32)

Databricks+1



## **7. Practical real-world use cases**

| Industry           | Use case                  | Spark's role                         |
| ------------------ | ------------------------- | ------------------------------------ |
| E-commerce         | Daily order analytics     | Aggregate billions of order records  |
| Banking            | Fraud detection pipelines | Process and enrich transaction data  |
| Healthcare         | Patient-data analytics    | Transform large datasets             |
| Telecommunications | Network logs              | Process and analyze events           |
| Retail             | Demand forecasting        | Prepare historical sales data for ML |
| Media              | Recommendation systems    | Generate features from user activity |

## **8. Interview-ready answer**

**Question: What is Apache Spark, and why was it created?**

Apache Spark is an open-source, distributed data processing engine designed for large-scale analytics. It was created at UC Berkeley in 2009 to address inefficiencies in Hadoop MapReduce, particularly for workloads requiring repeated computation and interactive analysis.

Spark improves performance by supporting data reuse through caching, distributed parallel execution, optimized query plans, and higher-level programming APIs.

It supports batch processing, SQL, streaming, and machine learning and is a foundational processing engine used in Databricks.

**Key takeaway:** Hadoop MapReduce made large-scale distributed batch processing practical. Apache Spark was designed to make repeated, interactive, and more diverse distributed computations more efficient and easier to program.
