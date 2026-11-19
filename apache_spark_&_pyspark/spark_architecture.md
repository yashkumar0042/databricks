# **Apache Spark Architecture: Driver, Executor & Cluster Manager**

Module 2: Apache Spark & PySpark Mastery | Databricks

## **1. What is Apache Spark Architecture?**

**Definition:** Apache Spark follows a distributed master-worker architecture in which a central **Driver** coordinates processing, **Executors** perform computations in parallel, and a **Cluster Manager** allocates resources.

Spark distributes large datasets into smaller partitions and executes tasks across multiple machines to achieve parallel processing.

### **Spark Architecture Diagram**

[Apache Spark architecture diagram showing Driver with SparkContext, Cluster Manager, and three worker nodes with executors and parallel tasks](https://images.openai.com/static-rsc-4/zPdCi7lwWlZmr7_5JtM450HGvLMYpTgcPCRGQJf7fUru2jqMuX91H1gnVCD7BmpqnGGMqyRjXdTtcfIxJhvFcq6iAUCT60vbOZvO8jN3yKsT26NEM5VwpRsI7sYxoYAXw16RpIlU4fFLQWkIf1s-TYEzAPNgdfFa7CsqLQQoK-g?purpose=inline)

[packtpub.com](https://www.packtpub.com/en-us/product/machine-learning-with-apache-spark-quick-start-guide-9781789346565?utm_source=chatgpt.com)

Conceptual Spark cluster architecture. The driver schedules work directly on executors, while the cluster manager is responsible for allocating cluster resources.

There are three main components:

- **Driver:** Coordinates and controls the Spark application.
- **Executors:** Perform the actual data processing.
- **Cluster Manager:** Allocates CPU, memory, and computing resources.

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Spark 4.2.0 Documentation+1

## **2. Driver — The Brain of Apache Spark**

**Definition:** The Driver is the central process responsible for managing the execution of a Spark application. It creates the SparkSession/SparkContext, constructs execution plans, schedules tasks, and coordinates executors.

In Databricks, the driver runs on the driver node of classic compute.

### **Responsibilities of the Driver**

| Responsibility        | Explanation                                             |
| --------------------- | ------------------------------------------------------- |
| SparkSession          | Entry point for working with Spark SQL and DataFrames   |
| Job planning          | Prepares the execution plan for Spark operations        |
| DAG Scheduler         | Organizes the work into execution stages                |
| Task Scheduler        | Assigns tasks to available executors                    |
| Resource coordination | Requests executor resources through the cluster manager |
| Result handling       | Receives task status and results when required          |
| Monitoring            | Tracks jobs, stages, tasks and failures                 |

### **Databricks notebook example**

In a Databricks notebook, SparkSession is normally available automatically as `spark`.

```
# Access the existing SparkSessionprint(spark)# Get Spark application ID (classic compute)print(spark.sparkContext.applicationId)# Spark versionprint(spark.version)# Create a DataFramedf = spark.range(1, 1001)# Define a transformationfiltered_df = df.filter(df.id > 500)# Trigger executionprint(filtered_df.count())
```

**Expected result:**

```
500
```

### **What happens internally?**

1. The Driver receives the Python commands.
2. It prepares a logical and physical execution plan.
3. The transformation `filter()` is recorded without immediately processing all records.
4. Calling `count()` triggers a Spark action.
5. The Driver schedules tasks on available Executors.
6. The Executors process the data and return partial results.
7. The Driver combines the final result and returns `500`.

**Important:** Avoid using `collect()` or `toPandas()` on large datasets because they bring data back to the Driver and can exhaust driver memory.

## **3. Executor — The Worker of Apache Spark**

**Definition:** An Executor is a process launched for a Spark application on a worker node. Its primary responsibility is to execute tasks assigned by the Driver and store intermediate or cached data.

Executors generally run as JVM processes and can execute multiple tasks concurrently using available CPU cores.

### **Responsibilities of an Executor**

- Executes Spark tasks on data partitions.
- Performs operations such as filtering, aggregations and joins.
- Uses memory and disk to process and cache data.
- Exchanges intermediate data during shuffle operations.
- Reports task progress, metrics, and failures to the Driver.
- Recomputes tasks when Spark needs to recover from certain failures.

### **Example: 3 executors processing 12 partitions**

**Spark Driver**

Schedules 12 partition tasks

Executor 1

4 task slots

P1

P2

P3

P4

Executor 2

4 task slots

P5

P6

P7

P8

Executor 3

4 task slots

P9

P10

P11

P12

Illustration: 3 executors × 4 available task slots = 12 concurrent tasks

Assume:

- Dataset contains 12 partitions.
- Spark has 3 executors.
- Each executor has 4 CPU cores.
- Each task requires 1 CPU core.

Then Spark can potentially execute all 12 partition tasks concurrently, provided resources are available and there are no scheduling constraints.

### **Databricks notebook: Observe data partitions**

```
from pyspark.sql import functions as F# Generate 1 million records with 8 partitionsdf = spark.range(    start=0,    end=1_000_000,    step=1,    numPartitions=8)# Identify the partition assigned to each rowpartitioned_df = df.withColumn(    "partition_id",    F.spark_partition_id())# Calculate records per original partitionresult = (    partitioned_df    .groupBy("partition_id")    .count()    .orderBy("partition_id"))display(result)
```

**Expected output**

| partition_id | count   |
| ------------ | ------- |
| 0            | 125,000 |
| 1            | 125,000 |
| 2            | 125,000 |
| 3            | 125,000 |
| 4            | 125,000 |
| 5            | 125,000 |
| 6            | 125,000 |
| 7            | 125,000 |

The 1 million records are divided into eight partitions. Spark creates tasks to process these partitions, and available executor resources determine how many tasks can run concurrently.

**Key distinction:** A partition is a logical piece of data, whereas an executor is a running process that performs the computation.

## **4. Cluster Manager — The Resource Allocator**

**Definition:** The Cluster Manager is responsible for allocating computing resources to Spark applications, including CPU and memory resources needed to launch Executors.

It manages cluster resources but does not create Spark execution plans or schedule individual Spark tasks. Those responsibilities belong to the Driver.

### **Popular cluster managers**

| Cluster manager            | Description                                     | Typical use case               |
| -------------------------- | ----------------------------------------------- | ------------------------------ |
| Spark Standalone           | Built-in resource manager                       | Self-managed Spark clusters    |
| Hadoop YARN                | Resource manager for Hadoop environments        | Enterprise Hadoop ecosystems   |
| Kubernetes                 | Container orchestration platform                | Cloud-native Spark deployments |
| Databricks-managed compute | Managed platform for provisioning Spark compute | Databricks data engineering    |

Technically, Databricks-managed compute is a managed environment rather than a fourth open-source Spark cluster manager type. Databricks handles the underlying infrastructure and resource orchestration for you.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

Spark 4.2.0 Documentation+1



### **Example: Resource allocation**

Suppose a Spark application needs:

- 4 executors
- 4 CPU cores per executor
- 8 GB executor memory

The application requests these resources. The cluster manager allocates the available resources and arranges executor launches.

Requested executor resources

# **4**

Executors

# **16**

Total CPU cores

# **32 GB**

Executor memory

Illustrative executor resources only; excludes driver resources and memory overhead.

If sufficient resources are unavailable, executor allocation may be delayed or limited, depending on the cluster manager and its scheduling policies.

## **5. How Driver, Executors, and Cluster Manager Work Together**

This is the most important part of Spark architecture.

**1. User submits Spark application**

Databricks notebook or scheduled job

**2. Driver prepares execution plan**

SparkSession · DAG Scheduler · Task Scheduler

**3. Cluster Manager allocates resources**

Executors are launched on worker nodes

**4. Driver schedules tasks on Executors**

Executor 1

Run tasks

Executor 2

Run tasks

Executor 3

Run tasks

**5. Tasks finish and results are combined**

Executors send metrics and required results; Spark completes the action

Note that resource allocation and execution planning may overlap in a running application. The diagram shows the logical responsibilities rather than a strict startup timeline.

## **6. Understanding Job, Stage, Task, and Partition**

These four concepts are closely connected to Spark architecture.

| Concept     | Definition                                         | Example                       |
| ----------- | -------------------------------------------------- | ----------------------------- |
| Application | Complete Spark program                             | Daily sales ETL               |
| Job         | Computation initiated by an action                 | `df.count()`                  |
| Stage       | Set of tasks executable without a shuffle boundary | Filter or partial aggregation |
| Task        | Smallest execution unit scheduled on an executor   | Process one partition         |
| Partition   | Logical division of distributed data               | One of eight data partitions  |

### **Practical example: Aggregation in Databricks**

```
from pyspark.sql import functions as F# Step 1: Create distributed datadf = spark.range(0, 1_000_000, 1, 8)# Step 2: Add a categorydf = df.withColumn(    "category",    F.col("id") % 4)# Step 3: Define aggregationresult = (    df.groupBy("category")      .agg(F.count("*").alias("total"))      .orderBy("category"))# Step 4: Examine physical execution planresult.explain(mode="formatted")# Step 5: Trigger executiondisplay(result)
```

**Expected result**

| category | total   |
| -------- | ------- |
| 0        | 250,000 |
| 1        | 250,000 |
| 2        | 250,000 |
| 3        | 250,000 |

### **What happens internally?**

- The Driver prepares the query plan and determines how the aggregation should execute.
- The source dataset initially has eight partitions.
- Executor tasks process these partitions.
- The aggregation generally requires a shuffle so matching categories can be combined.
- Spark divides the computation into stages around shuffle boundaries.
- The Driver coordinates task execution and the final result is displayed.

The actual number of stages and tasks depends on Spark optimizations, shuffle partition settings, and Adaptive Query Execution (AQE). It is not necessarily equal to the number of Python code lines.

## **7. How to Inspect Spark Architecture in Databricks**

On classic Databricks compute, open the **Compute** page, select your compute resource, and open **Spark UI**.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



[Spark UI Debugging Techniques: Monitor and Optimize PySpark Jobs | by Supriya Singh | May, 2026 | Medium](https://images.openai.com/static-rsc-4/SQtoP05U0XEBoKsl3ijKYM4ALJjdVxyL51UgknI80SCN9m5v7ECyXZdGcjaAWyYiJGZe5puKW-1OHKddUUoTX45xSJYUL8nBlGwG0XPUSNGlgsAyoCe2Q7VX0gDoIVxDTYmyoaOC69ZivDbGc00KEetF6RVjA7pa0iO6_s2JLqQ?purpose=inline)

**Jobs tab**

Displays Spark jobs and their execution progress. Useful for identifying slow or failed jobs.

[Web UI - Spark 3.5.6 Documentation](https://images.openai.com/static-rsc-4/7M7v1suqaKVS-gBCKDGaLT4lbw0v1MUJmBDKn1OinbfnQeJ08vGtyaXW-rNHhbt11I7-ErBP7rx3_rZoaCxUcBG3ab-b_Z_KoHoQ_5IiMAmzYdsYXo00kRxGdEVi7JpCn61JtrHrFH4zLhfXxzHNnx9G2QmuaAZzwHhFlixzLME?purpose=inline)

**Stages tab**

Shows tasks, execution time, shuffle read/write, spill, and related performance metrics.

[Web UI - Spark 4.0.0 Documentation](https://images.openai.com/static-rsc-4/PUro-h-PawsHKs4U6e5-tjHoBGX15HFudcpIKDsbvKwp7tjUoTorPifUmXM1DnsUULQOQPOsu0DXCvjKx69X4lX4sIIfJTgJu6CbZ-rKWKSN_gxQAYBDznAPitv-SgPu6390KrAozMdOmEXyv4FA4WsTD2d0q2KEMdDn9skm-YM?purpose=inline)

**Executors tab**

Displays executor activity, cores, memory usage, task counts, failures, and other executor metrics.

The Spark UI is one of the most important tools for troubleshooting slow Databricks workloads.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



## **8. Driver vs Executor vs Cluster Manager**

| Feature                     | Driver                               | Executor             | Cluster Manager       |
| --------------------------- | ------------------------------------ | -------------------- | --------------------- |
| Primary role                | Coordinates execution                | Processes data       | Allocates resources   |
| Runs Spark tasks            | Generally no                         | Yes                  | No                    |
| Creates execution plans     | Yes                                  | No                   | No                    |
| Schedules tasks             | Yes                                  | No                   | No                    |
| Allocates cluster resources | Requests them                        | Uses them            | Yes                   |
| Stores cached partitions    | Not normally                         | Yes                  | No                    |
| Failure impact              | Application can terminate or restart | Tasks can be retried | Depends on manager    |
| Typical count               | One per application                  | Multiple             | Depends on deployment |

## **9. Common Interview Questions**

**Q1. What happens when a Spark Driver fails?**

A Driver failure generally terminates the current Spark application. Automatic restart depends on the deployment platform and recovery configuration. Spark does not simply continue the same application on another executor.

**Q2. What happens when an Executor fails?**

Spark can reschedule unfinished tasks on healthy executors and recompute lost partitions when possible. Excessive failures can cause the job to fail.

**Q3. Can one Executor execute multiple tasks?**

Yes. Each executor can execute multiple tasks concurrently, depending on its available CPU cores and task resource requirements.

**Q4. What is the difference between a Worker and an Executor?**

A Worker is a machine or node providing compute resources. An Executor is a process running on that machine. On classic Databricks compute, each worker node typically runs one Spark executor.

**Q5. What is the difference between client mode and cluster mode?**

In client mode, the Driver runs in the submitting process. In cluster mode, the Driver is launched inside the cluster. This distinction is most relevant when deploying Spark through `spark-submit` in self-managed environments.

**Q6. Does Spark always process data in memory?**

No. Spark uses memory for processing and caching where appropriate, but it also reads and writes data to storage, performs disk-backed shuffles, and spills intermediate data when needed.

## **10. Key Takeaways**

**Driver = Brain**

Plans, schedules, coordinates and monitors the Spark application.

**Executor = Worker**

Runs parallel tasks and processes distributed data partitions.

**Cluster Manager = Resource Allocator**

Manages the CPU and memory resources needed by Spark applications.

The most important concept to remember is that **the Driver decides what work should be executed, the Executors perform the work, and the Cluster Manager provides the resources needed to run it.**

For deeper reading: [Apache Spark official architecture documentation](https://spark.apache.org/docs/latest/cluster-overview.html?utm_source=chatgpt.com).
