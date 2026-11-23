# **Writing Data in PySpark: Save Modes in Databricks**

In Apache Spark and Databricks, **Save Modes** control what happens when you write a DataFrame to a storage location or table that already contains data.

PySpark provides four main save modes:

1. **overwrite** – Replace existing data.
2. **append** – Add new data to existing data.
3. **error / errorifexists** – Raise an error if the destination already exists.
4. **ignore** – Skip writing if the destination already exists.

We'll cover definitions, syntax, real-world use cases, ready-to-execute Databricks notebook examples, Delta Lake operations, and interview questions.

## **1. Understanding the four save modes**

| Save mode   | Destination exists     | Destination doesn't exist |
| ----------- | ---------------------- | ------------------------- |
| `overwrite` | Replaces existing data | Creates new data          |
| `append`    | Adds data              | Creates new data          |
| `error`     | Throws exception       | Creates new data          |
| `ignore`    | Skips writing          | Creates new data          |

Visual example: Existing destination has 3 records

OverwriteAppendErrorIgnore

Existing data

1 — Asha

2 — Ravi

3 — Neha

Incoming data

4 — Priya

5 — Amit

**Resulting destination**

2 records

4 — Priya

Incoming

5 — Amit

Incoming

## **2. General syntax in Databricks**

```
df.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable("demo_table")
```

Here:

- `df.write` creates a DataFrameWriter.
- `.format("delta")` selects the Delta Lake format.
- `.mode("overwrite")` sets the save mode.
- `.saveAsTable("demo_table")` saves the DataFrame as a table.

You can also write to file paths:

```
df.write.mode("append").parquet("/Volumes/catalog/schema/volume/output")df.write.mode("overwrite").format("delta").save(    "/Volumes/catalog/schema/volume/delta_output")
```

For the hands-on examples below, we'll use Delta tables instead of DBFS file paths. This works particularly well with Databricks serverless compute and Unity Catalog.

## **3. Databricks notebook setup**

First, create a sample employee dataset. We will use this dataset to demonstrate all four save modes.

Notebook Cell 1 — Create sample DataFrames

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as T# Databricks provides the SparkSession automaticallyschema = T.StructType([    T.StructField("emp_id", T.IntegerType(), False),    T.StructField("name", T.StringType(), True),    T.StructField("department", T.StringType(), True),    T.StructField("salary", T.IntegerType(), True)])# Existing employee dataexisting_data = [    (101, "Asha", "IT", 60000),    (102, "Ravi", "HR", 50000),    (103, "Neha", "Finance", 70000)]# Incoming employee datanew_data = [    (104, "Priya", "IT", 65000),    (105, "Amit", "HR", 55000)]df_existing = spark.createDataFrame(    existing_data, schema)df_new = spark.createDataFrame(    new_data, schema)print("Existing employees:")df_existing.show()print("New employees:")df_new.show()
```

Expected existing dataset:

| emp_id | name | department | salary |
| ------ | ---- | ---------- | ------ |
| 101    | Asha | IT         | 60000  |
| 102    | Ravi | HR         | 50000  |
| 103    | Neha | Finance    | 70000  |

Incoming dataset:

| emp_id | name  | department | salary |
| ------ | ----- | ---------- | ------ |
| 104    | Priya | IT         | 65000  |
| 105    | Amit  | HR         | 55000  |

## **4. Save Mode: overwrite**

### **Definition**

The `overwrite` mode replaces existing data at the destination with the contents of the incoming DataFrame.

It is commonly used when the entire dataset needs to be refreshed.

### **Syntax**

```
df.write.mode("overwrite").format("delta").saveAsTable("table_name")
```

### **Ready-to-execute Databricks example**

Notebook Cell 2 — Overwrite demonstration

```
table_name = "demo_employee_overwrite"# Clean up any previous demonstrationspark.sql(f"DROP TABLE IF EXISTS {table_name}")# Step 1: Create the existing tabledf_existing.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable(table_name)print("Before overwrite:")spark.table(table_name).show()# Step 2: Overwrite with the new DataFramedf_new.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable(table_name)print("After overwrite:")spark.table(table_name).show()
```

Expected output after overwrite:

| emp_id | name  | department | salary |
| ------ | ----- | ---------- | ------ |
| 104    | Priya | IT         | 65000  |
| 105    | Amit  | HR         | 55000  |

**Explanation:** The original three employees have been replaced by the two incoming employees.

### **Real-world use case: Daily full refresh**

Imagine a company maintaining a daily employee master table.

Every night, the HR application generates a complete employee dataset. The Data Engineering pipeline overwrites yesterday's snapshot with today's authoritative snapshot.

```
# Assume df_daily_snapshot contains the complete# latest snapshot, not just changed recordsdf_daily_snapshot = df_existing.unionByName(df_new)df_daily_snapshot.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable("demo_employee_daily_snapshot")
```

Typical applications include daily master data snapshots, fully recalculated reports, and rebuilding derived tables.

**Important:** `overwrite` is dangerous when the incoming DataFrame contains only incremental records. You can unintentionally remove all records not included in that batch.

For Delta tables, overwriting normally creates a new table version, so previous versions may be recoverable using Delta time travel, subject to retention and cleanup settings.

## **5. Save Mode: append**

### **Definition**

The `append` mode adds incoming records to the existing destination without removing the old records.

### **Syntax**

```
df.write.mode("append").format("delta").saveAsTable("table_name")
```

### **Ready-to-execute Databricks example**

Notebook Cell 3 — Append demonstration

```
table_name = "demo_employee_append"spark.sql(f"DROP TABLE IF EXISTS {table_name}")# Step 1: Save existing employeesdf_existing.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable(table_name)print("Before append:")spark.table(table_name).show()# Step 2: Append new employeesdf_new.write \    .format("delta") \    .mode("append") \    .saveAsTable(table_name)print("After append:")spark.table(table_name).orderBy("emp_id").show()print("Total records:", spark.table(table_name).count())
```

Expected output:

| emp_id | name  | department | salary |
| ------ | ----- | ---------- | ------ |
| 101    | Asha  | IT         | 60000  |
| 102    | Ravi  | HR         | 50000  |
| 103    | Neha  | Finance    | 70000  |
| 104    | Priya | IT         | 65000  |
| 105    | Amit  | HR         | 55000  |

Total records: **5**

### **Real-world use case: Transaction ingestion**

Suppose an e-commerce application processes millions of transactions daily.

Instead of replacing historical transactions, the pipeline appends each new batch.

Notebook Cell 4 — Incremental sales ingestion

```
from datetime import datesales_schema = T.StructType([    T.StructField("order_id", T.IntegerType(), False),    T.StructField("customer_id", T.IntegerType(), True),    T.StructField("amount", T.DoubleType(), True),    T.StructField("order_date", T.DateType(), True)])day1 = [    (1001, 201, 1500.0, date(2026, 11, 20)),    (1002, 202, 2200.0, date(2026, 11, 20))]day2 = [    (1003, 203, 1800.0, date(2026, 11, 21)),    (1004, 204, 3200.0, date(2026, 11, 21))]df_day1 = spark.createDataFrame(day1, sales_schema)df_day2 = spark.createDataFrame(day2, sales_schema)table_name = "demo_sales_transactions"spark.sql(f"DROP TABLE IF EXISTS {table_name}")# First day's transactionsdf_day1.write \    .format("delta") \    .mode("append") \    .saveAsTable(table_name)# Second day's transactionsdf_day2.write \    .format("delta") \
```

Expected total revenue: **₹8,700** (assuming amounts are in INR).

### **Important: Append does not prevent duplicates**

If you rerun the second day's ingestion:

```
df_day2.write \    .format("delta") \    .mode("append") \    .saveAsTable("demo_sales_transactions")
```

Those two transactions are appended again.

The table now contains six rows instead of four.

**Interview point:** `append` is not equivalent to an UPSERT. It does not automatically check primary keys or update existing records.

For reliable incremental ingestion, Delta `MERGE` is often more appropriate. We'll demonstrate it later.

## **6. Save Mode: error / errorifexists**

### **Definition**

The `error` mode raises an exception if the output destination already exists.

It is also the default save mode when you do not explicitly specify a mode.

Accepted values include:

- `error`
- `errorifexists`
- `default`

### **Ready-to-execute Databricks example**

Notebook Cell 5 — ErrorIfExists demonstration

```
table_name = "demo_employee_error"spark.sql(f"DROP TABLE IF EXISTS {table_name}")# Step 1: First write succeedsdf_existing.write \    .format("delta") \    .mode("errorifexists") \    .saveAsTable(table_name)print("First write successful")# Step 2: Second write will failtry:    df_new.write \        .format("delta") \        .mode("errorifexists") \        .saveAsTable(table_name)except Exception as e:    print("Expected error: destination already exists")    print("Exception type:", type(e).__name__)# Original records remain unchangedspark.table(table_name).show()
```

Expected behavior:

```
First write successful
Expected error: destination already exists

Existing records remain:
101 Asha
102 Ravi
103 Neha
```

### **Real-world use case: Preventing accidental data replacement**

Imagine a financial pipeline that creates a year-end snapshot table.

If a table has already been created, you don't want another pipeline execution silently replacing or duplicating it.

```
df_existing.write \    .format("delta") \    .mode("errorifexists") \    .saveAsTable("demo_financial_snapshot_2026")
```

This helps protect write-once outputs from accidental repeated creation. It is not a substitute for access controls or a full audit and data integrity strategy.

## **7. Save Mode: ignore**

### **Definition**

The `ignore` mode skips the write operation if the destination already exists.

Unlike `errorifexists`, it does not raise an exception simply because the destination exists.

### **Ready-to-execute Databricks example**

Notebook Cell 6 — Ignore demonstration

```
table_name = "demo_employee_ignore"spark.sql(f"DROP TABLE IF EXISTS {table_name}")# Step 1: First writedf_existing.write \    .format("delta") \    .mode("ignore") \    .saveAsTable(table_name)print("First write completed")# Step 2: Attempt another writedf_new.write \    .format("delta") \    .mode("ignore") \    .saveAsTable(table_name)print("Second write completed or skipped")# Check the dataspark.table(table_name).show()print("Total records:", spark.table(table_name).count())
```

Expected result: only the original three employees exist.

The two incoming employees are not written.

### **Real-world use case: Initializing reference tables**

Suppose you maintain an initial set of country codes, departments, or default configuration data.

You want to populate the table when it doesn't exist, without modifying it on subsequent runs.

```
reference_data = [    (1, "India"),    (2, "USA"),    (3, "Germany")]df_reference = spark.createDataFrame(    reference_data,    ["country_id", "country_name"])df_reference.write \    .format("delta") \    .mode("ignore") \    .saveAsTable("demo_country_reference")spark.table("demo_country_reference").show()
```

**Important distinction:** `ignore` does not perform row-level deduplication. It checks whether the destination already exists, not whether particular records exist.

## **8. Writing to different file formats**

The four save modes are supported by Spark DataFrameWriter for multiple data sources, including CSV, JSON, Parquet and Delta.

The following examples use a Unity Catalog Volume. Replace the three identifiers with a catalog, schema and volume you have permission to write to.

```
# Change these to your own Unity Catalog locationcatalog = "main"schema_name = "default"volume = "my_volume"base_path = f"/Volumes/{catalog}/{schema_name}/{volume}/save_mode_lab"# Write CSVdf_existing.write \    .mode("overwrite") \    .option("header", "true") \    .csv(f"{base_path}/csv")# Write JSONdf_existing.write \    .mode("append") \    .json(f"{base_path}/json")# Write Parquetdf_existing.write \    .mode("overwrite") \    .parquet(f"{base_path}/parquet")# Write Deltadf_existing.write \    .format("delta") \    .mode("overwrite") \    .save(f"{base_path}/delta")
```

To read the results:

```
spark.read \    .option("header", "true") \    .option("inferSchema", "true") \    .csv(f"{base_path}/csv").show()spark.read.json(f"{base_path}/json").show()spark.read.parquet(f"{base_path}/parquet").show()spark.read.format("delta").load(    f"{base_path}/delta").show()
```

Note that Spark writes output directories containing one or more part files, rather than necessarily creating a single file called `employees.csv` or `employees.parquet`.

## **9. Important advanced concept: Overwrite vs. MERGE**

In production data engineering, it is essential to understand when overwriting or appending is inappropriate.

Imagine an existing employee record:

| emp_id | name | salary |
| ------ | ---- | ------ |
| 101    | Asha | 60000  |
| 102    | Ravi | 50000  |

An incoming batch contains an updated salary for employee 101 and a new employee 103.

| emp_id | name | salary |
| ------ | ---- | ------ |
| 101    | Asha | 75000  |
| 103    | Neha | 70000  |

With `overwrite`, employee 102 is lost. With `append`, employee 101 appears twice. With Delta `MERGE`, employee 101 is updated, employee 102 is preserved, and employee 103 is inserted.

### **Databricks notebook: MERGE example**

```
from delta.tables import DeltaTabletarget_table = "demo_employee_merge"# Existing recordsoriginal = [    (101, "Asha", 60000),    (102, "Ravi", 50000)]# Incremental recordsupdates = [    (101, "Asha", 75000),    (103, "Neha", 70000)]columns = ["emp_id", "name", "salary"]df_target = spark.createDataFrame(original, columns)df_updates = spark.createDataFrame(updates, columns)spark.sql(f"DROP TABLE IF EXISTS {target_table}")df_target.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable(target_table)# Execute Delta MERGEdelta_target = DeltaTable.forName(spark, target_table)delta_target.alias("target").merge(    df_updates.alias("source"),    "target.emp_id = source.emp_id").whenMatchedUpdateAll() \
```

Expected result:

| emp_id | name | salary |
| ------ | ---- | ------ |
| 101    | Asha | 75000  |
| 102    | Ravi | 50000  |
| 103    | Neha | 70000  |

This is the basis of many Change Data Capture (CDC) and incremental data pipelines. For production use, deduplicate the source on the merge key, handle deletes when needed, and consider late-arriving records.

## **10. Schema changes during writes**

Suppose a new `email` column is added to the employee dataset.

```
df_with_email = df_existing.withColumn(    "email",    F.concat(        F.lower(F.col("name")),        F.lit("@company.com")    ))df_with_email.printSchema()
```

For a Delta table, a full overwrite with an intentional schema replacement can use:

```
df_with_email.write \    .format("delta") \    .mode("overwrite") \    .option("overwriteSchema", "true") \    .saveAsTable("demo_employee_schema")
```

For schema evolution during append, use the appropriate Delta schema-evolution setting when supported:

```
df_with_email.write \    .format("delta") \    .mode("append") \    .option("mergeSchema", "true") \    .saveAsTable("demo_employee_schema")
```

`overwriteSchema` and `mergeSchema` are not save modes. They control schema handling. Their behavior depends on the operation and table configuration.

## **11. Save modes with partitioned data**

Partitions organize data by column values, such as `order_date`, `country`, or `department`.

```
partition_table = "demo_partitioned_employees"spark.sql(f"DROP TABLE IF EXISTS {partition_table}")df_existing.write \    .format("delta") \    .mode("overwrite") \    .partitionBy("department") \    .saveAsTable(partition_table)spark.sql(    f"DESCRIBE DETAIL {partition_table}").select("format", "partitionColumns").show()spark.table(partition_table).show()
```

A crucial distinction is that **partitioning does not automatically mean overwriting only one partition**.

For selective partition replacement, Delta Lake supports a `replaceWhere` option.

```
# Replace only the IT department partitionit_updates = [    (201, "Karan", "IT", 80000),    (202, "Simran", "IT", 85000)]df_it = spark.createDataFrame(it_updates, schema)df_it.write \    .format("delta") \    .mode("overwrite") \    .option("replaceWhere", "department = 'IT'") \    .saveAsTable(partition_table)spark.table(partition_table) \    .orderBy("department", "emp_id") \    .show()
```

Expected result:

| emp_id | name   | department | salary |
| ------ | ------ | ---------- | ------ |
| 102    | Ravi   | HR         | 50000  |
| 201    | Karan  | IT         | 80000  |
| 202    | Simran | IT         | 85000  |
| 103    | Neha   | Finance    | 70000  |

Only the IT records were replaced; the HR and Finance records remain.

## **12. Save modes in SQL**

In Databricks SQL, the same broad behaviors can be expressed using table-writing statements.

| PySpark mode    | Common SQL equivalent                  |
| --------------- | -------------------------------------- |
| `overwrite`     | `INSERT OVERWRITE`                     |
| `append`        | `INSERT INTO`                          |
| `errorifexists` | `CREATE TABLE` without `IF NOT EXISTS` |
| `ignore`        | `CREATE TABLE IF NOT EXISTS`           |

These SQL operations are not exact substitutes in every situation. In particular, `CREATE TABLE IF NOT EXISTS` is a table-creation guard, not a general-purpose equivalent for ignoring an existing file destination.

Example:

```

-- Create a new Delta table
CREATE TABLE IF NOT EXISTS demo_sql_employees (
    emp_id INT,
    name STRING,
    salary INT
) USING DELTA;

-- Append records
INSERT INTO demo_sql_employees
VALUES (101, 'Asha', 60000);

-- Replace all records
INSERT OVERWRITE demo_sql_employees
VALUES
    (102, 'Ravi', 50000),
    (103, 'Neha', 70000);

-- View records
SELECT * FROM demo_sql_employees;

```

## **13. Best practices for production pipelines**

| Scenario                                    | Recommended approach                                                   |
| ------------------------------------------- | ---------------------------------------------------------------------- |
| Daily full data refresh                     | `overwrite`                                                            |
| New log or event batch                      | `append`                                                               |
| Create a snapshot only once                 | `errorifexists`                                                        |
| Initialize a reference table if missing     | `ignore`                                                               |
| Update existing records and insert new ones | Delta `MERGE`                                                          |
| Replace a specific partition or subset      | Delta `replaceWhere`                                                   |
| Streaming data ingestion                    | Structured Streaming with appropriate checkpointing and sink semantics |

Three common mistakes to avoid are using `append` for updates, using `overwrite` on incremental-only data, and treating `ignore` as a row-level duplicate prevention mechanism.

## **14. Interview questions**

1\. Which save mode is the default in PySpark?

2\. What happens when append is executed twice with the same records?

3\. Does overwrite always permanently delete historical Delta data?

4\. What is the difference between ignore and errorifexists?

5\. How can you update an existing employee without inserting a duplicate?

## **15. Quick revision**

**Overwrite** — Replace existing records.

Full refresh / snapshot rebuild

**Append** — Add new records.

Transaction logs / incremental insertion

**ErrorIfExists** — Fail if destination exists.

Prevent repeated initial creation

**Ignore** — Skip if destination exists.

Initialize reference data once

For a Data Engineer working with Databricks, the most important distinction is **full refresh (**`overwrite`**) vs. incremental insertion (**`append`**) vs. incremental upsert (**`MERGE`**)**.

Official references: [Databricks DataFrameWriter.mode](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframewriter/mode?utm_source=chatgpt.com) and [Databricks saveAsTable documentation](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframewriter/saveAsTable?utm_source=chatgpt.com).
