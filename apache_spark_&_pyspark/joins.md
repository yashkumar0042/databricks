# **PySpark Joins in Databricks — Complete Notes with Examples**

**Module 2: Apache Spark & PySpark Mastery**

In this lesson, we will cover all six important types of joins in PySpark using **ready-to-execute Databricks notebook examples**, including definitions, syntax, sample datasets, expected outputs, real-world use cases, and performance considerations.

## **1. What is a Join in PySpark?**

A **join** is an operation used to combine data from two DataFrames based on a common column or matching condition.

For example, assume we have two datasets:

- **Employees:** Contains employee ID, name, department ID and salary.
- **Departments:** Contains department ID and department name.

We want to combine these datasets to identify which department each employee belongs to.

### **Types of joins**

| Join type      | Description                                                    |
| -------------- | -------------------------------------------------------------- |
| **Inner**      | Only matching records from both DataFrames                     |
| **Left**       | All records from left, matching records from right             |
| **Right**      | All records from right, matching records from left             |
| **Full Outer** | All records from both DataFrames, whether matched or unmatched |
| **Left Semi**  | Only left-side records that have a match on the right          |
| **Left Anti**  | Only left-side records that do not have a match on the right   |

## **2. Create Sample DataFrames in Databricks**

First, create two DataFrames. We will reuse these throughout the lesson.

DATABRICKS NOTEBOOK — CELL 1

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as T# Employee datasetemployees_data = [    (1, "Amit", 101, 50000),    (2, "Priya", 102, 60000),    (3, "Rahul", 103, 55000),    (4, "Sneha", 101, 70000),    (5, "Vikas", 105, 45000),    (6, "Neha", None, 40000)]employee_schema = T.StructType([    T.StructField("emp_id", T.IntegerType(), False),    T.StructField("emp_name", T.StringType(), True),    T.StructField("dept_id", T.IntegerType(), True),    T.StructField("salary", T.IntegerType(), True)])employees_df = spark.createDataFrame(    employees_data, employee_schema)# Department datasetdepartments_data = [    (101, "Engineering"),    (102, "Finance"),    (103, "HR"),    (104, "Marketing")]department_schema = T.StructType([    T.StructField("dept_id", T.IntegerType(), False),    T.StructField("dept_name", T.StringType(), True)])departments_df = spark.createDataFrame(    departments_data, department_schema)display(employees_df)display(departments_df)
```

**Employee DataFrame**

| emp_id | emp_name | dept_id | salary |
| ------ | -------- | ------- | ------ |
| 1      | Amit     | 101     | 50000  |
| 2      | Priya    | 102     | 60000  |
| 3      | Rahul    | 103     | 55000  |
| 4      | Sneha    | 101     | 70000  |
| 5      | Vikas    | 105     | 45000  |
| 6      | Neha     | NULL    | 40000  |

**Department DataFrame**

| dept_id | dept_name   |
| ------- | ----------- |
| 101     | Engineering |
| 102     | Finance     |
| 103     | HR          |
| 104     | Marketing   |

Observe that:

- Department `101` has two employees.
- Department `105` exists in employees but not departments.
- Department `104` exists in departments but has no employees.
- Neha has a `NULL` department ID.

These scenarios help us understand the differences between joins.

## **3. Visual understanding of joins**

INNER

Common rows

LEFT

All left rows

RIGHT

All right rows

FULL

All rows

LEFT SEMI

Left matches only

LEFT ANTI

Left nonmatches

Blue regions illustrate which records each join retains. Semi joins return only left-side columns, even when both sides match.

## **4. INNER JOIN**

### **Definition**

An **Inner Join** returns only records where the join condition matches in both DataFrames.

If a record exists in the left DataFrame but has no corresponding match in the right DataFrame, it is excluded.

**Syntax**

```
df1.join(df2, on="common_column", how="inner")
```

### **Databricks Notebook — Cell 2**

```
inner_df = employees_df.join(    departments_df,    on="dept_id",    how="inner")inner_df.orderBy("emp_id").show()
```

**Expected output**

| dept_id | emp_id | emp_name | salary | dept_name   |
| ------- | ------ | -------- | ------ | ----------- |
| 101     | 1      | Amit     | 50000  | Engineering |
| 102     | 2      | Priya    | 60000  | Finance     |
| 103     | 3      | Rahul    | 55000  | HR          |
| 101     | 4      | Sneha    | 70000  | Engineering |

**Explanation**

- Amit and Sneha match Engineering (`101`).
- Priya matches Finance (`102`).
- Rahul matches HR (`103`).
- Vikas is excluded because department `105` does not exist in the department dataset.
- Neha is excluded because her department ID is NULL.

Result: 4 rows

### **Real-world use case: Generate an employee payroll report**

Suppose a company wants to generate a payroll report containing only employees assigned to valid departments.

```
payroll_df = (    employees_df    .join(departments_df, "dept_id", "inner")    .select(        "emp_id",        "emp_name",        "dept_name",        "salary"    )    .orderBy("emp_id"))display(payroll_df)
```

**Other real-world applications:** Joining customer orders with registered customers, combining sales with product master data, and enriching transactions with valid account details.

## **5. LEFT JOIN (LEFT OUTER JOIN)**

### **Definition**

A **Left Join** returns all records from the left DataFrame and the matching records from the right DataFrame.

When there is no corresponding match, columns from the right DataFrame contain `NULL`.

**Syntax**

```
df1.join(df2, on="common_column", how="left")
```

You can also use `how="left_outer"`.

### **Databricks Notebook — Cell 3**

```
left_df = employees_df.join(    departments_df,    on="dept_id",    how="left")left_df.orderBy("emp_id").show()
```

**Expected output**

| dept_id | emp_id | emp_name | salary | dept_name   |
| ------- | ------ | -------- | ------ | ----------- |
| 101     | 1      | Amit     | 50000  | Engineering |
| 102     | 2      | Priya    | 60000  | Finance     |
| 103     | 3      | Rahul    | 55000  | HR          |
| 101     | 4      | Sneha    | 70000  | Engineering |
| 105     | 5      | Vikas    | 45000  | NULL        |
| NULL    | 6      | Neha     | 40000  | NULL        |

Result: 6 rows

### **Explanation**

Every employee is retained, even if the department does not exist.

Vikas and Neha have NULL department names because no matching department was found.

### **Real-world use case: Find employees without valid departments**

```
missing_departments_df = (    employees_df    .join(departments_df, "dept_id", "left")    .filter(F.col("dept_name").isNull())    .select("emp_id", "emp_name", "dept_id")    .orderBy("emp_id"))display(missing_departments_df)
```

**Expected output**

| emp_id | emp_name | dept_id |
| ------ | -------- | ------- |
| 5      | Vikas    | 105     |
| 6      | Neha     | NULL    |

**Production use case:** In ETL pipelines, a left join is commonly used to enrich incoming records while preserving the original records, including those whose reference data is missing.

## **6. RIGHT JOIN (RIGHT OUTER JOIN)**

### **Definition**

A **Right Join** returns every record from the right DataFrame, along with matching records from the left DataFrame.

If no corresponding left-side record exists, columns from the left DataFrame contain `NULL`.

**Syntax**

```
df1.join(df2, on="common_column", how="right")
```

### **Databricks Notebook — Cell 4**

```
right_df = employees_df.join(    departments_df,    on="dept_id",    how="right")right_df.orderBy("dept_id", "emp_id").show()
```

**Expected output**

| dept_id | emp_id | emp_name | salary | dept_name   |
| ------- | ------ | -------- | ------ | ----------- |
| 101     | 1      | Amit     | 50000  | Engineering |
| 101     | 4      | Sneha    | 70000  | Engineering |
| 102     | 2      | Priya    | 60000  | Finance     |
| 103     | 3      | Rahul    | 55000  | HR          |
| 104     | NULL   | NULL     | NULL   | Marketing   |

Result: 5 rows

### **Explanation**

All departments are preserved, including Marketing, even though it has no employees.

Vikas and Neha are excluded because they have no matching department in the right DataFrame.

### **Real-world use case: Find departments with no employees**

```
empty_departments_df = (    employees_df    .join(departments_df, "dept_id", "right")    .filter(F.col("emp_id").isNull())    .select("dept_id", "dept_name"))display(empty_departments_df)
```

**Expected output**

| dept_id | dept_name |
| ------- | --------- |
| 104     | Marketing |

**Important:** A right join can usually be rewritten as a left join by swapping the DataFrames.

```
# Equivalent to the original RIGHT JOINdepartments_df.join(    employees_df,    on="dept_id",    how="left")
```

## **7. FULL OUTER JOIN**

### **Definition**

A **Full Outer Join** returns all matching and unmatched records from both DataFrames.

- Matching records are combined.
- Unmatched left-side records have NULLs for right-side columns.
- Unmatched right-side records have NULLs for left-side columns.

**Syntax**

```
df1.join(df2, on="common_column", how="full")
```

You can also use `full_outer` or `outer`.

### **Databricks Notebook — Cell 5**

```
full_df = employees_df.join(    departments_df,    on="dept_id",    how="full")full_df.orderBy(    F.col("dept_id").asc_nulls_last(),    F.col("emp_id").asc_nulls_last()).show()
```

**Expected output**

| dept_id | emp_id | emp_name | salary | dept_name   |
| ------- | ------ | -------- | ------ | ----------- |
| 101     | 1      | Amit     | 50000  | Engineering |
| 101     | 4      | Sneha    | 70000  | Engineering |
| 102     | 2      | Priya    | 60000  | Finance     |
| 103     | 3      | Rahul    | 55000  | HR          |
| 104     | NULL   | NULL     | NULL   | Marketing   |
| 105     | 5      | Vikas    | 45000  | NULL        |
| NULL    | 6      | Neha     | 40000  | NULL        |

Result: 7 rows

### **Real-world use case: Data reconciliation**

Suppose your employee database and department master are maintained by different teams. You want to identify inconsistencies across both systems.

```
reconciliation_df = (    employees_df.alias("e")    .join(        departments_df.alias("d"),        F.col("e.dept_id") == F.col("d.dept_id"),        "full"    )    .select(        F.col("e.emp_id"),        F.col("e.emp_name"),        F.col("e.dept_id").alias("employee_dept_id"),        F.col("d.dept_id").alias("master_dept_id"),        F.col("d.dept_name"),        F.when(            F.col("e.emp_id").isNull(),            "DEPARTMENT_WITHOUT_EMPLOYEE"        ).when(            F.col("d.dept_id").isNull(),            "EMPLOYEE_WITHOUT_VALID_DEPARTMENT"        ).otherwise("MATCHED").alias("status")    ))display(reconciliation_df)
```

This creates a `status` field that identifies records matched across systems or missing on either side.

**Other use cases:** Comparing old and new datasets during migration, reconciling source and target tables, and finding missing records across financial systems.

## **8. LEFT SEMI JOIN**

### **Definition**

A **Left Semi Join** returns rows from the left DataFrame only when a match exists in the right DataFrame.

Unlike an inner join, **it does not return columns from the right DataFrame**.

Think of it as an existence check:

> Does this employee have a matching department?

If yes, return the employee.

**Syntax**

```
df1.join(df2, on="common_column", how="left_semi")
```

### **Databricks Notebook — Cell 6**

```
semi_df = employees_df.join(    departments_df,    on="dept_id",    how="left_semi")semi_df.orderBy("emp_id").show()
```

**Expected output**

| emp_id | emp_name | dept_id | salary |
| ------ | -------- | ------- | ------ |
| 1      | Amit     | 101     | 50000  |
| 2      | Priya    | 102     | 60000  |
| 3      | Rahul    | 103     | 55000  |
| 4      | Sneha    | 101     | 70000  |

Result: 4 rows

### **Why use a semi join instead of an inner join?**

Suppose the departments DataFrame contains duplicate department IDs.

An inner join may duplicate employee records because it generates a result for every matching pair.

A semi join will retain each qualifying left-side row once, regardless of how many matching right-side rows exist.

```
duplicate_departments = spark.createDataFrame(    [        (101, "Engineering"),        (101, "Engineering"),        (102, "Finance")    ],    ["dept_id", "dept_name"])# Inner joininner_count = employees_df.join(    duplicate_departments, "dept_id", "inner").count()# Semi joinsemi_count = employees_df.join(    duplicate_departments, "dept_id", "left_semi").count()print("Inner Join Count:", inner_count)print("Semi Join Count:", semi_count)
```

**Expected output**

```
Inner Join Count: 5
Semi Join Count: 3
```

### **Real-world use case: Filter only valid transactions**

Suppose you have millions of transaction records and a list of valid customers. You want transactions belonging only to valid customers without attaching customer columns.

```
transactions_df = spark.createDataFrame([    (1001, 1, 500),    (1002, 2, 1200),    (1003, 5, 800),    (1004, 8, 400)], ["txn_id", "customer_id", "amount"])valid_customers_df = spark.createDataFrame([    (1,),    (2,),    (3,)], ["customer_id"])valid_transactions_df = transactions_df.join(    valid_customers_df,    on="customer_id",    how="left_semi")display(valid_transactions_df)
```

**Expected output**

| customer_id | txn_id | amount |
| ----------- | ------ | ------ |
| 1           | 1001   | 500    |
| 2           | 1002   | 1200   |

Semi joins are useful for existence-based filtering, allowing Spark to avoid producing duplicate match combinations or unnecessary right-side output columns.

## **9. LEFT ANTI JOIN**

### **Definition**

A **Left Anti Join** returns rows from the left DataFrame that have **no matching records in the right DataFrame**.

It is the opposite of a left semi join.

Think of it as:

> Which employees do NOT have a matching department?

**Syntax**

```
df1.join(df2, on="common_column", how="left_anti")
```

### **Databricks Notebook — Cell 7**

```
anti_df = employees_df.join(    departments_df,    on="dept_id",    how="left_anti")anti_df.orderBy("emp_id").show()
```

**Expected output**

| emp_id | emp_name | dept_id | salary |
| ------ | -------- | ------- | ------ |
| 5      | Vikas    | 105     | 45000  |
| 6      | Neha     | NULL    | 40000  |

Result: 2 rows

### **Real-world use case: Find unprocessed transactions in an ETL pipeline**

Imagine a Databricks pipeline processing thousands of orders daily.

You have two DataFrames:

- `orders_df`: All incoming orders.
- `processed_orders_df`: Orders already processed.

You want to identify orders that have not been processed yet.

```
orders_df = spark.createDataFrame([    (101, "Laptop", 50000),    (102, "Mobile", 20000),    (103, "Keyboard", 1500),    (104, "Monitor", 12000),    (105, "Mouse", 800)], ["order_id", "product", "amount"])processed_orders_df = spark.createDataFrame([    (101,),    (103,),    (105,)], ["order_id"])unprocessed_orders_df = orders_df.join(    processed_orders_df,    on="order_id",    how="left_anti")display(unprocessed_orders_df)
```

**Expected output**

| order_id | product | amount |
| -------- | ------- | ------ |
| 102      | Mobile  | 20000  |
| 104      | Monitor | 12000  |

This pattern is useful in incremental ETL processing, missing-record detection, data-quality checks and pipeline reconciliation.

For reliable production processing, also consider transactional writes and idempotency: an anti join alone does not prevent two concurrent jobs from processing the same order.

## **10. Compare all six joins in one notebook**

The following cell performs all six joins and shows the resulting row counts.

DATABRICKS NOTEBOOK — CELL 8

```
join_types = [    "inner",    "left",    "right",    "full",    "left_semi",    "left_anti"]for join_type in join_types:    result = employees_df.join(        departments_df,        on="dept_id",        how=join_type    )    print(f"{join_type}: {result.count()} rows")
```

**Expected output**

```
inner: 4 rows
left: 6 rows
right: 5 rows
full: 7 rows
left_semi: 4 rows
left_anti: 2 rows
```

Join output counts

Number of rows returned using the example employee and department DataFrames.

02468InnerLeftRightFullSemiAnti

These counts are specific to our sample data, not fixed rules for join types. Real datasets can generate many more rows when multiple matches exist.

## **11. Joining DataFrames with different column names**

So far, both DataFrames have used `dept_id`. But in real-world applications, column names often differ.

For example:

- Employee DataFrame: `dept_id`
- Department DataFrame: `department_id`

We must explicitly define the join condition.

DATABRICKS NOTEBOOK — CELL 9

```
departments_new_df = departments_df.withColumnRenamed(    "dept_id",    "department_id")e = employees_df.alias("e")d = departments_new_df.alias("d")result = (    e.join(        d,        F.col("e.dept_id") == F.col("d.department_id"),        "inner"    )    .select(        "e.emp_id",        "e.emp_name",        "e.dept_id",        "d.dept_name"    ))display(result)
```

**Key concepts**

`alias()` gives DataFrames short names, which helps avoid ambiguous column references.

`F.col("e.dept_id") == F.col("d.department_id")` explicitly defines the matching condition.

`select()` chooses the required output columns and prevents unwanted duplicate join-key columns.

## **12. Joining on multiple columns**

Sometimes matching a single key is insufficient.

For example, an employee ID might repeat across different companies. We should match on both `company_id` and `emp_id`.

DATABRICKS NOTEBOOK — CELL 10

```
employees_multi = spark.createDataFrame([    (1, 101, "Amit"),    (1, 102, "Priya"),    (2, 101, "Rahul")], ["company_id", "emp_id", "emp_name"])salaries_multi = spark.createDataFrame([    (1, 101, 50000),    (1, 102, 60000),    (2, 101, 70000)], ["company_id", "emp_id", "salary"])# Join using two common keysresult = employees_multi.join(    salaries_multi,    on=["company_id", "emp_id"],    how="inner")display(result)
```

**Expected output**

| company_id | emp_id | emp_name | salary |
| ---------- | ------ | -------- | ------ |
| 1          | 101    | Amit     | 50000  |
| 1          | 102    | Priya    | 60000  |
| 2          | 101    | Rahul    | 70000  |

This technique is common in multi-tenant applications, financial datasets, order processing and composite-key data models.

## **13. How NULL values behave in joins**

An important PySpark concept is that ordinary equality does not match NULL with NULL.

Consider two rows:

```
Left:  dept_id = NULL
Right: dept_id = NULL
```

Even though both values are NULL, they do not match with a normal `==` join condition.

To explicitly match NULLs, use null-safe equality with `eqNullSafe()`.

DATABRICKS NOTEBOOK — CELL 11

```
left_schema = T.StructType([    T.StructField("id", T.IntegerType(), True),    T.StructField("left_value", T.StringType(), True)])right_schema = T.StructType([    T.StructField("id", T.IntegerType(), True),    T.StructField("right_value", T.StringType(), True)])left_null_df = spark.createDataFrame(    [(1, "A"), (None, "Unknown Employee")],    left_schema)right_null_df = spark.createDataFrame(    [(1, "Matched"), (None, "Unknown Department")],    right_schema)print("Normal inner join:")left_null_df.join(    right_null_df,    on="id",    how="inner").show()print("Null-safe inner join:")left_null_df.alias("l").join(    right_null_df.alias("r"),    F.col("l.id").eqNullSafe(F.col("r.id")),    "inner").show()
```

The normal join returns one row. The null-safe join returns two rows.

Use null-safe equality only when treating two missing keys as a valid match makes sense for your data model.

## **14. Join optimization in Databricks**

Joins can be expensive because Spark may need to redistribute large amounts of data between executors.

A common optimization is a **Broadcast Hash Join**.

### **What is a broadcast join?**

Instead of shuffling both DataFrames, Spark can distribute a small DataFrame to executors processing a larger DataFrame.

Conceptual broadcast join: executors receive a copy of the small dataset, reducing the need to shuffle the large side.

### **Databricks Notebook — Cell 12**

```
# Generate a larger datasetlarge_orders_df = (    spark.range(1, 100001)    .withColumn(        "dept_id",        (F.col("id") % 4 + 101).cast("int")    )    .withColumn(        "amount",        (F.col("id") * 10).cast("long")    ))# Broadcast the small department datasetbroadcast_join_df = large_orders_df.join(    F.broadcast(departments_df),    on="dept_id",    how="inner")print("Rows:", broadcast_join_df.count())# Examine the physical execution planbroadcast_join_df.explain("formatted")
```

**Expected row count:** 100,000.

Look for `BroadcastHashJoin` and `BroadcastExchange` in the physical execution plan. Spark can select other strategies when appropriate, but this hint requests broadcasting the departments DataFrame.

### **What is shuffle in a join?**

A shuffle occurs when Spark redistributes records across partitions or executors so that rows with matching join keys can be processed together.

For large datasets, this can introduce network transfer, disk I/O and additional processing overhead.

Databricks also supports Adaptive Query Execution (AQE), which can adjust join strategies and shuffle partitions using runtime statistics.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



**Production recommendations:** Filter unnecessary rows early, select only required columns, broadcast genuinely small reference datasets, monitor skewed keys, and inspect execution plans before forcing join strategies.

## **15. Joins using Databricks SQL**

You can also execute the same operations using SQL instead of PySpark.

### **Register DataFrames as temporary views**

DATABRICKS NOTEBOOK — CELL 13 (PYTHON)

```
employees_df.createOrReplaceTempView("employees")departments_df.createOrReplaceTempView("departments")print("Temporary views created successfully")
```

Now use a SQL cell in your Databricks notebook.

DATABRICKS NOTEBOOK — CELL 14 (SQL)

```

-- INNER JOIN
SELECT e.emp_id, e.emp_name, d.dept_name
FROM employees e
INNER JOIN departments d
ON e.dept_id = d.dept_id;

-- LEFT JOIN
SELECT e.emp_id, e.emp_name, d.dept_name
FROM employees e
LEFT JOIN departments d
ON e.dept_id = d.dept_id;

-- RIGHT JOIN
SELECT e.emp_id, e.emp_name, d.dept_name
FROM employees e
RIGHT JOIN departments d
ON e.dept_id = d.dept_id;

-- FULL OUTER JOIN
SELECT e.emp_id, e.emp_name, d.dept_name
FROM employees e
FULL OUTER JOIN departments d
ON e.dept_id = d.dept_id;

-- LEFT SEMI JOIN
SELECT e.*
FROM employees e
LEFT SEMI JOIN departments d
ON e.dept_id = d.dept_id;

-- LEFT ANTI JOIN
SELECT e.*
FROM employees e
LEFT ANTI JOIN departments d
ON e.dept_id = d.dept_id;

```

For a Databricks SQL notebook cell, select the SQL language in the cell menu or put `%sql` as the first line. Each statement can be executed separately to inspect its result.

## **16. Practice exercises**

Use the DataFrames created in this notebook to solve the following exercises.

1\.

Find all employees who belong to the Engineering department.

Show solution&#x20;

2\.

Find employees whose department ID is missing or invalid.

Show solution&#x20;

3\.

Find departments with no employees.

Show solution&#x20;

4\.

Return only the employees who have valid department IDs, without department columns.

Show solution&#x20;

5\.

Find the average employee salary per valid department.

Show solution&#x20;

## **17. Summary: Which join should you use?**

| Requirement                                        | Recommended join |
| -------------------------------------------------- | ---------------- |
| Keep only matching records                         | `inner`          |
| Preserve all left-side records                     | `left`           |
| Preserve all right-side records                    | `right`          |
| Keep matched and unmatched records from both sides | `full`           |
| Check existence and retain only left columns       | `left_semi`      |
| Identify records missing from right side           | `left_anti`      |

**Important interview points:**

- Left semi and left anti joins return columns from the left DataFrame only.
- Inner joins can generate duplicate combinations if join keys are repeated.
- A left join does not necessarily return the exact number of left-side rows; duplicates on the right can multiply matches.
- Ordinary equality joins do not match NULL with NULL.
- Join performance depends on dataset sizes, join-key distribution, join conditions, Spark's selected physical join strategy and shuffle costs.
- A broadcast join is often effective for large fact tables joined to small dimension tables.

The syntax and supported join types are confirmed in the [official Databricks DataFrame.join documentation](https://docs.databricks.com/aws/en/pyspark/reference/classes/dataframe/join?utm_source=chatgpt.com).

All examples above use DataFrames created directly inside Databricks, so they do not require external files, DBFS access or direct SparkContext access. They are suitable for Databricks notebooks using serverless compute as well as supported cluster-based compute.
