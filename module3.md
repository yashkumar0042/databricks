# **Module 3: Spark SQL & Databricks SQL — Complete Study Notes**

Theory • Architecture • SQL Syntax • PySpark Integration • Executable Databricks Labs • Expected Outputs • Real-World Use Cases • Interview Preparation

## **1. Introduction to Spark SQL and Databricks SQL**

### **1.1 What is Spark SQL?**

**Spark SQL is a module of Apache Spark that enables structured data processing using SQL queries and the DataFrame API.**

Traditional SQL databases process structured data using relational database engines. Apache Spark extends SQL-based processing to distributed systems, allowing large datasets to be processed across multiple machines.

Spark SQL allows developers, data engineers, and analysts to execute familiar SQL operations on data stored in:

- Delta Lake tables
- Parquet, CSV, JSON, and other supported formats
- Cloud object storage such as Amazon S3 and Azure Data Lake Storage
- Structured and semi-structured datasets
- DataFrames created using PySpark

For example, a traditional SQL query:

```
SELECT department, SUM(salary) AS total_salary
FROM employees
GROUP BY department;
```

Can also be executed in Spark SQL against a Spark table or view.

### **1.2 Why was Spark SQL introduced?**

Before Spark SQL, developers frequently relied on lower-level RDD operations and had to manually implement filtering, mapping, grouping, and aggregations.

Consider an RDD-based calculation:

```
rdd = sc.parallelize([    ("IT", 50000),    ("HR", 40000),    ("IT", 60000)])result = (    rdd    .map(lambda x: (x[0], x[1]))    .reduceByKey(lambda a, b: a + b))print(result.collect())
```

Expected output (order may vary):

```
[('IT', 110000), ('HR', 40000)]
```

Equivalent Spark SQL:

```
SELECT department, SUM(salary) AS total_salary
FROM employees
GROUP BY department;
```

Spark SQL was introduced to provide a higher-level, declarative interface, enabling the engine to optimize execution automatically.

### **1.3 Advantages of Spark SQL**

| Feature                | Explanation                                               |
| ---------------------- | --------------------------------------------------------- |
| Distributed processing | Queries execute across a Spark cluster                    |
| Declarative SQL        | Define what result is needed instead of how to compute it |
| Query optimization     | Catalyst generates optimized query plans                  |
| Execution optimization | Tungsten improves CPU and memory efficiency               |
| DataFrame integration  | SQL and PySpark can operate on the same data              |
| Multiple data formats  | Supports Delta, Parquet, JSON, CSV, and more              |
| Analytical functions   | Supports grouping, joins, CTEs, subqueries, and windows   |
| Scalable workloads     | Suitable for large-scale ETL and analytics                |

### **1.4 Spark SQL vs. Databricks SQL**

Spark SQL is the SQL processing capability within Apache Spark.

Databricks SQL is a managed analytics and data warehousing experience built around the Databricks lakehouse platform.

| Spark SQL                                           | Databricks SQL                                                            |
| --------------------------------------------------- | ------------------------------------------------------------------------- |
| Component of Apache Spark                           | Managed SQL analytics capability in Databricks                            |
| Accessed through SQL and DataFrame APIs             | Accessed through SQL editor, warehouses, APIs, and BI tools               |
| Runs using Spark execution infrastructure           | Uses Databricks-managed SQL warehouses                                    |
| Includes Catalyst and Spark execution optimizations | Adds managed compute, query monitoring, collaboration, and BI integration |
| Can be used in many Spark environments              | Specific to the Databricks platform                                       |

**Real-world example:** An e-commerce company might use PySpark to transform millions of order records, save them in Delta tables, run SQL queries to calculate revenue, and create Databricks dashboards for management.

## **2. Spark SQL Engine Architecture: Catalyst and Tungsten**

This is one of the most important theoretical topics in Spark SQL.

### **2.1 Spark SQL execution flow**

Conceptual Spark SQL processing pipeline. Actual plans may also include adaptive query optimization and Databricks-specific execution features.

### **2.2 What is the Catalyst optimizer?**

**Catalyst is Spark SQL's query optimization framework.** It analyzes logical queries, transforms them using optimization rules, and participates in selecting efficient physical execution plans.

Catalyst uses tree transformations to optimize query execution.

Its major stages are:

1. Parsing
2. Analysis
3. Logical optimization
4. Physical planning

#### **Stage 1: Parsing**

Spark converts SQL text into a logical representation called an unresolved logical plan.

Example:

```
SELECT employee_name, salary
FROM employees
WHERE salary > 50000;
```

Conceptual unresolved plan:

```
Project [employee_name, salary]
  Filter [salary > 50000]
    UnresolvedRelation [employees]
```

At this point Spark has not necessarily resolved which underlying table and columns the query references.

#### **Stage 2: Analysis**

Catalyst resolves the references using catalog metadata and schema information.

It checks whether the table exists, whether the referenced columns are available, and whether expressions have compatible types.

For example, if `salary` is absent, Spark may return an unresolved-column error.

Conceptual analyzed plan:

```
Project [employee_name, salary]
  Filter [salary > 50000]
    Relation [employees]
```

#### **Stage 3: Logical optimization**

Catalyst applies rule-based optimizations and, where supported and appropriate, cost-based optimizations.

Important techniques include:

| Optimization           | What happens                                 | Benefit                       |
| ---------------------- | -------------------------------------------- | ----------------------------- |
| Predicate pushdown     | Apply supported filters closer to data reads | Read fewer rows               |
| Column pruning         | Read only required columns                   | Reduce I/O                    |
| Constant folding       | Evaluate constant expressions early          | Avoid unnecessary computation |
| Boolean simplification | Simplify conditional expressions             | Reduce redundant work         |
| Join reordering        | Choose a potentially better join order       | Reduce processing cost        |
| Projection pruning     | Eliminate unnecessary projected fields       | Reduce intermediate data      |

**Example: Column pruning**

```
SELECT employee_name
FROM employees;
```

If the source has 30 columns, the engine may only need to read the column required by the query, depending on the storage format and source capabilities.

**Example: Predicate pushdown**

```
SELECT *
FROM employees
WHERE department = 'IT';
```

Spark attempts to apply the filter as early as possible, potentially pushing it to the data source.

Predicate pushdown depends on the source, format, and expression. It should not be assumed for every condition.

**Example: Constant folding**

```
SELECT employee_name
FROM employees
WHERE salary > (10000 * 5);
```

The optimizer can simplify the comparison to:

```
WHERE salary > 50000
```

#### **Stage 4: Physical planning**

Catalyst produces candidate strategies and selects a physical execution plan.

Examples of physical operators:

- `FileScan` or `Scan` — read the data
- `Filter` — filter rows
- `Project` — select or compute columns
- `HashAggregate` — perform grouping and aggregation
- `Exchange` — redistribute data across partitions
- `BroadcastHashJoin` — join using a broadcast relation
- `SortMergeJoin` — join using sorted partitions

For joins, Catalyst may select broadcast hash joins, sort-merge joins, or other strategies based on available statistics, settings, and query requirements.

### **2.3 What is Tungsten?**

**Project Tungsten is a set of Spark execution-engine improvements designed to improve CPU and memory efficiency.**

Its focus includes reducing Java object overhead, improving memory management, and generating efficient execution code.

Three major concepts:

**1. Memory management**

Tungsten introduced techniques to manage data more compactly, including binary representations of rows and off-heap memory support.

Benefits include reduced object allocation and garbage collection overhead.

**2. Whole-stage code generation**

Spark can combine compatible physical operators into generated Java code, reducing per-row overhead and avoiding repeated virtual function calls.

For example, filtering and projecting rows may be fused into one generated execution stage.

**3. CPU efficiency**

Compact binary data representations and generated execution code can improve cache locality and processing efficiency.

### **2.4 Catalyst vs. Tungsten**

| Catalyst                                                   | Tungsten                                             |
| ---------------------------------------------------------- | ---------------------------------------------------- |
| Query planning and optimization framework                  | Execution-engine optimization initiative             |
| Determines efficient logical and physical query strategies | Improves execution efficiency                        |
| Performs rule-based and cost-based optimization            | Improves memory, code generation, and CPU efficiency |
| Relevant before physical execution                         | Relevant during physical execution                   |
| Example: prune unused columns                              | Example: generated code processes rows efficiently   |

An easy way to remember:

**Catalyst decides how the query should be executed. Tungsten helps execute the plan efficiently.**

In Databricks, Photon is another important execution engine to know. Photon is a native vectorized query engine that can accelerate supported SQL and DataFrame workloads. It complements the broader Spark SQL planning ecosystem rather than being another name for Tungsten.

### **2.5 Executable Databricks notebook: Catalyst**

Notebook cell 1 — Python

```
from pyspark.sql import functions as Femployees = [    (1, "Asha", "IT", 60000),    (2, "Rahul", "HR", 45000),    (3, "Neha", "IT", 75000),    (4, "Amit", "Finance", 55000)]df = spark.createDataFrame(    employees,    ["employee_id", "employee_name",     "department", "salary"])df.createOrReplaceTempView("employees")df.show()
```

Expected output:

```
+-----------+-------------+----------+------+
|employee_id|employee_name|department|salary|
+-----------+-------------+----------+------+
|          1|         Asha|        IT| 60000|
|          2|        Rahul|        HR| 45000|
|          3|         Neha|        IT| 75000|
|          4|         Amit|   Finance| 55000|
+-----------+-------------+----------+------+
```

Notebook cell 2 — Python

```
filtered_df = (    df.filter(F.col("salary") > 50000)      .select("employee_name", "salary"))filtered_df.show()filtered_df.explain("formatted")
```

Expected data output:

```
+-------------+------+
|employee_name|salary|
+-------------+------+
|         Asha| 60000|
|         Neha| 75000|
|         Amit| 55000|
+-------------+------+
```

Representative physical-plan operators:

```
Project
  Filter
    Scan ExistingRDD
```

Exact plans vary with runtime, source format, and optimizer settings.

**What the output signifies:** Spark applies a salary filter and returns only the requested columns. The explain plan lets you inspect how Spark intends to execute that operation.

**Real-world use case:** A company processes 500 million transaction records. Catalyst can reduce unnecessary data access and choose efficient execution strategies, while execution optimizations make the processing more efficient.

## **3. Databricks Lab Setup: Employee Analytics Dataset**

We will use one realistic employee dataset for most of this module.

This makes it easier to understand how SQL functions work together rather than learning each command in isolation.

### **3.1 Dataset structure**

Our sample company has eight employees across four departments.

| ID | Employee | Department | Salary (₹) | City      | Joining Date |
| -- | -------- | ---------- | ---------- | --------- | ------------ |
| 1  | Asha     | IT         | 60,000     | Delhi     | 2026-01-10   |
| 2  | Rahul    | HR         | 45,000     | Mumbai    | 2026-02-12   |
| 3  | Neha     | IT         | 75,000     | Delhi     | 2026-03-15   |
| 4  | Amit     | Finance    | 55,000     | Pune      | 2026-03-20   |
| 5  | Priya    | HR         | 50,000     | Mumbai    | 2026-04-05   |
| 6  | Karan    | IT         | 90,000     | Bengaluru | 2026-04-15   |
| 7  | Meera    | Finance    | 65,000     | Pune      | 2026-05-01   |
| 8  | Rohan    | Sales      | 40,000     | Delhi     | 2026-05-10   |

### **3.2 Create a DataFrame in Databricks**

Notebook cell — Python

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as Tdata = [    (1, "Asha",  "IT",      60000, "Delhi",     "2026-01-10"),    (2, "Rahul", "HR",      45000, "Mumbai",    "2026-02-12"),    (3, "Neha",  "IT",      75000, "Delhi",     "2026-03-15"),    (4, "Amit",  "Finance", 55000, "Pune",      "2026-03-20"),    (5, "Priya", "HR",      50000, "Mumbai",    "2026-04-05"),    (6, "Karan", "IT",      90000, "Bengaluru", "2026-04-15"),    (7, "Meera", "Finance", 65000, "Pune",      "2026-05-01"),    (8, "Rohan", "Sales",   40000, "Delhi",     "2026-05-10")]schema = T.StructType([    T.StructField("employee_id", T.IntegerType(), False),    T.StructField("employee_name", T.StringType(), False),    T.StructField("department", T.StringType(), False),    T.StructField("salary", T.IntegerType(), False),    T.StructField("city", T.StringType(), False),    T.StructField("joining_date", T.StringType(), False)])employee_df = (    spark.createDataFrame(data, schema)    .withColumn(        "joining_date",        F.to_date("joining_date", "yyyy-MM-dd")    ))employee_df.createOrReplaceTempView("employees")display(employee_df.orderBy("employee_id"))
```

**Expected output:** The eight-row employee table shown above, with `joining_date` stored as a SQL `DATE` instead of a string.

### **3.3 Create a permanent Delta table**

For the SQL editor, dashboards, and BI integration, we need a persistent table. A notebook temporary view alone is not suitable because a SQL warehouse uses a separate session.

First, select a writable catalog and schema in your Databricks notebook.

Notebook cell — SQL

```
SELECT
  current_catalog() AS catalog_name,
  current_schema() AS schema_name;
```

If necessary, set a catalog and schema for which you have `CREATE TABLE` permissions.

```
-- Replace these names with your accessible catalog/schema.
-- USE CATALOG workspace;
-- USE SCHEMA default;
```

Then run:

```
CREATE OR REPLACE TABLE module3_employees
USING DELTA
AS
SELECT *
FROM employees;
```

Expected result: successful table creation.

Verify:

```
SELECT COUNT(*) AS total_employees
FROM module3_employees;
```

| total_employees |
| --------------- |
| 8               |

This permanent table is our **base table** for the remaining SQL labs.

> Prerequisites: The persistent-table labs require a writable catalog/schema. On a restricted Databricks Free Edition or shared training workspace, you may be able to execute temporary-view SELECT examples but not create persistent tables. SQL warehouse, dashboard, and external-table availability also depends on workspace features and permissions.

## **4. Temporary Views and Global Temporary Views**

### **4.1 What is a view?**

A view is a named SQL query that can be referenced as if it were a table.

A regular view typically stores its query definition, not a separate physical copy of the query's results.

Views simplify repeated SQL logic and help expose selected columns or rows to consumers.

### **4.2 Temporary view**

A temporary view is a named logical view with a limited session lifetime.

**Key characteristics:**

- Available within the current Spark session.
- Not registered as a permanent catalog table.
- Normally disappears when the session ends.
- Useful for intermediate calculations.
- Can be created from PySpark DataFrames or SQL queries.
- Other independent sessions cannot automatically access it.

### **4.3 Databricks example: Creating a temporary view**

Python

```
employee_df.createOrReplaceTempView("employees")
```

Now execute SQL:

```
SELECT employee_name, department, salary
FROM employees
WHERE department = 'IT'
ORDER BY salary DESC;
```

Expected output:

| employee_name | department | salary |
| ------------- | ---------- | ------ |
| Karan         | IT         | 90000  |
| Neha          | IT         | 75000  |
| Asha          | IT         | 60000  |

**Explanation:** We have registered the DataFrame using the name `employees`. Spark SQL can now reference it without converting the DataFrame into a physical table.

The Python DataFrame and the SQL temporary view refer to the same logical dataset.

### **4.4 Creating a temporary view using SQL**

```
CREATE OR REPLACE TEMP VIEW high_salary_employees
AS
SELECT employee_id, employee_name, salary
FROM module3_employees
WHERE salary >= 70000;
```

Read it:

```
SELECT *
FROM high_salary_employees
ORDER BY salary DESC;
```

Expected output:

| employee_id | employee_name | salary |
| ----------- | ------------- | ------ |
| 6           | Karan         | 90000  |
| 3           | Neha          | 75000  |

**Use case:** A data engineer creates temporary views for cleaned data, filtered records, and joined datasets before saving the final output to a Delta table.

### **4.5 What is a global temporary view?**

A global temporary view is a Spark view that can be shared across multiple Spark sessions associated with the same Spark application or compatible compute context.

It is registered in the reserved `global_temp` namespace.

Its lifetime is tied to the Spark application or associated compute lifecycle rather than one ordinary SQL session.

### **4.6 Global temporary view example**

Python — compatible classic Spark compute

```
employee_df.createOrReplaceGlobalTempView(    "global_employees")
```

Query:

```
SELECT employee_name, salary
FROM global_temp.global_employees
ORDER BY salary DESC
LIMIT 3;
```

Expected output:

| employee_name | salary |
| ------------- | ------ |
| Karan         | 90000  |
| Neha          | 75000  |
| Meera         | 65000  |

Another supported session within the same Spark application may reference the global view.

**Databricks limitation:** Global temporary views are legacy functionality. The `global_temp` namespace is not supported on Databricks serverless compute; Databricks recommends session-scoped temporary views for serverless. For cross-session sharing, use persistent Unity Catalog tables or views.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



### **4.7 Temp view vs. global temp view vs. permanent view**

| Feature                     | Temp view                    | Global temp view                        | Permanent view            |
| --------------------------- | ---------------------------- | --------------------------------------- | ------------------------- |
| Lifetime                    | Session                      | Spark application/compute               | Until dropped             |
| Scope                       | Current session              | Compatible sessions sharing application | Authorized users/sessions |
| Catalog persistence         | No                           | No ordinary catalog persistence         | Yes                       |
| Stores separate result data | No                           | No                                      | No                        |
| Serverless compatibility    | Supported                    | Not supported                           | Supported                 |
| Best use case               | Intermediate transformations | Legacy cross-session sharing            | Reusable governed SQL     |

**Interview question:** Does a temporary view physically store data?

**Answer:** No. A normal temporary view stores a logical query definition or DataFrame reference, not a separately materialized result dataset. However, underlying data or computations may still be cached independently if caching is explicitly used.

## **5. Running SQL Directly on DataFrames**

One of the biggest advantages of Spark is the ability to combine Python and SQL.

### **5.1 How does the integration work?**

When a DataFrame is registered as a temporary view, it becomes accessible through SQL in the same Spark session.

The basic flow:

### **5.2 Method 1:&#x20;**`spark.sql()`

```
result_df = spark.sql("""    SELECT        department,        COUNT(*) AS employee_count,        SUM(salary) AS total_salary    FROM employees    GROUP BY department    ORDER BY department""")result_df.show()
```

Expected output:

```
+----------+--------------+------------+
|department|employee_count|total_salary|
+----------+--------------+------------+
|   Finance|             2|      120000|
|        HR|             2|       95000|
|        IT|             3|      225000|
|     Sales|             1|       40000|
+----------+--------------+------------+
```

**What the output signifies:** The SQL engine has grouped the employee records by department and calculated the employee count and total salaries.

The result is itself a DataFrame. You can continue processing it using PySpark.

```
result_df.filter(    F.col("total_salary") > 100000).show()
```

Expected output:

```
+----------+--------------+------------+
|department|employee_count|total_salary|
+----------+--------------+------------+
|   Finance|             2|      120000|
|        IT|             3|      225000|
+----------+--------------+------------+
```

### **5.3 Method 2: Databricks&#x20;**`%sql`**&#x20;magic**

In a Databricks Python notebook, begin a cell with `%sql`.

```
%sql
SELECT employee_name, salary
FROM employees
ORDER BY salary DESC
LIMIT 3;
```

The `%sql` directive switches the notebook cell to SQL even when the notebook's default language is Python.

Expected output is the same three highest-paid employees.

Do not add `%sql` when the cell language is already set to SQL.

### **5.4 Method 3: SQL query results back into Python**

```
high_earners = spark.sql("""    SELECT employee_name, salary    FROM employees    WHERE salary >= 70000""")high_earners = high_earners.withColumn(    "bonus",    F.col("salary") * F.lit(0.10))high_earners.orderBy("salary").show()
```

Expected output:

```
+-------------+------+------+
|employee_name|salary| bonus|
+-------------+------+------+
|         Neha| 75000|7500.0|
|        Karan| 90000|9000.0|
+-------------+------+------+
```

**Real-world use case:** A data engineer uses Python for API ingestion and custom transformations, Spark SQL for joins and aggregations, then the DataFrame writer to save results as Delta tables.

### **5.5 SQL vs. DataFrame API**

Both Spark SQL and the DataFrame API commonly produce equivalent optimized execution plans.

For example:

```
# DataFrame APIemployee_df.filter(    F.col("salary") > 70000).select("employee_name")
```

```
-- SQL equivalent
SELECT employee_name
FROM employees
WHERE salary > 70000;
```

For equivalent expressions and input data, neither interface is inherently faster. Performance differences usually arise from the specific operations, functions, source characteristics, or resulting plan.

## **6. Databricks SQL Editor Walkthrough**

The Databricks SQL editor is a browser-based environment for developing, executing, organizing, and analyzing SQL queries.

Unlike an ordinary notebook, it is designed especially for SQL analytics and warehouse-based workloads.

### **6.1 Main components of Databricks SQL**

- **SQL editor:** Write and execute SQL statements.
- **SQL warehouse:** Compute resource that executes queries.
- **Catalog Explorer:** Browse catalogs, schemas, tables, and views.
- **Query results:** Inspect query output.
- **Query profile:** Investigate performance.
- **Visualizations:** Convert query results into charts.
- **AI/BI dashboards:** Publish and share analytics.

Databricks continues to evolve the editor, so some interface labels can vary between workspaces.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+2



### **6.2 Step-by-step walkthrough**

1. Open your Databricks workspace and select **SQL Editor** from the sidebar.
2. Create a new query and select a running SQL warehouse for execution.
3. Choose the catalog and schema containing `module3_employees`.
4. Write your query in the editor.
5. Click **Run** and examine the results, row count, and execution information.
6. Save the query using a meaningful name, such as `Module 3 - Employee Analytics`.
7. Use the results visualization features or add the query to an AI/BI dashboard.

### **6.3 First SQL editor query**

```
SELECT
    department,
    COUNT(*) AS total_employees,
    ROUND(AVG(salary), 2) AS average_salary
FROM module3_employees
GROUP BY department
ORDER BY average_salary DESC;
```

Expected output:

| department | total_employees | average_salary |
| ---------- | --------------- | -------------- |
| IT         | 3               | 75000.00       |
| Finance    | 2               | 60000.00       |
| HR         | 2               | 47500.00       |
| Sales      | 1               | 40000.00       |

**What the output signifies:** IT has the highest average salary, while Sales has the lowest in this sample.

**Use case:** Analysts can write SQL and create reports without needing to author PySpark code.

## **7. Managed Tables vs. External Tables**

### **7.1 What is a managed table?**

A managed table is a table whose data lifecycle and storage management are handled by the catalog/platform.

In Unity Catalog, managed tables use managed storage locations determined by Unity Catalog.

When you create a managed table without specifying an external location, Databricks manages its storage placement.

```
CREATE TABLE department_budget (
    department STRING,
    budget INT
)
USING DELTA;
```

Insert data:

```
INSERT INTO department_budget VALUES
('IT', 500000),
('HR', 200000),
('Finance', 300000);
```

Verify:

```
SELECT *
FROM department_budget
ORDER BY department;
```

| department | budget |
| ---------- | ------ |
| Finance    | 300000 |
| HR         | 200000 |
| IT         | 500000 |

### **7.2 What is an external table?**

An external table is a table whose data resides at an explicitly specified external storage location.

For example, an organization may store Delta files in S3 and register an external table pointing to those files.

```
CREATE TABLE external_sales (
    sale_id INT,
    amount DOUBLE
)
USING DELTA
LOCATION 's3://your-approved-bucket/sales/';
```

This is a template requiring an actual authorized S3 path.

In Unity Catalog, the cloud storage path must be configured through an appropriate external location and storage credential, and the user needs relevant permissions.

### **7.3 Difference between managed and external tables**

| Feature                     | Managed table                                           | External table                           |
| --------------------------- | ------------------------------------------------------- | ---------------------------------------- |
| Storage location            | Platform/catalog managed                                | Explicitly defined                       |
| Data lifecycle              | Managed by Databricks/Unity Catalog                     | Managed externally                       |
| Metadata                    | Catalog                                                 | Catalog                                  |
| `DROP TABLE`                | Removes registration and initiates managed data cleanup | Removes registration, retains data files |
| Ownership of storage layout | Platform                                                | External owner/process                   |
| Common use case             | Databricks-native analytics                             | Data shared with external platforms      |

**Important:** In Unity Catalog, dropping a managed table does not necessarily mean its data files disappear instantaneously. Databricks retains recoverability for a period under its managed lifecycle rules. Dropping an external table removes its metadata registration but leaves the underlying files intact.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on Google Cloud+1



### **7.4 Check whether a table is managed**

```
DESCRIBE TABLE EXTENDED module3_employees;
```

Look for table metadata such as provider, location, and table type.

You can also inspect details for supported Delta tables:

```
DESCRIBE DETAIL module3_employees;
```

**Real-world decision:** Use managed Delta tables for your primary Databricks analytics datasets unless external ownership, interoperability, or independent storage management is a specific requirement.

## **8. CREATE TABLE, INSERT, UPDATE, DELETE**

SQL commands are often classified into Data Definition Language and Data Manipulation Language.

| Category | Command      | Purpose                                           |
| -------- | ------------ | ------------------------------------------------- |
| DDL      | CREATE TABLE | Define a new table                                |
| DDL      | ALTER TABLE  | Change table definition                           |
| DDL      | DROP TABLE   | Remove a table                                    |
| DML      | INSERT       | Add records                                       |
| DML      | UPDATE       | Modify records                                    |
| DML      | DELETE       | Remove records                                    |
| DML      | MERGE        | Perform conditional inserts, updates, and deletes |

Databricks supports transactional data modification through Delta Lake tables.

### **8.1 CREATE TABLE**

```
CREATE OR REPLACE TABLE module3_transactions (
    transaction_id INT,
    customer_name STRING,
    amount DECIMAL(10, 2),
    transaction_date DATE
)
USING DELTA;
```

This creates an empty Delta table with four columns.

### **8.2 INSERT INTO**

```
INSERT INTO module3_transactions VALUES
(101, 'Asha',  5000.00, DATE '2026-11-01'),
(102, 'Rahul', 8000.00, DATE '2026-11-02'),
(103, 'Neha',  3000.00, DATE '2026-11-03'),
(104, 'Amit',  7000.00, DATE '2026-11-04');
```

Query:

```
SELECT *
FROM module3_transactions
ORDER BY transaction_id;
```

Expected output:

| transaction_id | customer_name | amount  | transaction_date |
| -------------- | ------------- | ------- | ---------------- |
| 101            | Asha          | 5000.00 | 2026-11-01       |
| 102            | Rahul         | 8000.00 | 2026-11-02       |
| 103            | Neha          | 3000.00 | 2026-11-03       |
| 104            | Amit          | 7000.00 | 2026-11-04       |

### **8.3 UPDATE**

Update Asha's amount from ₹5,000 to ₹6,000.

```
UPDATE module3_transactions
SET amount = 6000.00
WHERE transaction_id = 101;
```

Verification:

```
SELECT transaction_id, amount
FROM module3_transactions
WHERE transaction_id = 101;
```

| transaction_id | amount  |
| -------------- | ------- |
| 101            | 6000.00 |

**Explanation:** The `WHERE` clause identifies the row to update.

Without a suitable `WHERE` condition, an UPDATE can modify every record in the table.

### **8.4 DELETE**

Delete transaction 103:

```
DELETE FROM module3_transactions
WHERE transaction_id = 103;
```

Verify:

```
SELECT transaction_id, customer_name
FROM module3_transactions
ORDER BY transaction_id;
```

| transaction_id | customer_name |
| -------------- | ------------- |
| 101            | Asha          |
| 102            | Rahul         |
| 104            | Amit          |

The table now has three rows.

### **8.5 INSERT OVERWRITE**

```
INSERT OVERWRITE TABLE module3_transactions
VALUES
(201, 'Karan', 10000.00, DATE '2026-11-05'),
(202, 'Meera', 12000.00, DATE '2026-11-06');
```

Expected result after querying:

| transaction_id | customer_name | amount   |
| -------------- | ------------- | -------- |
| 201            | Karan         | 10000.00 |
| 202            | Meera         | 12000.00 |

The previous transaction rows have been replaced.

This is different from an `INSERT INTO`, which adds records.

### **8.6 MERGE INTO — Important production command**

`MERGE` is commonly used for CDC processing, incremental loads, and upserts.

**Upsert = UPDATE existing records + INSERT new records.**

Create the target table:

```
CREATE OR REPLACE TABLE module3_customers (
    customer_id INT,
    customer_name STRING,
    city STRING
)
USING DELTA;

INSERT INTO module3_customers VALUES
(1, 'Asha', 'Delhi'),
(2, 'Rahul', 'Mumbai');
```

Create incoming data:

```
CREATE OR REPLACE TEMP VIEW incoming_customers AS
SELECT *
FROM VALUES
    (2, 'Rahul', 'Pune'),
    (3, 'Neha', 'Bengaluru')
AS incoming(customer_id, customer_name, city);
```

Merge:

```
MERGE INTO module3_customers AS target
USING incoming_customers AS source
ON target.customer_id = source.customer_id

WHEN MATCHED THEN
    UPDATE SET
        target.customer_name = source.customer_name,
        target.city = source.city

WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        customer_name,
        city
    )
    VALUES (
        source.customer_id,
        source.customer_name,
        source.city
    );
```

Result:

| customer_id | customer_name | city      |
| ----------- | ------------- | --------- |
| 1           | Asha          | Delhi     |
| 2           | Rahul         | Pune      |
| 3           | Neha          | Bengaluru |

**What happened?** Rahul existed, so his city was updated. Neha did not exist, so a new row was inserted.

**Production use case:** An upstream operational database sends customer changes to Databricks. Delta Lake MERGE applies the new and changed records to the customer dimension table.

## **9. Common Table Expressions (CTEs)**

### **9.1 What is a CTE?**

A Common Table Expression is a named intermediate query defined using the `WITH` keyword.

A CTE exists within the scope of the SQL statement in which it is defined.

It is useful for breaking complex SQL queries into understandable, reusable logical blocks.

**Basic syntax**

```
WITH cte_name AS (
    SELECT column1, column2
    FROM table_name
)
SELECT *
FROM cte_name;
```

A CTE is not automatically a materialized table. The optimizer may inline or otherwise transform its logic, so repeated CTE references do not guarantee that a computation runs only once.

### **9.2 Example 1: Department average salaries**

```
WITH department_avg AS (
    SELECT
        department,
        AVG(salary) AS avg_salary
    FROM module3_employees
    GROUP BY department
)
SELECT
    department,
    avg_salary
FROM department_avg
ORDER BY avg_salary DESC;
```

Expected output:

| department | avg_salary |
| ---------- | ---------- |
| IT         | 75000.0    |
| Finance    | 60000.0    |
| HR         | 47500.0    |
| Sales      | 40000.0    |

**Explanation:** The CTE first calculates the average salary per department. The outer query reads the CTE and sorts the results.

### **9.3 Example 2: Multiple CTEs**

Suppose we want employees earning more than their department's average salary.

```
WITH department_avg AS (
    SELECT
        department,
        AVG(salary) AS avg_salary
    FROM module3_employees
    GROUP BY department
),

above_average AS (
    SELECT
        e.employee_name,
        e.department,
        e.salary,
        d.avg_salary
    FROM module3_employees e
    JOIN department_avg d
      ON e.department = d.department
    WHERE e.salary > d.avg_salary
)

SELECT *
FROM above_average
ORDER BY department, salary DESC;
```

Expected output:

| employee_name | department | salary | avg_salary |
| ------------- | ---------- | ------ | ---------- |
| Meera         | Finance    | 65000  | 60000.0    |
| Priya         | HR         | 50000  | 47500.0    |
| Karan         | IT         | 90000  | 75000.0    |

**Output significance:** Only three employees earn strictly more than their respective department averages.

**Use case:** HR analysts identify higher-paid employees within individual departments rather than comparing everyone against a company-wide salary threshold.

### **9.4 Example 3: Recursive CTE**

A recursive CTE repeatedly executes recursive logic until the termination condition is met.

It is useful for:

- Organization hierarchy traversal
- Parent-child relationships
- Bill-of-materials processing
- Hierarchical category trees

For example, generate integers from 1 to 5:

```
WITH RECURSIVE numbers(n) AS (
    SELECT 1

    UNION ALL

    SELECT n + 1
    FROM numbers
    WHERE n < 5
)
SELECT n
FROM numbers;
```

Expected output:

```
1
2
3
4
5
```

Recursive CTEs require a supported Databricks SQL or Runtime version, specifically Databricks SQL / DBR 17.0 or newer. Older runtime configurations may not support this example.

### **9.5 CTE vs. temporary view**

| CTE                               | Temporary view                                  |
| --------------------------------- | ----------------------------------------------- |
| Defined inside one SQL statement  | Created as a named session object               |
| Only accessible in that statement | Accessible to subsequent queries in the session |
| No permanent catalog registration | No permanent catalog registration               |
| Good for one complex query        | Good for multiple session queries               |
| Not automatically materialized    | Not automatically materialized                  |

## **10. Subqueries in Spark SQL**

### **10.1 What is a subquery?**

A subquery is a query nested inside another SQL statement.

It can be used to calculate values, filter records, test existence, or build derived datasets.

Important categories include:

1. Scalar subquery
2. `IN` subquery
3. `EXISTS` subquery
4. Correlated subquery
5. Subquery in the `FROM` clause

### **10.2 Scalar subquery**

A scalar subquery returns a single value.

**Example: Employees earning above the company average**

```
SELECT
    employee_name,
    salary
FROM module3_employees
WHERE salary > (
    SELECT AVG(salary)
    FROM module3_employees
)
ORDER BY salary DESC;
```

Company-wide average:

\\[ \frac{480000}{8}=60000 \\]

Expected output:

| employee_name | salary |
| ------------- | ------ |
| Karan         | 90000  |
| Neha          | 75000  |
| Meera         | 65000  |

**Explanation:** The inner query calculates an average salary of ₹60,000. The outer query returns employees earning more than that amount.

### **10.3 IN subquery**

Use `IN` to check whether a value belongs to a set of values returned by a subquery.

```
SELECT employee_name, department
FROM module3_employees
WHERE department IN (
    SELECT department
    FROM module3_employees
    GROUP BY department
    HAVING COUNT(*) >= 3
)
ORDER BY employee_name;
```

Expected output:

| employee_name | department |
| ------------- | ---------- |
| Asha          | IT         |
| Karan         | IT         |
| Neha          | IT         |

Only IT has at least three employees.

### **10.4 Correlated subquery**

A correlated subquery references one or more columns from the outer query.

```
SELECT
    e.employee_name,
    e.department,
    e.salary
FROM module3_employees e
WHERE e.salary > (
    SELECT AVG(e2.salary)
    FROM module3_employees e2
    WHERE e2.department = e.department
)
ORDER BY e.department;
```

Expected output:

| employee_name | department | salary |
| ------------- | ---------- | ------ |
| Meera         | Finance    | 65000  |
| Priya         | HR         | 50000  |
| Karan         | IT         | 90000  |

**Explanation:** The subquery is correlated through `e.department`. Logically, each employee's salary is compared with the average of that employee's department.

That does not necessarily mean Spark physically executes the subquery once per employee. The optimizer may rewrite it into a more efficient form, such as a join or aggregate.

### **10.5 EXISTS subquery**

`EXISTS` checks whether the inner query returns at least one row.

```
SELECT DISTINCT e.department
FROM module3_employees e
WHERE EXISTS (
    SELECT 1
    FROM module3_employees x
    WHERE x.department = e.department
      AND x.salary > 80000
);
```

Expected output:

| department |
| ---------- |
| IT         |

The IT department qualifies because Karan earns ₹90,000.

**Real-world use case:** Retrieve customers who have at least one successful transaction or vendors who have at least one active product listing.

### **10.6 CTE vs. subquery**

| Factor             | CTE                                      | Subquery                                  |
| ------------------ | ---------------------------------------- | ----------------------------------------- |
| Readability        | Often better for complex queries         | Good for concise nested logic             |
| Reuse in statement | Can reference a named CTE multiple times | Reuse may require duplication             |
| Query scope        | One statement                            | Parent statement                          |
| Performance        | Depends on optimized plan                | Depends on optimized plan                 |
| Best use case      | Multi-stage transformations              | Filtering, scalar calculations, existence |

**Interview tip:** A CTE is not automatically faster than a subquery. Compare their actual execution plans rather than relying on a syntax-based assumption.

## **11. Aggregate Functions in Databricks SQL**

### **11.1 What are aggregate functions?**

Aggregate functions summarize multiple rows into one result or one result per group.

They are heavily used in financial reporting, sales analytics, data quality checks, and business intelligence.

### **11.2 Common aggregate functions**

| Function          | Purpose                      | Example                 |
| ----------------- | ---------------------------- | ----------------------- |
| `COUNT(*)`        | Count rows                   | Number of employees     |
| `COUNT(column)`   | Count non-null values        | Employees with a salary |
| `SUM()`           | Add values                   | Total salary            |
| `AVG()`           | Arithmetic mean              | Average salary          |
| `MIN()`           | Smallest value               | Minimum salary          |
| `MAX()`           | Largest value                | Maximum salary          |
| `COUNT(DISTINCT)` | Count distinct values        | Unique departments      |
| `STDDEV_SAMP()`   | Sample standard deviation    | Salary variation        |
| `COLLECT_LIST()`  | Collect values into an array | Salaries per department |
| `COLLECT_SET()`   | Collect distinct values      | Distinct cities         |

### **11.3 Example: Basic aggregate calculations**

```
SELECT
    COUNT(*) AS employee_count,
    SUM(salary) AS total_salary,
    AVG(salary) AS average_salary,
    MIN(salary) AS minimum_salary,
    MAX(salary) AS maximum_salary
FROM module3_employees;
```

Expected output:

| employee_count | total_salary | average_salary | minimum_salary | maximum_salary |
| -------------- | ------------ | -------------- | -------------- | -------------- |
| 8              | 480000       | 60000.0        | 40000          | 90000          |

### **11.4 GROUP BY**

`GROUP BY` groups rows sharing specified values so that aggregate functions can be applied separately.

```
SELECT
    department,
    COUNT(*) AS employees,
    SUM(salary) AS total_salary,
    AVG(salary) AS avg_salary
FROM module3_employees
GROUP BY department
ORDER BY department;
```

Expected output:

| department | employees | total_salary | avg_salary |
| ---------- | --------- | ------------ | ---------- |
| Finance    | 2         | 120000       | 60000.0    |
| HR         | 2         | 95000        | 47500.0    |
| IT         | 3         | 225000       | 75000.0    |
| Sales      | 1         | 40000        | 40000.0    |

### **11.5 HAVING**

`HAVING` filters groups after aggregation.

Compare:

- `WHERE` filters individual rows.
- `HAVING` filters aggregate groups.

```
SELECT
    department,
    COUNT(*) AS employee_count
FROM module3_employees
GROUP BY department
HAVING COUNT(*) >= 2
ORDER BY department;
```

Expected output:

| department | employee_count |
| ---------- | -------------- |
| Finance    | 2              |
| HR         | 2              |
| IT         | 3              |

Sales is excluded because it contains only one employee.

### **11.6 WHERE vs. HAVING example**

```
SELECT
    department,
    AVG(salary) AS avg_salary
FROM module3_employees
WHERE salary >= 50000
GROUP BY department
HAVING AVG(salary) > 60000;
```

Expected output:

| department | avg_salary |
| ---------- | ---------- |
| IT         | 75000.0    |

Explanation:

1. The `WHERE` clause excludes employees earning less than ₹50,000.
2. Rows are grouped by department.
3. Average salaries are calculated for those remaining rows.
4. `HAVING` retains only groups with average salary strictly greater than ₹60,000.

### **11.7 Conditional aggregation**

A useful pattern for dashboards:

```
SELECT
    department,
    COUNT(*) AS total_employees,
    SUM(
        CASE
            WHEN salary >= 70000 THEN 1
            ELSE 0
        END
    ) AS high_salary_count
FROM module3_employees
GROUP BY department
ORDER BY department;
```

Expected output:

| department | total_employees | high_salary_count |
| ---------- | --------------- | ----------------- |
| Finance    | 2               | 0                 |
| HR         | 2               | 0                 |
| IT         | 3               | 2                 |
| Sales      | 1               | 0                 |

**Use case:** Count successful versus failed orders, premium versus regular customers, or completed versus pending deliveries in the same aggregation.

## **12. SQL Window Functions**

### **12.1 What are window functions?**

A window function performs calculations across a group of related rows while preserving individual row records.

The key difference from ordinary `GROUP BY` is that window functions do not automatically collapse multiple input rows into a single output row per group.

### **12.2 Window function syntax**

```
FUNCTION_NAME() OVER (
    PARTITION BY column_name
    ORDER BY column_name
)
```

Three important components:

| Component      | Purpose                                                      |
| -------------- | ------------------------------------------------------------ |
| `PARTITION BY` | Divides records into logical groups                          |
| `ORDER BY`     | Defines order within each group                              |
| Window frame   | Defines the rows considered for frame-sensitive calculations |

### **12.3 Difference between GROUP BY and PARTITION BY**

Consider the department IT.

**GROUP BY**

Three employees become one aggregate row.

| IT | ₹75,000 avg. |
| -- | ------------ |

**PARTITION BY**

All three employees remain visible.

| Asha  | ₹75,000 |
| ----- | ------- |
| Neha  | ₹75,000 |
| Karan | ₹75,000 |

### **12.4 ROW_NUMBER()**

Assigns a unique sequence number to each record within its window partition.

```
SELECT
    employee_name,
    department,
    salary,
    ROW_NUMBER() OVER (
        PARTITION BY department
        ORDER BY salary DESC, employee_id
    ) AS row_num
FROM module3_employees
ORDER BY department, row_num;
```

Expected output:

| employee_name | department | salary | row_num |
| ------------- | ---------- | ------ | ------- |
| Meera         | Finance    | 65000  | 1       |
| Amit          | Finance    | 55000  | 2       |
| Priya         | HR         | 50000  | 1       |
| Rahul         | HR         | 45000  | 2       |
| Karan         | IT         | 90000  | 1       |
| Neha          | IT         | 75000  | 2       |
| Asha          | IT         | 60000  | 3       |
| Rohan         | Sales      | 40000  | 1       |

**Use case:** Choose the latest record for each customer, or the highest-paid employee in each department.

### **12.5 RANK() vs. DENSE_RANK() vs. ROW_NUMBER()**

The three functions behave differently when duplicate ordering values occur.

Create a sample:

```
CREATE OR REPLACE TEMP VIEW ranking_scores AS
SELECT *
FROM VALUES
    ('Asha',  100),
    ('Rahul', 100),
    ('Neha',   90),
    ('Amit',   80)
AS scores(name, score);
```

Execute:

```
SELECT
    name,
    score,
    ROW_NUMBER() OVER (
        ORDER BY score DESC, name
    ) AS row_number_value,
    RANK() OVER (
        ORDER BY score DESC
    ) AS rank_value,
    DENSE_RANK() OVER (
        ORDER BY score DESC
    ) AS dense_rank_value
FROM ranking_scores
ORDER BY score DESC, name;
```

Expected output:

| name  | score | row_number_value | rank_value | dense_rank_value |
| ----- | ----- | ---------------- | ---------- | ---------------- |
| Asha  | 100   | 1                | 1          | 1                |
| Rahul | 100   | 2                | 1          | 1                |
| Neha  | 90    | 3                | 3          | 2                |
| Amit  | 80    | 4                | 4          | 3                |

**Interpretation:**

- `ROW_NUMBER()` assigns unique sequential numbers.
- `RANK()` assigns equal ranks to ties and skips subsequent rank numbers.
- `DENSE_RANK()` assigns equal ranks to ties without gaps.

### **12.6 LAG() and LEAD()**

`LAG()` accesses a previous row's value within an ordered window.

`LEAD()` accesses a following row's value.

```
SELECT
    employee_name,
    department,
    salary,
    LAG(salary) OVER (
        PARTITION BY department
        ORDER BY salary, employee_id
    ) AS previous_salary,
    LEAD(salary) OVER (
        PARTITION BY department
        ORDER BY salary, employee_id
    ) AS next_salary
FROM module3_employees
WHERE department = 'IT'
ORDER BY salary;
```

Expected output:

| employee_name | department | salary | previous_salary | next_salary |
| ------------- | ---------- | ------ | --------------- | ----------- |
| Asha          | IT         | 60000  | null            | 75000       |
| Neha          | IT         | 75000  | 60000           | 90000       |
| Karan         | IT         | 90000  | 75000           | null        |

**Use case:** Compare current sales with previous month's sales or calculate a change between consecutive events.

### **12.7 Running total with SUM() OVER()**

```
SELECT
    employee_name,
    department,
    salary,
    SUM(salary) OVER (
        PARTITION BY department
        ORDER BY salary, employee_id
        ROWS BETWEEN UNBOUNDED PRECEDING
                 AND CURRENT ROW
    ) AS running_salary
FROM module3_employees
WHERE department = 'IT'
ORDER BY salary;
```

Expected output:

| employee_name | salary | running_salary |
| ------------- | ------ | -------------- |
| Asha          | 60000  | 60000          |
| Neha          | 75000  | 135000         |
| Karan         | 90000  | 225000         |

**Explanation:**

- First row: 60,000
- Second row: 60,000 + 75,000 = 135,000
- Third row: 135,000 + 90,000 = 225,000

The explicit `ROWS` frame ensures a row-based running total.

### **12.8 Window frames: ROWS vs. RANGE**

`ROWS` considers row positions relative to the current row.

`RANGE` considers ordering values and their relationships rather than simply counting physical row positions.

This difference matters when ordering values are duplicated.

For an explicit three-row moving average:

```
AVG(amount) OVER (
    ORDER BY transaction_date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)
```

This calculates an average over the current row and up to two preceding rows.

**Production use case:** A retail data analyst calculates a rolling seven-day revenue average to smooth daily sales fluctuations.

### **12.9 Advanced: Top two salaries per department**

```
WITH ranked_employees AS (
    SELECT
        employee_name,
        department,
        salary,
        ROW_NUMBER() OVER (
            PARTITION BY department
            ORDER BY salary DESC, employee_id
        ) AS rn
    FROM module3_employees
)

SELECT
    employee_name,
    department,
    salary
FROM ranked_employees
WHERE rn <= 2
ORDER BY department, salary DESC;
```

Expected output:

| employee_name | department | salary |
| ------------- | ---------- | ------ |
| Meera         | Finance    | 65000  |
| Amit          | Finance    | 55000  |
| Priya         | HR         | 50000  |
| Rahul         | HR         | 45000  |
| Karan         | IT         | 90000  |
| Neha          | IT         | 75000  |
| Rohan         | Sales      | 40000  |

**Interview note:** Sales contains only one employee, so its only record is still returned.

## **13. String Functions in Databricks SQL**

### **13.1 What are string functions?**

String functions transform, clean, search, and format text-based data.

They are frequently used in ETL pipelines, customer data cleansing, address standardization, log parsing, and report preparation.

### **13.2 Important string functions**

| Function           | Purpose                        | Example result   |
| ------------------ | ------------------------------ | ---------------- |
| `UPPER()`          | Convert to uppercase           | ASHA             |
| `LOWER()`          | Convert to lowercase           | asha             |
| `INITCAP()`        | Capitalize words               | New Delhi        |
| `LENGTH()`         | Calculate string length        | 4                |
| `TRIM()`           | Remove leading/trailing spaces | Asha             |
| `CONCAT()`         | Join strings                   | Asha-IT          |
| `CONCAT_WS()`      | Join using separator           | Asha,IT          |
| `SUBSTRING()`      | Extract substring              | Ash              |
| `REPLACE()`        | Replace text                   | New City         |
| `SPLIT()`          | Split into an array            | `['Asha', 'IT']` |
| `REGEXP_REPLACE()` | Replace text using regex       | Cleaned ID       |
| `REGEXP_EXTRACT()` | Extract pattern using regex    | Domain or ID     |

### **13.3 Lab: String transformations**

```
SELECT
    employee_name,
    UPPER(employee_name) AS upper_name,
    LOWER(employee_name) AS lower_name,
    LENGTH(employee_name) AS name_length,
    CONCAT(employee_name, '-', department) AS employee_label
FROM module3_employees
WHERE employee_id IN (1, 2)
ORDER BY employee_id;
```

Expected output:

| employee_name | upper_name | lower_name | name_length | employee_label |
| ------------- | ---------- | ---------- | ----------- | -------------- |
| Asha          | ASHA       | asha       | 4           | Asha-IT        |
| Rahul         | RAHUL      | rahul      | 5           | Rahul-HR       |

### **13.4 Lab: Data cleaning**

Suppose customer names arrive with inconsistent capitalization and extra spaces.

```
WITH raw_customers AS (
    SELECT *
    FROM VALUES
        (1, '  asha SHARMA  '),
        (2, 'RAHUL kumar'),
        (3, '   neha singh')
    AS t(customer_id, raw_name)
)

SELECT
    customer_id,
    INITCAP(LOWER(TRIM(raw_name))) AS clean_name
FROM raw_customers
ORDER BY customer_id;
```

Expected output:

| customer_id | clean_name  |
| ----------- | ----------- |
| 1           | Asha Sharma |
| 2           | Rahul Kumar |
| 3           | Neha Singh  |

**Explanation:** The transformation removes outside whitespace, converts the text to lowercase, then capitalizes words.

### **13.5 Lab: Extracting domain names from email addresses**

```
WITH contacts AS (
    SELECT *
    FROM VALUES
        ('asha@gmail.com'),
        ('rahul@amazon.com'),
        ('neha@company.org')
    AS t(email)
)

SELECT
    email,
    SPLIT(email, '@')[1] AS domain_name
FROM contacts;
```

Expected output:

| email             | domain_name |
| ----------------- | ----------- |
| asha\@gmail.com   | gmail.com   |
| rahul\@amazon.com | amazon.com  |
| neha\@company.org | company.org |

Databricks SQL array indexing with square brackets uses zero-based indexes, so `[1]` retrieves the second element returned by `SPLIT`.

**Real-world use cases:** Extract email domains, clean customer records, normalize names, and parse structured identifiers from logs.

## **14. Date and Time Functions**

Date/time functions are essential for reporting, incremental ingestion, SLA calculations, and time-series analytics.

### **14.1 Common date/time functions**

| Function              | Purpose                                  |
| --------------------- | ---------------------------------------- |
| `CURRENT_DATE()`      | Current date                             |
| `CURRENT_TIMESTAMP()` | Current timestamp                        |
| `YEAR()`              | Extract year                             |
| `MONTH()`             | Extract month                            |
| `DAY()`               | Extract day of month                     |
| `DATE_ADD()`          | Add days                                 |
| `DATE_SUB()`          | Subtract days                            |
| `DATEDIFF()`          | Difference between dates in days         |
| `MONTHS_BETWEEN()`    | Difference in months                     |
| `DATE_TRUNC()`        | Truncate timestamp to a chosen unit      |
| `DATE_FORMAT()`       | Format a date/timestamp                  |
| `TO_DATE()`           | Convert text into a date                 |
| `TO_TIMESTAMP()`      | Convert compatible text into a timestamp |
| `LAST_DAY()`          | Last day of the month                    |
| `ADD_MONTHS()`        | Add calendar months                      |

### **14.2 Lab: Date extraction**

```
SELECT
    employee_name,
    joining_date,
    YEAR(joining_date) AS joining_year,
    MONTH(joining_date) AS joining_month,
    DAY(joining_date) AS joining_day
FROM module3_employees
WHERE employee_id IN (1, 3)
ORDER BY employee_id;
```

Expected output:

| employee_name | joining_date | joining_year | joining_month | joining_day |
| ------------- | ------------ | ------------ | ------------- | ----------- |
| Asha          | 2026-01-10   | 2026         | 1             | 10          |
| Neha          | 2026-03-15   | 2026         | 3             | 15          |

### **14.3 Lab: DATE_ADD and DATEDIFF**

```
SELECT
    employee_name,
    joining_date,
    DATE_ADD(joining_date, 30) AS probation_review_date,
    DATEDIFF(
        DATE '2026-06-01',
        joining_date
    ) AS days_since_joining
FROM module3_employees
WHERE employee_id IN (1, 2)
ORDER BY employee_id;
```

Expected output:

| employee_name | joining_date | probation_review_date | days_since_joining |
| ------------- | ------------ | --------------------- | ------------------ |
| Asha          | 2026-01-10   | 2026-02-09            | 142                |
| Rahul         | 2026-02-12   | 2026-03-14            | 109                |

We have used a fixed reference date so the results remain reproducible.

### **14.4 Lab: Monthly employee joining count**

```
SELECT
    DATE_FORMAT(joining_date, 'yyyy-MM') AS joining_month,
    COUNT(*) AS employee_count
FROM module3_employees
GROUP BY DATE_FORMAT(joining_date, 'yyyy-MM')
ORDER BY joining_month;
```

Expected output:

| joining_month | employee_count |
| ------------- | -------------- |
| 2026-01       | 1              |
| 2026-02       | 1              |
| 2026-03       | 2              |
| 2026-04       | 2              |
| 2026-05       | 2              |

Employee joinings by month

Expected output from the monthly joining query

0123JanFebMarAprMay

### **14.5 Lab: Date truncation**

```
SELECT
    DATE_TRUNC(
        'MONTH',
        TIMESTAMP '2026-11-24 14:35:20'
    ) AS month_start;
```

Expected output:

```
2026-11-01 00:00:00
```

**Explanation:** `DATE_TRUNC` resets smaller timestamp components to the beginning of the requested time unit.

Timestamp display can be influenced by session time-zone configuration.

### **14.6 Use case: Incremental data ingestion**

A data pipeline may process records belonging to a particular processing window.

```
SELECT *
FROM module3_employees
WHERE joining_date >= DATE '2026-04-01'
  AND joining_date < DATE '2026-05-01';
```

Expected employees: Priya and Karan.

The half-open interval pattern (`>= start` and `< next_boundary`) is particularly useful when filtering timestamp columns.

## **15. CASE WHEN Conditional Logic**

### **15.1 What is CASE WHEN?**

`CASE` is a SQL expression used to implement conditional logic.

It resembles `if / else if / else` in programming languages.

Basic syntax:

```
CASE
    WHEN condition1 THEN result1
    WHEN condition2 THEN result2
    ELSE default_result
END
```

There are two common forms:

- Searched `CASE` — evaluates Boolean conditions.
- Simple `CASE` — compares one expression to alternative values.

### **15.2 Lab: Salary classification**

```
SELECT
    employee_name,
    salary,
    CASE
        WHEN salary >= 80000 THEN 'High'
        WHEN salary >= 60000 THEN 'Medium'
        ELSE 'Low'
    END AS salary_category
FROM module3_employees
ORDER BY salary DESC;
```

Expected output:

| employee_name | salary | salary_category |
| ------------- | ------ | --------------- |
| Karan         | 90000  | High            |
| Neha          | 75000  | Medium          |
| Meera         | 65000  | Medium          |
| Asha          | 60000  | Medium          |
| Amit          | 55000  | Low             |
| Priya         | 50000  | Low             |
| Rahul         | 45000  | Low             |
| Rohan         | 40000  | Low             |

### **15.3 Lab: Salary increment calculation**

Suppose an organization wants to offer:

- 15% increments to employees earning below ₹50,000
- 10% to employees earning from ₹50,000 to below ₹70,000
- 5% to employees earning ₹70,000 or above

```
SELECT
    employee_name,
    salary,
    CASE
        WHEN salary < 50000 THEN 15
        WHEN salary < 70000 THEN 10
        ELSE 5
    END AS increment_percent,

    CAST(
        salary * (
            1 + CASE
                    WHEN salary < 50000 THEN 0.15
                    WHEN salary < 70000 THEN 0.10
                    ELSE 0.05
                END
        )
        AS DECIMAL(12, 2)
    ) AS revised_salary
FROM module3_employees
ORDER BY salary;
```

Expected output:

| employee_name | salary | increment_percent | revised_salary |
| ------------- | ------ | ----------------- | -------------- |
| Rohan         | 40000  | 15                | 46000.00       |
| Rahul         | 45000  | 15                | 51750.00       |
| Priya         | 50000  | 10                | 55000.00       |
| Amit          | 55000  | 10                | 60500.00       |
| Asha          | 60000  | 10                | 66000.00       |
| Meera         | 65000  | 10                | 71500.00       |
| Neha          | 75000  | 5                 | 78750.00       |
| Karan         | 90000  | 5                 | 94500.00       |

**Explanation:** This query calculates projected salaries without changing the source table.

To persist the result, you would need an appropriate INSERT, UPDATE, MERGE, or table-creation operation.

### **15.4 CASE with aggregate functions**

```
SELECT
    SUM(
        CASE WHEN salary >= 60000
             THEN 1 ELSE 0 END
    ) AS employees_60k_or_above,

    SUM(
        CASE WHEN salary < 60000
             THEN 1 ELSE 0 END
    ) AS employees_below_60k
FROM module3_employees;
```

Expected output:

| employees_60k_or_above | employees_below_60k |
| ---------------------- | ------------------- |
| 4                      | 4                   |

**Production use cases:** Customer segmentation, order status mapping, risk scoring, conditional billing, and SLA classification.

## **16. Views vs. Tables**

### **16.1 What is a table?**

A table is a catalog object representing stored or otherwise registered data.

In Databricks, a managed Delta table generally has data files and metadata describing its schema, location, and transaction state.

A standard table query reads the data represented by that table.

### **16.2 What is a SQL view?**

A SQL view stores a reusable query definition.

When the view is queried, its definition is resolved and executed against its underlying sources, subject to the applicable semantics and permissions.

### **16.3 Lab: Creating a permanent view**

```
CREATE OR REPLACE VIEW module3_it_employees AS
SELECT
    employee_id,
    employee_name,
    department,
    salary
FROM module3_employees
WHERE department = 'IT';
```

Query:

```
SELECT employee_name, salary
FROM module3_it_employees
ORDER BY salary DESC;
```

Expected output:

| employee_name | salary |
| ------------- | ------ |
| Karan         | 90000  |
| Neha          | 75000  |
| Asha          | 60000  |

### **16.4 Table vs. view vs. materialized view**

| Feature               | Table                                                | Standard view                        | Materialized view                       |
| --------------------- | ---------------------------------------------------- | ------------------------------------ | --------------------------------------- |
| Stores data/result    | Yes, for a managed physical table                    | No separate result copy              | Yes, materialized result                |
| Stores SQL definition | May be created from SQL                              | Yes                                  | Yes                                     |
| Data refresh          | Changes through writes or source-specific mechanisms | Reads underlying sources on query    | Managed refresh process                 |
| Storage cost          | Yes                                                  | Minimal view metadata                | Yes                                     |
| Query performance     | Depends on data and plan                             | Depends on underlying query          | Can improve repeated analytical queries |
| Use case              | Main data storage                                    | Reusable logic and controlled access | Repeated expensive calculations         |

Databricks supports materialized views in appropriate SQL and pipeline environments. They are useful when recurring analytical queries would otherwise repeat expensive aggregations.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on Google Cloud+1



### **16.5 Lab: Materialized view concept**

An example for a supported Databricks SQL warehouse:

```
CREATE MATERIALIZED VIEW module3_department_summary
AS
SELECT
    department,
    COUNT(*) AS employee_count,
    SUM(salary) AS total_salary,
    AVG(salary) AS avg_salary
FROM module3_employees
GROUP BY department;
```

Expected data:

| department | employee_count | total_salary | avg_salary |
| ---------- | -------------- | ------------ | ---------- |
| Finance    | 2              | 120000       | 60000.0    |
| HR         | 2              | 95000        | 47500.0    |
| IT         | 3              | 225000       | 75000.0    |
| Sales      | 1              | 40000        | 40000.0    |

To request a refresh where supported:

```
REFRESH MATERIALIZED VIEW module3_department_summary;
```

Materialized-view creation and refresh require a supported compute environment and appropriate Unity Catalog privileges. They may not be available in every Databricks Free Edition workspace.

**Use case:** A sales dashboard frequently requests aggregate revenue over billions of transaction records. A materialized view can reduce repeated computation and improve dashboard responsiveness.

## **17. Understanding Query Execution Plans Using EXPLAIN**

### **17.1 Why EXPLAIN is important**

Writing valid SQL does not guarantee efficient SQL.

Two queries can return identical results but have very different performance characteristics.

`EXPLAIN` helps data engineers understand how Spark plans to execute a query.

### **17.2 EXPLAIN syntax**

```
EXPLAIN
SELECT *
FROM module3_employees
WHERE salary > 50000;
```

Common modes:

```
EXPLAIN EXTENDED
SELECT * FROM module3_employees;
```

```
EXPLAIN FORMATTED
SELECT * FROM module3_employees;
```

```
EXPLAIN COST
SELECT * FROM module3_employees;
```

```
EXPLAIN CODEGEN
SELECT * FROM module3_employees;
```

The supported modes include `EXTENDED`, `FORMATTED`, `COST`, and `CODEGEN`. The details shown vary by execution engine and runtime.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



### **17.3 Understanding a typical plan**

Run:

```
EXPLAIN FORMATTED
SELECT
    department,
    COUNT(*) AS employee_count
FROM module3_employees
GROUP BY department;
```

A simplified, representative Spark physical plan:

```
AdaptiveSparkPlan
  HashAggregate
    Exchange
      HashAggregate
        Scan delta table
```

Conceptual execution of distributed GROUP BY aggregation. An actual plan may differ, including with Photon.

### **17.4 What the operators mean**

**Scan:** Reads underlying records from the Delta table.

**Partial HashAggregate:** Performs local aggregation within partitions, reducing the amount of data that may need to be shuffled.

**Exchange:** Redistributes data across partitions, often over the network.

**Final HashAggregate:** Combines local results to produce the final aggregated output.

### **17.5 Important operators to recognize**

| Operator          | Meaning                      | Performance consideration                     |
| ----------------- | ---------------------------- | --------------------------------------------- |
| Scan              | Read source records          | Check data volume and filters                 |
| Filter            | Remove unwanted rows         | Can reduce work early                         |
| Project           | Select or compute columns    | Look for unnecessary columns                  |
| Exchange          | Repartition or shuffle data  | Can cause network and disk overhead           |
| Sort              | Order records                | May require expensive sorting                 |
| HashAggregate     | Aggregate rows               | Can involve hash-table memory and shuffle     |
| BroadcastHashJoin | Broadcast one join input     | Efficient when one side is sufficiently small |
| SortMergeJoin     | Join sorted partitions       | Often involves shuffle and sorting            |
| AdaptiveSparkPlan | Adaptive execution framework | May change plan based on runtime statistics   |

### **17.6 EXPLAIN and predicate pushdown**

```
EXPLAIN FORMATTED
SELECT employee_name
FROM module3_employees
WHERE department = 'IT';
```

Look for evidence of filter pushdown or pruning in the scan and filter operators.

The exact scan output depends on whether Delta, Photon, and other optimization capabilities are being used.

### **17.7 EXPLAIN and joins**

```
EXPLAIN FORMATTED
SELECT
    e.employee_name,
    d.budget
FROM module3_employees e
JOIN department_budget d
  ON e.department = d.department;
```

Inspect the join strategy.

Depending on statistics, settings, and execution environment, Spark might select a broadcast join or another join strategy.

To explicitly provide a broadcast hint:

```
EXPLAIN FORMATTED
SELECT /*+ BROADCAST(d) */
    e.employee_name,
    d.budget
FROM module3_employees e
JOIN department_budget d
  ON e.department = d.department;
```

A hint can influence planning but is not an unconditional guarantee of a particular physical operator.

### **17.8 Real-world performance troubleshooting**

Suppose a query processing 100 million orders takes several minutes.

A practical investigation includes checking:

- The amount of data scanned.
- Whether partition pruning and filters are effective.
- Whether unnecessary columns are being read.
- Whether joins involve heavy shuffling.
- Whether one partition is processing far more data than others.
- Whether spilling to disk or memory pressure occurs.
- Whether statistics and physical data layout are appropriate.

For actual runtime measurements, inspect the **Databricks Query Profile** or Spark execution metrics rather than relying on `EXPLAIN` alone.

**Interview question:** What is the difference between `EXPLAIN` and Query Profile?

**Answer:** EXPLAIN describes the planned execution of a query, while the runtime query profile provides observed execution metrics such as time spent, rows processed, and expensive stages or operators.

## **18. Databricks SQL Dashboards and Visualizations**

### **18.1 What is a Databricks SQL dashboard?**

A dashboard is a collection of visualizations, metrics, and explanatory content that helps users understand business performance.

Databricks provides **AI/BI dashboards**, which allow users to build interactive reports backed by SQL datasets.

They support charts, tables, KPI indicators, filters, publishing, sharing, and scheduled updates.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



### **18.2 Common visualization types**

| Visualization | Best use case                             |
| ------------- | ----------------------------------------- |
| Bar chart     | Compare departmental salaries             |
| Line chart    | Revenue trends over time                  |
| Pie chart     | Distribution across categories            |
| KPI / Counter | Total revenue or employee count           |
| Table         | Detailed operational reports              |
| Scatter plot  | Relationship between two numeric measures |
| Heatmap       | Patterns across two dimensions            |
| Area chart    | Volume or trends over time                |

### **18.3 Practical lab: Employee analytics dashboard**

We will build a dashboard with employee counts, average salaries, departmental salary expenses, and monthly joinings.

#### **Dataset query 1: Department summary**

```
SELECT
    department,
    COUNT(*) AS employee_count,
    SUM(salary) AS total_salary,
    AVG(salary) AS average_salary
FROM module3_employees
GROUP BY department
ORDER BY department;
```

Expected output:

| Department | Employees | Total salary | Average salary |
| ---------- | --------- | ------------ | -------------- |
| Finance    | 2         | 120,000      | 60,000         |
| HR         | 2         | 95,000       | 47,500         |
| IT         | 3         | 225,000      | 75,000         |
| Sales      | 1         | 40,000       | 40,000         |

Total salary by department

Expected visualization from the department summary dataset

₹0₹60,000₹120,000₹180,000₹240,000FinanceHRITSales

#### **Dataset query 2: KPI metrics**

```
SELECT
    COUNT(*) AS total_employees,
    COUNT(DISTINCT department) AS departments,
    SUM(salary) AS total_salary,
    AVG(salary) AS average_salary
FROM module3_employees;
```

Total employees

# **8**

Departments

# **4**

Total salary

# **₹4,80,000**

Average salary

# **₹60,000**

#### **Dataset query 3: Employee distribution**

```
SELECT
    department,
    COUNT(*) AS employee_count
FROM module3_employees
GROUP BY department;
```

Employee distribution by department

Finance

HR

IT

Sales

### **18.4 How to create the dashboard in Databricks**

1. Open your Databricks workspace and navigate to **Dashboards**.
2. Select **Create dashboard** and enter a title, such as `Employee Analytics - Module 3`.
3. Open the **Data** tab and create a SQL dataset using `module3_employees`.
4. Enter the department summary query and execute it using a SQL warehouse.
5. Open the dashboard canvas and add a bar chart using `department` as the category and `total_salary` as the measure.
6. Add a KPI/counter for `total_employees`, another chart for department distribution, and a table for details.
7. Preview the dashboard, configure filters if needed, and select **Publish** when it is ready for consumption.
8. Configure appropriate sharing permissions and optional scheduled dashboard updates.

Databricks' current dashboard documentation describes dataset management, publishing, sharing, and scheduled updates.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+2



### **18.5 Practical dashboard use cases**

An e-commerce organization might create dashboards for daily orders, gross revenue, payment failure rates, delivery success rates, seller performance, and inventory availability.

The underlying SQL transforms raw transactional data into business metrics. The dashboard then presents those metrics in a form that business users can interpret without writing queries themselves.

## **19. Connecting Databricks to Power BI and Tableau**

### **19.1 Why connect BI tools to Databricks?**

Databricks provides scalable data storage, transformation, governance, and SQL processing.

Power BI and Tableau provide rich report authoring, interactive visualizations, and business-user reporting experiences.

The integration allows users to analyze Databricks data directly rather than manually exporting datasets as CSV files.

### **19.2 Connection architecture**

### **19.3 Prerequisites**

You generally need:

- A Databricks workspace.
- A suitable SQL warehouse.
- Permissions to use the warehouse and query the target data.
- Power BI Desktop or Tableau Desktop.
- Appropriate authentication credentials or OAuth authorization.
- The warehouse server hostname and HTTP path.

You can retrieve connection details from the SQL warehouse's connection information in Databricks.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



### **19.4 Connect Power BI Desktop**

1. Open Power BI Desktop and select **Get Data**.
2. Search for the supported **Databricks** connector appropriate to your cloud environment.
3. Enter the Databricks SQL warehouse **Server Hostname**.
4. Enter the **HTTP Path** associated with the SQL warehouse.
5. Authenticate using a supported organizational or OAuth method.
6. Choose the catalog, schema, and `module3_employees` table.
7. Select **Import** or **DirectQuery**, where the connector supports that option.
8. Create visualizations and publish the report to the Power BI service as needed.

Current Databricks documentation supports connecting Power BI Desktop through manual configuration or Partner Connect, with authentication choices dependent on environment.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



### **19.5 Import vs. DirectQuery in Power BI**

| Import                                         | DirectQuery                                                     |
| ---------------------------------------------- | --------------------------------------------------------------- |
| Data is loaded into a Power BI semantic model  | Queries are issued against the data source                      |
| Fast interactive performance for many reports  | More direct access to current source data                       |
| Requires refresh for imported changes          | Queries source during interactions, subject to caching          |
| Requires data-model storage capacity           | Relies on Databricks SQL execution                              |
| Useful for smaller aggregated reporting models | Useful when data volume or freshness favors querying the source |

**Example:** A monthly HR report could use Import mode, while a frequently updated operational report may benefit from DirectQuery if the underlying query performance and freshness requirements justify it.

### **19.6 Connect Tableau to Databricks**

1. Open Tableau Desktop.
2. Select the Databricks connector from the connection options.
3. Enter the SQL warehouse Server Hostname and HTTP Path.
4. Authenticate with a supported mechanism.
5. Select the catalog, schema, and table.
6. Build Tableau worksheets and dashboards.
7. Publish to Tableau Server or Tableau Cloud when required.

For Tableau, Databricks also provides Partner Connect workflows. Tableau Cloud may have additional OAuth and connectivity requirements.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **19.7 Best practices for production BI integration**

**Use curated Gold-layer datasets.** BI dashboards generally should not need to reconstruct complicated joins and transformations from raw Bronze data.

**Avoid excessive data movement.** Choose Import, DirectQuery, or Tableau live/extract models according to data volume and freshness requirements.

**Use secure authentication.** Prefer supported OAuth-based integration over embedding long-lived credentials in dashboards or scripts.

**Control compute costs.** BI tools can generate many queries from interactive filters, so SQL warehouse sizing, query optimization, and efficient datasets matter.

**Enforce governance.** Use Unity Catalog privileges, and where appropriate row filters, column masks, and governed views to control what users can access.

## **20. End-to-End Databricks SQL Project**

### **Project: Retail Sales Analytics**

This project combines many Module 3 concepts into one practical example.

**Business problem:** A retail business wants to understand which products generate the most revenue, which customers spend the most, and how sales change over time.

### **20.1 Create a sales table**

Run in a Databricks SQL notebook or SQL editor, with a writable catalog and schema selected.

```
CREATE OR REPLACE TABLE module3_retail_sales (
    order_id INT,
    customer_id INT,
    customer_name STRING,
    product STRING,
    category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    order_date DATE,
    order_status STRING
)
USING DELTA;
```

### **20.2 Insert sample data**

```
INSERT INTO module3_retail_sales VALUES
(1, 101, 'Asha',  'Laptop',   'Electronics', 1, 60000, DATE '2026-11-01', 'Delivered'),
(2, 102, 'Rahul', 'Mouse',    'Electronics', 2,  1000, DATE '2026-11-02', 'Delivered'),
(3, 103, 'Neha',  'Chair',    'Furniture',   2,  5000, DATE '2026-11-02', 'Delivered'),
(4, 101, 'Asha',  'Keyboard', 'Electronics', 1,  3000, DATE '2026-11-03', 'Cancelled'),
(5, 104, 'Amit',  'Desk',     'Furniture',   1, 12000, DATE '2026-11-03', 'Delivered'),
(6, 102, 'Rahul', 'Monitor',  'Electronics', 1, 15000, DATE '2026-11-04', 'Delivered');
```

Verify:

```
SELECT *
FROM module3_retail_sales
ORDER BY order_id;
```

Expected: Six order records.

### **20.3 Query 1: Revenue calculation**

Revenue is calculated as:

\\[ \text{Revenue}=\sum(\text{Quantity}\times\text{Unit Price}) \\]

Here we exclude cancelled orders.

```
SELECT
    SUM(quantity * unit_price) AS total_revenue
FROM module3_retail_sales
WHERE order_status = 'Delivered';
```

Expected output:

| total_revenue |
| ------------- |
| 99000.00      |

**Explanation:** The five delivered orders contribute ₹60,000 + ₹2,000 + ₹10,000 + ₹12,000 + ₹15,000 = ₹99,000.

### **20.4 Query 2: Revenue by category**

```
SELECT
    category,
    SUM(quantity * unit_price) AS revenue
FROM module3_retail_sales
WHERE order_status = 'Delivered'
GROUP BY category
ORDER BY revenue DESC;
```

Expected output:

| category    | revenue  |
| ----------- | -------- |
| Electronics | 77000.00 |
| Furniture   | 22000.00 |

Delivered order revenue by category

₹0₹25K₹50K₹75K₹100KElectronicsFurniture

### **20.5 Query 3: Rank customers by spending**

```
WITH customer_revenue AS (
    SELECT
        customer_id,
        customer_name,
        SUM(quantity * unit_price) AS total_spent
    FROM module3_retail_sales
    WHERE order_status = 'Delivered'
    GROUP BY customer_id, customer_name
)

SELECT
    customer_name,
    total_spent,
    DENSE_RANK() OVER (
        ORDER BY total_spent DESC
    ) AS spending_rank
FROM customer_revenue
ORDER BY spending_rank, customer_name;
```

Expected output:

| customer_name | total_spent | spending_rank |
| ------------- | ----------- | ------------- |
| Asha          | 60000.00    | 1             |
| Rahul         | 17000.00    | 2             |
| Amit          | 12000.00    | 3             |
| Neha          | 10000.00    | 4             |

**Concepts used:** CTE, `SUM`, `GROUP BY`, filtering, and `DENSE_RANK`.

### **20.6 Query 4: Daily revenue trend**

```
SELECT
    order_date,
    SUM(quantity * unit_price) AS daily_revenue
FROM module3_retail_sales
WHERE order_status = 'Delivered'
GROUP BY order_date
ORDER BY order_date;
```

Expected output:

| order_date | daily_revenue |
| ---------- | ------------- |
| 2026-11-01 | 60000.00      |
| 2026-11-02 | 12000.00      |
| 2026-11-03 | 12000.00      |
| 2026-11-04 | 15000.00      |

Daily delivered revenue

Sales trend across four days

₹0₹20K₹40K₹60K₹80KNov 1Nov 2Nov 3Nov 4

### **20.7 Query 5: Running total revenue**

```
WITH daily_sales AS (
    SELECT
        order_date,
        SUM(quantity * unit_price) AS daily_revenue
    FROM module3_retail_sales
    WHERE order_status = 'Delivered'
    GROUP BY order_date
)

SELECT
    order_date,
    daily_revenue,
    SUM(daily_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING
                 AND CURRENT ROW
    ) AS cumulative_revenue
FROM daily_sales
ORDER BY order_date;
```

Expected output:

| order_date | daily_revenue | cumulative_revenue |
| ---------- | ------------- | ------------------ |
| 2026-11-01 | 60000.00      | 60000.00           |
| 2026-11-02 | 12000.00      | 72000.00           |
| 2026-11-03 | 12000.00      | 84000.00           |
| 2026-11-04 | 15000.00      | 99000.00           |

This combines CTEs, aggregate functions, and SQL windows.

### **20.8 Query 6: Order status classification**

```
SELECT
    order_id,
    customer_name,
    quantity * unit_price AS order_amount,
    CASE
        WHEN order_status = 'Cancelled'
            THEN 'Excluded from Revenue'
        WHEN quantity * unit_price >= 20000
            THEN 'High Value'
        WHEN quantity * unit_price >= 10000
            THEN 'Medium Value'
        ELSE 'Low Value'
    END AS order_category
FROM module3_retail_sales
ORDER BY order_id;
```

Expected output:

| order_id | customer_name | order_amount | order_category        |
| -------- | ------------- | ------------ | --------------------- |
| 1        | Asha          | 60000.00     | High Value            |
| 2        | Rahul         | 2000.00      | Low Value             |
| 3        | Neha          | 10000.00     | Medium Value          |
| 4        | Asha          | 3000.00      | Excluded from Revenue |
| 5        | Amit          | 12000.00     | Medium Value          |
| 6        | Rahul         | 15000.00     | Medium Value          |

### **20.9 Create a reusable reporting view**

```
CREATE OR REPLACE VIEW module3_sales_reporting AS
SELECT
    order_id,
    customer_id,
    customer_name,
    product,
    category,
    quantity,
    unit_price,
    quantity * unit_price AS revenue,
    order_date
FROM module3_retail_sales
WHERE order_status = 'Delivered';
```

This view becomes a reusable reporting layer.

```
SELECT
    category,
    SUM(revenue) AS total_revenue
FROM module3_sales_reporting
GROUP BY category;
```

It should return the same category revenue values calculated earlier.

**End-to-end production design:** A real implementation would ingest raw order events into Bronze Delta tables, standardize and deduplicate them in Silver, generate curated revenue datasets in Gold, and connect Gold tables or views to dashboards and BI tools.

## **21. Spark SQL Performance Best Practices**

| Practice                              | Why it matters                                 |
| ------------------------------------- | ---------------------------------------------- |
| Select only required columns          | Reduces unnecessary data processing and I/O    |
| Filter early where possible           | Reduces records passed to later operators      |
| Avoid unnecessary `DISTINCT`          | Can introduce expensive aggregation/shuffling  |
| Use appropriate join strategies       | Reduces movement of data                       |
| Check data skew                       | Prevents disproportionately slow tasks         |
| Inspect `EXPLAIN` and query profiles  | Reveals expensive stages and scans             |
| Use appropriate Delta table layout    | Improves relevant scan and pruning efficiency  |
| Maintain useful table statistics      | Helps cost-based optimization                  |
| Avoid repeated expensive calculations | Consider reusable tables or materialized views |
| Use SQL warehouses sized for workload | Balances latency, concurrency, and cost        |

### **Example: Avoid unnecessarily reading all columns**

Less selective:

```
SELECT *
FROM module3_retail_sales;
```

More selective:

```
SELECT order_id, order_date, quantity, unit_price
FROM module3_retail_sales
WHERE order_status = 'Delivered';
```

The second query requests fewer columns and applies a filter. It can potentially reduce work, particularly on large columnar datasets.

### **Example: Use broadcast joins appropriately**

```
SELECT /*+ BROADCAST(d) */
    e.employee_name,
    e.salary,
    d.budget
FROM module3_employees e
JOIN department_budget d
ON e.department = d.department;
```

Broadcasting a small dimension table can avoid a large shuffle. Broadcasting a large dataset, however, can increase memory pressure and lead to poor performance or failures.

## **22. Module 3 — Interview Preparation**

18 common interview questions

Show all answers

1\. What is Spark SQL?

2\. What is Catalyst?

3\. What is Tungsten?

4\. Does SQL run faster than the DataFrame API?

5\. Are temp views physically stored?

6\. Can a temp view be used in a different SQL warehouse session?

7\. Managed vs. external tables?

8\. CTE vs. subquery?

9\. WHERE vs. HAVING?

10\. GROUP BY vs. PARTITION BY?

11\. RANK vs. DENSE_RANK?

12\. LAG vs. LEAD?

13\. What is MERGE INTO?

14\. View vs. materialized view?

15\. What does Exchange mean in EXPLAIN?

16\. What is predicate pushdown?

17\. Why use a SQL warehouse?

18\. Import vs. DirectQuery in Power BI?

## **23. Hands-on Exercises**

After completing the notebook examples, try these tasks without referring to the solutions.

**Practice checklist**

0/10 completed

Find the second-highest salary in each department using DENSE_RANK.

Calculate how much each employee's salary differs from the department average.

Create a permanent view containing only HR and Finance employees.

List employees who joined after March 31, 2026.

Calculate the percentage contribution of each department to the total salary expenditure.

Find customers whose delivered spending exceeds ₹15,000.

Calculate the daily number of delivered orders.

Find the highest-value delivered order for every category.

Create a dashboard containing revenue, daily sales, revenue by category, and top customers.

Use EXPLAIN FORMATTED to compare a GROUP BY query and a window aggregation.

Reset checklist

## **24. Module 3 Revision Summary**

| Topic                       | Key takeaway                                              |
| --------------------------- | --------------------------------------------------------- |
| Spark SQL                   | Distributed structured-data processing using SQL          |
| Catalyst                    | Logical and physical query optimization                   |
| Tungsten                    | Efficient execution and memory/code-generation techniques |
| Temp views                  | Session-scoped logical views                              |
| Global temp views           | Legacy application-scoped views; not serverless-supported |
| `spark.sql()`               | Execute SQL using a Spark session                         |
| SQL editor                  | Browser-based Databricks SQL development                  |
| Managed tables              | Platform-managed data storage and lifecycle               |
| External tables             | Externally managed storage registered in the catalog      |
| CREATE/INSERT/UPDATE/DELETE | Core SQL DDL and DML operations                           |
| MERGE                       | Conditional updates and inserts                           |
| CTE                         | Named intermediate query within a statement               |
| Subquery                    | Nested SQL query                                          |
| Aggregates                  | Summarize groups of records                               |
| Window functions            | Analyze related rows while preserving row-level detail    |
| String functions            | Clean, transform, and parse text                          |
| Date functions              | Analyze temporal data                                     |
| CASE WHEN                   | Apply conditional SQL logic                               |
| Views                       | Reusable logical query definitions                        |
| EXPLAIN                     | Understand query execution plans                          |
| AI/BI dashboards            | Visualize and share SQL-based analytics                   |
| Power BI / Tableau          | External BI reporting against Databricks                  |

### **Official reference documentation**

For additional syntax details and interface changes, refer to the official Databricks documentation:

- [Databricks SQL language reference](https://docs.databricks.com/aws/en/sql/language-manual?utm_source=chatgpt.com)
- [Databricks SQL editor guide](https://docs.databricks.com/aws/en/sql/user/sql-editor/?utm_source=chatgpt.com)
- [Databricks AI/BI dashboards](https://docs.databricks.com/aws/en/dashboards/?utm_source=chatgpt.com)
- [Power BI integration](https://docs.databricks.com/aws/en/partners/bi/power-bi/?utm_source=chatgpt.com)
- [Tableau integration](https://docs.databricks.com/aws/en/partners/bi/tableau?utm_source=chatgpt.com)

**Module 3 learning outcome:** By completing these examples, you should be able to use Spark SQL and Databricks SQL to query and transform structured data, work with Delta tables, apply aggregations and advanced analytical SQL, investigate query execution, and build datasets suitable for business dashboards.

For a data engineering role, prioritize hands-on mastery of **CTEs, joins, window functions, Delta DML and MERGE, query execution plans, and SQL performance optimization**. These are especially important for building and maintaining production Databricks pipelines.
