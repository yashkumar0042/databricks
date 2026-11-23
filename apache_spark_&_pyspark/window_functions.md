# **PySpark Window Functions in Databricks — Complete Notes & Practical Labs**

**Topics:** `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`

Window functions are among the most important concepts in PySpark, especially for **Data Engineering, Databricks, ETL pipelines, analytics, and SQL interviews**.

In this tutorial, you'll learn definitions, syntax, differences, real-world use cases, and ready-to-execute Databricks notebook examples.

## **1. What is a Window Function in PySpark?**

A **Window Function** performs calculations across a group of related rows without collapsing them into a single row.

Unlike `groupBy()`, which combines multiple rows into summary results, window functions preserve individual records while adding calculated values.

For example, consider an employee salary dataset:

| Department | Employee | Salary |
| ---------- | -------- | ------ |
| IT         | Amit     | 90,000 |
| IT         | Rahul    | 80,000 |
| IT         | Priya    | 80,000 |
| HR         | Neha     | 70,000 |
| HR         | Karan    | 60,000 |

With window functions, we can calculate the rank of employees by salary within each department.

| Department | Employee | Salary | Rank |
| ---------- | -------- | ------ | ---- |
| IT         | Amit     | 90,000 | 1    |
| IT         | Rahul    | 80,000 | 2    |
| IT         | Priya    | 80,000 | 2    |
| HR         | Neha     | 70,000 | 1    |
| HR         | Karan    | 60,000 | 2    |

Notice that the original five records remain unchanged in number.

## **2. How Window Functions Work**

Each window processes related rows and returns a result for every input row.

### **General syntax**

```
from pyspark.sql import Windowfrom pyspark.sql import functions as Fwindow_spec = (    Window    .partitionBy("department")    .orderBy(F.col("salary").desc()))df.withColumn(    "rank",    F.rank().over(window_spec))
```

**Key components:**

- `Window`: Creates a window specification.
- `partitionBy()`: Divides data into logical groups.
- `orderBy()`: Defines the order of records inside each group.
- `over()`: Applies the window function using the defined specification.
- `withColumn()`: Creates a new column containing the calculated result.

`partitionBy()` is optional. Without it, ranking functions operate across the entire dataset, which can be expensive at scale.

# **3. Databricks Notebook — Create a Practice Dataset**

Execute the following cells in order in a **Python Databricks notebook**. All examples use in-memory data, so you don't need to upload files or configure storage.

CELL 1 — IMPORT LIBRARIES

```
from pyspark.sql import functions as Ffrom pyspark.sql import Windowfrom pyspark.sql.types import (    StructType,    StructField,    IntegerType,    StringType,    DoubleType)
```

CELL 2 — CREATE EMPLOYEE DATAFRAME

```
employee_data = [    (101, "Amit",   "IT",      90000.0),    (102, "Rahul",  "IT",      80000.0),    (103, "Priya",  "IT",      80000.0),    (104, "Sneha",  "IT",      65000.0),    (105, "Neha",   "HR",      70000.0),    (106, "Karan",  "HR",      60000.0),    (107, "Pooja",  "HR",      60000.0),    (108, "Rohan",  "Finance", 95000.0),    (109, "Meena",  "Finance", 85000.0),    (110, "Vikas",  "Finance", 85000.0)]employee_schema = StructType([    StructField("employee_id", IntegerType(), False),    StructField("employee_name", StringType(), False),    StructField("department", StringType(), False),    StructField("salary", DoubleType(), False)])employee_df = spark.createDataFrame(    employee_data,    employee_schema)display(employee_df.orderBy("department", F.col("salary").desc()))
```

This dataset deliberately contains duplicate salaries so you can understand the differences between ranking functions.

# **4. row_number() — Unique Sequential Row Numbers**

## **Definition**

`row_number()` assigns a unique, sequential number to every row within each partition.

The numbering starts at `1` and increases by `1`, regardless of duplicate values.

### **Syntax**

```
F.row_number().over(window_spec)
```

### **Databricks example**

```
window_spec = (    Window    .partitionBy("department")    .orderBy(F.col("salary").desc()))df_row_number = employee_df.withColumn(    "row_number",    F.row_number().over(window_spec))display(    df_row_number.orderBy(        "department", "row_number"    ))
```

### **Expected output (IT department)**

| Employee | Salary | row_number |
| -------- | ------ | ---------- |
| Amit     | 90,000 | 1          |
| Rahul    | 80,000 | 2          |
| Priya    | 80,000 | 3          |
| Sneha    | 65,000 | 4          |

**Important:** Rahul and Priya have identical salaries, but different row numbers. Because salary alone does not uniquely determine the order, their row numbers may swap between runs.

For deterministic results, add a unique tie-breaker:

```
window_spec = (    Window.partitionBy("department")    .orderBy(        F.col("salary").desc(),        F.col("employee_id").asc()    ))
```

### **Real-world use case: Remove duplicate records**

Suppose an ETL pipeline ingests multiple versions of a customer record. You want to keep the latest version of each customer.

```
customer_data = [    (1, "Amit",  "2026-01-10"),    (1, "Amit",  "2026-02-15"),    (2, "Neha",  "2026-01-12"),    (2, "Neha",  "2026-03-20"),    (3, "Rahul", "2026-02-01")]customer_df = (    spark.createDataFrame(        customer_data,        ["customer_id", "name", "updated_at"]    )    .withColumn(        "updated_at",        F.to_date("updated_at")    ))latest_window = (    Window.partitionBy("customer_id")    .orderBy(F.col("updated_at").desc()))latest_customers = (    customer_df    .withColumn(        "rn",        F.row_number().over(latest_window)    )    .filter(F.col("rn") == 1)    .drop("rn"))display(latest_customers.orderBy("customer_id"))
```

Expected result:

| customer_id | name  | updated_at |
| ----------- | ----- | ---------- |
| 1           | Amit  | 2026-02-15 |
| 2           | Neha  | 2026-03-20 |
| 3           | Rahul | 2026-02-01 |

This technique is commonly used for CDC deduplication, incremental pipelines, and selecting the latest records before a Delta Lake `MERGE`.

For production use, include a deterministic tie-breaker such as an event ID if multiple records can have identical update timestamps.

# **5. rank() — Ranking With Gaps**

## **Definition**

`rank()` assigns the same rank to rows that have identical ordering values.

However, when duplicate ranks exist, the next rank is skipped.

### **Databricks example**

```
rank_window = (    Window.partitionBy("department")    .orderBy(F.col("salary").desc()))df_rank = employee_df.withColumn(    "salary_rank",    F.rank().over(rank_window))display(    df_rank.orderBy(        "department",        F.col("salary").desc()    ))
```

### **Expected output (IT department)**

| Employee | Salary | rank |
| -------- | ------ | ---- |
| Amit     | 90,000 | 1    |
| Rahul    | 80,000 | 2    |
| Priya    | 80,000 | 2    |
| Sneha    | 65,000 | 4    |

Notice the ranks: **1, 2, 2, 4**.

Rank `3` is skipped because two employees share rank `2`.

### **Real-world use case: Competition leaderboard**

If two participants have the second-highest score, they both receive second place, and the next participant receives fourth place.

```
scores = [    ("Amit", 100),    ("Neha",  90),    ("Priya", 90),    ("Rahul", 80)]scores_df = spark.createDataFrame(    scores,    ["player", "score"])score_window = Window.orderBy(    F.col("score").desc())leaderboard = scores_df.withColumn(    "position",    F.rank().over(score_window))display(    leaderboard.orderBy("position", "player"))
```

Typical use cases include sports rankings, sales competitions, examination results, and employee performance rankings.

# **6. dense_rank() — Ranking Without Gaps**

## **Definition**

`dense_rank()` assigns the same rank to equal values but does not leave gaps after ties.

### **Databricks example**

```
dense_window = (    Window.partitionBy("department")    .orderBy(F.col("salary").desc()))df_dense_rank = employee_df.withColumn(    "dense_rank",    F.dense_rank().over(dense_window))display(    df_dense_rank.orderBy(        "department",        F.col("salary").desc()    ))
```

### **Expected output (IT department)**

| Employee | Salary | dense_rank |
| -------- | ------ | ---------- |
| Amit     | 90,000 | 1          |
| Rahul    | 80,000 | 2          |
| Priya    | 80,000 | 2          |
| Sneha    | 65,000 | 3          |

Ranks are **1, 2, 2, 3**.

No rank is skipped.

### **Real-world use case: Find the second-highest salary in every department**

```
salary_window = (    Window.partitionBy("department")    .orderBy(F.col("salary").desc()))second_highest_salary = (    employee_df    .withColumn(        "salary_rank",        F.dense_rank().over(salary_window)    )    .filter(F.col("salary_rank") == 2))display(    second_highest_salary.orderBy(        "department", "employee_id"    ))
```

Expected result:

| Department | Employee | Salary |
| ---------- | -------- | ------ |
| Finance    | Meena    | 85,000 |
| Finance    | Vikas    | 85,000 |
| HR         | Karan    | 60,000 |
| HR         | Pooja    | 60,000 |
| IT         | Rahul    | 80,000 |
| IT         | Priya    | 80,000 |

This returns every employee who earns the second-highest **distinct** salary within their department.

# **7. Difference Between row_number(), rank(), and dense_rank()**

The same salaries, three different ranking results

| **Salary** | **row_number** | **rank** | **dense_rank** |
| ---------- | -------------- | -------- | -------------- |
| 90,000     | 1              | 1        | 1              |
| 80,000     | 2              | 2        | 2              |
| 80,000     | 3              | 2        | 2              |
| 65,000     | 4              | 4        | 3              |

row_number

Unique sequence

rank

Ties + gaps

dense_rank

Ties, no gaps

### **Databricks code to compare all three**

```
compare_window = (    Window.partitionBy("department")    .orderBy(F.col("salary").desc()))comparison_df = (    employee_df    .withColumn(        "row_number",        F.row_number().over(compare_window)    )    .withColumn(        "rank",        F.rank().over(compare_window)    )    .withColumn(        "dense_rank",        F.dense_rank().over(compare_window)    ))display(    comparison_df.orderBy(        "department",        F.col("salary").desc(),        "employee_id"    ))
```

# **8. lag() — Access a Previous Row**

## **Definition**

`lag()` retrieves a value from a previous row within the same window partition.

It is commonly used for comparing current data with historical data.

**Syntax:**

```
F.lag(column, offset=1, default=None).over(window_spec)
```

Parameters:

- `column`: The column whose previous value you need.
- `offset`: Number of rows to look backward. Default is `1`.
- `default`: Value returned when the previous row doesn't exist. Default is `NULL`.
- `over()`: Applies the window specification.

## **Real-world example: Month-over-month sales growth**

Suppose you have monthly sales for different products and want to compare each month's sales with the previous month.

CELL 3 — CREATE SALES DATAFRAME

```
sales_data = [    ("Laptop", "2026-01", 100000),    ("Laptop", "2026-02", 120000),    ("Laptop", "2026-03", 150000),    ("Laptop", "2026-04", 140000),    ("Mobile", "2026-01",  50000),    ("Mobile", "2026-02",  65000),    ("Mobile", "2026-03",  80000),    ("Mobile", "2026-04",  90000)]sales_df = spark.createDataFrame(    sales_data,    ["product", "month", "sales"])display(sales_df.orderBy("product", "month"))
```

CELL 4 — CALCULATE PREVIOUS MONTH'S SALES

```
sales_window = (    Window.partitionBy("product")    .orderBy("month"))sales_lag_df = (    sales_df    .withColumn(        "previous_month_sales",        F.lag("sales", 1).over(sales_window)    ))display(sales_lag_df.orderBy("product", "month"))
```

### **Expected output (Laptop)**

| Month | Sales   | Previous month's sales |
| ----- | ------- | ---------------------- |
| Jan   | 100,000 | NULL                   |
| Feb   | 120,000 | 100,000                |
| Mar   | 150,000 | 120,000                |
| Apr   | 140,000 | 150,000                |

January returns `NULL` because there is no preceding month in the dataset.

## **Calculate month-over-month growth**

Formula:

\\[ \text{Growth \\%}=\frac{\text{Current Sales}-\text{Previous Sales}}{\text{Previous Sales}}\times100 \\]

```
sales_growth_df = (    sales_lag_df    .withColumn(        "sales_difference",        F.col("sales") - F.col("previous_month_sales")    )    .withColumn(        "growth_percentage",        F.when(            F.col("previous_month_sales") != 0,            F.round(                (                    (F.col("sales") -                     F.col("previous_month_sales"))                    / F.col("previous_month_sales")                ) * 100,                2            )        )    ))display(    sales_growth_df.orderBy("product", "month"))
```

### **Expected result (Laptop)**

| Month | Sales   | Previous | Difference | Growth % |
| ----- | ------- | -------- | ---------- | -------- |
| Jan   | 100,000 | NULL     | NULL       | NULL     |
| Feb   | 120,000 | 100,000  | +20,000    | 20.00    |
| Mar   | 150,000 | 120,000  | +30,000    | 25.00    |
| Apr   | 140,000 | 150,000  | -10,000    | -6.67    |

This is useful for revenue monitoring, customer activity trends, financial analytics, and monthly reporting.

**Important:** `lag()` retrieves the preceding row, not necessarily the preceding calendar month. If a month is missing, you must first create a complete monthly calendar or explicitly join the previous month's data.

# **9. lead() — Access the Next Row**

## **Definition**

`lead()` retrieves a value from a following row in the same partition.

It works in the opposite direction to `lag()`.

**Syntax:**

```
F.lead(column, offset=1, default=None).over(window_spec)
```

## **Example: Compare current sales with next month's sales**

CELL 5 — LEAD FUNCTION

```
lead_window = (    Window.partitionBy("product")    .orderBy("month"))sales_lead_df = (    sales_df    .withColumn(        "next_month_sales",        F.lead("sales", 1).over(lead_window)    ))display(sales_lead_df.orderBy("product", "month"))
```

### **Expected output (Laptop)**

| Month | Sales   | Next month's sales |
| ----- | ------- | ------------------ |
| Jan   | 100,000 | 120,000            |
| Feb   | 120,000 | 150,000            |
| Mar   | 150,000 | 140,000            |
| Apr   | 140,000 | NULL               |

For April, `lead()` returns `NULL` because no later row exists.

## **Using lag() and lead() together**

```
comparison_df = (    sales_df    .withColumn(        "previous_sales",        F.lag("sales", 1).over(sales_window)    )    .withColumn(        "current_sales",        F.col("sales")    )    .withColumn(        "next_sales",        F.lead("sales", 1).over(sales_window)    ))display(    comparison_df.select(        "product",        "month",        "previous_sales",        "current_sales",        "next_sales"    ).orderBy("product", "month"))
```

This allows you to view previous, current, and next values simultaneously.

# **10. Advanced Practical Example — Customer Purchase Analysis**

This is a particularly useful Data Engineering scenario.

**Business requirement:** For every customer transaction, calculate the previous purchase date, next purchase date, and number of days between purchases.

CELL 6 — CREATE TRANSACTIONS

```
transaction_data = [    (1, 101, "2026-01-01", 500),    (2, 101, "2026-01-10", 700),    (3, 101, "2026-01-25", 900),    (4, 102, "2026-02-01", 300),    (5, 102, "2026-02-15", 400),    (6, 102, "2026-03-05", 600)]transactions_df = (    spark.createDataFrame(        transaction_data,        [            "transaction_id",            "customer_id",            "purchase_date",            "amount"        ]    )    .withColumn(        "purchase_date",        F.to_date("purchase_date")    ))display(transactions_df)
```

CELL 7 — CUSTOMER PURCHASE HISTORY

```
customer_window = (    Window.partitionBy("customer_id")    .orderBy("purchase_date", "transaction_id"))purchase_analysis = (    transactions_df    .withColumn(        "previous_purchase_date",        F.lag("purchase_date").over(customer_window)    )    .withColumn(        "next_purchase_date",        F.lead("purchase_date").over(customer_window)    )    .withColumn(        "days_since_last_purchase",        F.datediff(            F.col("purchase_date"),            F.col("previous_purchase_date")        )    )    .withColumn(        "days_until_next_purchase",        F.datediff(            F.col("next_purchase_date"),            F.col("purchase_date")        )    ))display(    purchase_analysis.orderBy(        "customer_id", "purchase_date"    )
```

### **Expected output for customer 101**

| Purchase date | Previous date | Next date | Days since last |
| ------------- | ------------- | --------- | --------------- |
| Jan 1         | NULL          | Jan 10    | NULL            |
| Jan 10        | Jan 1         | Jan 25    | 9               |
| Jan 25        | Jan 10        | NULL      | 15              |

Applications include repeat-purchase tracking, churn analytics, customer retention analysis, and event sequence processing.

# **11. Practical Example — Top 3 Salaries per Department**

A very common interview question:

> Find the employees who receive the top three distinct salaries in each department.

```
top_salary_window = (    Window.partitionBy("department")    .orderBy(F.col("salary").desc()))top_3_employees = (    employee_df    .withColumn(        "salary_rank",        F.dense_rank().over(top_salary_window)    )    .filter(F.col("salary_rank") <= 3))display(    top_3_employees.orderBy(        "department",        "salary_rank",        "employee_id"    ))
```

Why `dense_rank()` instead of `row_number()`?

Because we want the top three **distinct salary values**, including all employees tied at those salaries.

If we wanted exactly three employees per department, we would use `row_number()` and a deterministic tie-breaker.

# **12. Window Functions in Databricks SQL**

Databricks supports window functions in both PySpark and SQL.

First, register the sample DataFrames as temporary views.

```
employee_df.createOrReplaceTempView("employees")sales_df.createOrReplaceTempView("sales")
```

You can now execute the following examples in SQL cells.

## **SQL example 1: All three ranking functions**

```
%sql
SELECT
    employee_name,
    department,
    salary,

    ROW_NUMBER() OVER (
        PARTITION BY department
        ORDER BY salary DESC, employee_id ASC
    ) AS row_num,

    RANK() OVER (
        PARTITION BY department
        ORDER BY salary DESC
    ) AS salary_rank,

    DENSE_RANK() OVER (
        PARTITION BY department
        ORDER BY salary DESC
    ) AS dense_salary_rank

FROM employees
ORDER BY department, salary DESC, employee_id;
```

## **SQL example 2: Second-highest salary per department**

```
%sql
WITH ranked_employees AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY department
            ORDER BY salary DESC
        ) AS salary_rank
    FROM employees
)

SELECT *
FROM ranked_employees
WHERE salary_rank = 2;
```

## **SQL example 3: Previous and next month's sales**

```
%sql
SELECT
    product,
    month,
    sales,

    LAG(sales, 1) OVER (
        PARTITION BY product
        ORDER BY month
    ) AS previous_sales,

    LEAD(sales, 1) OVER (
        PARTITION BY product
        ORDER BY month
    ) AS next_sales

FROM sales
ORDER BY product, month;
```

# **13. Performance Optimization in Databricks**

Window functions can be computationally expensive, particularly when working with millions of records.

Spark may need to redistribute data across executors and sort rows within partitions.

### **Understand the execution plan**

```
window_spec = (    Window.partitionBy("department")    .orderBy(F.col("salary").desc()))result_df = employee_df.withColumn(    "salary_rank",    F.rank().over(window_spec))result_df.explain("formatted")
```

Depending on the execution plan and optimizations, you may observe operators such as `Exchange`, `Sort`, and `Window`.

### **Best practices**

| Technique                                 | Benefit                                          |
| ----------------------------------------- | ------------------------------------------------ |
| Use `partitionBy()` with appropriate keys | Limits window calculations to related records    |
| Filter unwanted rows early                | Reduces data volume                              |
| Select only necessary columns             | Reduces processing overhead                      |
| Avoid unnecessary window expressions      | Reduces computation                              |
| Reuse identical window specifications     | Allows Spark to optimize compatible calculations |
| Handle skewed partitions                  | Avoids disproportionately expensive tasks        |
| Use deterministic ordering                | Produces reproducible results                    |

A particularly important distinction:

`Window.partitionBy()` creates logical SQL window partitions. It is **not the same** as `DataFrame.repartition()` or Delta Lake partitioning.

You should not blindly repartition a DataFrame before every window operation because an additional shuffle may make performance worse.

Also, using `Window.orderBy()` without `partitionBy()` for ranking can force all rows into a single logical window partition, creating a substantial bottleneck.

# **14. Interactive Practice — Test Your Understanding**

PySpark Window Functions Quiz

0 / 5 answered

1\. Two employees have equal salaries. Which function assigns different sequential numbers?

rank()

dense_rank()

row_number()

lag()

2\. Which function produces ranks 1, 2, 2, 4?

dense_rank()

rank()

row_number()

lead()

3\. Which function is best for the second-highest distinct salary?

row_number()

lag()

dense_rank()

lead()

4\. Which function retrieves the previous transaction amount?

lead()

rank()

lag()

row_number()

5\. What does lead() return for the last row by default?

Zero

The first row

NULL

An error

Check answersReset

# **15. Interview Summary — Five Window Functions**

| Function       | Purpose                      | Typical use case              |
| -------------- | ---------------------------- | ----------------------------- |
| `row_number()` | Unique sequence              | Deduplication, latest records |
| `rank()`       | Ranking with gaps after ties | Competition leaderboard       |
| `dense_rank()` | Ranking without gaps         | Nth highest distinct salary   |
| `lag()`        | Previous row value           | Month-over-month growth       |
| `lead()`       | Following row value          | Next purchase/event analysis  |

## **Important interview question: groupBy() vs. Window\.partitionBy()**

| Feature               | groupBy()                 | Window\.partitionBy()           |
| --------------------- | ------------------------- | ------------------------------- |
| Output rows           | Usually reduces row count | Preserves rows                  |
| Aggregation           | Yes                       | Yes, with window aggregates     |
| Row-level information | Usually lost              | Preserved                       |
| Ranking               | Not directly              | Yes                             |
| Previous/next row     | Not directly              | Yes                             |
| Typical usage         | Department total salary   | Employee rank within department |

**Key takeaway:** Use `groupBy()` when you need aggregated results for each group. Use window functions when you need calculations across related rows while preserving the original row-level information.

For Databricks Data Engineering projects, the three most valuable patterns to practice are **CDC deduplication using&#x20;**`row_number()`, **top-N analysis using&#x20;**`dense_rank()`, and **time-series comparisons using&#x20;**`lag()`**&#x20;and&#x20;**`lead()`.
