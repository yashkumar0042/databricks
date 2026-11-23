# **Databricks PySpark: select, filter, withColumn, drop, alias**

Module 2 — Apache Spark & PySpark Mastery | Detailed notes, syntax, practical examples, and ready-to-execute Databricks notebook labs

These five functions are among the most frequently used **DataFrame transformations** in PySpark. They help you select columns, filter records, create or modify columns, remove unwanted columns, and rename columns.

All five are transformations: they return a new DataFrame rather than changing the original DataFrame in place. Spark also evaluates them lazily, meaning most computation occurs when an action is executed.

## **1. Overview of the five functions**

| Function       | Purpose                        | SQL equivalent                |
| -------------- | ------------------------------ | ----------------------------- |
| `select()`     | Choose columns or expressions  | `SELECT`                      |
| `filter()`     | Keep rows matching a condition | `WHERE`                       |
| `withColumn()` | Add or replace a column        | `SELECT expression AS column` |
| `drop()`       | Remove columns                 | Exclude columns from `SELECT` |
| `alias()`      | Assign a temporary name        | `AS`                          |

## **2. Create our sample dataset in Databricks**

We'll use a realistic employee dataset throughout all five topics. Every example below can run in a Databricks Python notebook using the built-in `spark` session.

Notebook Cell 1 — Import functions and create a DataFrame

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as T# Define an explicit schemaemployee_schema = T.StructType([    T.StructField("emp_id", T.IntegerType(), False),    T.StructField("name", T.StringType(), True),    T.StructField("department", T.StringType(), True),    T.StructField("salary", T.DoubleType(), True),    T.StructField("age", T.IntegerType(), True),    T.StructField("city", T.StringType(), True),    T.StructField("experience", T.IntegerType(), True)])# Sample employee recordsemployee_data = [    (101, "Amit",   "IT",      75000.0, 28, "Bangalore", 5),    (102, "Priya",  "HR",      55000.0, 26, "Delhi",     3),    (103, "Rahul",  "IT",      95000.0, 32, "Bangalore", 8),    (104, "Sneha",  "Finance", 65000.0, 29, "Mumbai",    6),    (105, "Vikas",  "IT",      45000.0, 24, "Delhi",     2),    (106, "Neha",   "HR",      80000.0, 31, "Pune",      7),    (107, "Arjun",  "Finance", 90000.0, 35, "Mumbai",   10),    (108, "Pooja",  "IT",      60000.0, 27, "Pune",      4)]df = spark.createDataFrame(    employee_data,    schema=employee_schema)df.printSchema()display(df)
```

Expected DataFrame

| emp_id | name  | department | salary | age | city      | experience |
| ------ | ----- | ---------- | ------ | --- | --------- | ---------- |
| 101    | Amit  | IT         | 75000  | 28  | Bangalore | 5          |
| 102    | Priya | HR         | 55000  | 26  | Delhi     | 3          |
| 103    | Rahul | IT         | 95000  | 32  | Bangalore | 8          |
| 104    | Sneha | Finance    | 65000  | 29  | Mumbai    | 6          |
| 105    | Vikas | IT         | 45000  | 24  | Delhi     | 2          |
| 106    | Neha  | HR         | 80000  | 31  | Pune      | 7          |
| 107    | Arjun | Finance    | 90000  | 35  | Mumbai    | 10         |
| 108    | Pooja | IT         | 60000  | 27  | Pune      | 4          |

# **3. select() — Selecting columns**

## **Definition**

The `select()` function is used to retrieve specific columns or expressions from a PySpark DataFrame.

It is equivalent to the `SELECT` statement in SQL.

### **Syntax**

```
df.select("column1", "column2")
```

You can also use column expressions:

```
df.select(    F.col("column1"),    F.col("column2"))
```

### **Example 1 — Select specific columns**

Notebook Cell 2

```
# Select only employee name and salaryresult = df.select("name", "salary")display(result)
```

| name  | salary |
| ----- | ------ |
| Amit  | 75000  |
| Priya | 55000  |
| Rahul | 95000  |
| Sneha | 65000  |
| Vikas | 45000  |
| Neha  | 80000  |
| Arjun | 90000  |
| Pooja | 60000  |

Only the selected columns are included. The underlying employee records are not modified.

### **Example 2 — Select columns using F.col()**

```
result = df.select(    F.col("emp_id"),    F.col("name"),    F.col("department"))display(result)
```

`F.col()` returns a Spark Column expression. It is especially useful when performing calculations, filtering, casting, and renaming.

### **Example 3 — Calculate a value inside select()**

You can perform calculations without creating a permanently named column in the original DataFrame.

```
result = df.select(    "name",    "salary",    (F.col("salary") * 12).alias("annual_salary"))display(result)
```

Expected output:

| name  | salary | annual_salary |
| ----- | ------ | ------------- |
| Amit  | 75000  | 900000        |
| Priya | 55000  | 660000        |
| Rahul | 95000  | 1140000       |
| Sneha | 65000  | 780000        |
| Vikas | 45000  | 540000        |
| Neha  | 80000  | 960000        |
| Arjun | 90000  | 1080000       |
| Pooja | 60000  | 720000        |

### **Example 4 — Select distinct values**

Find the departments represented in the company.

```
result = df.select("department").distinct()display(result)
```

Expected department values: `IT`, `HR`, and `Finance`. The order is not guaranteed unless you explicitly sort the DataFrame.

### **Example 5 — Select columns using selectExpr()**

`selectExpr()` allows SQL-style expressions.

```
result = df.selectExpr(    "name",    "salary",    "salary * 12 AS annual_salary",    "UPPER(department) AS department_upper")display(result)
```

### **Real-world use case**

Imagine a source dataset containing 100 columns, but your downstream reporting system requires only 10.

You can use `select()` to retain the required columns and avoid carrying unnecessary data through later operations.

# **4. filter() — Filtering rows**

## **Definition**

The `filter()` function selects rows that satisfy a specified condition.

It is equivalent to SQL's `WHERE` clause.

`where()` is an alias for `filter()` in PySpark.

### **Syntax**

```
df.filter(condition)
```

Or:

```
df.where(condition)
```

### **Example 1 — Employees with salaries above ₹70,000**

Notebook Cell 3

```
result = df.filter(    F.col("salary") > 70000)display(result)
```

Expected employees:

| name  | department | salary |
| ----- | ---------- | ------ |
| Amit  | IT         | 75000  |
| Rahul | IT         | 95000  |
| Neha  | HR         | 80000  |
| Arjun | Finance    | 90000  |

The displayed DataFrame also includes the other original columns because `filter()` removes rows, not columns.

### **Example 2 — Filter employees belonging to IT**

```
result = df.filter(    F.col("department") == "IT")display(result)
```

This returns Amit, Rahul, Vikas, and Pooja.

### **Example 3 — Multiple conditions using AND**

Find IT employees earning more than ₹60,000.

```
result = df.filter(    (F.col("department") == "IT") &    (F.col("salary") > 60000))display(result)
```

Expected employees: Amit and Rahul.

The `&` operator combines conditions using logical AND.

### **Example 4 — Multiple conditions using OR**

Find employees working in IT or HR.

```
result = df.filter(    (F.col("department") == "IT") |    (F.col("department") == "HR"))display(result)
```

The `|` operator represents logical OR.

### **Example 5 — NOT condition**

Find employees who do not belong to IT.

```
result = df.filter(    ~(F.col("department") == "IT"))display(result)
```

The `~` operator represents NOT.

### **Example 6 — Filter using isin()**

Find employees in Bangalore or Mumbai.

```
result = df.filter(    F.col("city").isin("Bangalore", "Mumbai"))display(result)
```

### **Example 7 — Filter using between()**

Find employees earning between ₹50,000 and ₹80,000, including both endpoints.

```
result = df.filter(    F.col("salary").between(50000, 80000))display(result)
```

### **Example 8 — Filter using string conditions**

```
# Names starting with Adisplay(    df.filter(F.col("name").startswith("A")))# Names ending with adisplay(    df.filter(F.col("name").endswith("a")))# Names containing 'ah'display(    df.filter(F.col("name").contains("ah")))
```

### **Example 9 — Filter null values**

```
# Employees where city is missingdisplay(    df.filter(F.col("city").isNull()))# Employees where city is not missingdisplay(    df.filter(F.col("city").isNotNull()))
```

Our sample has no null cities, so the first result is empty.

**Important:** Use `isNull()` instead of `== None` for SQL null checks.

### **Logical operators in PySpark**

| Operator | Meaning               | Example                         |
| -------- | --------------------- | ------------------------------- |
| `==`     | Equals                | `F.col("age") == 25`            |
| `!=`     | Not equal             | `F.col("city") != "Delhi"`      |
| `>`      | Greater than          | `F.col("salary") > 50000`       |
| `<`      | Less than             | `F.col("age") < 30`             |
| `>=`     | Greater than or equal | `F.col("age") >= 25`            |
| `<=`     | Less than or equal    | `F.col("salary") <= 80000`      |
| `&`      | AND                   | `(condition1) & (condition2)`   |
| `\\|`    | OR                    | `(condition1) \\| (condition2)` |
| `~`      | NOT                   | `~(condition)`                  |

Always wrap individual conditions in parentheses when using `&`, `|`, or `~`. Python's `and`, `or`, and `not` are not substitutes for Spark Column conditions.

### **Real-world use case**

In an e-commerce data pipeline, you might filter transactions to retain only completed orders.

```
completed_orders = orders_df.filter(    F.col("order_status") == "COMPLETED")
```

This is useful when creating revenue reports or preparing clean datasets for downstream analytics.

# **5. withColumn() — Add or modify columns**

## **Definition**

The `withColumn()` function creates a new column or replaces an existing column using a Spark expression.

### **Syntax**

```
df.withColumn(    "new_column_name",    expression)
```

If the column already exists, its values are replaced in the returned DataFrame.

### **Example 1 — Create an annual salary column**

Notebook Cell 4

```
result = df.withColumn(    "annual_salary",    F.col("salary") * 12)display(result)
```

### **Example 2 — Give employees a 10% salary increment**

```
result = df.withColumn(    "salary",    F.round(F.col("salary") * 1.10, 2))display(    result.select("name", "salary"))
```

Here, the existing `salary` column is replaced in `result`. The original `df` remains unchanged.

### **Example 3 — Create a bonus column**

```
result = df.withColumn(    "bonus",    F.round(F.col("salary") * 0.15, 2))display(    result.select("name", "salary", "bonus"))
```

### **Example 4 — Conditional logic using when() and otherwise()**

Suppose the company gives bonuses based on salary:

- Salary greater than ₹80,000: 20% bonus.
- Salary greater than ₹60,000 and up to ₹80,000: 15% bonus.
- Otherwise: 10% bonus.

```
result = df.withColumn(    "bonus",    F.when(        F.col("salary") > 80000,        F.col("salary") * 0.20    )    .when(        F.col("salary") > 60000,        F.col("salary") * 0.15    )    .otherwise(        F.col("salary") * 0.10    ))display(    result.select("name", "salary", "bonus"))
```

Expected output

| name  | salary | bonus |
| ----- | ------ | ----- |
| Amit  | 75000  | 11250 |
| Priya | 55000  | 5500  |
| Rahul | 95000  | 19000 |
| Sneha | 65000  | 9750  |
| Vikas | 45000  | 4500  |
| Neha  | 80000  | 12000 |
| Arjun | 90000  | 18000 |
| Pooja | 60000  | 6000  |

### **Example 5 — Create employee experience categories**

```
result = df.withColumn(    "experience_level",    F.when(        F.col("experience") < 3,        "Junior"    )    .when(        F.col("experience") <= 6,        "Mid-Level"    )    .otherwise("Senior"))display(    result.select(        "name",        "experience",        "experience_level"    ))
```

### **Example 6 — Change string values to uppercase**

```
result = df.withColumn(    "name_upper",    F.upper(F.col("name")))display(result)
```

### **Example 7 — Add the current processing date**

```
result = df.withColumn(    "processing_date",    F.current_date()).withColumn(    "processing_timestamp",    F.current_timestamp())display(result)
```

This is useful for data auditing and ETL pipelines.

### **Example 8 — Change a column's data type**

```
result = df.withColumn(    "salary",    F.col("salary").cast("decimal(12,2)"))result.printSchema()display(result)
```

This changes the returned DataFrame's `salary` column from `DoubleType` to `DecimalType(12,2)`.

For financial amounts, decimal types are often preferable to floating-point types when exact decimal representation is needed.

### **Example 9 — Create multiple columns**

```
result = (    df.withColumn(        "annual_salary",        F.col("salary") * 12    )    .withColumn(        "bonus",        F.col("salary") * 0.10    )    .withColumn(        "total_monthly_compensation",        F.col("salary") + F.col("bonus")    ))display(result)
```

**Performance note:** Avoid constructing very deep chains of `withColumn()` calls for hundreds of independent columns. In those cases, using `select()` or `withColumns()` to create multiple expressions together can produce a simpler logical plan.

### **Real-world use case**

In a transaction-processing pipeline, `withColumn()` can calculate tax, discounts, revenue, processing timestamps, and data-quality flags.

# **6. drop() — Remove columns from a DataFrame**

## **Definition**

The `drop()` function removes one or more columns from a PySpark DataFrame.

It is especially useful when removing unnecessary, temporary, sensitive, or intermediate columns.

### **Syntax**

```
df.drop("column_name")
```

To drop multiple columns:

```
df.drop("column1", "column2")
```

### **Example 1 — Remove a single column**

Notebook Cell 5

```
# Remove age columnresult = df.drop("age")result.printSchema()display(result)
```

The resulting DataFrame contains all the original fields except `age`.

### **Example 2 — Remove multiple columns**

```
result = df.drop(    "age",    "city",    "experience")display(result)
```

Expected columns:

```
emp_id
name
department
salary
```

### **Example 3 — Remove an intermediate calculation**

Imagine creating a temporary salary calculation, using it to calculate a bonus, and then removing the temporary field.

```
result = (    df    .withColumn(        "temp_salary",        F.col("salary") * 12    )    .withColumn(        "annual_bonus",        F.col("temp_salary") * 0.10    )    .drop("temp_salary"))display(result)
```

The final DataFrame contains `annual_bonus` but not `temp_salary`.

### **Example 4 — What if the column does not exist?**

```
result = df.drop("unknown_column")display(result)
```

This normally does not produce an error. The DataFrame is returned without changes because that column does not exist.

### **Example 5 — Drop a column using F.col()**

```
result = df.drop(F.col("age"))display(result)
```

Both `drop("age")` and `drop(F.col("age"))` can remove a column, but they have different resolution semantics in cases involving ambiguous or duplicate column names. Using string names is generally clearer for ordinary DataFrames.

### **Real-world use case**

An ETL pipeline may temporarily load columns used only for calculations or internal processing.

For instance, after deriving customer age from a date of birth, you may not need to retain the original date-of-birth field in the downstream output.

Dropping unnecessary fields can also help reduce the amount of data carried through subsequent operations.

# **7. alias() — Rename columns or DataFrames**

## **Definition**

The `alias()` function assigns a temporary name to a column expression or a DataFrame.

It is widely used when:

- Giving calculated columns readable names.
- Renaming aggregation outputs.
- Differentiating DataFrames during joins.
- Resolving column references when both joined tables have similar field names.

### **Syntax — Column alias**

```
df.select(    F.col("salary").alias("monthly_salary"))
```

### **Syntax — DataFrame alias**

```
df.alias("e")
```

## **Example 1 — Rename a selected column**

Notebook Cell 6

```
result = df.select(    F.col("emp_id").alias("employee_id"),    F.col("name").alias("employee_name"),    F.col("salary").alias("monthly_salary"))display(result)
```

Expected output:

| employee_id | employee_name | monthly_salary |
| ----------- | ------------- | -------------- |
| 101         | Amit          | 75000          |
| 102         | Priya         | 55000          |
| 103         | Rahul         | 95000          |
| 104         | Sneha         | 65000          |
| 105         | Vikas         | 45000          |
| 106         | Neha          | 80000          |
| 107         | Arjun         | 90000          |
| 108         | Pooja         | 60000          |

### **Example 2 — Alias calculated columns**

```
result = df.select(    "name",    (F.col("salary") * 12).alias("annual_salary"),    (F.col("salary") * 0.10).alias("monthly_bonus"))display(result)
```

### **Example 3 — Alias aggregation outputs**

```
result = df.groupBy("department").agg(    F.count("*").alias("total_employees"),    F.sum("salary").alias("total_salary"),    F.avg("salary").alias("average_salary"),    F.max("salary").alias("highest_salary"))display(result)
```

Expected values:

| department | total_employees | total_salary | average_salary | highest_salary |
| ---------- | --------------- | ------------ | -------------- | -------------- |
| IT         | 4               | 275000       | 68750          | 95000          |
| HR         | 2               | 135000       | 67500          | 80000          |
| Finance    | 2               | 155000       | 77500          | 90000          |

### **Example 4 — DataFrame alias during a join**

This is one of the most important applications of `alias()`.

First, create a department reference DataFrame.

```
department_data = [    ("IT", "Technology"),    ("HR", "Human Resources"),    ("Finance", "Finance and Accounting")]department_df = spark.createDataFrame(    department_data,    ["department", "department_full_name"])display(department_df)
```

Now join employees to departments using aliases.

```
employees = df.alias("e")departments = department_df.alias("d")result = employees.join(    departments,    F.col("e.department") == F.col("d.department"),    "inner").select(    F.col("e.emp_id"),    F.col("e.name"),    F.col("e.salary"),    F.col("d.department_full_name"))display(result)
```

### **Explanation**

- `df.alias("e")` refers to the employee DataFrame as `e`.
- `department_df.alias("d")` refers to the department DataFrame as `d`.
- `F.col("e.department")` selects the department column from the employee side.
- `F.col("d.department")` selects the department column from the reference table.

This is particularly important for avoiding ambiguous column references when joined DataFrames share column names.

## **8. alias() vs. withColumnRenamed()**

Both can rename columns, but their typical applications differ.

| `alias()`                                  | `withColumnRenamed()`                |
| ------------------------------------------ | ------------------------------------ |
| Renames a column expression                | Renames an existing DataFrame column |
| Commonly used inside `select()` or `agg()` | Called directly on a DataFrame       |
| Also supports DataFrame aliases            | Does not alias a DataFrame for joins |

Example:

```
# Approach 1: alias()df1 = df.select(    F.col("name").alias("employee_name"),    "salary")# Approach 2: withColumnRenamed()df2 = df.withColumnRenamed(    "name",    "employee_name")display(df1)display(df2)
```

Notice that `df1` contains only the selected columns, while `df2` retains all the other original columns.

# **9. Complete real-world Databricks ETL example**

Now combine all five functions into one employee-processing pipeline.

### **Business requirements**

Suppose your company receives employee information from an HR system. You need to:

1. Select the required columns.
2. Filter employees with a salary of at least ₹50,000.
3. Calculate annual salary and bonus.
4. Create a salary category.
5. Remove unwanted fields.
6. Rename final output columns.

Notebook Cell 7 — Complete pipeline

```
from pyspark.sql import functions as F# STEP 1: Select required source columnsstep1 = df.select(    "emp_id",    "name",    "department",    "salary",    "experience",    "city")# STEP 2: Filter eligible employeesstep2 = step1.filter(    F.col("salary") >= 50000)# STEP 3: Add calculated columnsstep3 = (    step2    .withColumn(        "annual_salary",        F.col("salary") * 12    )    .withColumn(        "annual_bonus",        F.round(F.col("salary") * 12 * 0.10, 2)    )    .withColumn(        "salary_category",        F.when(            F.col("salary") >= 80000,            "High"        )        .when(
```

Expected final output

| employee_id | employee_name | monthly_salary | annual_salary | annual_bonus | salary_category |
| ----------- | ------------- | -------------- | ------------- | ------------ | --------------- |
| 101         | Amit          | 75000          | 900000        | 90000        | Medium          |
| 102         | Priya         | 55000          | 660000        | 66000        | Standard        |
| 103         | Rahul         | 95000          | 1140000       | 114000       | High            |
| 104         | Sneha         | 65000          | 780000        | 78000        | Medium          |
| 106         | Neha          | 80000          | 960000        | 96000        | High            |
| 107         | Arjun         | 90000          | 1080000       | 108000       | High            |
| 108         | Pooja         | 60000          | 720000        | 72000        | Medium          |

The table shows the principal reporting columns; the actual DataFrame also retains department and experience.

End-to-end transformation flow

Source DataFrame

8 employees, 7 columns

select()

Keep required fields

filter()

Remove salaries below ₹50,000

withColumn()

Calculate annual salary, bonus and category

drop()

Remove city

alias()

Rename reporting columns

Final DataFrame

7 employees, 8 reporting columns

## **10. Run the equivalent transformation in Databricks SQL**

PySpark DataFrames and Spark SQL use the same underlying Spark query engine.

Register your sample DataFrame as a temporary SQL view:

```
df.createOrReplaceTempView("employees")
```

Now run the following code in a Databricks SQL notebook cell:

```

SELECT
    emp_id AS employee_id,
    name AS employee_name,
    department,
    salary AS monthly_salary,
    experience,
    salary * 12 AS annual_salary,
    ROUND(salary * 12 * 0.10, 2) AS annual_bonus,
    CASE
        WHEN salary >= 80000 THEN 'High'
        WHEN salary >= 60000 THEN 'Medium'
        ELSE 'Standard'
    END AS salary_category
FROM employees
WHERE salary >= 50000
ORDER BY employee_id;

```

This produces the same logical reporting output as the PySpark pipeline.

# **11. Performance optimization in Databricks**

Because Spark uses lazy evaluation, transformations do not normally execute immediately.

For example:

```
optimized_df = (    df.select(        "emp_id",        "name",        "department",        "salary"    )    .filter(F.col("salary") > 60000)    .withColumn(        "annual_salary",        F.col("salary") * 12    ))# Inspect the physical execution planoptimized_df.explain("formatted")# Trigger executiondisplay(optimized_df)
```

Spark's Catalyst optimizer can apply optimizations such as:

- **Column pruning:** Avoid reading unnecessary columns where supported by the source.
- **Predicate pushdown:** Move eligible filters closer to the source.
- **Projection optimization:** Simplify selected columns and expressions.
- **Expression simplification:** Optimize compatible calculations.

A useful distinction is that `filter()` does not necessarily force a separate full scan, and `withColumn()` does not necessarily materialize a separate physical dataset. Spark plans transformations together when the action is triggered.

## **12. Common interview questions**

**Q1. What is the difference between select() and withColumn()?**

`select()` controls which columns appear in the result and can calculate expressions. `withColumn()` adds or replaces a specified column while retaining the other columns.

**Q2. What is the difference between filter() and where()?**

They are equivalent DataFrame methods. Both filter records based on conditions.

**Q3. Does withColumn() modify the original DataFrame?**

No. It returns a new DataFrame. PySpark DataFrames are immutable from the perspective of these transformation APIs.

**Q4. Can we use filter() after select()?**

Yes, provided the columns needed for the filter are still available.

**Q5. Why do we use alias() during joins?**

It helps disambiguate columns when multiple DataFrames contain fields with the same names.

**Q6. Is drop() an action or transformation?**

It is a transformation. It returns a new DataFrame without the specified columns.

**Q7. Which function should you use to create 100 new columns?**

Prefer a single `select()` with multiple expressions or `withColumns()` rather than building an unnecessarily deep chain of 100 `withColumn()` calls.

**Q8. Which is faster: select() or filter()?**

There is no universal answer. They do different things, and performance depends on the data source, selected columns, filter selectivity, physical plan, and optimizations.

# **13. Practice exercises**

Use the same `df` created in Notebook Cell 1.

Exercise 1 — select()

Display only employee name, department and salary.

&#x20;Show solution

Exercise 2 — filter()

Find employees in Bangalore with salaries above ₹70,000.

&#x20;Show solution

Exercise 3 — withColumn()

Create a new column named annual_salary with salary multiplied by 12.

&#x20;Show solution

Exercise 4 — drop()

Remove age, city and experience.

&#x20;Show solution

Exercise 5 — alias()

Rename emp_id to employee_id and name to employee_name.

&#x20;Show solution

## **Final revision cheat sheet**

```
from pyspark.sql import functions as F    F.col("salary") * 12)# WITHCOLUMN — Modify an existing columndf.withColumn(    "salary",    F.col("salary") * 1.10)# DROP — Remove columnsdf.drop("age", "city")# ALIAS — Rename a selected expressiondf.select(    F.col("name").alias("employee_name"))# COMBINED EXAMPLEresult = (    df    .filter(F.col("salary") > 50000)    .withColumn(        "annual_salary",        F.col("salary") * 12    )    .drop("age")    .select(        F.col("name").alias("employee_name"),        "department",        "annual_salary"    ))display(result)
```

**Key takeaway:** `select()` works with columns, `filter()` works with rows, `withColumn()` adds or replaces columns, `drop()` removes columns, and `alias()` gives expressions or DataFrames meaningful names. Together, they form the foundation of data transformation in Databricks PySpark.
