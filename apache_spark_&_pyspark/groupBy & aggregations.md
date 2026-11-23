# **Databricks PySpark Notes: groupBy() and Aggregations**

Module 2 · Apache Spark & PySpark Mastery

**Topics:** `groupBy()`, `sum()`, `avg()`, `count()`, `min()`, `max()`, `agg()`, `alias()`, `filter()`, `orderBy()`, `having`, and multiple aggregations.

**Learning objective:** Understand how to group large datasets and calculate meaningful business metrics using PySpark in Databricks, with practical examples you can run directly in a notebook.

## **1. What is groupBy() in PySpark?**

### **Definition**

`groupBy()` is a PySpark DataFrame transformation used to **group rows having the same value in one or more columns**.

After grouping, we apply aggregate functions to calculate summaries such as total sales, average salary, number of employees, minimum price, and maximum revenue.

### **Real-world use cases**

| Industry     | groupBy column   | Aggregation                     |
| ------------ | ---------------- | ------------------------------- |
| E-commerce   | product_category | Total sales                     |
| Banking      | account_id       | Total transaction amount        |
| HR analytics | department       | Average salary                  |
| Retail       | store_id         | Number of orders                |
| Healthcare   | hospital_id      | Minimum and maximum patient age |
| Logistics    | delivery_region  | Number of deliveries            |

### **How groupBy() works**

Input: Sales transactions

| Electronics | ₹5,000 |
| ----------- | ------ |
| Clothing    | ₹2,000 |
| Electronics | ₹3,000 |
| Clothing    | ₹1,000 |

groupBy("category").sum("amount")

Electronics

# **₹8,000**

5,000 + 3,000

Clothing

# **₹3,000**

2,000 + 1,000

### **Basic syntax**

```
df.groupBy("column_name").agg(    F.sum("numeric_column").alias("total"),    F.avg("numeric_column").alias("average"),    F.count("*").alias("count"),    F.min("numeric_column").alias("minimum"),    F.max("numeric_column").alias("maximum"))
```

**Important:** `groupBy()` is a lazy transformation. Spark does not immediately execute the aggregation. Execution is triggered by an action such as `show()`, `display()`, `count()`, or `collect()`.

## **2. Create sample data in a Databricks notebook**

We will build an e-commerce sales analytics notebook. The same dataset will be used throughout the examples.

The examples use DataFrame APIs and do not require direct `SparkContext` access, making them suitable for Databricks serverless compute as well as supported classic compute.

### **Cell 1 — Import PySpark functions**

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as Tprint("PySpark aggregation tutorial started!")
```

**Why use&#x20;**`F`**?** It is an alias for `pyspark.sql.functions`, which contains functions such as `sum`, `avg`, `count`, `min`, `max`, and `round`.

### **Cell 2 — Create the sample sales DataFrame**

```
sales_data = [    (1, "Laptop", "Electronics", "North", 50000, 2),    (2, "Mobile", "Electronics", "South", 20000, 3),    (3, "Shirt", "Clothing", "North", 1500, 5),    (4, "Jeans", "Clothing", "South", 2500, 4),    (5, "Laptop", "Electronics", "North", 55000, 1),    (6, "Mobile", "Electronics", "West", 25000, 2),    (7, "Shirt", "Clothing", "North", 1800, 3),    (8, "Jeans", "Clothing", "West", 3000, 2),    (9, "Table", "Furniture", "South", 10000, 2),    (10, "Chair", "Furniture", "North", 5000, 4)]schema = T.StructType([    T.StructField("order_id", T.IntegerType(), False),    T.StructField("product", T.StringType(), False),    T.StructField("category", T.StringType(), False),    T.StructField("region", T.StringType(), False),    T.StructField("price", T.IntegerType(), False),    T.StructField("quantity", T.IntegerType(), False)])df = spark.createDataFrame(sales_data, schema)display(df)
```

### **Cell 3 — Calculate revenue**

Revenue = Price × Quantity

```
df = df.withColumn(    "revenue",    F.col("price") * F.col("quantity"))display(df)
```

The resulting DataFrame has seven columns: `order_id`, `product`, `category`, `region`, `price`, `quantity`, and `revenue`.

## **3. The five main aggregation functions**

**sum() — Total value**

Adds all values in a numeric column. Use it for revenue, sales quantity, total expenses, or transaction amounts.

**avg() — Average value**

Calculates the arithmetic mean of non-null numeric values. Useful for average salary, average transaction value, or average product price.

**count() — Number of records**

Counts rows or non-null values. Useful for customers, employees, transactions, and orders.

**min() — Smallest value**

Finds the minimum value within a group, such as the lowest product price or earliest order date.

**max() — Largest value**

Finds the maximum value within a group, such as the highest sale, highest salary, or latest order date.

### **Cell 4 — sum(): Total revenue by category**

```
total_sales = (    df.groupBy("category")      .agg(          F.sum("revenue").alias("total_revenue")      )      .orderBy("category"))display(total_sales)
```

**Expected output**

| category    | total_revenue |
| ----------- | ------------- |
| Clothing    | 28,900        |
| Electronics | 265,000       |
| Furniture   | 40,000        |

**Use case:** Identify which product categories generate the most revenue.

### **Cell 5 — avg(): Average product price by category**

```
avg_price = (    df.groupBy("category")      .agg(          F.round(F.avg("price"), 2).alias("average_price")      )      .orderBy("category"))display(avg_price)
```

**Expected output**

| category    | average_price |
| ----------- | ------------- |
| Clothing    | 2,200         |
| Electronics | 37,500        |
| Furniture   | 7,500         |

Here, the average is calculated using the number of records in each category. This is not a quantity-weighted average or the average order revenue.

**Use case:** Compare average listed transaction prices across product categories.

### **Cell 6 — count(): Number of orders per region**

```
orders_per_region = (    df.groupBy("region")      .agg(          F.count("*").alias("total_orders")      )      .orderBy("region"))display(orders_per_region)
```

**Expected output**

| region | total_orders |
| ------ | ------------ |
| North  | 5            |
| South  | 3            |
| West   | 2            |

**Use case:** Determine which geographic region processes the highest number of orders.

### **Cell 7 — min(): Minimum product price**

```
minimum_price = (    df.groupBy("category")      .agg(          F.min("price").alias("minimum_price")      )      .orderBy("category"))display(minimum_price)
```

**Expected output**

| category    | minimum_price |
| ----------- | ------------- |
| Clothing    | 1,500         |
| Electronics | 20,000        |
| Furniture   | 5,000         |

**Use case:** Find the cheapest recorded product price within each category.

### **Cell 8 — max(): Maximum product price**

```
maximum_price = (    df.groupBy("category")      .agg(          F.max("price").alias("maximum_price")      )      .orderBy("category"))display(maximum_price)
```

**Expected output**

| category    | maximum_price |
| ----------- | ------------- |
| Clothing    | 3,000         |
| Electronics | 55,000        |
| Furniture   | 10,000        |

**Use case:** Find premium-priced products across different categories.

## **4. Multiple aggregations using agg()**

In real-world data engineering projects, we rarely calculate only one metric. We often need total revenue, average price, order count, minimum price, and maximum price in a single query.

The `agg()` method allows multiple aggregate functions to execute together.

### **Cell 9 — Complete category-level sales report**

```
category_report = (    df.groupBy("category")      .agg(          F.count("*").alias("total_orders"),          F.sum("quantity").alias("units_sold"),          F.sum("revenue").alias("total_revenue"),          F.round(F.avg("price"), 2).alias("avg_price"),          F.min("price").alias("min_price"),          F.max("price").alias("max_price")      )      .orderBy("category"))display(category_report)
```

**Expected output**

| category    | total_orders | units_sold | total_revenue | avg_price | min_price | max_price |
| ----------- | ------------ | ---------- | ------------- | --------- | --------- | --------- |
| Clothing    | 4            | 14         | 28,900        | 2,200     | 1,500     | 3,000     |
| Electronics | 4            | 8          | 265,000       | 37,500    | 20,000    | 55,000    |
| Furniture   | 2            | 6          | 40,000        | 7,500     | 5,000     | 10,000    |

### **Explanation**

- `groupBy("category")`: Groups records into Electronics, Clothing, and Furniture.
- `count("*")`: Counts the number of order records in each category.
- `sum("quantity")`: Calculates the total items sold.
- `sum("revenue")`: Calculates the total sales amount.
- `avg("price")`: Calculates the arithmetic average of the price column.
- `min("price")`: Finds the lowest price.
- `max("price")`: Finds the highest price.
- `alias()`: Assigns readable names to output columns.

**Real-world use case:** An e-commerce company creates daily category performance dashboards to track sales, monitor product pricing, and identify categories that need marketing investment.

## **5. groupBy() on multiple columns**

Sometimes grouping by only one column is insufficient.

For example, suppose a business wants to know:

*How much revenue did each product category generate in each region?*

We need two grouping columns: `category` and `region`.

### **Cell 10 — Group by category and region**

```
region_category_sales = (    df.groupBy("category", "region")      .agg(          F.sum("revenue").alias("total_revenue"),          F.count("*").alias("total_orders")      )      .orderBy("category", "region"))display(region_category_sales)
```

**Expected output**

| category    | region | total_revenue | total_orders |
| ----------- | ------ | ------------- | ------------ |
| Clothing    | North  | 12,900        | 2            |
| Clothing    | South  | 10,000        | 1            |
| Clothing    | West   | 6,000         | 1            |
| Electronics | North  | 155,000       | 2            |
| Electronics | South  | 60,000        | 1            |
| Electronics | West   | 50,000        | 1            |
| Furniture   | North  | 20,000        | 1            |
| Furniture   | South  | 20,000        | 1            |

### **How multiple-column grouping works**

Each unique combination becomes a separate group.

For example, Electronics + North is different from Electronics + South.

This is useful in retail analytics, financial reporting, supply chain monitoring, and customer segmentation.

## **6. Filtering before and after groupBy()**

This is an important concept for Databricks interviews.

### **Filtering before aggregation**

Suppose we only want orders from the North region, grouped by category.

### **Cell 11 — filter() before groupBy()**

```
north_sales = (    df.filter(F.col("region") == "North")      .groupBy("category")      .agg(          F.sum("revenue").alias("total_revenue")      )      .orderBy("category"))display(north_sales)
```

**Expected output**

| category    | total_revenue |
| ----------- | ------------- |
| Clothing    | 12,900        |
| Electronics | 155,000       |
| Furniture   | 20,000        |

Here, `filter()` reduces the input records before aggregation.

### **Filtering after aggregation**

Now suppose we want only categories whose total sales exceed ₹30,000.

### **Cell 12 — filter() after groupBy()**

```
high_revenue_categories = (    df.groupBy("category")      .agg(          F.sum("revenue").alias("total_revenue")      )      .filter(F.col("total_revenue") > 30000)      .orderBy(F.desc("total_revenue")))display(high_revenue_categories)
```

**Expected output**

| category    | total_revenue |
| ----------- | ------------- |
| Electronics | 265,000       |
| Furniture   | 40,000        |

### **Difference: WHERE vs HAVING**

| WHERE                                     | HAVING                               |
| ----------------------------------------- | ------------------------------------ |
| Filters individual records                | Filters aggregated groups            |
| Happens logically before grouping         | Happens logically after grouping     |
| Uses original row columns                 | Can use aggregate calculations       |
| In PySpark: `filter()` before `groupBy()` | In PySpark: `filter()` after `agg()` |

For example:

```
-- WHERE: filter input rows
SELECT category, SUM(revenue)
FROM sales
WHERE region = 'North'
GROUP BY category;
```

```
-- HAVING: filter summary values
SELECT category, SUM(revenue)
FROM sales
GROUP BY category
HAVING SUM(revenue) > 30000;
```

PySpark does not need a separate `having()` DataFrame method. We use `.filter()` after `.agg()`.

## **7. groupBy() and aggregation in Databricks SQL**

Databricks supports both PySpark and SQL. SQL is especially useful for business analysts, dashboards, and reporting.

### **Cell 13 — Register a temporary view**

Run this in a Python notebook cell.

```
df.createOrReplaceTempView("sales")print("Temporary view 'sales' created successfully")
```

A temporary view exists within the Spark session. It does not create a permanent Unity Catalog table.

### **Cell 14 — SQL aggregation**

Create a SQL cell or add `%sql` at the beginning of a new Databricks notebook cell.

```

%sql

SELECT
    category,
    COUNT(*) AS total_orders,
    SUM(quantity) AS units_sold,
    SUM(revenue) AS total_revenue,
    ROUND(AVG(price), 2) AS average_price,
    MIN(price) AS minimum_price,
    MAX(price) AS maximum_price
FROM sales
GROUP BY category
ORDER BY total_revenue DESC;

```

### **Cell 15 — SQL GROUP BY with HAVING**

```

%sql

SELECT
    region,
    COUNT(*) AS total_orders,
    SUM(revenue) AS total_revenue
FROM sales
GROUP BY region
HAVING SUM(revenue) > 60000
ORDER BY total_revenue DESC;

```

Expected result:

| region | total_orders | total_revenue |
| ------ | ------------ | ------------- |
| North  | 5            | 187,900       |
| South  | 3            | 90,000        |

### **PySpark vs SQL**

| Operation       | PySpark                    | Databricks SQL |
| --------------- | -------------------------- | -------------- |
| Grouping        | `groupBy()`                | `GROUP BY`     |
| Sum             | `F.sum()`                  | `SUM()`        |
| Average         | `F.avg()`                  | `AVG()`        |
| Count           | `F.count()`                | `COUNT()`      |
| Minimum         | `F.min()`                  | `MIN()`        |
| Maximum         | `F.max()`                  | `MAX()`        |
| Group filtering | `.filter()` after `.agg()` | `HAVING`       |
| Sorting         | `orderBy()`                | `ORDER BY`     |

## **8. Advanced aggregation examples**

### **Cell 16 — count() vs countDistinct()**

A common business question is:

*How many order records do we have versus how many distinct products were purchased?*

These are different metrics.

```
unique_product_report = (    df.groupBy("category")      .agg(          F.count("*").alias("order_count"),          F.countDistinct("product").alias("unique_products")      )      .orderBy("category"))display(unique_product_report)
```

**Expected output**

| category    | order_count | unique_products |
| ----------- | ----------- | --------------- |
| Clothing    | 4           | 2               |
| Electronics | 4           | 2               |
| Furniture   | 2           | 2               |

**Use case:** Determine product diversity and buying activity within a category.

### **Cell 17 — Conditional aggregation**

Suppose we want to count how many order records have revenue greater than ₹30,000 in each region.

We can use `when()` inside `sum()`.

```
large_orders = (    df.groupBy("region")      .agg(          F.count("*").alias("total_orders"),          F.sum(              F.when(F.col("revenue") > 30000, 1)               .otherwise(0)          ).alias("large_orders")      )      .orderBy("region"))display(large_orders)
```

**Expected output**

| region | total_orders | large_orders |
| ------ | ------------ | ------------ |
| North  | 5            | 2            |
| South  | 3            | 1            |
| West   | 2            | 1            |

**Use case:** Identify high-value transactions, suspicious transactions, or sales above a business threshold.

### **Cell 18 — Percentage contribution of each category**

Suppose management wants to understand what percentage of overall revenue is generated by each category.

First aggregate revenue by category, then calculate its proportion of overall revenue using a window expression.

```
from pyspark.sql.window import Windowcategory_contribution = (    df.groupBy("category")      .agg(          F.sum("revenue").alias("total_revenue")      )      .withColumn(          "overall_revenue",          F.sum("total_revenue").over(Window.partitionBy())      )      .withColumn(          "revenue_percentage",          F.round(              F.col("total_revenue") /              F.col("overall_revenue") * 100,              2          )      )      .drop("overall_revenue")      .orderBy(F.desc("total_revenue")))display(category_contribution)
```

**Expected output**

| category    | total_revenue | revenue_percentage |
| ----------- | ------------- | ------------------ |
| Electronics | 265,000       | 79.37              |
| Furniture   | 40,000        | 11.98              |
| Clothing    | 28,900        | 8.66               |

Percentages are rounded independently, so they may not add up to exactly 100%.

**Use case:** Identify top revenue-driving business segments.

For very large datasets with a relatively small number of categories, this unpartitioned window should operate on the aggregated category result, not millions of raw transactions.

### **Cell 19 — Global aggregation without groupBy()**

We can also calculate aggregates across the entire dataset without grouping.

```
overall_sales = df.agg(    F.count("*").alias("total_orders"),    F.sum("quantity").alias("total_units"),    F.sum("revenue").alias("overall_revenue"),    F.round(F.avg("revenue"), 2).alias("avg_order_revenue"),    F.min("price").alias("lowest_price"),    F.max("price").alias("highest_price"))display(overall_sales)
```

**Expected output**

| total_orders | total_units | overall_revenue | avg_order_revenue | lowest_price | highest_price |
| ------------ | ----------- | --------------- | ----------------- | ------------ | ------------- |
| 10           | 28          | 333,900         | 33,390            | 1,500        | 55,000        |

This calculates summary statistics for all records and returns one row for this nonempty sample dataset.

## **9. Handling NULL values in aggregations**

In real-world datasets, missing data is common. Understanding how aggregations handle `NULL` values is essential.

### **Cell 20 — Create data containing NULL values**

```
null_data = [    ("IT", 50000),    ("IT", 60000),    ("IT", None),    ("HR", 40000),    ("HR", None),    ("Sales", 45000),    ("Sales", 55000)]null_schema = T.StructType([    T.StructField("department", T.StringType()),    T.StructField("salary", T.IntegerType())])employee_df = spark.createDataFrame(    null_data,    null_schema)display(employee_df)
```

### **Cell 21 — Compare count, sum, avg, min, and max**

```
null_report = (    employee_df.groupBy("department")    .agg(        F.count("*").alias("total_rows"),        F.count("salary").alias("non_null_salaries"),        F.sum("salary").alias("total_salary"),        F.avg("salary").alias("average_salary"),        F.min("salary").alias("minimum_salary"),        F.max("salary").alias("maximum_salary")    )    .orderBy("department"))display(null_report)
```

**Expected output**

| department | total_rows | non_null_salaries | total_salary | average_salary | minimum_salary | maximum_salary |
| ---------- | ---------- | ----------------- | ------------ | -------------- | -------------- | -------------- |
| HR         | 2          | 1                 | 40,000       | 40,000         | 40,000         | 40,000         |
| IT         | 3          | 2                 | 110,000      | 55,000         | 50,000         | 60,000         |
| Sales      | 2          | 2                 | 100,000      | 50,000         | 45,000         | 55,000         |

### **Important rules**

- `count("*")` counts all rows, including those containing NULL.
- `count("salary")` counts only non-NULL salary values.
- `sum()` ignores NULL values.
- `avg()` ignores NULL values when calculating the average.
- `min()` and `max()` also ignore NULL values.
- If all values in a group are NULL, `sum`, `avg`, `min`, and `max` return NULL.

### **Cell 22 — Replace NULL salaries before aggregation**

Sometimes business logic requires treating missing values as zero.

```
cleaned_employees = employee_df.fillna({    "salary": 0})cleaned_report = (    cleaned_employees.groupBy("department")    .agg(        F.avg("salary").alias("average_salary")    )    .orderBy("department"))display(cleaned_report)
```

Notice that IT's average now changes from ₹55,000 to approximately ₹36,666.67 because zero is included in the calculation.

**Best practice:** Never replace missing financial values with zero unless that is consistent with the business definition.

## **10. How groupBy() works internally in Spark**

Understanding Spark's execution is especially useful for Databricks developers and data engineering interviews.

A groupBy aggregation commonly involves a **shuffle**, because Spark must bring records with the same grouping key together.

Simplified distributed aggregation flow

Partition 1

Electronics 100

Clothing 30

Partition 2

Clothing 50

Electronics 40

Partition 3

Electronics 200

Clothing 20

Partial aggregation (local sums)

E:100, C:30

E:40, C:50

E:200, C:20

**Shuffle: same keys are brought together**

Electronics

## **340**

100 + 40 + 200

Clothing

## **100**

30 + 50 + 20

In a typical aggregation, Spark performs the following stages:

1. **Read input:** Data is read from distributed partitions.
2. **Partial aggregation:** Spark computes local intermediate aggregates on each partition.
3. **Shuffle:** Intermediate data is redistributed by grouping keys.
4. **Final aggregation:** Partial results are combined to produce the final values.
5. **Output:** Results are returned to the next stage or displayed.

### **Cell 23 — View the Spark physical execution plan**

```
execution_demo = (    df.groupBy("category")      .agg(          F.sum("revenue").alias("total_revenue")      ))execution_demo.explain("formatted")
```

A typical physical plan may contain operations such as:

```
AdaptiveSparkPlan
  HashAggregate
    Exchange
      HashAggregate
        Scan ExistingRDD
```

The actual plan can vary depending on the Databricks runtime, execution engine, and optimizations.

| Operator                | Meaning                                |
| ----------------------- | -------------------------------------- |
| Scan                    | Read the underlying dataset            |
| HashAggregate (partial) | Aggregate locally                      |
| Exchange                | Redistribute data between partitions   |
| HashAggregate (final)   | Combine partial aggregates             |
| AdaptiveSparkPlan       | Allows runtime query-plan optimization |

The `Exchange` operation is particularly important because it often represents network data movement, which can be expensive.

Databricks also supports adaptive query execution to optimize certain execution decisions at runtime.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



## **11. Performance optimization best practices**

For a small dataset of 10 rows, aggregation completes quickly. With millions or billions of records, careful optimization becomes important.

| Technique                                        | Benefit                                      |
| ------------------------------------------------ | -------------------------------------------- |
| Filter early                                     | Reduces input volume                         |
| Select necessary columns                         | Reduces data processed                       |
| Avoid unnecessary `distinct()`                   | Prevents extra work                          |
| Use built-in aggregate functions                 | Allows Spark engine optimizations            |
| Avoid high-cardinality grouping when unnecessary | Reduces aggregation-state size               |
| Use `explain()`                                  | Helps identify shuffles and expensive stages |
| Use appropriate partitioning                     | Can improve distributed execution            |
| Inspect skewed grouping keys                     | Helps identify overloaded tasks              |

### **Cell 24 — Optimized aggregation example**

```
optimized_sales = (    df.filter(F.col("revenue") > 5000)      .select("category", "revenue")      .groupBy("category")      .agg(          F.sum("revenue").alias("total_revenue")      )      .orderBy(F.desc("total_revenue")))display(optimized_sales)optimized_sales.explain("formatted")
```

**Explanation:** We filter records and select only the required columns before grouping. This can reduce the amount of input data the aggregation processes.

The Spark optimizer can perform some of these optimizations automatically; the physical plan should be inspected rather than assuming every rewrite improves speed.

## **12. Practical project: Build a sales analytics report**

This is a complete end-to-end aggregation example resembling an analytics task in a real data engineering project.

### **Business requirement**

Your company wants to generate a sales performance report containing:

- Total orders per region
- Total revenue per region
- Total units sold
- Average revenue per order
- Minimum and maximum order revenue
- Revenue ranking by region

### **Cell 25 — Generate the business report**

```
regional_report = (    df.groupBy("region")      .agg(          F.count("*").alias("total_orders"),          F.sum("quantity").alias("units_sold"),          F.sum("revenue").alias("total_revenue"),          F.round(              F.avg("revenue"), 2          ).alias("average_order_revenue"),          F.min("revenue").alias("min_order_revenue"),          F.max("revenue").alias("max_order_revenue")      )      .orderBy(F.desc("total_revenue")))display(regional_report)
```

### **Cell 26 — Add revenue ranking**

```
from pyspark.sql.window import Windowrank_window = Window.orderBy(    F.desc("total_revenue"))ranked_report = regional_report.withColumn(    "revenue_rank",    F.dense_rank().over(rank_window))display(ranked_report)
```

### **Cell 27 — Save the result as a Delta table**

This optional cell creates a managed Delta table in the current schema. Make sure you have permission to create tables there.

```
ranked_report.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable("regional_sales_report")print("Sales report saved successfully!")
```

### **Cell 28 — Read the saved table**

```
saved_report = spark.table(    "regional_sales_report")display(saved_report)
```

The table can now be used by downstream Databricks notebooks, scheduled jobs, or SQL dashboards where permissions allow.

## **13. Practice exercises**

Use the `df` DataFrame created in Cell 2 and extended in Cell 3.

**Databricks notebook challenges**

0 of 5 revealed

1

Find total revenue generated by each product.

Use groupBy("product") with F.sum("revenue").

Show solution

2

Find the average quantity sold per region.

Group by region, then use F.avg("quantity").

Show solution

3

Find categories having more than 2 order records.

Use count("\*") followed by filter on the aggregated result.

Show solution

4

Find the highest revenue from an individual order in each category.

Apply F.max("revenue") rather than F.max("price").

Show solution

5

Find regions with total revenue greater than ₹80,000.

Aggregate revenue by region, then apply filter.

Show solution

## **14. Interview questions and answers**

**Q1. Is groupBy() a transformation or an action?**

`groupBy()` is a transformation that creates a grouped DataFrame representation. The aggregation is also lazily evaluated until an action is called.

**Q2. Does groupBy() always cause a shuffle?**

Not necessarily. A typical distributed groupBy involves shuffling to bring identical keys together. However, the optimizer can sometimes avoid an additional shuffle if the existing data distribution already satisfies the grouping requirements.

**Q3. Which is more expensive: groupBy() or partitionBy()?**

They serve different purposes, so there is no universal winner.

- `groupBy()` aggregates rows into groups.
- `Window.partitionBy()` creates logical groups while retaining individual rows.
- `DataFrameWriter.partitionBy()` controls how output files are organized.

A groupBy aggregation often requires a shuffle, while a window operation can require a shuffle and additional sorting. Actual cost depends on the execution plan and data distribution.

**Q4. What is the difference between count("\*") and count("column")?**

`count("*")` counts every row. `count("column")` counts only rows where that column is not NULL.

**Q5. Can groupBy() use multiple aggregate functions at once?**

Yes. Use `agg()` to calculate several metrics in one operation.

**Q6. What happens if there are NULL values in the grouping column?**

Rows having NULL in a grouping key are grouped together into a NULL-key group.

**Q7. How do you optimize a slow groupBy operation?**

Start by inspecting the Spark execution plan and Spark UI. Look at shuffle volume, high-cardinality keys, data skew, spill to disk, and task execution times. Filter unnecessary records early and use built-in aggregate functions.

## **15. Quick revision cheat sheet**

| Requirement                 | PySpark code                            |
| --------------------------- | --------------------------------------- |
| Total sales                 | `df.groupBy("category").sum("revenue")` |
| Average price               | `df.groupBy("category").avg("price")`   |
| Count rows                  | `df.groupBy("category").count()`        |
| Minimum price               | `df.groupBy("category").min("price")`   |
| Maximum price               | `df.groupBy("category").max("price")`   |
| Multiple aggregates         | `df.groupBy("category").agg(...)`       |
| Multiple grouping columns   | `df.groupBy("category", "region")`      |
| Filter rows before grouping | `df.filter(...).groupBy(...)`           |
| Filter after aggregation    | `df.groupBy(...).agg(...).filter(...)`  |
| Count unique values         | `F.countDistinct("product")`            |
| Sort aggregate results      | `.orderBy(F.desc("total"))`             |
| View execution plan         | `.explain("formatted")`                 |

For additional reference, see the official [Databricks GroupedData documentation](https://docs.databricks.com/aws/en/pyspark/reference/classes/groupeddata?utm_source=chatgpt.com) and [Apache Spark aggregation API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.agg.html?utm_source=chatgpt.com).

**Key takeaway:** `groupBy()` defines how records are grouped, while functions such as `sum()`, `avg()`, `count()`, `min()`, and `max()` define the calculations performed on each group. Mastering these operations is fundamental to building ETL pipelines, financial reports, customer analytics, and data aggregation workflows in Databricks.
