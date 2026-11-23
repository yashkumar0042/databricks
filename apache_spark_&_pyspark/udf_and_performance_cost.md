# **PySpark UDFs and Their Performance Cost — Complete Databricks Notes**

Module 2: Apache Spark & PySpark Mastery | Theory + Practical Labs + Performance Optimization

## **1. What is a UDF in PySpark?**

**UDF stands for User-Defined Function.**

A UDF allows developers to write custom functions in Python and apply them to DataFrame columns when Spark's built-in functions are insufficient.

In simple terms:

- Spark provides built-in functions such as `upper()`, `lower()`, `concat()`, `when()`, `round()` and `length()`.
- Sometimes, our business logic cannot easily be implemented using these functions.
- In such cases, we can create a Python function and register it as a Spark UDF.
- Spark then executes that function on DataFrame records in a distributed manner.

### **Real-world example**

Imagine an e-commerce company processing millions of orders every day.

The company wants to classify customers according to their total purchase amounts.

| Purchase amount | Customer category |
| --------------- | ----------------- |
| ₹80,000         | Platinum          |
| ₹35,000         | Gold              |
| ₹15,000         | Silver            |
| ₹4,000          | Bronze            |

We can implement this logic using a Python UDF or Spark's built-in conditional expressions.

Both can produce the same results, but they may have very different execution costs.

## **2. Types of UDFs in Databricks**

| Type                       | Description                                                       | Typical performance                       |
| -------------------------- | ----------------------------------------------------------------- | ----------------------------------------- |
| Standard Python UDF        | Executes custom Python logic row by row                           | Higher overhead                           |
| Pandas UDF                 | Uses Apache Arrow and pandas for batch processing                 | Often faster                              |
| Arrow-optimized Python UDF | Uses Arrow serialization but still runs ordinary Python row logic | Lower transfer overhead                   |
| SQL UDF                    | Reusable functions defined using SQL                              | Efficient when expressed using native SQL |
| Native Arrow UDF           | Processes Arrow arrays directly, available in newer runtimes      | Efficient for suitable workloads          |

Databricks documentation explains that vectorized pandas UDFs can achieve substantially better performance than regular Python UDFs, sometimes by as much as 100× in suitable cases. This is not a guaranteed speedup.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+2



**Important:** For ordinary column transformations, Spark built-in functions should usually be your first choice.

## **3. Why are Python UDFs expensive?**

Execution flow of a traditional Python UDF

Conceptual view. Actual execution may batch data and reuse Python workers.

A traditional Python UDF introduces several performance costs:

1. **Serialization:** Spark must convert data into a representation Python can consume.
2. **Python execution:** Custom logic runs outside Spark's JVM execution engine.
3. **Deserialization:** Results must be transferred back to Spark.
4. **Optimizer limitations:** Catalyst generally cannot inspect arbitrary Python code to optimize its internal logic.
5. **Memory overhead:** Python workers and temporary data representations require additional memory.

Unlike built-in expressions, Python UDFs can introduce an execution boundary that prevents the entire expression pipeline from benefiting from Spark's normal optimization and code-generation mechanisms.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

PySpark 4.3.0 documentation+1



# **4. Databricks Practical Lab — Standard Python UDF**

All examples below can be executed in a Databricks Python notebook. They use in-memory sample data, so you do not need to upload CSV files or create database tables.

## **Step 1. Import required libraries**

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as Tfrom pyspark.sql.functions import udf, pandas_udf
```

## **Step 2. Create an employee DataFrame**

```
employees_data = [    (101, "Rahul", 85000, "IT"),    (102, "Priya", 45000, "HR"),    (103, "Amit", 25000, "Finance"),    (104, "Sneha", 95000, "IT"),    (105, "Karan", 15000, "Sales"),    (106, "Neha", 55000, "Finance")]schema = T.StructType([    T.StructField("emp_id", T.IntegerType(), False),    T.StructField("emp_name", T.StringType(), False),    T.StructField("salary", T.IntegerType(), False),    T.StructField("department", T.StringType(), False)])df = spark.createDataFrame(    employees_data,    schema)display(df)
```

## **Step 3. Define a normal Python function**

Our business requirement is to create a salary category:

- Salary ≥ ₹80,000 → High
- Salary ≥ ₹40,000 → Medium
- Salary below ₹40,000 → Low

```
def salary_category(salary):    if salary is None:        return "Unknown"    if salary >= 80000:        return "High"    elif salary >= 40000:        return "Medium"    else:        return "Low"
```

At this point, `salary_category` is just a normal Python function.

It cannot directly be applied as a Spark column expression.

## **Step 4. Convert Python function into Spark UDF**

```
salary_udf = F.udf(    salary_category,    T.StringType())
```

Explanation:

| Component         | Purpose                                   |
| ----------------- | ----------------------------------------- |
| `F.udf()`         | Creates a Spark UDF                       |
| `salary_category` | Python function containing business logic |
| `T.StringType()`  | Declares the output data type             |

Always specify the correct return type. Otherwise, implicit type assumptions or mismatches can cause errors or unexpected results.

## **Step 5. Apply the UDF using&#x20;**`withColumn()`

```
df_udf = df.withColumn(    "salary_category",    salary_udf(F.col("salary")))display(df_udf)
```

Expected output:

| emp_id | emp_name | salary | salary_category |
| ------ | -------- | ------ | --------------- |
| 101    | Rahul    | 85000  | High            |
| 102    | Priya    | 45000  | Medium          |
| 103    | Amit     | 25000  | Low             |
| 104    | Sneha    | 95000  | High            |
| 105    | Karan    | 15000  | Low             |
| 106    | Neha     | 55000  | Medium          |

## **Step 6. Inspect the execution plan**

```
df_udf.explain("formatted")
```

In many Spark environments, a standard Python UDF introduces an execution operator similar to:

```
BatchEvalPython
```

This indicates that Spark needs to execute Python code as a separate part of the physical plan.

# **5. Implement the Same Logic Without a UDF**

Spark provides a built-in function called `when()`, which works similarly to `if / elif / else`.

```
df_optimized = df.withColumn(    "salary_category",    F.when(        F.col("salary") >= 80000,        "High"    )    .when(        F.col("salary") >= 40000,        "Medium"    )    .otherwise("Low"))display(df_optimized)
```

This produces the same categories for the sample data.

Notice the difference:

| Python UDF                       | Built-in Spark function                        |
| -------------------------------- | ---------------------------------------------- |
| Custom Python code               | Spark expression                               |
| Requires Python execution        | Runs within Spark's optimized execution engine |
| Internal code opaque to Catalyst | Visible to Catalyst                            |
| Serialization overhead           | Avoids Python UDF transfer overhead            |
| Useful for complex custom logic  | Preferred for standard operations              |

Inspect its physical plan:

```
df_optimized.explain("formatted")
```

You should not see a `BatchEvalPython` operator for this built-in expression.

**Interview takeaway:** Prefer built-in Spark SQL expressions over Python UDFs whenever the required business logic can be expressed using native functions.

# **6. Pandas UDF — Vectorized Processing**

A **Pandas UDF** is a User-Defined Function that processes data in batches using pandas Series instead of calling a normal Python function separately for every record.

It uses Apache Arrow for efficient data transfer between Spark and Python.

### **Standard Python UDF vs. Pandas UDF**

**Standard UDF**

Row-oriented function logic

**Pandas UDF**

Columnar batch processing

The difference is the granularity of the Python computation. A traditional UDF invokes user logic for individual values, while a scalar pandas UDF receives a pandas Series containing a batch of values.

## **Databricks Lab — Pandas UDF**

### **Step 1. Import pandas**

```
import pandas as pdfrom pyspark.sql.functions import pandas_udffrom pyspark.sql.types import StringType
```

### **Step 2. Create the Pandas UDF**

```
@pandas_udf(StringType())def salary_category_pandas(    salary: pd.Series) -> pd.Series:    def classify(value):        if pd.isna(value):            return "Unknown"        if value >= 80000:            return "High"        elif value >= 40000:            return "Medium"        return "Low"    return salary.apply(classify)
```

The key difference is that the input is a `pd.Series`, not a single salary value.

This example is educational. Because `Series.apply()` itself performs element-wise Python work, it does not fully exploit vectorization.

### **Step 3. Apply to the DataFrame**

```
df_pandas = df.withColumn(    "salary_category",    salary_category_pandas(        F.col("salary")    ))display(df_pandas)
```

### **Step 4. Check the execution plan**

```
df_pandas.explain("formatted")
```

You will typically see a physical execution operator like:

```
ArrowEvalPython
```

This shows that Spark is executing a Python function using Arrow-based data transfer.

## **7. Better Pandas UDF — True Vectorization**

Consider a company that awards employees a 10% salary bonus.

Instead of Python `if/else` logic, pandas can perform arithmetic on the entire Series.

```
@pandas_udf("double")def calculate_bonus_pandas(    salary: pd.Series) -> pd.Series:    return salary * 0.10
```

Apply it:

```
df_bonus = df.withColumn(    "bonus",    calculate_bonus_pandas(        F.col("salary")    ))display(df_bonus)
```

Expected output:

| Employee | Salary | Bonus |
| -------- | ------ | ----- |
| Rahul    | 85000  | 8500  |
| Priya    | 45000  | 4500  |
| Amit     | 25000  | 2500  |
| Sneha    | 95000  | 9500  |
| Karan    | 15000  | 1500  |
| Neha     | 55000  | 5500  |

This is vectorized because the multiplication is applied to the entire pandas Series.

However, this particular task does not need a Pandas UDF at all.

### **Recommended native Spark solution**

```
df_bonus_optimized = df.withColumn(    "bonus",    F.col("salary") * F.lit(0.10))display(df_bonus_optimized)
```

For this simple arithmetic operation, the built-in expression generally performs better.

# **8. Arrow-Optimized Python UDF**

Arrow-optimized Python UDFs reduce serialization costs while retaining the ordinary Python function style.

They do not automatically vectorize the Python function's business logic.

```
@F.udf(returnType=T.StringType(), useArrow=True)def salary_category_arrow(salary):    if salary is None:        return "Unknown"    if salary >= 80000:        return "High"    elif salary >= 40000:        return "Medium"    return "Low"
```

Apply:

```
df_arrow = df.withColumn(    "salary_category",    salary_category_arrow(        F.col("salary")    ))display(df_arrow)
```

This requires a Databricks Runtime/Spark version supporting `useArrow=True`, together with compatible Arrow dependencies.

**Distinction to remember:**

- Standard UDF: Python row logic, traditional serialization.
- Arrow-optimized UDF: Python row logic, Arrow serialization.
- Pandas UDF: Python batch logic using pandas and Arrow.

Recent Databricks runtimes also introduce native Arrow UDF capabilities, but those require runtime-specific APIs and should not be assumed available in every Databricks Free or Community environment.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://www.databricks.com\&sz=32)

Databricks Blog+1



# **9. Performance Benchmark — 1 Million Records**

This is one of the most useful practical exercises for understanding UDF performance.

We will compare three implementations of the same calculation:

1. Built-in Spark expression.
2. Standard Python UDF.
3. Pandas UDF.

The test creates its own data and measures actual execution rather than simply defining transformations.

## **Step 1. Generate one million records**

```
from pyspark.sql import functions as Frecords = 1_000_000df_large = (    spark.range(records)    .select(        F.col("id"),        (            F.col("id") % 100000        ).cast("double").alias("salary")    ))print("Total rows:", df_large.count())display(df_large.limit(10))
```

The dataset contains one million records.

## **Step 2. Define all three methods**

### **Method A — Spark built-in expression**

```
df_builtin = df_large.withColumn(    "bonus",    F.col("salary") * F.lit(0.10))
```

### **Method B — Standard Python UDF**

```
@F.udf("double")def bonus_python(salary):    if salary is None:        return None    return salary * 0.10df_python = df_large.withColumn(    "bonus",    bonus_python(F.col("salary")))
```

### **Method C — Pandas UDF**

```
import pandas as pd@F.pandas_udf("double")def bonus_pandas(    salary: pd.Series) -> pd.Series:    return salary * 0.10df_pandas_benchmark = df_large.withColumn(    "bonus",    bonus_pandas(F.col("salary")))
```

## **Step 3. Create a proper benchmarking function**

Spark uses lazy evaluation. Merely creating the DataFrame does not execute the transformation.

Also, this is an important mistake to avoid:

```
# Not a valid measurement of the bonus calculationdf_python.count()
```

Spark may prune the unused `bonus` column because the count does not depend on its value.

Instead, use an aggregation that actually consumes the calculated bonus.

```
import timedef benchmark(df, name):    start = time.perf_counter()    result = (        df.agg(            F.sum("bonus").alias("total_bonus")        )        .collect()[0]["total_bonus"]    )    duration = time.perf_counter() - start    print(        f"{name}: {duration:.3f} seconds, "        f"Result: {result}"    )    return (name, duration, result)
```

## **Step 4. Execute all benchmarks**

```
results = []results.append(    benchmark(        df_builtin,        "Spark Built-in"    ))results.append(    benchmark(        df_python,        "Python UDF"    ))results.append(    benchmark(        df_pandas_benchmark,        "Pandas UDF"    ))
```

## **Step 5. Display performance results**

```
results_df = spark.createDataFrame(    results,    ["method", "seconds", "total_bonus"])display(results_df.orderBy("seconds"))
```

All three methods should produce the same aggregate bonus total, subject to floating-point precision.

Expected aggregate

```
4,999,950,000.0
```

The running times will depend on your cluster configuration, Databricks Runtime, available CPU and memory, Python worker startup, and other active workloads.

Do not assume a fixed speedup.

## **Step 6. Calculate performance ratios**

```
timings = {    name: duration    for name, duration, _ in results}baseline = timings["Spark Built-in"]for name, duration in timings.items():    ratio = duration / baseline    print(        f"{name}: {ratio:.2f}x "        "the built-in execution time"    )
```

A result such as `3.00x` means that method took three times as long as the built-in implementation in that run.

### **Example of how to interpret your results**

Illustrative timings only — not measured results

0 s4 s8 s12 s16 sBuilt-inPandas UDFPython UDF

Example values for understanding benchmark output; actual results vary.

In this hypothetical example, the built-in expression is six times faster than the traditional Python UDF.

A more rigorous experiment should warm up execution, run each method multiple times, use the same cluster resources, rotate the measurement order, and compare median timings rather than relying on one run.

# **10. Real-World Databricks Use Case — Data Cleaning**

Suppose an e-commerce company receives customer names in inconsistent formats:

```
"  rahul kumar  "
"PRIYA SHARMA"
"  amit singh"
```

Our requirement is to remove unnecessary whitespace and standardize names to title case.

### **Create sample DataFrame**

```
customers = [    (1, "  rahul kumar  "),    (2, "PRIYA SHARMA"),    (3, "  amit singh"),    (4, "NEHA GUPTA  "),    (5, None)]df_customers = spark.createDataFrame(    customers,    ["customer_id", "customer_name"])display(df_customers)
```

### **Approach A — Python UDF**

```
@F.udf("string")def clean_customer_name(name):    if name is None:        return None    return name.strip().title()df_clean_udf = df_customers.withColumn(    "clean_name",    clean_customer_name(        F.col("customer_name")    ))display(df_clean_udf)
```

### **Approach B — Built-in Spark functions**

```
df_clean_native = df_customers.withColumn(    "clean_name",    F.initcap(        F.trim(            F.col("customer_name")        )    ))display(df_clean_native)
```

Expected output for this dataset:

| customer_id | clean_name   |
| ----------- | ------------ |
| 1           | Rahul Kumar  |
| 2           | Priya Sharma |
| 3           | Amit Singh   |
| 4           | Neha Gupta   |
| 5           | null         |

Both approaches produce equivalent results for these examples, although Python's `title()` and Spark's `initcap()` can behave differently for certain punctuation and Unicode strings.

**Production recommendation:** Use built-in expressions for cleaning operations such as trimming, capitalization, replacing characters, extracting substrings, parsing dates and handling null values.

# **11. Real-World Use Case — Complex Custom Business Rules**

UDFs are useful when business rules require custom Python libraries or algorithms that cannot reasonably be expressed using built-in Spark functions.

For example, a company may want to calculate a risk score from multiple customer attributes.

```
risk_data = [    (101, 950000, 2, 780),    (102, 120000, 8, 540),    (103, 500000, 4, 690),    (104, 250000, 6, 610)]df_risk = spark.createDataFrame(    risk_data,    [        "customer_id",        "annual_income",        "existing_loans",        "credit_score"    ])
```

Define a reusable function:

```
@F.udf("string")def calculate_risk(    income,    loans,    score):    if (        income is None        or loans is None        or score is None    ):        return "Unknown"    risk_points = 0    if income < 300000:        risk_points += 2    if loans > 5:        risk_points += 2    if score < 600:        risk_points += 3    if risk_points >= 5:        return "High Risk"    elif risk_points >= 2:        return "Medium Risk"    return "Low Risk"
```

Apply the UDF to multiple columns:

```
df_risk_result = df_risk.withColumn(    "risk_category",    calculate_risk(        F.col("annual_income"),        F.col("existing_loans"),        F.col("credit_score")    ))display(df_risk_result)
```

Expected results:

| Customer | Risk category |
| -------- | ------------- |
| 101      | Low Risk      |
| 102      | High Risk     |
| 103      | Low Risk      |
| 104      | Medium Risk   |

This demonstrates how a UDF can accept multiple columns.

However, even this example can be expressed with `when()` and arithmetic. A native Spark implementation would generally be preferable for a high-volume production pipeline.

# **12. How to Optimize UDF Performance in Databricks**

| Optimization technique                        | Why it helps                                              |
| --------------------------------------------- | --------------------------------------------------------- |
| Prefer built-in functions                     | Uses Spark's optimized execution engine                   |
| Use Pandas UDFs for suitable vectorized logic | Reduces Python call and serialization overhead            |
| Consider Arrow optimization                   | Improves data transfer efficiency                         |
| Filter early                                  | Reduces the number of rows requiring expensive processing |
| Select necessary columns                      | Limits unnecessary data movement                          |
| Avoid external API calls per row              | Prevents network latency and rate-limit bottlenecks       |
| Avoid loading heavy models per row            | Reduces repeated initialization work                      |
| Profile execution plans                       | Helps identify Python execution boundaries                |
| Test on representative data                   | Exposes real serialization, CPU and memory costs          |

## **Filter before applying a UDF**

Less efficient:

```
df_result = (    df_large    .withColumn(        "bonus",        bonus_python("salary")    )    .filter(F.col("salary") > 50000))
```

Preferred logical structure:

```
df_result = (    df_large    .filter(F.col("salary") > 50000)    .withColumn(        "bonus",        bonus_python("salary")    ))
```

This makes the intent explicit: filter the dataset first, then apply expensive custom logic only to qualifying records. Spark may already push an eligible filter ahead of the UDF, but it is still important to inspect the physical execution plan.

# **13. Interview Questions and Answers**

**Q1. What is a UDF in PySpark?**

A UDF is a custom function that allows developers to implement business logic not readily available through Spark built-in functions.

**Q2. Why are Python UDFs slower than native Spark functions?**

Python UDFs can introduce serialization costs, JVM-to-Python data transfer, extra Python worker processing and limitations on Catalyst optimization.

**Q3. What is the difference between a normal Python UDF and a Pandas UDF?**

A standard Python UDF applies ordinary Python logic to individual records. A Pandas UDF receives batches of values as pandas Series, using Apache Arrow for transfer and supporting vectorized operations.

**Q4. Are Pandas UDFs always faster than Python UDFs?**

No. Performance depends on the complexity of the computation, the dataset, batch sizes, runtime and whether the pandas implementation is actually vectorized. For simple operations, built-in Spark functions are often faster than either.

**Q5. What is Apache Arrow?**

Apache Arrow is a cross-language, columnar in-memory data format designed for efficient analytical processing and data exchange. Spark can use it to reduce serialization overhead between JVM and Python processes.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

PySpark 4.3.0 documentation



**Q6. How can you identify UDF execution in a Spark physical plan?**

Use:

```
df.explain("formatted")
```

Look for operators such as `BatchEvalPython` and `ArrowEvalPython`, depending on the UDF type and runtime.

**Q7. Does using a UDF automatically cause a shuffle?**

No. A UDF introduces computation and potentially serialization overhead, but does not inherently require a shuffle. Shuffles typically arise from operations such as joins, aggregations and repartitioning.

**Q8. Can UDFs be used inside Spark SQL?**

Yes. You can register a Python UDF in the current Spark session.

```
spark.udf.register(    "salary_category_sql",    salary_category,    T.StringType())df.createOrReplaceTempView("employees")spark.sql("""    SELECT        emp_id,        emp_name,        salary,        salary_category_sql(salary)            AS salary_category    FROM employees""").show()
```

This registration creates a session-scoped function, not a permanent Unity Catalog function.

## **14. Quick Revision**

### **Which approach should you use?**

Choose a processing requirement to see the recommended approach.

Simple calculations or conditions

Custom batch calculations using pandas

Existing row-level Python function

Reusable SQL business rules

**Built-in Spark expressions**

Arithmetic, CASE WHEN, strings and dates can usually be optimized directly by Spark.

The most important principle for Databricks production development is:

**Built-in Spark functions first → vectorized or Arrow-based UDFs when justified → traditional Python UDFs when custom Python logic is necessary.**

The goal is not to avoid UDFs completely. It is to use the right execution method for the business requirement, and benchmark the real performance impact before deploying large-scale pipelines.
