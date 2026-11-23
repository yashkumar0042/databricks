# **Databricks PySpark: Schema Inference vs. Explicit Schema Definition**

Module 2 — Apache Spark & PySpark Mastery | Detailed notes with practical Databricks labs

## **1. What is a schema in PySpark?**

A **schema** defines the structure of a DataFrame. It tells Apache Spark what columns exist, what data types they contain, and whether they can contain null values.

For example, consider an employee dataset:

| employee_id | name  | salary   | joining_date |
| ----------- | ----- | -------- | ------------ |
| 101         | Amit  | 50000.50 | 2025-01-10   |
| 102         | Priya | 65000.00 | 2025-02-15   |
| 103         | Rahul | 72000.75 | 2025-03-20   |

Spark needs to understand that `employee_id` is an integer, `name` is text, `salary` is a decimal number, and `joining_date` represents a date.

An appropriate schema would look like this:

```
root
 |-- employee_id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: double (nullable = true)
 |-- joining_date: date (nullable = true)
```

There are two major ways to define this structure:

- **Schema inference:** Spark automatically determines column names and data types from the input data.
- **Explicit schema:** We manually define the data types and structure before reading the data.

## **2. Schema inference**

### **Definition**

Schema inference is the process by which Apache Spark automatically determines the column names and their data types by inspecting the input data.

For example:

```
df = (    spark.read    .option("header", "true")    .option("inferSchema", "true")    .csv("/path/to/employees.csv"))
```

Spark reads the CSV, inspects the data, and attempts to determine the appropriate data types.

### **How schema inference works**

**Source file — CSV / JSON**

Employee records

**Spark reads and inspects input**

Evaluates values and possible data types

**Spark infers schema**

String, Integer, Double, Date, etc.

**DataFrame created with inferred types**

### **Advantages**

- Requires less code.
- Useful for quickly exploring unknown datasets.
- Helpful during development and experimentation.
- Automatically discovers new columns when a new DataFrame is read.

### **Disadvantages**

- Schema inference can require an additional read of source data.
- Column types can change when input values change.
- Unexpected values may produce undesirable data types.
- Less predictable for production ETL pipelines.

For example, an employee ID such as `00101` may be inferred as a numeric type, losing the leading zeros. Even though it contains digits, it may be more appropriate to represent it as a string.

## **3. Explicit schema definition**

### **Definition**

An explicit schema is a predefined structure that a developer provides to Spark before loading data.

Instead of allowing Spark to guess the types, we define them using `StructType` and `StructField`.

### **Example**

```
from pyspark.sql.types import (    StructType,    StructField,    IntegerType,    StringType,    DoubleType,    DateType)employee_schema = StructType([    StructField("employee_id", IntegerType(), True),    StructField("name", StringType(), True),    StructField("salary", DoubleType(), True),    StructField("joining_date", DateType(), True)])
```

Now read the file:

```
df = (    spark.read    .schema(employee_schema)    .option("header", "true")    .option("dateFormat", "yyyy-MM-dd")    .csv("/path/to/employees.csv"))df.printSchema()display(df)
```

Spark uses the schema we supply instead of inferring it from the source.

### **Understanding&#x20;**`StructType`**&#x20;and&#x20;**`StructField`

Anatomy of a PySpark schema

StructType([

　StructField("salary", DoubleType(), True)

])

**StructType**

Represents the complete schema containing multiple fields.

**StructField**

Defines one column inside the schema.

**"salary"**

Column name.

**DoubleType()**

Column data type.

**True**

The field is declared nullable, meaning nulls are permitted in the schema.

Importantly, `nullable=False` expresses a non-nullable schema requirement, but it does not automatically provide comprehensive source-data validation for every file-reading scenario. Production pipelines should explicitly validate required fields.

## **4. Schema inference vs. explicit schema comparison**

| Feature                   | Schema inference       | Explicit schema      |
| ------------------------- | ---------------------- | -------------------- |
| Data types                | Automatically detected | Developer-defined    |
| Initial coding effort     | Low                    | Moderate             |
| Predictability            | Depends on input data  | Consistent contract  |
| Additional inference cost | May be required        | Avoided              |
| Production reliability    | Less predictable       | Generally preferred  |
| Unknown input structure   | Convenient             | Requires design      |
| Data type control         | Limited                | Full                 |
| Best use case             | Exploration            | Production pipelines |

**Recommendation:** Use inference to investigate unfamiliar datasets, but prefer explicit schemas for production ingestion, especially when downstream transformations depend on stable data types. Databricks confirms that providing a schema can avoid the inference step.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on Google Cloud+1



# **Part 2 — Ready-to-execute Databricks notebook labs**

The following labs use only Python and the Spark session already available in a Databricks notebook.

They are designed to work on Databricks serverless compute without directly accessing `spark.sparkContext`, the JVM, or DBFS file-writing utilities.

## **Lab 1 — Create sample employee records**

Cell 1 — Python

First create a sample dataset in Python. No uploads are required.

```
import jsonfrom pyspark.sql import functions as Ffrom pyspark.sql import types as Temployees = [    {        "employee_id": 101,        "name": "Amit",        "department": "IT",        "salary": 50000.50,        "joining_date": "2025-01-10",        "age": 25    },    {        "employee_id": 102,        "name": "Priya",        "department": "HR",        "salary": 65000.00,        "joining_date": "2025-02-15",        "age": 28    },    {        "employee_id": 103,        "name": "Rahul",        "department": "Finance",        "salary": 72000.75,        "joining_date": "2025-03-20",        "age": 30    }]# Convert each dictionary into a JSON record.json_records = [    json.dumps(row) for row in employees]
```

## **Lab 2 — Read JSON with schema inference**

Cell 2 — Python

Spark can infer the schema of JSON strings. In this lab, we first create a DataFrame of JSON strings, then parse them with `from_json`.

```
# Create a DataFrame containing JSON stringsraw_df = spark.createDataFrame(    [(record,) for record in json_records],    ["json_data"])# Infer the structure using Spark's JSON reader.inferred_df = (    spark.read.json(        raw_df.select("json_data").asNondeterministic()        if False else json_records    ))
```

For serverless compatibility, the code above is better replaced with the following implementation, which uses SQL expressions and avoids RDD-based operations.

```
# Infer a schema from the sample JSON records.# This is appropriate for exploring the sample structure.inferred_schema_ddl = spark.range(1).select(    F.schema_of_json(F.lit(json_records[0])).alias("schema")).first()["schema"]print("Inferred schema:", inferred_schema_ddl)# Parse all JSON records with the inferred schema.raw_df = spark.createDataFrame(    [(record,) for record in json_records],    ["json_data"])inferred_df = raw_df.select(    F.from_json(        F.col("json_data"),        inferred_schema_ddl    ).alias("employee")).select("employee.*")inferred_df.printSchema()display(inferred_df)
```

This approach infers the structure from the first sample record. For real files, `spark.read.json(path)` can inspect input records across files to infer a schema.

Expected schema (representative)

```
root
 |-- age: long (nullable = true)
 |-- department: string (nullable = true)
 |-- employee_id: long (nullable = true)
 |-- joining_date: string (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: double (nullable = true)
```

Notice that `joining_date` is a string because JSON itself doesn't contain a dedicated date data type.

## **Lab 3 — Create an explicit schema using StructType**

Cell 3 — Python

```
employee_schema = T.StructType([    T.StructField(        "employee_id", T.IntegerType(), False    ),    T.StructField(        "name", T.StringType(), True    ),    T.StructField(        "department", T.StringType(), True    ),    T.StructField(        "salary", T.DoubleType(), True    ),    T.StructField(        "joining_date", T.StringType(), True    ),    T.StructField(        "age", T.IntegerType(), True    )])print("Explicit schema:")print(employee_schema.simpleString())
```

### **Explanation**

We explicitly set `employee_id` and `age` to `IntegerType`, rather than the `LongType` that JSON number inference commonly produces.

We also explicitly define a non-nullable employee ID. This communicates our intended schema contract.

## **Lab 4 — Apply the explicit schema to JSON records**

Cell 4 — Python

```
explicit_df = (    raw_df    .select(        F.from_json(            F.col("json_data"),            employee_schema        ).alias("employee")    )    .select("employee.*"))explicit_df.printSchema()display(explicit_df)# Convert the joining date to a real DateTypeexplicit_df = explicit_df.withColumn(    "joining_date",    F.to_date("joining_date", "yyyy-MM-dd"))explicit_df.printSchema()display(explicit_df)
```

With this approach, Spark uses the exact column types provided in `employee_schema`. The date conversion is performed separately.

**Practical point:** `from_json()` is for parsing JSON strings contained in DataFrame columns. `spark.read.schema(...).json(path)` is for reading JSON files directly from storage. Both approaches accept a `StructType` schema.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

PySpark 4.3.0 documentation+1



## **5. Lab 5 — Create an explicit schema using a JSON file**

This is particularly useful in real-world Databricks projects.

Suppose your organization has 20 ETL notebooks that process employee data. Instead of defining the same schema in every notebook, you can maintain one central JSON schema file and reuse it.

**Example project structure**

employee_pipeline/

`employee_schema.json` — Reusable schema

`employees.json` — Source records

`employee_etl` — Databricks notebook

I've also created both sample files for your practice.

**Databricks schema practice files**

Includes employee_schema.json and employees.json

Ready

Download both JSON files (ZIP)

&#x20;Schema JSON

&#x20;Employee data JSON

### **Step 1 — Understand the schema JSON file**

A Spark-compatible schema JSON is different from an ordinary employee data JSON file. It describes the columns and their data types rather than containing employee records.

The file `employee_schema.json` contains:

```

{
  "type": "struct",
  "fields": [
    {
      "name": "employee_id",
      "type": "integer",
      "nullable": false,
      "metadata": {}
    },
    {
      "name": "name",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "department",
      "type": "string",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "salary",
      "type": "double",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "joining_date",
      "type": "date",
      "nullable": true,
      "metadata": {}
    },
    {
      "name": "age",
      "type": "integer",
      "nullable": true,
      "metadata": {}
    }
  ]
}

```

Each property has a specific meaning:

| JSON property    | Meaning                              |
| ---------------- | ------------------------------------ |
| `type: "struct"` | Represents the whole Spark structure |
| `fields`         | List of columns                      |
| `name`           | Column name                          |
| `type`           | Spark data type                      |
| `nullable`       | Whether null values are permitted    |
| `metadata`       | Optional field-level information     |

This is the native JSON representation accepted by PySpark's `StructType.fromJson()`, not a generic JSON Schema document using `properties` and `required`.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://spark.apache.org\&sz=32)

PySpark 4.2.0 documentation+1



### **Step 2 — Convert a JSON schema into a PySpark StructType**

The most important function is:

```
T.StructType.fromJson(schema_dictionary)
```

It takes a Python dictionary containing a Spark-compatible JSON schema definition and returns a `StructType` object.

Cell 5 — Python, runs without uploading a file

```
import jsonfrom pyspark.sql import types as T# Define a Spark schema in JSON formatschema_json = """{  "type": "struct",  "fields": [    {      "name": "employee_id",      "type": "integer",      "nullable": false,      "metadata": {}    },    {      "name": "name",      "type": "string",      "nullable": true,      "metadata": {}    },    {      "name": "department",      "type": "string",      "nullable": true,      "metadata": {}    },    {      "name": "salary",      "type": "double",      "nullable": true,      "metadata": {}    },    {      "name": "joining_date",      "type": "date",
```

### **Explanation of the conversion**

**JSON text**

The schema definition

json.loads()

**Python dictionary**

StructType.fromJson()

**PySpark StructType**

spark.read.schema()

**DataFrame with explicit schema**

## **6. Lab 6 — How to upload the schema JSON file into Databricks**

In Unity Catalog-enabled workspaces, a **Volume** is an appropriate location to store reusable data and schema files.

1. Download and extract the ZIP file provided above.
2. Open your Databricks workspace and go to **Catalog**.
3. Select a catalog and schema where you have access to a Volume, or create a Volume if you have the required permissions.
4. Open the Volume and use its file-upload option to upload `employee_schema.json` and `employees.json`.
5. Copy the exact file paths from the Databricks interface.

For this example, suppose you upload both files to a Volume at:

```
/Volumes/main/default/employee_files/
    employee_schema.json
    employees.json
```

`main`, `default`, and `employee_files` are example names. Replace them with your actual catalog, schema, and volume.

### **Step 1 — Read the schema JSON file**

Cell 6 — Python

```
import jsonfrom pyspark.sql.types import StructType# Update to your actual Unity Catalog Volume.schema_file_path = (    "/Volumes/main/default/employee_files/"    "employee_schema.json")# Read the schema JSON file from the Volumewith open(schema_file_path, "r", encoding="utf-8") as f:    schema_definition = json.load(f)# Convert the JSON dictionary into StructTypeemployee_file_schema = StructType.fromJson(    schema_definition)print("Loaded schema from JSON file:")print(employee_file_schema.simpleString())
```

### **Step 2 — Load employee records using that schema**

Cell 7 — Python

```
data_file_path = (    "/Volumes/main/default/employee_files/"    "employees.json")# Read the JSON data using the external schemadf_employees = (    spark.read    .schema(employee_file_schema)    .json(data_file_path))# View DataFrame schemadf_employees.printSchema()# View the datadisplay(df_employees)
```

Expected result:

| employee_id | name  | department | salary   | joining_date | age |
| ----------- | ----- | ---------- | -------- | ------------ | --- |
| 101         | Amit  | IT         | 50000.5  | 2025-01-10   | 25  |
| 102         | Priya | HR         | 65000.0  | 2025-02-15   | 28  |
| 103         | Rahul | Finance    | 72000.75 | 2025-03-20   | 30  |

In this example, `joining_date` is directly loaded as Spark `DateType` because the schema file defines it as a date and the source values use a supported date representation.

### **Step 3 — Perform transformations**

Cell 8 — Python

```
from pyspark.sql import functions as F# Add 10% salary bonusdf_with_bonus = df_employees.withColumn(    "bonus",    F.round(F.col("salary") * 0.10, 2))# Calculate total compensationdf_with_bonus = df_with_bonus.withColumn(    "total_compensation",    F.round(        F.col("salary") + F.col("bonus"),        2    ))display(df_with_bonus)# Filter employees who earn more than 60000display(    df_with_bonus.filter(        F.col("salary") > 60000    ))# Group by departmentdisplay(    df_with_bonus    .groupBy("department")    .agg(        F.count("*").alias("employee_count"),        F.sum("salary").alias("total_salary"),        F.avg("salary").alias("average_salary")    )
```

This is a complete basic ingestion flow: external schema → JSON data → DataFrame → transformations.

## **7. Lab 7 — Automatically generate a reusable JSON schema from a DataFrame**

Another practical requirement is to create a schema JSON file from an existing DataFrame.

For example, you may already have a DataFrame with a schema that you want other notebooks or jobs to reuse.

PySpark provides two useful methods:

```
df.schema.json()df.schema.jsonValue()
```

- `json()` returns the Spark schema serialized as JSON text.
- `jsonValue()` returns its dictionary representation.

Cell 9 — Python

```
# Use the explicit DataFrame created in Lab 4.# Its joining_date is already DateType.# Convert DataFrame schema to JSON textschema_as_json = explicit_df.schema.json()print(schema_as_json)# Convert DataFrame schema to a Python dictionaryschema_as_dictionary = (    explicit_df.schema.jsonValue())print(    json.dumps(        schema_as_dictionary,        indent=2    ))# Recreate a StructType using the dictionaryreconstructed_schema = T.StructType.fromJson(    schema_as_dictionary)print(reconstructed_schema.simpleString())# Check that the structure has been preservedassert reconstructed_schema == explicit_df.schemaprint("Schema round-trip successful")
```

### **Save the generated schema to a JSON file**

Cell 10 — Python; requires a writable Volume

```
import jsonoutput_schema_file = (    "/Volumes/main/default/employee_files/"    "generated_employee_schema.json")with open(    output_schema_file, "w", encoding="utf-8") as f:    json.dump(        explicit_df.schema.jsonValue(),        f,        indent=2    )print("Schema saved to:", output_schema_file)
```

Now other notebooks can use the same generated file by calling `StructType.fromJson()`.

This is useful when you want centralized schema management for multiple data pipelines.

## **8. Lab 8 — Schema inference vs. explicit schema with CSV**

CSV is one of the best ways to understand why explicit schemas matter.

Consider this dataset:

```
employee_id,name,salary,age
00101,Amit,50000.50,25
00102,Priya,65000.00,28
00103,Rahul,72000.75,30
```

The employee ID contains leading zeros.

With schema inference enabled, Spark may interpret the ID as a number and convert `00101` to `101`.

With an explicit schema, we can preserve the ID as a string.

### **Create the CSV data in Databricks**

Cell 11 — Python; no file upload needed

```
from pyspark.sql import functions as Ffrom pyspark.sql import types as Tcsv_rows = [    ("00101", "Amit", "50000.50", "25"),    ("00102", "Priya", "65000.00", "28"),    ("00103", "Rahul", "72000.75", "30")]# Simulate raw CSV data already loaded as stringsraw_csv_df = spark.createDataFrame(    csv_rows,    ["employee_id", "name", "salary", "age"])display(raw_csv_df)raw_csv_df.printSchema()
```

All four columns are strings because they were supplied as strings.

### **Apply an explicit schema through transformations**

Cell 12 — Python

```
csv_explicit_df = raw_csv_df.select(    F.col("employee_id").cast("string"),    F.col("name").cast("string"),    F.col("salary").cast("double"),    F.col("age").cast("integer"))csv_explicit_df.printSchema()display(csv_explicit_df)
```

Expected schema:

```
root
 |-- employee_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- salary: double (nullable = true)
 |-- age: integer (nullable = true)
```

The ID retains its leading zeros.

### **Read a real CSV with an explicit schema**

If you upload a CSV file to your Databricks Volume, use:

```
csv_schema = T.StructType([    T.StructField("employee_id", T.StringType()),    T.StructField("name", T.StringType()),    T.StructField("salary", T.DoubleType()),    T.StructField("age", T.IntegerType())])csv_path = (    "/Volumes/main/default/employee_files/"    "employees.csv")csv_df = (    spark.read    .option("header", "true")    .schema(csv_schema)    .csv(csv_path))display(csv_df)
```

Compare this with inferred types:

```
inferred_csv_df = (    spark.read    .option("header", "true")    .option("inferSchema", "true")    .csv(csv_path))inferred_csv_df.printSchema()display(inferred_csv_df)
```

The second example may infer `employee_id` as an integer or long, whereas the explicit schema guarantees that it is interpreted as a string.

## **9. Lab 9 — Working with nested JSON schemas**

Real-world applications often produce complex JSON rather than simple flat records.

For example, an API might return:

```
{
  "employee_id": 101,
  "name": "Amit",
  "address": {
    "city": "Bangalore",
    "state": "Karnataka"
  },
  "skills": ["Python", "SQL", "Databricks"]
}
```

There are two important Spark data types here:

- `StructType`: Represents an object containing nested fields.
- `ArrayType`: Represents a list of values.

### **Create an explicit nested schema**

Cell 13 — Python

```
from pyspark.sql import types as Tfrom pyspark.sql import functions as Fnested_schema = T.StructType([    T.StructField(        "employee_id", T.IntegerType(), True    ),    T.StructField(        "name", T.StringType(), True    ),    T.StructField(        "address",        T.StructType([            T.StructField(                "city", T.StringType(), True            ),            T.StructField(                "state", T.StringType(), True            )        ]),        True    ),    T.StructField(        "skills",        T.ArrayType(T.StringType()),        True    )])sample_json = """{  "employee_id": 101,  "name": "Amit",  "address": {    "city": "Bangalore",
```

Expected schema:

```
root
 |-- employee_id: integer
 |-- name: string
 |-- address: struct
 |    |-- city: string
 |    |-- state: string
 |-- skills: array
 |    |-- element: string
```

### **Extract nested fields**

Cell 14 — Python

```
# Extract fields inside the nested objectflattened_df = nested_df.select(    F.col("employee_id"),    F.col("name"),    F.col("address.city").alias("city"),    F.col("address.state").alias("state"),    F.col("skills"))display(flattened_df)# Convert skill array into separate rowsskills_df = flattened_df.select(    "employee_id",    "name",    F.explode("skills").alias("skill"))display(skills_df)
```

The result of `explode()` is:

| employee_id | name | skill      |
| ----------- | ---- | ---------- |
| 101         | Amit | Python     |
| 101         | Amit | SQL        |
| 101         | Amit | Databricks |

You can also export the nested schema using:

```
print(    json.dumps(        nested_schema.jsonValue(),        indent=2    ))
```

Then save the result as a JSON schema file and reuse it with `StructType.fromJson()`.

## **10. Lab 10 — What happens when data doesn't match the explicit schema?**

This is an important interview and production scenario.

Suppose your salary column is defined as `DoubleType`, but a source record contains `"invalid_salary"`.

A typical ingestion pipeline must detect the invalid value and decide whether to reject, quarantine, or correct the record.

### **Create invalid records and validate them**

Cell 15 — Python

```
from pyspark.sql import functions as Finvalid_data = [    (101, "Amit", "50000.50"),    (102, "Priya", "invalid_salary"),    (103, "Rahul", "72000.75"),    (104, "Neha", None)]raw_salary_df = spark.createDataFrame(    invalid_data,    ["employee_id", "name", "salary_raw"])# try_cast returns null if the value cannot# be converted to the requested data type.validated_df = raw_salary_df.withColumn(    "salary",    F.expr("try_cast(salary_raw AS DOUBLE)"))# Identify invalid non-null input valuesvalidated_df = validated_df.withColumn(    "is_invalid_salary",    F.col("salary_raw").isNotNull()    & F.col("salary").isNull())display(validated_df)
```

Expected output:

| employee_id | salary_raw     | salary   | is_invalid_salary |
| ----------- | -------------- | -------- | ----------------- |
| 101         | 50000.50       | 50000.5  | false             |
| 102         | invalid_salary | null     | true              |
| 103         | 72000.75       | 72000.75 | false             |
| 104         | null           | null     | false             |

Notice the difference between a genuinely missing salary and a salary that cannot be converted.

### **Separate valid and invalid records**

Cell 16 — Python

```
valid_df = validated_df.filter(    ~F.col("is_invalid_salary"))invalid_df = validated_df.filter(    F.col("is_invalid_salary"))print("Valid records:")display(valid_df)print("Invalid records:")display(invalid_df)
```

This pattern is useful for a Bronze-to-Silver ETL pipeline:

**Bronze — Raw employee data**

Ingest source values as received

**Schema validation**

Check types, required fields and business rules

**Silver**

Valid typed records

**Quarantine**

Invalid records

An explicit schema alone should not be treated as a complete data-quality system. JSON and CSV readers have parsing modes such as `PERMISSIVE` and `FAILFAST`, and their behavior also depends on format options and runtime settings. Use explicit validation to make quality decisions reliable.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on Google Cloud+1



## **11. Lab 11 — Schema evolution**

Schema evolution means modifying a data structure when new columns are introduced or existing columns change.

For example, your original employee data may contain:

```
employee_id, name, salary
```

A new version of the source API may introduce:

```
employee_id, name, salary, location
```

With a manually defined schema, you need to decide whether to add the new column.

Cell 17 — Python

```
from pyspark.sql import types as T# Version 1schema_v1 = T.StructType([    T.StructField("employee_id", T.IntegerType()),    T.StructField("name", T.StringType()),    T.StructField("salary", T.DoubleType())])# Version 2: additional location fieldschema_v2 = T.StructType(    schema_v1.fields + [        T.StructField(            "location", T.StringType()        )    ])print("Version 1:")print(schema_v1.simpleString())print("Version 2:")print(schema_v2.simpleString())# Export the new schemaprint(    json.dumps(        schema_v2.jsonValue(),        indent=2    ))
```

When an explicit schema omits a source JSON field, that field is normally not included in the resulting DataFrame. To use new fields, update the schema or select an ingestion strategy that supports evolution.

Databricks Auto Loader supports additional schema-evolution strategies for pipelines that receive changing files over time.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



## **12. Three ways to define a PySpark schema**

These are the three approaches you'll most commonly use.

**Method 1 — Using StructType and StructField**

Most expressive for programmatic schema creation.

```
schema = T.StructType([    T.StructField("id", T.IntegerType()),    T.StructField("name", T.StringType())])
```

**Method 2 — Using DDL syntax**

Concise for simple and moderately complex schemas.

```
schema = "id INT, name STRING, salary DOUBLE"df = spark.read.schema(schema).json(data_file_path)
```

**Method 3 — Loading a schema from JSON**

Useful for sharing a versioned schema across multiple notebooks.

```
import jsonwith open(schema_file_path) as file:    schema_dict = json.load(file)schema = T.StructType.fromJson(schema_dict)df = spark.read.schema(schema).json(data_file_path)
```

### **Which method should you choose?**

| Situation                           | Recommended approach                      |
| ----------------------------------- | ----------------------------------------- |
| Quick data exploration              | Schema inference                          |
| Small, fixed schema                 | DDL syntax                                |
| Complex nested schema               | StructType                                |
| Multiple jobs with shared structure | External schema JSON                      |
| Production ingestion                | Explicit schema with validation           |
| Frequently changing file feeds      | Auto Loader with planned schema evolution |

## **13. Important interview questions**

**Q1. What is schema inference in PySpark?**

It is the automatic determination of column names and data types by Spark based on input records.

**Q2. Why is explicit schema faster?**

For formats requiring schema inference, Spark may need to inspect additional source data before processing. An explicit schema can eliminate this inference step. It doesn't guarantee that every subsequent transformation or query will be faster.

**Q3. What is the difference between StructType and StructField?**

`StructType` represents a complete structured data type containing fields, while `StructField` defines an individual field's name, data type, nullability, and metadata.

**Q4. How can we create a PySpark schema from a JSON file?**

Load the schema file using Python's `json.load()`, then call `StructType.fromJson()` to convert the resulting dictionary into a Spark schema.

**Q5. Can a schema created from JSON be reused?**

Yes. The same schema can be loaded by different Databricks notebooks and used to read JSON, CSV, and other compatible input formats.

**Q6. Is an ordinary JSON data file the same as a JSON schema file?**

No. A data file contains records, whereas a schema file describes the structure of those records. A Spark `StructType` JSON definition is also distinct from standard JSON Schema.

**Q7. Does nullable=False automatically reject every null input?**

No. Nullability is part of the Spark schema contract, but you should implement explicit quality checks and use appropriate storage constraints where strict enforcement is required.

## **14. Quick practice exercise**

Try this to verify your understanding.

1\. Which function converts a Spark-compatible JSON schema dictionary into a StructType?

json.loads()

StructType.fromJson()

spark.read.json()

2\. Which data type should usually be chosen for a code such as 00101 when leading zeros matter?

IntegerType

DoubleType

StringType

3\. Which approach is generally preferable for a production ETL pipeline with a stable contract?

Explicit schema with validation

Infer all types on every execution

Read all columns as DoubleType

## **Final recap**

The key commands to remember are:

```
# 1. Infer schema from a JSON filedf = spark.read.json(data_file_path)# 2. Define explicit schemaschema = T.StructType([    T.StructField("id", T.IntegerType()),    T.StructField("name", T.StringType())])# 3. Apply explicit schemadf = spark.read.schema(schema).json(data_file_path)# 4. Export schema to JSONschema_json = df.schema.json()# 5. Reconstruct schema from a JSON dictionaryschema = T.StructType.fromJson(    json.loads(schema_json))# 6. Read a saved JSON schema filewith open(schema_file_path) as f:    schema = T.StructType.fromJson(json.load(f))
```

One correction for Lab 2: use the **second, serverless-compatible code block** for inference and skip the first `spark.read.json(json_records)` illustration, which is not a reliable way to provide a Python list of JSON strings to the reader.

The most important real-world concept is that **schema inference discovers a structure, whereas explicit schema definition establishes the structure you expect**. For dependable Databricks ETL pipelines, combine the latter with validation, schema versioning, and a strategy for handling invalid records.
