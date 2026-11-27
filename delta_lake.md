# **Delta Lake in Databricks**

## **1. What is Delta Lake?**

**Delta Lake is an open-source transactional storage layer that brings database-like reliability, consistency, and performance to a data lake.**

It extends Apache Parquet files by introducing a transaction log (`_delta_log`), which keeps track of changes made to a table.

Delta Lake provides capabilities such as:

- ACID transactions
- Insert, Update, Delete and Merge operations
- Schema enforcement and evolution
- Time travel and data versioning
- Concurrent read and write operations
- Batch and streaming data processing
- Storage and query optimization

Delta Lake is the default table format in Databricks.

## **2. Why do we need Delta Lake?**

Before understanding Delta Lake, consider a traditional data lake where organizations store data in cloud storage such as Amazon S3, Azure Data Lake Storage or Google Cloud Storage.

### **Traditional data lake vs Delta Lake**

Delta Lake adds a transactional management layer over Parquet data files stored in cloud storage.

Suppose Amazon maintains an e-commerce orders dataset containing 500 million records in Parquet files.

The business wants to update cancelled orders, delete invalid records, insert new orders, recover accidentally deleted data and allow multiple pipelines to write concurrently.

With plain Parquet, implementing these capabilities reliably requires significant custom engineering. Delta Lake provides these capabilities at the table level.

## **3. Delta Lake architecture**

Delta Lake primarily consists of two components:

**Delta Lake = Parquet data files + Transaction log**

Simplified Delta Lake physical storage architecture. Transaction log filenames are abbreviated for readability.

### **A. Parquet data files**

These contain the actual business data in a compressed, columnar format.

For example:

| order_id | customer | amount | status    |
| -------- | -------- | ------ | --------- |
| 101      | Rahul    | 5000   | Delivered |
| 102      | Amit     | 3000   | Pending   |
| 103      | Priya    | 7000   | Delivered |

### **B. Transaction log (**`_delta_log`**)**

The transaction log is the most important component of Delta Lake. It acts as the source of truth for the table's state.

It maintains information about:

- Which data files belong to the current table version.
- Which files have been added or logically removed.
- Table schema and metadata.
- Transaction history.
- Table protocol and supported features.
- File-level statistics used for query optimization.

Whenever a successful write transaction occurs, Delta Lake creates a new table version.

## **4. How does Delta Lake work internally?**

Suppose we create an orders table, insert records, update an order and delete another order.

### **Transaction lifecycle**

V0

**CREATE TABLE**

Initial table metadata and schema created

V1

**INSERT**

New data files added to the table

File A

File B

V2

**UPDATE**

File A logically removed; replacement File C added

File C

File B

V3

**DELETE**

File B logically removed; replacement File D added

File C

File D

Illustrative copy-on-write workflow. Modern Delta tables may also use deletion vectors to avoid immediately rewriting entire affected files.

Old data files may remain physically available until they become eligible for removal through `VACUUM`.

This is what allows Delta Lake to query previous versions of a table using time travel.

## **5. Parquet vs Delta Lake**

| Feature            | Plain Parquet              | Delta Lake             |
| ------------------ | -------------------------- | ---------------------- |
| Columnar storage   | Yes                        | Yes                    |
| Compression        | Yes                        | Yes                    |
| Transaction log    | No                         | Yes                    |
| ACID transactions  | No                         | Yes                    |
| UPDATE and DELETE  | Custom rewriting           | Native support         |
| MERGE / Upsert     | Custom implementation      | Native support         |
| Time travel        | No native support          | Yes                    |
| Schema enforcement | Limited                    | Yes                    |
| Schema evolution   | Manual or engine-dependent | Supported              |
| Concurrent writes  | Custom coordination        | Transaction management |
| Audit history      | No native support          | Yes                    |
| Change Data Feed   | No                         | Supported              |

**Remember:** Delta Lake does not replace Parquet as the underlying data file format. It adds transactional table management capabilities on top of it.

## **6. Ready-to-execute Databricks notebook**

Create a new SQL notebook in Databricks. This example uses your current catalog and schema, where you need permission to create tables.

### **Cell 1: Create a Delta table**

```
%sql
CREATE TABLE IF NOT EXISTS delta_orders_demo (
    order_id INT,
    customer_name STRING,
    amount DOUBLE,
    status STRING
)
USING DELTA;
```

`USING DELTA` explicitly specifies the table format.

### **Cell 2: Insert data**

```
%sql
INSERT INTO delta_orders_demo VALUES
(101, 'Rahul', 5000, 'Delivered'),
(102, 'Amit', 3000, 'Pending'),
(103, 'Priya', 7000, 'Delivered'),
(104, 'Neha', 4000, 'Pending');
```

### **Cell 3: Read the data**

```
%sql
SELECT * FROM delta_orders_demo
ORDER BY order_id;
```

Expected output on the first run:

| order_id | customer_name | amount | status    |
| -------- | ------------- | ------ | --------- |
| 101      | Rahul         | 5000   | Delivered |
| 102      | Amit          | 3000   | Pending   |
| 103      | Priya         | 7000   | Delivered |
| 104      | Neha          | 4000   | Pending   |

### **Cell 4: Update a record**

```
%sql
UPDATE delta_orders_demo
SET status = 'Delivered'
WHERE order_id = 102;
```

### **Cell 5: Delete a record**

```
%sql
DELETE FROM delta_orders_demo
WHERE order_id = 104;
```

### **Cell 6: Verify the changes**

```
%sql
SELECT * FROM delta_orders_demo
ORDER BY order_id;
```

Expected output:

| order_id | customer_name | amount | status    |
| -------- | ------------- | ------ | --------- |
| 101      | Rahul         | 5000   | Delivered |
| 102      | Amit          | 3000   | Delivered |
| 103      | Priya         | 7000   | Delivered |

### **Cell 7: View transaction history**

```
%sql
DESCRIBE HISTORY delta_orders_demo;
```

Expected simplified output for a newly created table:

| Version | Operation    |
| ------- | ------------ |
| 3       | DELETE       |
| 2       | UPDATE       |
| 1       | WRITE        |
| 0       | CREATE TABLE |

Every successful modifying transaction creates a new table version.

### **Cell 8: Perform time travel**

```
%sql
SELECT *
FROM delta_orders_demo VERSION AS OF 1
ORDER BY order_id;
```

Expected output:

| order_id | customer_name | amount | status    |
| -------- | ------------- | ------ | --------- |
| 101      | Rahul         | 5000   | Delivered |
| 102      | Amit          | 3000   | Pending   |
| 103      | Priya         | 7000   | Delivered |
| 104      | Neha          | 4000   | Pending   |

Notice that the deleted record is visible again, and Amit's order status is still Pending because we are reading an older snapshot.

### **Cell 9: Inspect Delta table metadata**

```
%sql
DESCRIBE DETAIL delta_orders_demo;
```

This returns metadata such as:

| Field            | Significance                    |
| ---------------- | ------------------------------- |
| format           | Table format, Delta             |
| id               | Unique table identifier         |
| location         | Physical storage location       |
| numFiles         | Number of active data files     |
| sizeInBytes      | Size of active data files       |
| partitionColumns | Partitioning columns            |
| properties       | Table configuration             |
| minReaderVersion | Minimum reader protocol version |
| minWriterVersion | Minimum writer protocol version |

The notebook assumes a fresh table and has not been executed against your workspace. Repeated execution of the INSERT cell will add duplicate records, and version numbers will change.

## **7. Delta Lake in Medallion architecture**

Delta Lake is commonly used across all three layers of the Databricks Medallion architecture.

| Layer  | Delta Lake use case                         |
| ------ | ------------------------------------------- |
| Bronze | Reliable raw data ingestion                 |
| Silver | Deduplication, validation and upserts       |
| Gold   | Business aggregations and optimized queries |

## **8. Alternatives to Delta Lake**

| Technology        | Description                                                      |
| ----------------- | ---------------------------------------------------------------- |
| Delta Lake        | Open table format with strong Databricks and Spark integration   |
| Apache Iceberg    | Open table format with broad multi-engine interoperability       |
| Apache Hudi       | Open table format focused on incremental processing and upserts  |
| Plain Parquet     | Columnar file format without transactional table management      |
| Traditional RDBMS | Transactional database with its own storage and execution engine |

## **9. Is Delta Lake a database, storage or file format?**

This is a common interview question.

| Component        | Responsibility                               |
| ---------------- | -------------------------------------------- |
| Amazon S3 / ADLS | Physical storage                             |
| Parquet          | Data file format                             |
| Delta Lake       | Transactional storage layer and table format |
| Apache Spark     | Data processing engine                       |
| Unity Catalog    | Metadata, governance and access control      |
| Databricks       | Lakehouse platform                           |

Delta Lake is not a standalone database, physical storage service or compute engine.

## **10. Real-world use cases**

| Industry         | Use case                             | Delta feature               |
| ---------------- | ------------------------------------ | --------------------------- |
| E-commerce       | Updating order status                | UPDATE, MERGE               |
| Banking          | Maintaining transaction consistency  | ACID                        |
| Healthcare       | Validating incoming records          | Schema enforcement          |
| Retail           | Maintaining inventory                | MERGE                       |
| Data engineering | Recovering accidentally deleted data | Time travel                 |
| Analytics        | Improving query performance          | OPTIMIZE, Liquid clustering |
| CDC pipelines    | Processing incremental changes       | Change Data Feed            |
| Auditing         | Tracking table modifications         | DESCRIBE HISTORY            |

