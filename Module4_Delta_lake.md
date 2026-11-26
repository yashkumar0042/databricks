# **Module 4: Delta Lake Deep Dive**


## **1. Module overview**

**Recommended training duration:** 7–9 hours, including practical demonstrations.

| Section                 | Topics                                        | Duration |
| ----------------------- | --------------------------------------------- | -------- |
| Fundamentals            | Parquet limitations, Delta architecture, ACID | 90 min   |
| Data manipulation       | CREATE, INSERT, UPDATE, DELETE, MERGE         | 90 min   |
| Version management      | Time travel, history, auditing                | 45 min   |
| Data quality            | Schema enforcement and evolution, constraints | 60 min   |
| Performance engineering | VACUUM, OPTIMIZE, Z-ORDER, liquid clustering  | 120 min  |
| Advanced concepts       | CDF, deep and shallow cloning                 | 60 min   |

### **Learning objectives**

By the end of this module, participants should be able to:

- Understand how Delta Lake works internally.
- Explain how a data lake provides database-like transactional guarantees.
- Build transactional ETL pipelines using Delta tables.
- Implement incremental data ingestion and upserts.
- Recover historical data using time travel.
- Implement schema validation and data quality rules.
- Optimize large-scale Delta tables.
- Design enterprise-level incremental data pipelines using Change Data Feed.

# **Part 1: Delta Lake fundamentals**

## **2. What is Delta Lake?**

### **Definition**

Delta Lake is an open-source storage framework that extends Apache Parquet with a transaction log, enabling reliable, transactional and scalable data management on top of cloud object storage.

It provides capabilities traditionally associated with relational databases while retaining the scalability and flexibility of a data lake.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on Google Cloud+1



### **Where does Delta Lake fit in Databricks?**

Conceptual architecture: compute, table management and physical storage are separate layers.

### **Understand the responsibilities**

| Component           | Responsibility                                    |
| ------------------- | ------------------------------------------------- |
| Apache Spark        | Distributed data processing                       |
| Parquet             | Columnar physical file format                     |
| Delta Lake          | Transactions, table versions and data reliability |
| AWS S3 / ADLS / GCS | Physical data storage                             |
| Unity Catalog       | Governance, permissions, discovery and metadata   |
| Databricks          | Integrated data and AI platform                   |

### **Important trainer explanation**

Delta Lake is not a replacement for Apache Spark, and it is not a separate database server. Spark processes the data, Parquet stores the data, and Delta Lake manages the transactional state of the table.

A useful conceptual formula is:

\\[ \text{Delta Lake}=\text{Parquet}+\text{Transaction Log}+\text{Table Protocol} \\]

### **Real-world use cases**

- E-commerce order processing.
- Banking and financial transaction analytics.
- Customer master data management.
- Incremental data ingestion from operational databases.
- Large-scale data warehousing.
- Batch and streaming data pipelines.
- Historical reporting and audit compliance.

## **3. Why plain Parquet isn't enough at scale**

### **What is Parquet?**

Apache Parquet is an open-source columnar file format optimized for analytical workloads. It provides compression, column pruning, predicate pushdown and efficient storage of structured data.

### **Row-based vs. columnar storage**

**Row-oriented**

Values belonging to each row are grouped together.

**Columnar (Parquet)**

Values belonging to each column are grouped together.

If a table has 100 columns and your analytical query needs only three, Parquet allows compatible query engines to avoid reading the other 97 columns.

However, Parquet is fundamentally a **file format, not a transactional table management system**.

### **3.1 Problems with plain Parquet**

#### **Problem 1: No table-level atomic transactions**

Suppose an ETL job writes 100 Parquet files to an S3 location. After writing 60 files, the job crashes.

**ETL job progress**

Failed

60 files written

40 files missing

**Plain Parquet directory**

Partial data may become visible to readers, depending on the write mechanism.

**Delta table**

An unsuccessful transaction does not publish a new committed table snapshot.

This is a serious concern in financial reporting, payment reconciliation and inventory management.

#### **Problem 2: Concurrent writes**

Consider two ETL processes writing to the same table.

- Job A updates an order's payment status.
- Job B updates its shipment status.
- Job C reads the table while both jobs are running.

Plain Parquet has no built-in transactional coordination between these operations. Applications must implement their own coordination mechanisms.

Delta Lake provides snapshot-based reads and concurrency control.

#### **Problem 3: No built-in row-level data manipulation**

Parquet files are not designed for in-place row modification.

To update or delete records, applications generally need to determine affected files, generate replacement files and manage the original files safely.

Delta Lake manages those operations at the table level through SQL commands such as `UPDATE`, `DELETE` and `MERGE`.

#### **Problem 4: No native table version history**

Plain Parquet does not natively provide:

- Historical table snapshots.
- Transaction auditing.
- SQL-based time travel.
- Table restoration.

Object storage versioning can help preserve individual files, but it does not automatically provide a consistent historical snapshot of an entire table.

#### **Problem 5: Inconsistent schemas**

One pipeline might write an integer column while another writes a string column with the same name. Plain Parquet files contain their own schemas, but a directory of Parquet files does not inherently enforce one authoritative schema across independent writers.

Delta Lake validates writes against the table schema.

#### **Problem 6: Small-file and metadata problems**

When thousands of pipelines generate small Parquet files, the query engine may spend significant time listing, opening and scheduling those files.

Delta Lake provides transaction-log metadata, file compaction and data-skipping capabilities to manage this more effectively.

### **Parquet vs. Delta Lake**

| Feature                        | Plain Parquet           | Delta Lake              |
| ------------------------------ | ----------------------- | ----------------------- |
| Columnar storage               | Yes                     | Yes                     |
| Compression                    | Yes                     | Yes                     |
| ACID transactions              | No native support       | Yes                     |
| UPDATE and DELETE              | Application-managed     | Native table operations |
| MERGE / UPSERT                 | Application-managed     | Yes                     |
| Time travel                    | No native support       | Yes                     |
| Table-level schema enforcement | No                      | Yes                     |
| Concurrent write coordination  | External implementation | Built-in                |
| Change Data Feed               | No                      | Supported               |
| Transaction history            | No                      | Yes                     |
| File compaction                | Manually implemented    | OPTIMIZE                |

### **Alternatives to Delta Lake**

| Technology        | Primary strength                                             |
| ----------------- | ------------------------------------------------------------ |
| Apache Iceberg    | Open table format with broad multi-engine interoperability   |
| Apache Hudi       | Incremental processing and record-level ingestion            |
| Apache Parquet    | Efficient analytical file storage                            |
| Traditional RDBMS | Low-latency operational transactions                         |
| Delta Lake        | Transactional lakehouse workloads and Databricks integration |

**Trainer discussion:** If Parquet is already fast and compressed, why do we need Delta Lake?

The answer is that Delta Lake primarily solves **table reliability and management problems**, rather than merely introducing another file format.

# **Part 2: Hands-on Databricks notebooks**

## **4. Download the complete practical notebook**

I have prepared a practical notebook containing **28 Markdown and code cells**, covering the topics in this module with generated datasets, SQL commands, PySpark examples, expected results and validation checks.

**Module 4 — Delta Lake Deep Dive**

Databricks native source notebook (.py)

Download Databricks notebook

**Jupyter notebook version**

Notebook with separate explanation and code cells (.ipynb)

Download IPYNB notebook

### **How to import the notebook**

1. Log in to your Databricks workspace.
2. Open **Workspace**.
3. Select **Import** from your workspace folder's menu.
4. Upload the downloaded `.py` or `.ipynb` notebook.
5. Connect to serverless compute or compatible Databricks compute.
6. Execute the notebook cells sequentially.

The notebook uses the built-in Spark session and generated datasets, so no external datasets or package installations are required. It creates a separate training schema to avoid modifying existing tables.

**Compatibility:** I have designed the labs with serverless compute and Databricks Free Edition in mind. Advanced features may depend on runtime, table features, permissions and workspace configuration. The notebook's Python syntax has been checked, but I cannot execute it against a live Databricks workspace here. Outputs shown throughout these notes are expected results, not recorded execution results.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



# **Part 3: Delta Lake architecture**

## **5. The Delta transaction log (\_delta_log)**

### **Definition**

The Delta transaction log is a versioned record of transactions performed on a Delta table. It describes the table's logical state, including which physical files are currently part of the table, its schema, properties and protocol requirements.

It is the central mechanism behind Delta Lake's transactional capabilities.

### **5.1 Physical directory structure**

For a traditional file-based Delta table, the directory structure looks approximately like this:

**orders_delta/**

part-00000-a.parquet

part-00001-b.parquet

part-00002-c.parquet

**\_delta_log/**

00000000000000000000.json

00000000000000000001.json

00000000000000000002.json

…

00000000000000000010.checkpoint.parquet

\_last_checkpoint

Parquet files store records. Transaction-log entries and checkpoints describe the logical table state. Actual filenames and checkpoint formats can vary.

### **5.2 Components of the transaction log**

| Component          | Purpose                                                       |
| ------------------ | ------------------------------------------------------------- |
| JSON commit files  | Store actions associated with transactions                    |
| Checkpoint files   | Consolidate table state to accelerate snapshot reconstruction |
| `_last_checkpoint` | Helps readers locate a recent checkpoint                      |
| `add` action       | Registers a data file in a table snapshot                     |
| `remove` action    | Logically removes a file from the table                       |
| `metaData` action  | Records schema and table configuration                        |
| `protocol` action  | Specifies compatibility requirements for clients              |
| `commitInfo`       | Records information about a transaction                       |
| `txn` action       | Supports application-level transaction identification         |

The Delta protocol specification describes how these actions are used to reconstruct table snapshots.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://github.com\&sz=32)

GitHub+1



### **5.3 Understanding table versions**

Suppose we perform the following operations:

| Version | Operation    | Files added | Files removed | Active files |
| ------- | ------------ | ----------- | ------------- | ------------ |
| V0      | CREATE TABLE | —           | —             | —            |
| V1      | INSERT       | A, B        | —             | A, B         |
| V2      | UPDATE       | C           | A             | B, C         |
| V3      | DELETE       | D           | B             | C, D         |

### **Interactive Delta table snapshot**

V0V1V2V3

Version 2 · UPDATE

Transaction log determines the active files

**File A**

Removed

**File B**

Active

**File C**

Active

**File D**

Not created

Querying version 2 reads File B and File C.

Select different versions to demonstrate how time travel changes the logical snapshot without modifying the current table. Removed files may remain physically stored until cleanup.

This is a simplified copy-on-write example. Modern Delta tables can also use **deletion vectors**, which allow certain row modifications to be recorded without immediately rewriting an entire Parquet file.

### **5.4 What does a JSON transaction look like?**

Illustrative transaction entry:

```
{
  "add": {
    "path": "part-00002.parquet",
    "size": 1048576,
    "dataChange": true,
    "partitionValues": {}
  }
}
```

When a file is logically removed:

```
{
  "remove": {
    "path": "part-00000.parquet",
    "dataChange": true
  }
}
```

These actions do not represent individual business records. They describe changes to the physical files and logical table state.

### **5.5 Write architecture**

The important concept is that writing data files does not, by itself, make those files part of a committed Delta table.

### **5.6 Read architecture**

\#chatgpt-mermaid-\_r_1n0\_{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;fill:rgb(13, 13, 13);}@keyframes edge-animation-frame{from{stroke-dashoffset:0;}}@keyframes dash{to{stroke-dashoffset:0;}}#chatgpt-mermaid-\_r_1n0\_ .edge-animation-slow{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 50s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_1n0\_ .edge-animation-fast{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 20s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_1n0\_ .error-icon{fill:rgb(243, 243, 243);}#chatgpt-mermaid-\_r_1n0\_ .error-text{fill:rgb(13, 13, 13);stroke:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ .edge-thickness-normal{stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ .edge-thickness-thick{stroke-width:3.5px;}#chatgpt-mermaid-\_r_1n0\_ .edge-pattern-solid{stroke-dasharray:0;}#chatgpt-mermaid-\_r_1n0\_ .edge-thickness-invisible{stroke-width:0;fill:none;}#chatgpt-mermaid-\_r_1n0\_ .edge-pattern-dashed{stroke-dasharray:3;}#chatgpt-mermaid-\_r_1n0\_ .edge-pattern-dotted{stroke-dasharray:2;}#chatgpt-mermaid-\_r_1n0\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1n0\_ .marker.cross{stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1n0\_ svg{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;}#chatgpt-mermaid-\_r_1n0\_ p{margin:0;}#chatgpt-mermaid-\_r_1n0\_ .label{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ .cluster-label text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ .cluster-label span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ .cluster-label span p{background-color:transparent;}#chatgpt-mermaid-\_r_1n0\_ .label text,#chatgpt-mermaid-\_r_1n0\_ span{fill:rgb(13, 13, 13);color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ .node rect,#chatgpt-mermaid-\_r_1n0\_ .node circle,#chatgpt-mermaid-\_r_1n0\_ .node ellipse,#chatgpt-mermaid-\_r_1n0\_ .node polygon,#chatgpt-mermaid-\_r_1n0\_ .node path{fill:rgb(240, 234, 251);stroke:rgb(184, 151, 244);stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ .rough-node .label text,#chatgpt-mermaid-\_r_1n0\_ .node .label text,#chatgpt-mermaid-\_r_1n0\_ .image-shape .label,#chatgpt-mermaid-\_r_1n0\_ .icon-shape .label{text-anchor:middle;}#chatgpt-mermaid-\_r_1n0\_ .node .katex path{fill:#000;stroke:#000;stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ .rough-node .label,#chatgpt-mermaid-\_r_1n0\_ .node .label,#chatgpt-mermaid-\_r_1n0\_ .image-shape .label,#chatgpt-mermaid-\_r_1n0\_ .icon-shape .label{text-align:center;}#chatgpt-mermaid-\_r_1n0\_ .node.clickable{cursor:pointer;}#chatgpt-mermaid-\_r_1n0\_ .root .anchor path{fill:rgb(143, 143, 143)!important;stroke-width:0;stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1n0\_ .arrowheadPath{fill:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1n0\_ .edgePath .path{stroke:rgb(143, 143, 143);stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ .flowchart-link{stroke:rgb(143, 143, 143);fill:none;}#chatgpt-mermaid-\_r_1n0\_ .edgeLabel{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_1n0\_ .edgeLabel p{background-color:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1n0\_ .edgeLabel rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1n0\_ .labelBkg{background-color:rgba(252, 252, 252, 0.5);}#chatgpt-mermaid-\_r_1n0\_ .cluster rect{fill:rgb(243, 243, 243);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ .cluster text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ .cluster span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:12px;background:rgb(243, 243, 243);border:1px solid rgba(0, 0, 0, 0.1);border-radius:2px;pointer-events:none;z-index:100;}#chatgpt-mermaid-\_r_1n0\_ .flowchartTitleText{text-anchor:middle;font-size:18px;fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1n0\_ rect.text{fill:none;stroke-width:0;}#chatgpt-mermaid-\_r_1n0\_ .icon-shape,#chatgpt-mermaid-\_r_1n0\_ .image-shape{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_1n0\_ .icon-shape p,#chatgpt-mermaid-\_r_1n0\_ .image-shape p{background-color:rgb(252, 252, 252);padding:2px;}#chatgpt-mermaid-\_r_1n0\_ .icon-shape .label rect,#chatgpt-mermaid-\_r_1n0\_ .image-shape .label rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1n0\_ .label-icon{display:inline-block;height:1em;overflow:visible;vertical-align:-0.125em;}#chatgpt-mermaid-\_r_1n0\_ .node .label-icon path{fill:currentColor;stroke:revert;stroke-width:revert;}#chatgpt-mermaid-\_r_1n0\_ .node .neo-node{stroke:rgb(184, 151, 244);}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].node rect,#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].cluster rect,#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].node polygon{stroke:url(#chatgpt-mermaid-\_r_1n0\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].swimlane.cluster rect{filter:none;}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].node path{stroke:url(#chatgpt-mermaid-\_r_1n0\_-gradient);stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].node .outer-path{filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].node .neo-line path{stroke:rgb(184, 151, 244);filter:none;}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].node circle{stroke:url(#chatgpt-mermaid-\_r_1n0\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].node circle .state-start{fill:#000000;}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].icon-shape .icon{fill:url(#chatgpt-mermaid-\_r_1n0\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1n0\_ [data-look="neo"].icon-shape .icon-neo path{stroke:url(#chatgpt-mermaid-\_r_1n0\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1n0\_ .node text{font-size:14px;font-weight:600;letter-spacing:normal;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_1n0\_ .edgeLabels text{font-size:13px;font-weight:600;letter-spacing:-0.08px;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_1n0\_ .node tspan[font-weight="normal"],#chatgpt-mermaid-\_r_1n0\_ .edgeLabels tspan[font-weight="normal"]{font-weight:600;}#chatgpt-mermaid-\_r_1n0\_ .edgeLabel .label rect{opacity:1;rx:13px;ry:13px;fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ .node rect,#chatgpt-mermaid-\_r_1n0\_ .node circle,#chatgpt-mermaid-\_r_1n0\_ .node ellipse,#chatgpt-mermaid-\_r_1n0\_ .node polygon,#chatgpt-mermaid-\_r_1n0\_ .node path{fill:rgb(239, 229, 254);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_1n0\_ .node rect{rx:16px;ry:16px;}#chatgpt-mermaid-\_r_1n0\_ .mermaid-decision .label-container{fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-dasharray:2,2;}#chatgpt-mermaid-\_r_1n0\_ .edgePaths .flowchart-link{stroke:rgb(143, 143, 143);stroke-width:1px;stroke-linecap:round;stroke-linejoin:round;}#chatgpt-mermaid-\_r_1n0\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1n0\_ :root{--mermaid-font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";}SQL queryResolve Delta table snapshotLoad checkpoint andsubsequent log entriesIdentify active data filesApply partition pruning anddata skippingRead relevant Parquet dataExecute remaining SparkoperationsReturn results

The query engine does not simply read every Parquet file in the directory. It identifies files belonging to the required table version and selects relevant data.

### **5.7 What are checkpoints?**

Imagine a table has accumulated 100,000 transactions. Replaying every historical transaction whenever a query executes would be inefficient.

Checkpoints consolidate table state so that readers can reconstruct a snapshot from a recent checkpoint plus subsequent transaction-log entries.

#### **Reconstructing version 14**

**Checkpoint V10**

V11

V12

V13

V14

**Snapshot V14**

Readers do not necessarily need to replay all transactions from version zero. Checkpoint frequency and format depend on configuration and table features.

**Advanced architecture note:** Newer catalog-managed commits can shift transaction coordination to Unity Catalog rather than relying exclusively on filesystem-based coordination. The traditional `_delta_log` model is still the appropriate starting point for explaining Delta snapshots.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **Practical demonstration: Inspect a Delta table**

```
display(spark.sql("DESCRIBE DETAIL orders"))display(spark.sql("DESCRIBE HISTORY orders"))
```

Run these commands after creating the `orders` table in the upcoming lab or downloaded notebook.

To inspect actual physical log files, use a separate Delta dataset stored in a Unity Catalog volume, as implemented in the downloadable notebook.

Do not try to directly access the physical storage path of a Unity Catalog managed table. Managed tables should be accessed by their registered table names.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



## **6. ACID transactions in Delta Lake**

### **Definition**

ACID represents four properties that ensure reliable transaction processing, even when multiple applications access or modify data concurrently.

# **A**

**Atomicity**

A transaction succeeds completely or fails without publishing partial changes.

# **C**

**Consistency**

Transactions maintain the applicable schema, constraints and table integrity rules.

# **I**

**Isolation**

Concurrent transactions are coordinated so readers see valid table snapshots.

# **D**

**Durability**

Successfully committed changes survive compute failures and restarts.

### **6.1 Atomicity**

Suppose a banking application ingests one million payment records in one Delta write.

If the job fails before committing, the table should not expose a partially completed transaction.

Uncommitted physical files may remain in storage, but they are not included in the committed table snapshot.

### **6.2 Consistency**

Suppose the table has a constraint requiring the transaction amount to be greater than zero.

| Transaction ID | Amount | Result  |
| -------------- | ------ | ------- |
| 1001           | 5,000  | Valid   |
| 1002           | -3,000 | Invalid |

If both are part of the same transactional insert, violation of the constraint can cause the complete write to fail.

Consistency applies to rules actually defined and enforced. It does not automatically validate every business requirement.

### **6.3 Isolation and optimistic concurrency control**

Delta Lake generally uses **Optimistic Concurrency Control (OCC)**.

Instead of holding a traditional exclusive lock throughout a potentially long-running distributed operation, transactions process against snapshots and validate potential conflicts before committing.

### **Concurrent transaction example**

Conflicts depend on the operations, isolation level, table features and whether file-level or supported row-level concurrency is applicable.

Databricks supports `WriteSerializable` and `Serializable` isolation levels.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **6.4 Durability**

If a Databricks compute cluster terminates after a transaction is successfully committed, the committed table data and metadata remain in persistent cloud storage.

### **Delta Lake vs. traditional relational databases**

| Feature           | Traditional RDBMS                        | Delta Lake                               |
| ----------------- | ---------------------------------------- | ---------------------------------------- |
| Typical workload  | OLTP                                     | OLAP and data engineering                |
| Storage           | Database-managed pages                   | Cloud object storage                     |
| Concurrency       | Locks, MVCC or other mechanisms          | Snapshot isolation and OCC               |
| Modification      | Page or row-oriented operations          | File-level changes and deletion vectors  |
| Scaling           | Database-dependent                       | Distributed storage and compute          |
| Transaction scope | Often multiple SQL statements and tables | Traditional Delta writes are table-level |

Do not assume that separate Spark SQL statements form one atomic transaction. Newer Databricks transaction capabilities support multi-statement transactions under specific requirements, which are separate from the foundational Delta write model.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **Enterprise use cases**

ACID is particularly important in financial data, inventory reconciliation, incremental ETL, billing and customer information systems.

The downloadable notebook includes an atomicity lab that intentionally attempts an invalid multi-row insert and checks that neither the table version nor its committed records change.

# **Part 4: Creating and manipulating Delta tables**

## **7. CREATE TABLE USING DELTA**

### **Definition**

`CREATE TABLE USING DELTA` creates a table backed by the Delta Lake storage format.

Databricks uses Delta as its default table format for typical table creation operations.

### **7.1 Unity Catalog architecture**

Fully qualified table name:

```
workspace.delta_training.orders
```

The three levels are catalog, schema and table.

### **7.2 Managed vs. external Delta tables**

| Feature           | Managed table                     | External table                    |
| ----------------- | --------------------------------- | --------------------------------- |
| Storage location  | Managed by Unity Catalog          | Specified by organization         |
| Metadata          | Managed by catalog                | Managed by catalog                |
| Data lifecycle    | Managed by Databricks             | Managed externally                |
| DROP TABLE        | Data lifecycle handled by catalog | Underlying data generally remains |
| Typical use       | New Databricks workloads          | Existing enterprise storage       |
| LOCATION required | No                                | Yes                               |

Unity Catalog managed tables are generally recommended for new Databricks workloads.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **Notebook Lab 1 — Initial environment setup**

Create a Python notebook and execute these cells. The following manual examples assume `workspace` is your writable catalog. Replace it if your catalog has a different name.

```
spark.sql("SHOW CATALOGS").show()
```

```
spark.sql("USE CATALOG workspace")spark.sql("""    CREATE SCHEMA IF NOT EXISTS delta_training""")spark.sql("USE SCHEMA delta_training")
```

Alternatively, use the downloaded notebook, which automatically creates a uniquely named schema.

### **Notebook Lab 2 — Create a managed table**

```
%sql

CREATE TABLE IF NOT EXISTS orders (
    order_id BIGINT,
    customer STRING,
    city STRING,
    amount DECIMAL(12,2),
    status STRING
)
USING DELTA;
```

Inspect the table:

```
%sql

DESCRIBE DETAIL orders;
```

Important output fields:

| Field            | Meaning                            |
| ---------------- | ---------------------------------- |
| format           | Storage format — Delta             |
| id               | Unique table identifier            |
| location         | Physical storage location          |
| numFiles         | Number of active data files        |
| sizeInBytes      | Size of active data files          |
| minReaderVersion | Minimum compatible reader protocol |
| minWriterVersion | Minimum compatible writer protocol |
| properties       | Table configuration                |

### **7.3 Creating a Delta table using PySpark**

```
data = [    (1, "Amit", "Delhi"),    (2, "Neha", "Mumbai"),    (3, "Ravi", "Bengaluru")]schema = """    customer_id LONG,    customer_name STRING,    city STRING"""df = spark.createDataFrame(data, schema)df.write \    .format("delta") \    .mode("overwrite") \    .saveAsTable("customers")display(spark.table("customers"))
```

Expected output:

| customer_id | customer_name | city      |
| ----------- | ------------- | --------- |
| 1           | Amit          | Delhi     |
| 2           | Neha          | Mumbai    |
| 3           | Ravi          | Bengaluru |

### **7.4 Creating a table using CTAS**

CTAS stands for **Create Table As Select**.

```
%sql

CREATE TABLE customers_delhi
USING DELTA
AS
SELECT *
FROM customers
WHERE city = 'Delhi';
```

Use cases include creating reporting tables, derived datasets, testing datasets and ETL outputs.

### **7.5 External table syntax**

```
CREATE TABLE external_orders (
    order_id BIGINT,
    amount DECIMAL(12,2)
)
USING DELTA
LOCATION 's3://your-bucket/delta/orders/';
```

This is an illustrative enterprise example. It requires an actual storage location and appropriate Unity Catalog external-location permissions, so it is not part of the default executable lab.

## **8. INSERT, UPDATE and DELETE**

These are Data Manipulation Language (DML) operations used to modify records in a table.

### **8.1 INSERT**

Adds new records to an existing table.

```
%sql

INSERT INTO orders VALUES
(101, 'Amit', 'Delhi', 10000, 'PENDING'),
(102, 'Neha', 'Mumbai', 20000, 'PENDING'),
(103, 'Ravi', 'Bengaluru', 15000, 'DELIVERED');
```

```
%sql

SELECT * FROM orders
ORDER BY order_id;
```

Expected output:

| order_id | customer | city      | amount | status    |
| -------- | -------- | --------- | ------ | --------- |
| 101      | Amit     | Delhi     | 10,000 | PENDING   |
| 102      | Neha     | Mumbai    | 20,000 | PENDING   |
| 103      | Ravi     | Bengaluru | 15,000 | DELIVERED |

Capture the current table version for later time-travel demonstrations:

```
base_version = spark.sql("""    DESCRIBE HISTORY orders LIMIT 1""").first()["version"]print(base_version)
```

For a freshly created table with one insert, this is normally version 1, but capturing the actual version is more reliable than hardcoding it.

### **8.2 UPDATE**

Modifies existing records satisfying a specified condition.

```
%sql

UPDATE orders
SET status = 'PAID'
WHERE order_id = 101;
```

Expected output for order 101:

| order_id | Previous status | New status |
| -------- | --------------- | ---------- |
| 101      | PENDING         | PAID       |

Use cases include correcting data, updating payments, modifying customer information and changing shipment statuses.

### **8.3 How UPDATE works internally**

#### **Traditional copy-on-write update**

Before — File A

101 · PENDING

102 · PENDING

103 · DELIVERED

After — File B

101 · PAID

102 · PENDING

103 · DELIVERED

**New Delta transaction**

REMOVE File A from active snapshot

ADD File B to active snapshot

File A can remain physically available for historical queries. Deletion vectors can avoid some immediate full-file rewrites in supported tables.

### **8.4 DELETE**

Removes records from the current logical table state.

```
%sql

DELETE FROM orders
WHERE order_id = 102;
```

Now insert another record:

```
%sql

INSERT INTO orders VALUES
(104, 'Priya', 'Pune', 12000, 'PENDING');
```

Expected final table:

| order_id | customer | amount | status    |
| -------- | -------- | ------ | --------- |
| 101      | Amit     | 10,000 | PAID      |
| 103      | Ravi     | 15,000 | DELIVERED |
| 104      | Priya    | 12,000 | PENDING   |

### **8.5 DELETE vs. TRUNCATE vs. DROP**

| Operation  | Removes selected rows | Removes all current rows | Removes table definition |
| ---------- | --------------------- | ------------------------ | ------------------------ |
| DELETE     | Yes                   | If no condition          | No                       |
| TRUNCATE   | No                    | Yes                      | No                       |
| DROP TABLE | No                    | Removes table object     | Yes                      |

**Important:** SQL `DELETE` does not necessarily erase all historical physical copies immediately. Data-retention and privacy-deletion requirements must account for old files and deletion vectors.

# **Part 5: Incremental processing**

## **9. MERGE INTO — Upsert logic in depth**

### **Definition**

`MERGE INTO` combines conditional insert, update and delete operations into a single transaction by comparing an incoming source dataset against a target Delta table.

Upsert means:

\\[ \text{UPSERT}=\text{UPDATE existing records}+\text{INSERT new records} \\]

### **Why is MERGE important?**

Imagine an enterprise CRM contains 100 million customers, but only 50,000 records change every day.

Instead of recreating the entire customer table daily, a pipeline can process just the changed records.

### **9.1 MERGE architecture**

### **9.2 Interactive MERGE example**

**Target table**

| ID  | Amount |
| --- | ------ |
| 101 | 10,000 |
| 103 | 15,000 |
| 104 | 12,000 |

**Incoming source**

| ID  | Amount |
| --- | ------ |
| 101 | 11,000 |
| 105 | 8,000  |

Execute MERGE Reset

Execute MERGE to demonstrate how matching and non-matching records are handled.

### **9.3 Notebook Lab — MERGE using SQL**

Create the incoming dataset:

```
%sql

CREATE OR REPLACE TEMP VIEW incoming_orders AS
SELECT * FROM VALUES
    (101, 'Amit', 'Delhi', 11000, 'SHIPPED'),
    (105, 'Sara', 'Chennai', 8000, 'PENDING')
AS source(order_id, customer, city, amount, status);
```

Execute MERGE:

```
%sql

MERGE INTO orders AS target
USING incoming_orders AS source

ON target.order_id = source.order_id

WHEN MATCHED THEN
    UPDATE SET
        target.customer = source.customer,
        target.city = source.city,
        target.amount = source.amount,
        target.status = source.status

WHEN NOT MATCHED THEN
    INSERT (
        order_id,
        customer,
        city,
        amount,
        status
    )
    VALUES (
        source.order_id,
        source.customer,
        source.city,
        source.amount,
        source.status
    );
```

```
%sql

SELECT * FROM orders
ORDER BY order_id;
```

Expected output:

| order_id | customer | amount | status    |
| -------- | -------- | ------ | --------- |
| 101      | Amit     | 11,000 | SHIPPED   |
| 103      | Ravi     | 15,000 | DELIVERED |
| 104      | Priya    | 12,000 | PENDING   |
| 105      | Sara     | 8,000  | PENDING   |

### **9.4 Different MERGE conditions**

| Clause                                 | Purpose                    |
| -------------------------------------- | -------------------------- |
| WHEN MATCHED THEN UPDATE               | Modify matching records    |
| WHEN MATCHED THEN DELETE               | Delete matching records    |
| WHEN NOT MATCHED THEN INSERT           | Insert new source records  |
| WHEN NOT MATCHED BY SOURCE THEN UPDATE | Update target-only records |
| WHEN NOT MATCHED BY SOURCE THEN DELETE | Remove target-only records |

#### **Advanced example — CDC operations**

Assume the source contains an operation column representing changes from an operational database.

```
%sql

CREATE OR REPLACE TEMP VIEW order_events AS
SELECT * FROM VALUES
    (101, 'SHIPPED', 'U'),
    (104, 'CANCELLED', 'D')
AS events(order_id, status, operation);
```

```
%sql

MERGE INTO orders AS t
USING order_events AS s
ON t.order_id = s.order_id

WHEN MATCHED AND s.operation = 'D'
    THEN DELETE

WHEN MATCHED AND s.operation = 'U'
    THEN UPDATE SET t.status = s.status;
```

This is an additional demonstration. Run it on a separate copy if you want to preserve the original four-row dataset.

### **9.5 Handling duplicate source records**

This is one of the most important production considerations.

Suppose two incoming rows have the same business key:

| order_id | amount | event_seq |
| -------- | ------ | --------- |
| 101      | 10,500 | 1         |
| 101      | 11,000 | 2         |

When multiple source rows attempt to update the same target row, MERGE can fail due to an ambiguous match.

Databricks Runtime 16.0 and newer evaluates both applicable `WHEN MATCHED` conditions and the `ON` condition when determining duplicate matches; older runtimes evaluate the `ON` condition for this purpose.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



#### **Solution: Deduplicate before MERGE**

```
%sql

CREATE OR REPLACE TEMP VIEW source_events AS
SELECT * FROM VALUES
    (101, 10500, 1),
    (101, 11000, 2),
    (105, 8000, 1)
AS events(order_id, amount, event_seq);
```

```
%sql

SELECT order_id, amount, event_seq
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY event_seq DESC
        ) AS rn
    FROM source_events
)
WHERE rn = 1;
```

Expected output:

| order_id | amount | event_seq |
| -------- | ------ | --------- |
| 101      | 11,000 | 2         |
| 105      | 8,000  | 1         |

In production, use a deterministic sequence or event identifier and store the latest applied sequence in the target if out-of-order events are possible.

### **9.6 MERGE using PySpark SQL**

```
spark.sql("""    MERGE INTO orders AS t    USING incoming_orders AS s    ON t.order_id = s.order_id    WHEN MATCHED THEN        UPDATE SET            t.amount = s.amount,            t.status = s.status    WHEN NOT MATCHED THEN        INSERT *""")
```

The DeltaTable API is another alternative on compatible compute, but using `spark.sql()` is convenient for serverless environments and for students familiar with SQL.

### **9.7 MERGE vs. other ingestion strategies**

| Strategy          | Best suited for                   | Limitation                          |
| ----------------- | --------------------------------- | ----------------------------------- |
| Append            | Immutable events and logs         | Does not update existing records    |
| Overwrite         | Full refresh of small datasets    | Reprocesses large amounts of data   |
| UPDATE            | Changes to existing records       | Cannot insert unmatched records     |
| MERGE             | Incremental updates and inserts   | Requires matching logic and compute |
| Lakeflow AUTO CDC | Continuous CDC and SCD processing | Requires pipeline configuration     |

### **Production best practices**

Deduplicate the source, use meaningful matching keys, restrict the search space when safe, avoid unnecessary updates, monitor operation metrics and design the pipeline to be idempotent.

Avoid unrestricted `WHEN NOT MATCHED BY SOURCE THEN DELETE` for incremental feeds. It can unintentionally remove target rows that are simply absent from the current batch.

# **Part 6: Data versioning and auditing**

## **10. Time travel**

### **Definition**

Time travel allows users to query historical snapshots of a Delta table using a previous version number or timestamp.

The current table is not modified when a historical snapshot is queried.

### **Why is it required?**

Consider a production pipeline that accidentally deletes 10 million records. You need to investigate the previous state, compare it with the current state and potentially restore the data.

Delta time travel provides this capability as long as the required historical metadata and data files remain available.

### **Architecture**

### **10.1 Query by version number**

```
%sql

DESCRIBE HISTORY orders;
```

Then select an available historical version:

```
%sql

SELECT *
FROM orders VERSION AS OF 1;
```

For the initial fresh-table insert, the historical snapshot contains:

| order_id | customer | amount | status    |
| -------- | -------- | ------ | --------- |
| 101      | Amit     | 10,000 | PENDING   |
| 102      | Neha     | 20,000 | PENDING   |
| 103      | Ravi     | 15,000 | DELIVERED |

Even though order 102 was subsequently deleted, it remains visible in the historical snapshot if its required files are available.

### **10.2 Query by timestamp**

```
SELECT *
FROM orders
TIMESTAMP AS OF '2026-11-26 10:30:00';
```

The timestamp above illustrates the syntax; it must correspond to an available snapshot in your own table.

For a genuinely executable demonstration, derive the timestamp dynamically.

```
from pyspark.sql import functions as Fhistory = spark.sql("DESCRIBE HISTORY orders")timestamp = (    history    .filter(F.col("version") == base_version)    .select(        F.date_format(            "timestamp",            "yyyy-MM-dd HH:mm:ss.SSSSSS"        ).alias("ts")    )    .first()["ts"])display(spark.sql(f"""    SELECT *    FROM orders TIMESTAMP AS OF '{timestamp}'"""))
```

Version-based time travel is preferable when exact reproducibility is required.

### **10.3 PySpark time travel**

```
historical_df = (    spark.read    .format("delta")    .option("versionAsOf", base_version)    .table("orders"))display(historical_df)
```

### **10.4 Comparing historical vs. current data**

```
display(spark.sql(f"""    SELECT * FROM orders VERSION AS OF {base_version}    EXCEPT    SELECT * FROM orders"""))
```

This identifies historical rows whose full values do not exist in the current table, including rows that were modified or deleted.

### **10.5 RESTORE TABLE**

Time travel only reads historical data. `RESTORE` changes the current logical state.

```
RESTORE TABLE orders
TO VERSION AS OF 1;
```

**Important:** RESTORE creates a new table version. It does not erase the intervening transaction history.

Run RESTORE on the disposable `restore_demo` table in the downloaded notebook instead of modifying the main training table.

| Time travel                       | RESTORE                         |
| --------------------------------- | ------------------------------- |
| Read-only historical query        | Changes the current table       |
| No new commit                     | Creates a new commit            |
| Used for investigation            | Used for recovery               |
| Does not affect current consumers | Can affect downstream consumers |

### **Enterprise use cases**

Historical financial reporting, ML model reproducibility, production incident investigations, data comparison, reconciliation and accidental-deletion recovery.

Time travel is not a permanent backup solution because its availability depends on historical file and metadata retention.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



## **11. DESCRIBE HISTORY — Auditing table changes**

### **Definition**

`DESCRIBE HISTORY` returns transaction-level metadata for operations performed on a Delta table.

It can help answer questions such as: Who changed a table? When did the change occur? Which operation was executed? How many records were affected?

### **Notebook Lab**

```
%sql

DESCRIBE HISTORY orders;
```

Illustrative output:

| Version | Operation    | Description              |
| ------- | ------------ | ------------------------ |
| 5       | MERGE        | Incremental upsert       |
| 4       | WRITE        | New record inserted      |
| 3       | DELETE       | Order removed            |
| 2       | UPDATE       | Payment status updated   |
| 1       | WRITE        | Initial records inserted |
| 0       | CREATE TABLE | Table initialized        |

Actual version numbers and operation names can differ depending on execution and maintenance activity.

### **Important history fields**

| Field               | Description                          |
| ------------------- | ------------------------------------ |
| version             | Transaction version                  |
| timestamp           | Commit time                          |
| userName            | Executing user, when recorded        |
| operation           | Type of transaction                  |
| operationParameters | Parameters associated with operation |
| operationMetrics    | Operation-specific metrics           |
| readVersion         | Table version read by transaction    |
| isolationLevel      | Isolation level used                 |
| userMetadata        | Custom commit metadata, if supplied  |

### **Retrieve MERGE statistics**

```
from pyspark.sql import functions as Fhistory = spark.sql("DESCRIBE HISTORY orders")display(    history    .filter(F.col("operation") == "MERGE")    .select(        "version",        "timestamp",        F.col("operationMetrics")         .getItem("numTargetRowsInserted")         .alias("inserted"),        F.col("operationMetrics")         .getItem("numTargetRowsUpdated")         .alias("updated"),        F.col("operationMetrics")         .getItem("numTargetRowsDeleted")         .alias("deleted")    ))
```

Expected result for the first simple MERGE:

| inserted | updated | deleted |
| -------- | ------- | ------- |
| 1        | 1       | 0       |

### **Enterprise monitoring use case**

A production data-engineering team can periodically collect transaction metrics and store them in a separate monitoring table. This helps identify unexpected row-count changes, costly MERGE operations and unusual deletion activity.

`DESCRIBE HISTORY` is useful for operational investigation, but it is not a substitute for enterprise security audit logs or permanent row-level change archives.

# **Part 7: Schema management and data quality**

## **12. Schema enforcement**

### **Definition**

Schema enforcement ensures that incoming data conforms to the structure and supported data types of an existing Delta table.

It protects tables from unexpected or incompatible changes during data ingestion.

This concept is also known as **schema validation** or **schema-on-write**.

### **Architecture**

### **Notebook Lab — Schema enforcement**

Create a table with an explicit schema.

```
%sql

CREATE TABLE schema_demo (
    id BIGINT,
    amount INT
)
USING DELTA;
```

```
%sql

INSERT INTO schema_demo VALUES (1, 100);
```

Attempt to insert invalid data:

```
try:    spark.sql("""        INSERT INTO schema_demo        VALUES (2, 'not-a-number')    """).collect()except Exception as e:    print("Write rejected:")    print(str(e)[:1000])
```

Expected result: an incompatible value or cast error.

Databricks can perform supported implicit type conversions, so a numeric string such as `'200'` may be accepted. A genuinely incompatible value makes the demonstration more reliable.

Verify the table:

```
display(spark.table("schema_demo"))
```

| id | amount |
| -- | ------ |
| 1  | 100    |

### **Test an unexpected column**

```
new_df = spark.createDataFrame(    [(2, 200, "WEB")],    "id LONG, amount INT, channel STRING")try:    new_df.write \        .format("delta") \        .mode("append") \        .option("mergeSchema", "false") \        .saveAsTable("schema_demo")except Exception as e:    print("Schema mismatch:")    print(str(e)[:1000])
```

The incoming DataFrame contains a `channel` column that does not exist in the target table. With schema evolution disabled, the write should be rejected.

### **Schema-on-read vs. schema-on-write**

| Feature                | Schema-on-read            | Schema-on-write                  |
| ---------------------- | ------------------------- | -------------------------------- |
| Validation             | Primarily during reading  | During ingestion                 |
| Flexibility            | Higher                    | Controlled                       |
| Invalid data detection | Potentially later         | Earlier                          |
| Example                | Raw JSON or CSV data lake | Delta table with enforced schema |
| Typical use            | Raw ingestion layer       | Curated enterprise datasets      |

### **Enterprise use case**

Suppose a payment application changes the type of `transaction_amount` from a decimal number to an incompatible string. Without validation, the issue may become visible only when downstream revenue calculations fail.

Schema enforcement prevents incompatible writes from silently corrupting the trusted table structure.

## **13. Schema evolution — mergeSchema**

### **Definition**

Schema evolution is the controlled modification of an existing table's schema to accommodate changing data requirements.

A business application may introduce new fields over time, such as:

- Customer loyalty level.
- Payment method.
- Mobile application version.
- Country or sales channel.
- New product attributes.

Schema evolution helps accommodate approved changes without manually recreating the entire table.

### **Architecture**

**Existing schema**

id: BIGINT

amount: INT

**Incoming schema**

id: BIGINT

amount: INT

channel: STRING

`mergeSchema = true`

**Evolved target schema**

id: BIGINT

amount: INT

channel: STRING — added

### **Notebook Lab — Enable schema evolution**

Continue from the schema enforcement lab.

```
new_df.write \    .format("delta") \    .mode("append") \    .option("mergeSchema", "true") \    .saveAsTable("schema_demo")
```

```
display(    spark.table("schema_demo")    .orderBy("id"))
```

Expected output:

| id | amount | channel |
| -- | ------ | ------- |
| 1  | 100    | NULL    |
| 2  | 200    | WEB     |

The existing records remain available, but their newly added column has a `NULL` value.

```
spark.table("schema_demo").printSchema()
```

Expected schema:

```
root
 |-- id: long
 |-- amount: integer
 |-- channel: string
```

### **13.1 Explicit schema evolution using ALTER TABLE**

```
%sql

ALTER TABLE schema_demo
ADD COLUMNS (
    country STRING
);
```

### **13.2 Schema evolution methods**

| Method                      | Purpose                                           |
| --------------------------- | ------------------------------------------------- |
| mergeSchema                 | Merge compatible schema changes during a write    |
| ALTER TABLE                 | Explicitly modify the schema                      |
| overwriteSchema             | Replace the table schema during an overwrite      |
| MERGE WITH SCHEMA EVOLUTION | Evolve schema during supported MERGE operations   |
| Session-level autoMerge     | Enable evolution more broadly for a Spark session |

Schema evolution does not mean that every arbitrary type conversion, column removal or rename is automatically supported. Some changes require separate DDL operations, table features or rewrites.

Databricks recommends enabling schema evolution for individual operations rather than broadly enabling it across unrelated writes.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **Schema enforcement vs. schema evolution**

| Schema enforcement             | Schema evolution                   |
| ------------------------------ | ---------------------------------- |
| Protects the existing contract | Allows controlled contract changes |
| Rejects incompatible writes    | Accommodates compatible changes    |
| Improves data quality          | Improves adaptability              |
| Useful for curated tables      | Useful for changing data sources   |

### **Enterprise architecture recommendation**

\#chatgpt-mermaid-\_r_1u2\_{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;fill:rgb(13, 13, 13);}@keyframes edge-animation-frame{from{stroke-dashoffset:0;}}@keyframes dash{to{stroke-dashoffset:0;}}#chatgpt-mermaid-\_r_1u2\_ .edge-animation-slow{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 50s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_1u2\_ .edge-animation-fast{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 20s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_1u2\_ .error-icon{fill:rgb(243, 243, 243);}#chatgpt-mermaid-\_r_1u2\_ .error-text{fill:rgb(13, 13, 13);stroke:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ .edge-thickness-normal{stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ .edge-thickness-thick{stroke-width:3.5px;}#chatgpt-mermaid-\_r_1u2\_ .edge-pattern-solid{stroke-dasharray:0;}#chatgpt-mermaid-\_r_1u2\_ .edge-thickness-invisible{stroke-width:0;fill:none;}#chatgpt-mermaid-\_r_1u2\_ .edge-pattern-dashed{stroke-dasharray:3;}#chatgpt-mermaid-\_r_1u2\_ .edge-pattern-dotted{stroke-dasharray:2;}#chatgpt-mermaid-\_r_1u2\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1u2\_ .marker.cross{stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1u2\_ svg{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;}#chatgpt-mermaid-\_r_1u2\_ p{margin:0;}#chatgpt-mermaid-\_r_1u2\_ .label{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ .cluster-label text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ .cluster-label span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ .cluster-label span p{background-color:transparent;}#chatgpt-mermaid-\_r_1u2\_ .label text,#chatgpt-mermaid-\_r_1u2\_ span{fill:rgb(13, 13, 13);color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ .node rect,#chatgpt-mermaid-\_r_1u2\_ .node circle,#chatgpt-mermaid-\_r_1u2\_ .node ellipse,#chatgpt-mermaid-\_r_1u2\_ .node polygon,#chatgpt-mermaid-\_r_1u2\_ .node path{fill:rgb(240, 234, 251);stroke:rgb(184, 151, 244);stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ .rough-node .label text,#chatgpt-mermaid-\_r_1u2\_ .node .label text,#chatgpt-mermaid-\_r_1u2\_ .image-shape .label,#chatgpt-mermaid-\_r_1u2\_ .icon-shape .label{text-anchor:middle;}#chatgpt-mermaid-\_r_1u2\_ .node .katex path{fill:#000;stroke:#000;stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ .rough-node .label,#chatgpt-mermaid-\_r_1u2\_ .node .label,#chatgpt-mermaid-\_r_1u2\_ .image-shape .label,#chatgpt-mermaid-\_r_1u2\_ .icon-shape .label{text-align:center;}#chatgpt-mermaid-\_r_1u2\_ .node.clickable{cursor:pointer;}#chatgpt-mermaid-\_r_1u2\_ .root .anchor path{fill:rgb(143, 143, 143)!important;stroke-width:0;stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1u2\_ .arrowheadPath{fill:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1u2\_ .edgePath .path{stroke:rgb(143, 143, 143);stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ .flowchart-link{stroke:rgb(143, 143, 143);fill:none;}#chatgpt-mermaid-\_r_1u2\_ .edgeLabel{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_1u2\_ .edgeLabel p{background-color:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1u2\_ .edgeLabel rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1u2\_ .labelBkg{background-color:rgba(252, 252, 252, 0.5);}#chatgpt-mermaid-\_r_1u2\_ .cluster rect{fill:rgb(243, 243, 243);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ .cluster text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ .cluster span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:12px;background:rgb(243, 243, 243);border:1px solid rgba(0, 0, 0, 0.1);border-radius:2px;pointer-events:none;z-index:100;}#chatgpt-mermaid-\_r_1u2\_ .flowchartTitleText{text-anchor:middle;font-size:18px;fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1u2\_ rect.text{fill:none;stroke-width:0;}#chatgpt-mermaid-\_r_1u2\_ .icon-shape,#chatgpt-mermaid-\_r_1u2\_ .image-shape{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_1u2\_ .icon-shape p,#chatgpt-mermaid-\_r_1u2\_ .image-shape p{background-color:rgb(252, 252, 252);padding:2px;}#chatgpt-mermaid-\_r_1u2\_ .icon-shape .label rect,#chatgpt-mermaid-\_r_1u2\_ .image-shape .label rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1u2\_ .label-icon{display:inline-block;height:1em;overflow:visible;vertical-align:-0.125em;}#chatgpt-mermaid-\_r_1u2\_ .node .label-icon path{fill:currentColor;stroke:revert;stroke-width:revert;}#chatgpt-mermaid-\_r_1u2\_ .node .neo-node{stroke:rgb(184, 151, 244);}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].node rect,#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].cluster rect,#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].node polygon{stroke:url(#chatgpt-mermaid-\_r_1u2\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].swimlane.cluster rect{filter:none;}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].node path{stroke:url(#chatgpt-mermaid-\_r_1u2\_-gradient);stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].node .outer-path{filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].node .neo-line path{stroke:rgb(184, 151, 244);filter:none;}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].node circle{stroke:url(#chatgpt-mermaid-\_r_1u2\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].node circle .state-start{fill:#000000;}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].icon-shape .icon{fill:url(#chatgpt-mermaid-\_r_1u2\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1u2\_ [data-look="neo"].icon-shape .icon-neo path{stroke:url(#chatgpt-mermaid-\_r_1u2\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1u2\_ .node text{font-size:14px;font-weight:600;letter-spacing:normal;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_1u2\_ .edgeLabels text{font-size:13px;font-weight:600;letter-spacing:-0.08px;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_1u2\_ .node tspan[font-weight="normal"],#chatgpt-mermaid-\_r_1u2\_ .edgeLabels tspan[font-weight="normal"]{font-weight:600;}#chatgpt-mermaid-\_r_1u2\_ .edgeLabel .label rect{opacity:1;rx:13px;ry:13px;fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ .node rect,#chatgpt-mermaid-\_r_1u2\_ .node circle,#chatgpt-mermaid-\_r_1u2\_ .node ellipse,#chatgpt-mermaid-\_r_1u2\_ .node polygon,#chatgpt-mermaid-\_r_1u2\_ .node path{fill:rgb(239, 229, 254);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_1u2\_ .node rect{rx:16px;ry:16px;}#chatgpt-mermaid-\_r_1u2\_ .mermaid-decision .label-container{fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-dasharray:2,2;}#chatgpt-mermaid-\_r_1u2\_ .edgePaths .flowchart-link{stroke:rgb(143, 143, 143);stroke-width:1px;stroke-linecap:round;stroke-linejoin:round;}#chatgpt-mermaid-\_r_1u2\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1u2\_ :root{--mermaid-font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";}Source applicationsBronze: raw ingestionSchema validation andtransformationValid record?Silver: validated Delta tableQuarantine tableGold: reporting and businessaggregatesYesNo

Schema enforcement should generally be stricter in Silver and Gold layers. Bronze layers may allow more flexibility for evolving source data, with appropriate validation and monitoring.

# **Part 8: Delta Lake storage and performance optimization**

## **14. VACUUM — Retention and cleanup**

### **Definition**

`VACUUM` is a Delta Lake maintenance operation that physically removes obsolete or unreferenced data files that meet the applicable retention requirements.

When records are updated or deleted, their previous physical files may remain in storage to support historical queries and concurrent operations.

VACUUM reclaims storage occupied by eligible obsolete files.

### **14.1 VACUUM architecture**

### **Interactive retention demonstration**

Assume File A was logically removed on day zero and the retention policy is seven days.

# **Day 5**

Within retention

0

7 days

14

File A is still within the retention period and should not be removed by a normal seven-day VACUUM operation.Reaching the retention threshold does not itself execute VACUUM.

### **14.2 Default retention periods**

| Property                           | Typical default | Purpose                                   |
| ---------------------------------- | --------------- | ----------------------------------------- |
| delta.deletedFileRetentionDuration | 7 days          | Retention of logically removed data files |
| delta.logRetentionDuration         | 30 days         | Retention of transaction-log history      |

The two properties control different things. A transaction-log entry may still exist even when its required data files have already been vacuumed.

Consequently, 30 days of history does not automatically guarantee 30 days of usable time travel.

### **Notebook Lab — Inspect retention and preview VACUUM**

```
%sql

SHOW TBLPROPERTIES orders;
```

Preview eligible files without deleting anything:

```
%sql

VACUUM orders
RETAIN 168 HOURS
DRY RUN;
```

168 hours corresponds to seven days.

For a newly created training table, the expected output will usually be empty because no obsolete files have exceeded the retention threshold.

The following command performs the actual cleanup when authorized:

```
VACUUM orders RETAIN 168 HOURS;
```

### **14.3 Configuring longer retention**

For example, if an organization requires a 30-day historical data window:

```
%sql

ALTER TABLE orders
SET TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 30 days',
    'delta.logRetentionDuration' = 'interval 30 days'
);
```

This illustrates aligned retention settings. Production recovery requirements may also need additional retention margin, independent backups and monitoring.

### **14.4 What VACUUM does not do**

VACUUM does not compact active files, improve their physical sorting, eliminate duplicate business records or replace an enterprise backup strategy.

Transaction-log cleanup is handled separately through Delta's log-retention mechanisms.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



**Trainer warning:** Do not disable retention safety checks or execute zero-hour VACUUM merely to make a classroom demonstration visibly delete files. It can invalidate historical snapshots and disrupt ongoing workloads.

## **15. OPTIMIZE — File compaction**

### **Definition**

`OPTIMIZE` is a maintenance operation that reorganizes the physical layout of Delta data files. Its foundational use case is combining numerous small files into fewer, larger files.

### **15.1 The small-file problem**

Imagine a streaming pipeline writes a small amount of data every few seconds.

After several months, millions of small files may exist in cloud storage.

Even when the overall volume of data is moderate, processing can become expensive due to file-opening overhead, metadata processing, object-storage requests and Spark task scheduling.

### **Before and after file compaction**

Before OPTIMIZE — 24 small files

`OPTIMIZE orders`

After OPTIMIZE — 4 larger filesIllustrative compaction only. Actual output file sizes and counts depend on the data and optimization settings.

### **15.2 How OPTIMIZE works**

\#chatgpt-mermaid-\_r_1un\_{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;fill:rgb(13, 13, 13);}@keyframes edge-animation-frame{from{stroke-dashoffset:0;}}@keyframes dash{to{stroke-dashoffset:0;}}#chatgpt-mermaid-\_r_1un\_ .edge-animation-slow{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 50s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_1un\_ .edge-animation-fast{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 20s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_1un\_ .error-icon{fill:rgb(243, 243, 243);}#chatgpt-mermaid-\_r_1un\_ .error-text{fill:rgb(13, 13, 13);stroke:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ .edge-thickness-normal{stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ .edge-thickness-thick{stroke-width:3.5px;}#chatgpt-mermaid-\_r_1un\_ .edge-pattern-solid{stroke-dasharray:0;}#chatgpt-mermaid-\_r_1un\_ .edge-thickness-invisible{stroke-width:0;fill:none;}#chatgpt-mermaid-\_r_1un\_ .edge-pattern-dashed{stroke-dasharray:3;}#chatgpt-mermaid-\_r_1un\_ .edge-pattern-dotted{stroke-dasharray:2;}#chatgpt-mermaid-\_r_1un\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1un\_ .marker.cross{stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1un\_ svg{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;}#chatgpt-mermaid-\_r_1un\_ p{margin:0;}#chatgpt-mermaid-\_r_1un\_ .label{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ .cluster-label text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ .cluster-label span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ .cluster-label span p{background-color:transparent;}#chatgpt-mermaid-\_r_1un\_ .label text,#chatgpt-mermaid-\_r_1un\_ span{fill:rgb(13, 13, 13);color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ .node rect,#chatgpt-mermaid-\_r_1un\_ .node circle,#chatgpt-mermaid-\_r_1un\_ .node ellipse,#chatgpt-mermaid-\_r_1un\_ .node polygon,#chatgpt-mermaid-\_r_1un\_ .node path{fill:rgb(240, 234, 251);stroke:rgb(184, 151, 244);stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ .rough-node .label text,#chatgpt-mermaid-\_r_1un\_ .node .label text,#chatgpt-mermaid-\_r_1un\_ .image-shape .label,#chatgpt-mermaid-\_r_1un\_ .icon-shape .label{text-anchor:middle;}#chatgpt-mermaid-\_r_1un\_ .node .katex path{fill:#000;stroke:#000;stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ .rough-node .label,#chatgpt-mermaid-\_r_1un\_ .node .label,#chatgpt-mermaid-\_r_1un\_ .image-shape .label,#chatgpt-mermaid-\_r_1un\_ .icon-shape .label{text-align:center;}#chatgpt-mermaid-\_r_1un\_ .node.clickable{cursor:pointer;}#chatgpt-mermaid-\_r_1un\_ .root .anchor path{fill:rgb(143, 143, 143)!important;stroke-width:0;stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1un\_ .arrowheadPath{fill:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1un\_ .edgePath .path{stroke:rgb(143, 143, 143);stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ .flowchart-link{stroke:rgb(143, 143, 143);fill:none;}#chatgpt-mermaid-\_r_1un\_ .edgeLabel{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_1un\_ .edgeLabel p{background-color:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1un\_ .edgeLabel rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1un\_ .labelBkg{background-color:rgba(252, 252, 252, 0.5);}#chatgpt-mermaid-\_r_1un\_ .cluster rect{fill:rgb(243, 243, 243);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ .cluster text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ .cluster span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:12px;background:rgb(243, 243, 243);border:1px solid rgba(0, 0, 0, 0.1);border-radius:2px;pointer-events:none;z-index:100;}#chatgpt-mermaid-\_r_1un\_ .flowchartTitleText{text-anchor:middle;font-size:18px;fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_1un\_ rect.text{fill:none;stroke-width:0;}#chatgpt-mermaid-\_r_1un\_ .icon-shape,#chatgpt-mermaid-\_r_1un\_ .image-shape{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_1un\_ .icon-shape p,#chatgpt-mermaid-\_r_1un\_ .image-shape p{background-color:rgb(252, 252, 252);padding:2px;}#chatgpt-mermaid-\_r_1un\_ .icon-shape .label rect,#chatgpt-mermaid-\_r_1un\_ .image-shape .label rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_1un\_ .label-icon{display:inline-block;height:1em;overflow:visible;vertical-align:-0.125em;}#chatgpt-mermaid-\_r_1un\_ .node .label-icon path{fill:currentColor;stroke:revert;stroke-width:revert;}#chatgpt-mermaid-\_r_1un\_ .node .neo-node{stroke:rgb(184, 151, 244);}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].node rect,#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].cluster rect,#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].node polygon{stroke:url(#chatgpt-mermaid-\_r_1un\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].swimlane.cluster rect{filter:none;}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].node path{stroke:url(#chatgpt-mermaid-\_r_1un\_-gradient);stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].node .outer-path{filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].node .neo-line path{stroke:rgb(184, 151, 244);filter:none;}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].node circle{stroke:url(#chatgpt-mermaid-\_r_1un\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].node circle .state-start{fill:#000000;}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].icon-shape .icon{fill:url(#chatgpt-mermaid-\_r_1un\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1un\_ [data-look="neo"].icon-shape .icon-neo path{stroke:url(#chatgpt-mermaid-\_r_1un\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_1un\_ .node text{font-size:14px;font-weight:600;letter-spacing:normal;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_1un\_ .edgeLabels text{font-size:13px;font-weight:600;letter-spacing:-0.08px;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_1un\_ .node tspan[font-weight="normal"],#chatgpt-mermaid-\_r_1un\_ .edgeLabels tspan[font-weight="normal"]{font-weight:600;}#chatgpt-mermaid-\_r_1un\_ .edgeLabel .label rect{opacity:1;rx:13px;ry:13px;fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ .node rect,#chatgpt-mermaid-\_r_1un\_ .node circle,#chatgpt-mermaid-\_r_1un\_ .node ellipse,#chatgpt-mermaid-\_r_1un\_ .node polygon,#chatgpt-mermaid-\_r_1un\_ .node path{fill:rgb(239, 229, 254);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_1un\_ .node rect{rx:16px;ry:16px;}#chatgpt-mermaid-\_r_1un\_ .mermaid-decision .label-container{fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-dasharray:2,2;}#chatgpt-mermaid-\_r_1un\_ .edgePaths .flowchart-link{stroke:rgb(143, 143, 143);stroke-width:1px;stroke-linecap:round;stroke-linejoin:round;}#chatgpt-mermaid-\_r_1un\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_1un\_ :root{--mermaid-font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";}Inspect active Delta filesSelect eligible filesRead and reorganize recordsWrite optimized data filesCommit new Delta versionOld files logically removedLater VACUUM reclaimseligible storage

Logical business records remain unchanged during normal file compaction.

### **Notebook Lab — Generate a larger dataset**

```
%sql

CREATE TABLE layout_orders (
    order_id BIGINT,
    customer_id BIGINT,
    city STRING,
    amount BIGINT
)
USING DELTA;
```

Generate four separate batches of data using Spark:

```
for i in range(4):    spark.sql(f"""        INSERT INTO layout_orders        SELECT            id + {i} * 10000 AS order_id,            pmod(id, 1000) AS customer_id,            CASE pmod(id, 3)                WHEN 0 THEN 'Delhi'                WHEN 1 THEN 'Mumbai'                ELSE 'Pune'            END AS city,            pmod(id, 5000) + 100 AS amount        FROM range(10000)    """)
```

```
%sql

SELECT COUNT(*) FROM layout_orders;
```

Expected result:

```
40000
```

Inspect the file layout:

```
display(    spark.sql("DESCRIBE DETAIL layout_orders")    .select("numFiles", "sizeInBytes"))
```

Execute OPTIMIZE:

```
%sql

OPTIMIZE layout_orders;
```

Inspect the file count again:

```
display(    spark.sql("DESCRIBE DETAIL layout_orders")    .select("numFiles", "sizeInBytes"))
```

Illustrative enterprise-scale results:

### **Number of active files**

Before

1,000

After

100

# **90% fewer files**

Hypothetical example, not a benchmark or a guaranteed result of the notebook.

On the small classroom dataset, optimized writes or automatic compaction may already have consolidated the files. OPTIMIZE may therefore produce little or no further reduction.

### **Alternatives to manual OPTIMIZE**

| Technique               | Purpose                                                 |
| ----------------------- | ------------------------------------------------------- |
| Optimized writes        | Improve output file layout during ingestion             |
| Auto compaction         | Compact small files automatically after eligible writes |
| Predictive optimization | Automate supported maintenance operations               |
| Manual OPTIMIZE         | Explicitly schedule table optimization                  |
| Liquid clustering       | Maintain data layout around query access patterns       |

### **OPTIMIZE vs. VACUUM**

| Feature                              | OPTIMIZE                      | VACUUM                           |
| ------------------------------------ | ----------------------------- | -------------------------------- |
| Primary purpose                      | Improve file layout           | Reclaim obsolete storage         |
| Creates optimized files              | Yes                           | No                               |
| Removes obsolete files physically    | Not directly                  | Yes                              |
| Can improve query performance        | Yes                           | Not its primary purpose          |
| Affects historical file availability | Old versions remain initially | Can make old versions unreadable |

## **16. Z-ORDER indexing**

### **Definition**

Z-ORDER is a multidimensional data-clustering technique that colocates related column values into nearby physical data files.

Its primary purpose is improving **data skipping** for selective queries.

Although it is commonly described as Z-ORDER indexing, it is not a conventional B-tree database index.

### **16.1 What is data skipping?**

Delta Lake can use file-level statistics such as minimum values, maximum values and null counts to determine whether a data file is potentially relevant to a query.

Suppose the query is:

```
SELECT *
FROM layout_orders
WHERE customer_id = 450;
```

If a file contains customer IDs ranging from 1 to 100, that file cannot contain customer 450 and may be skipped.

### **16.2 Before and after Z-ORDER**

### **Interactive data-skipping example**

**Query: customer_id = 450**

1

500

999

**Unclustered files**

5/5 scanned

**Clustered files**

1/5 scanned

Simplified one-column clustering example. Multidimensional Z-ORDER does not guarantee these exact non-overlapping file ranges.

### **16.3 How multidimensional Z-ORDER works**

Ordinary sorting prioritizes one column before another. Z-ORDER conceptually maps multiple dimensions into a one-dimensional ordering that preserves locality.

Conceptual Z-order space-filling curve illustrating locality across two dimensions.

### **Notebook Lab — Z-ORDER**

Use the `layout_orders` table created in the OPTIMIZE lab.

```
%sql

OPTIMIZE layout_orders
ZORDER BY (customer_id, city);
```

Execute a selective query:

```
%sql

SELECT *
FROM layout_orders
WHERE customer_id = 42
AND city = 'Delhi';
```

Inspect the query plan:

```
%sql

EXPLAIN FORMATTED

SELECT *
FROM layout_orders
WHERE customer_id = 42
AND city = 'Delhi';
```

For a meaningful performance comparison, inspect query profiles, files read, data bytes scanned and execution time before and after optimization on a sufficiently large dataset.

### **When should Z-ORDER be used?**

It is most useful for large non-liquid-clustered tables where queries repeatedly filter on selected columns.

Example candidates include `customer_id`, `product_id`, `transaction_id` and event dates, depending on query patterns.

Statistics must be available for effective data skipping. Adding too many Z-ORDER columns can dilute its benefit and increase optimization costs.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **Z-ORDER vs. database indexes**

| Feature            | B-tree index               | Z-ORDER                        |
| ------------------ | -------------------------- | ------------------------------ |
| Data structure     | Separate searchable index  | Physical data organization     |
| Main purpose       | Locate records efficiently | Skip irrelevant data files     |
| Typical workload   | Transaction processing     | Distributed analytical queries |
| Modification       | Index maintenance          | Data-file clustering           |
| Unique constraints | Can support uniqueness     | Does not enforce uniqueness    |

## **17. Liquid clustering**

### **Definition**

Liquid clustering is a modern Delta Lake data-layout optimization technique that organizes records around specified clustering columns without relying on fixed directory partitioning or Z-ORDER.

It is especially useful when query patterns and data distributions evolve.

### **17.1 Why traditional partitioning can become problematic**

Suppose an e-commerce table is partitioned by city.

```
orders/
    city=Delhi/
    city=Mumbai/
    city=Pune/
    city=Chennai/
```

If the number of distinct partition values grows significantly, the system may generate excessive partitions and small files.

Changing an existing physical partition strategy can also require substantial data reorganization.

Liquid clustering provides a more flexible layout.

**Static partitioning**

Fixed partition boundaries can produce uneven file sizes.

**Liquid clustering**

Data files organized around clustering keys rather than fixed partition directories.

### **Notebook Lab — Create a liquid-clustered table**

```
%sql

CREATE TABLE liquid_orders (
    order_id BIGINT,
    customer_id BIGINT,
    city STRING,
    amount BIGINT
)
USING DELTA

CLUSTER BY (customer_id, city);
```

Insert data from the previous lab:

```
%sql

INSERT INTO liquid_orders
SELECT * FROM layout_orders;
```

Run maintenance:

```
%sql

OPTIMIZE liquid_orders;
```

Inspect the table:

```
%sql

DESCRIBE DETAIL liquid_orders;
```

### **17.2 Changing clustering keys**

Suppose the organization initially filters by customer, but new reporting requirements prioritize city and order ID.

```
%sql

ALTER TABLE liquid_orders
CLUSTER BY (city, order_id);
```

```
%sql

OPTIMIZE liquid_orders;
```

Changing clustering keys does not immediately rewrite all previously stored data. Maintenance operations incrementally organize the physical layout.

On compatible runtime versions, `OPTIMIZE FULL` can force reclustering.

```
OPTIMIZE liquid_orders FULL;
```

This can be expensive for large tables, so it should be used deliberately.

### **17.3 Automatic liquid clustering**

Supported Databricks managed tables can automatically select suitable clustering keys using workload information.

Example syntax on compatible environments:

```
ALTER TABLE liquid_orders
CLUSTER BY AUTO;
```

Availability depends on table type, runtime and supported optimization capabilities. Liquid clustering is generally available for Delta tables starting with Databricks Runtime 15.4 LTS.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



### **Partitioning vs. Z-ORDER vs. liquid clustering**

| Feature               | Partitioning                        | Z-ORDER                    | Liquid clustering            |
| --------------------- | ----------------------------------- | -------------------------- | ---------------------------- |
| Organization          | Fixed partitions                    | File colocation            | Flexible clustering          |
| High-cardinality keys | Often problematic                   | Supported                  | Supported                    |
| Changing keys         | Can require major reorganization    | Re-optimize                | Change clustering definition |
| Data skipping         | Partition pruning                   | File-level skipping        | File-level skipping          |
| Maintenance           | Depends on writes                   | OPTIMIZE ZORDER            | OPTIMIZE                     |
| Suitable for          | Established partition-based layouts | Existing non-liquid tables | Modern flexible layouts      |

Z-ORDER and static partitioning are not combined with liquid clustering on the same table.

### **Which optimization strategy should you choose?**

New Delta table with changing query patterns

Existing partitioned table with stable query patterns

Table suffering primarily from small files

Large non-liquid table with selective filters

Suggested starting strategy

## **Liquid clustering**

Flexible clustering is generally a good starting point for new supported large tables.

These are starting recommendations, not universal rules. Production decisions should be based on data size, query profiles, ingestion rate and maintenance costs.

# **Part 9: Advanced Delta Lake features**

## **18. Change Data Feed (CDF)**

### **Definition**

Change Data Feed is a Delta Lake capability that exposes row-level changes made between table versions.

Instead of reading the entire table every time, downstream applications can consume records that were inserted, updated or deleted.

This is particularly valuable for incremental ETL and real-time data synchronization.

### **18.1 Why is CDF required?**

Consider an enterprise customer table containing 100 million records. Every day, approximately 100,000 records change.

### **Full refresh vs. incremental processing**

**Full refresh**

100 million rows

**Incremental processing**

100,000 rows

# **99.9% fewer logical rows**

Illustrative change volume. Actual compute and storage savings depend on physical file layout and pipeline processing.

Without CDF, applications may need to compare snapshots, maintain modification timestamps or reprocess large datasets.

CDF provides a standardized mechanism for consuming committed row changes.

### **18.2 CDF architecture**

### **18.3 Types of CDF changes**

| Change type      | Meaning                    |
| ---------------- | -------------------------- |
| insert           | New record added           |
| delete           | Existing record removed    |
| update_preimage  | Record before modification |
| update_postimage | Record after modification  |

Additional metadata columns are `_change_type`, `_commit_version` and `_commit_timestamp`.

### **Notebook Lab — Enable Change Data Feed**

Create a separate table.

```
%sql

CREATE TABLE cdf_orders (
    id BIGINT,
    status STRING
)
USING DELTA

TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);
```

This demonstrates explicitly enabled CDF, which is useful for teaching the foundational feature.

### **Step 1: Insert initial records**

```
%sql

INSERT INTO cdf_orders VALUES
(1, 'PENDING'),
(2, 'PENDING');
```

Capture the initial version:

```
cdf_start = spark.sql("""    DESCRIBE HISTORY cdf_orders LIMIT 1""").first()["version"]
```

### **Step 2: Update a record**

```
%sql

UPDATE cdf_orders
SET status = 'PAID'
WHERE id = 1;
```

### **Step 3: Delete a record**

```
%sql

DELETE FROM cdf_orders
WHERE id = 2;
```

### **Step 4: Insert another record**

```
%sql

INSERT INTO cdf_orders
VALUES (3, 'PENDING');
```

### **Step 5: Read all changes**

```
cdf_end = spark.sql("""    DESCRIBE HISTORY cdf_orders LIMIT 1""").first()["version"]display(    spark.sql(f"""        SELECT *        FROM table_changes(            'cdf_orders',            {cdf_start},            {cdf_end}        )        ORDER BY _commit_version, id, _change_type    """))
```

Expected output, assuming sequential versions:

| id | status  | \_change_type    | \_commit_version |
| -- | ------- | ---------------- | ---------------- |
| 1  | PENDING | insert           | 1                |
| 2  | PENDING | insert           | 1                |
| 1  | PAID    | update_postimage | 2                |
| 1  | PENDING | update_preimage  | 2                |
| 2  | PENDING | delete           | 3                |
| 3  | PENDING | insert           | 4                |

Notice that there are six change records even though the table currently contains only two records.

### **18.4 Read CDF using PySpark**

```
changes_df = (    spark.read    .format("delta")    .option("readChangeFeed", "true")    .option("startingVersion", cdf_start)    .option("endingVersion", cdf_end)    .table("cdf_orders"))display(changes_df)
```

Count changes by type:

```
display(    changes_df    .groupBy("_change_type")    .count())
```

Expected output:

| \_change_type    | count |
| ---------------- | ----- |
| insert           | 3     |
| delete           | 1     |
| update_preimage  | 1     |
| update_postimage | 1     |

### **18.5 Structured Streaming with CDF**

A streaming source DataFrame can be created as follows:

```
streaming_changes = (    spark.readStream    .format("delta")    .option("readChangeFeed", "true")    .option("startingVersion", cdf_start)    .table("cdf_orders"))
```

To build a production streaming pipeline, this source must be connected to an appropriate sink with checkpointing and processing logic.

For example, a downstream current-state table must process inserts, update postimages and deletes. Simply appending all CDF records would create a change-event archive, not a synchronized current-state table.

### **18.6 CDC vs. CDF**

| Feature        | CDC                            | Delta CDF              |
| -------------- | ------------------------------ | ---------------------- |
| Full form      | Change Data Capture            | Change Data Feed       |
| Scope          | General change-capture pattern | Delta table capability |
| Typical source | MySQL, Oracle, PostgreSQL      | Delta table            |
| Mechanism      | Database logs or other sources | Delta table versions   |
| Example tools  | Debezium, AWS DMS              | Delta CDF              |

### **Enterprise architecture example**

\#chatgpt-mermaid-\_r_23k\_{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;fill:rgb(13, 13, 13);}@keyframes edge-animation-frame{from{stroke-dashoffset:0;}}@keyframes dash{to{stroke-dashoffset:0;}}#chatgpt-mermaid-\_r_23k\_ .edge-animation-slow{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 50s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_23k\_ .edge-animation-fast{stroke-dasharray:9,5!important;stroke-dashoffset:900;animation:dash 20s linear infinite;stroke-linecap:round;}#chatgpt-mermaid-\_r_23k\_ .error-icon{fill:rgb(243, 243, 243);}#chatgpt-mermaid-\_r_23k\_ .error-text{fill:rgb(13, 13, 13);stroke:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ .edge-thickness-normal{stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ .edge-thickness-thick{stroke-width:3.5px;}#chatgpt-mermaid-\_r_23k\_ .edge-pattern-solid{stroke-dasharray:0;}#chatgpt-mermaid-\_r_23k\_ .edge-thickness-invisible{stroke-width:0;fill:none;}#chatgpt-mermaid-\_r_23k\_ .edge-pattern-dashed{stroke-dasharray:3;}#chatgpt-mermaid-\_r_23k\_ .edge-pattern-dotted{stroke-dasharray:2;}#chatgpt-mermaid-\_r_23k\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_23k\_ .marker.cross{stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_23k\_ svg{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:16px;}#chatgpt-mermaid-\_r_23k\_ p{margin:0;}#chatgpt-mermaid-\_r_23k\_ .label{font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ .cluster-label text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ .cluster-label span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ .cluster-label span p{background-color:transparent;}#chatgpt-mermaid-\_r_23k\_ .label text,#chatgpt-mermaid-\_r_23k\_ span{fill:rgb(13, 13, 13);color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ .node rect,#chatgpt-mermaid-\_r_23k\_ .node circle,#chatgpt-mermaid-\_r_23k\_ .node ellipse,#chatgpt-mermaid-\_r_23k\_ .node polygon,#chatgpt-mermaid-\_r_23k\_ .node path{fill:rgb(240, 234, 251);stroke:rgb(184, 151, 244);stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ .rough-node .label text,#chatgpt-mermaid-\_r_23k\_ .node .label text,#chatgpt-mermaid-\_r_23k\_ .image-shape .label,#chatgpt-mermaid-\_r_23k\_ .icon-shape .label{text-anchor:middle;}#chatgpt-mermaid-\_r_23k\_ .node .katex path{fill:#000;stroke:#000;stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ .rough-node .label,#chatgpt-mermaid-\_r_23k\_ .node .label,#chatgpt-mermaid-\_r_23k\_ .image-shape .label,#chatgpt-mermaid-\_r_23k\_ .icon-shape .label{text-align:center;}#chatgpt-mermaid-\_r_23k\_ .node.clickable{cursor:pointer;}#chatgpt-mermaid-\_r_23k\_ .root .anchor path{fill:rgb(143, 143, 143)!important;stroke-width:0;stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_23k\_ .arrowheadPath{fill:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_23k\_ .edgePath .path{stroke:rgb(143, 143, 143);stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ .flowchart-link{stroke:rgb(143, 143, 143);fill:none;}#chatgpt-mermaid-\_r_23k\_ .edgeLabel{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_23k\_ .edgeLabel p{background-color:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_23k\_ .edgeLabel rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_23k\_ .labelBkg{background-color:rgba(252, 252, 252, 0.5);}#chatgpt-mermaid-\_r_23k\_ .cluster rect{fill:rgb(243, 243, 243);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ .cluster text{fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ .cluster span{color:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ div.mermaidTooltip{position:absolute;text-align:center;max-width:200px;padding:2px;font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";font-size:12px;background:rgb(243, 243, 243);border:1px solid rgba(0, 0, 0, 0.1);border-radius:2px;pointer-events:none;z-index:100;}#chatgpt-mermaid-\_r_23k\_ .flowchartTitleText{text-anchor:middle;font-size:18px;fill:rgb(13, 13, 13);}#chatgpt-mermaid-\_r_23k\_ rect.text{fill:none;stroke-width:0;}#chatgpt-mermaid-\_r_23k\_ .icon-shape,#chatgpt-mermaid-\_r_23k\_ .image-shape{background-color:rgb(252, 252, 252);text-align:center;}#chatgpt-mermaid-\_r_23k\_ .icon-shape p,#chatgpt-mermaid-\_r_23k\_ .image-shape p{background-color:rgb(252, 252, 252);padding:2px;}#chatgpt-mermaid-\_r_23k\_ .icon-shape .label rect,#chatgpt-mermaid-\_r_23k\_ .image-shape .label rect{opacity:0.5;background-color:rgb(252, 252, 252);fill:rgb(252, 252, 252);}#chatgpt-mermaid-\_r_23k\_ .label-icon{display:inline-block;height:1em;overflow:visible;vertical-align:-0.125em;}#chatgpt-mermaid-\_r_23k\_ .node .label-icon path{fill:currentColor;stroke:revert;stroke-width:revert;}#chatgpt-mermaid-\_r_23k\_ .node .neo-node{stroke:rgb(184, 151, 244);}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].node rect,#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].cluster rect,#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].node polygon{stroke:url(#chatgpt-mermaid-\_r_23k\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].swimlane.cluster rect{filter:none;}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].node path{stroke:url(#chatgpt-mermaid-\_r_23k\_-gradient);stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].node .outer-path{filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].node .neo-line path{stroke:rgb(184, 151, 244);filter:none;}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].node circle{stroke:url(#chatgpt-mermaid-\_r_23k\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].node circle .state-start{fill:#000000;}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].icon-shape .icon{fill:url(#chatgpt-mermaid-\_r_23k\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_23k\_ [data-look="neo"].icon-shape .icon-neo path{stroke:url(#chatgpt-mermaid-\_r_23k\_-gradient);filter:drop-shadow( 1px 2px 2px rgba(185,185,185,1));}#chatgpt-mermaid-\_r_23k\_ .node text{font-size:14px;font-weight:600;letter-spacing:normal;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_23k\_ .edgeLabels text{font-size:13px;font-weight:600;letter-spacing:-0.08px;fill:rgb(107, 58, 180);}#chatgpt-mermaid-\_r_23k\_ .node tspan[font-weight="normal"],#chatgpt-mermaid-\_r_23k\_ .edgeLabels tspan[font-weight="normal"]{font-weight:600;}#chatgpt-mermaid-\_r_23k\_ .edgeLabel .label rect{opacity:1;rx:13px;ry:13px;fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ .node rect,#chatgpt-mermaid-\_r_23k\_ .node circle,#chatgpt-mermaid-\_r_23k\_ .node ellipse,#chatgpt-mermaid-\_r_23k\_ .node polygon,#chatgpt-mermaid-\_r_23k\_ .node path{fill:rgb(239, 229, 254);stroke:rgba(0, 0, 0, 0.1);stroke-width:1px;}#chatgpt-mermaid-\_r_23k\_ .node rect{rx:16px;ry:16px;}#chatgpt-mermaid-\_r_23k\_ .mermaid-decision .label-container{fill:rgb(249, 245, 254);stroke:rgb(215, 206, 229);stroke-dasharray:2,2;}#chatgpt-mermaid-\_r_23k\_ .edgePaths .flowchart-link{stroke:rgb(143, 143, 143);stroke-width:1px;stroke-linecap:round;stroke-linejoin:round;}#chatgpt-mermaid-\_r_23k\_ .marker{fill:rgb(143, 143, 143);stroke:rgb(143, 143, 143);}#chatgpt-mermaid-\_r_23k\_ :root{--mermaid-font-family:-apple-system-body,ui-sans-serif,-apple-system,system-ui,"Segoe UI",Helvetica,"Apple Color Emoji",Arial,sans-serif,"Segoe UI Emoji","Segoe UI Symbol";}Operational MySQL databaseAWS DMS or DebeziumBronze Delta tableMERGE into Silver tableDelta Change Data FeedGold reporting tablesSearch indexAudit event archive

### **Important CDF limitations**

Explicitly enabled CDF captures changes after the feature is enabled. Historical changes cannot be assumed to be available indefinitely. Downstream consumers must track progress, handle retries and remain within the available change history.

If permanent change auditing is required, periodically persist CDF records to a separate durable audit table.

Databricks also offers newer automatic change-feed capabilities on supported configurations. The notebook intentionally uses explicitly enabled CDF for its straightforward learning workflow.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



## **19. Delta constraints — NOT NULL and CHECK**

### **Definition**

Constraints are rules enforced by a table to protect the validity and integrity of its records.

Delta Lake supports enforced `NOT NULL` and `CHECK` constraints.

### **19.1 NOT NULL constraint**

Ensures a column cannot contain a null value.

Example use cases include transaction IDs, customer IDs, product IDs and mandatory payment information.

### **19.2 CHECK constraint**

Ensures that incoming records satisfy a specified condition.

Examples:

```
CHECK (amount > 0)
```

```
CHECK (age BETWEEN 18 AND 100)
```

```
CHECK (status IN ('PAID', 'PENDING'))
```

### **Notebook Lab — Create a constrained table**

```
%sql

CREATE TABLE payments (
    payment_id BIGINT NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    status STRING NOT NULL
)
USING DELTA;
```

Add business validation rules:

```
%sql

ALTER TABLE payments
ADD CONSTRAINT positive_amount
CHECK (amount > 0);
```

```
%sql

ALTER TABLE payments
ADD CONSTRAINT valid_status
CHECK (status IN ('PAID', 'PENDING'));
```

Insert valid data:

```
%sql

INSERT INTO payments VALUES
(1, 500, 'PAID');
```

### **Test invalid transactions**

The following interactive example demonstrates the defined business rules.

### **Interactive constraint validator**

Payment ID (blank represents NULL)

Amount

Payment status

PAID

PENDING

FAILED

Validate transactionClassroom simulation of the defined constraints, not a connection to a live Delta table.

### **19.3 Demonstrate atomicity using constraints**

```
try:    spark.sql("""        INSERT INTO payments VALUES        (2, 100, 'PAID'),        (3, -200, 'PAID')    """).collect()except Exception as e:    print("Transaction rejected:")    print(str(e)[:1000])
```

Although payment 2 is valid, payment 3 violates the amount constraint. The complete insert should fail.

```
display(spark.table("payments"))
```

Expected output:

| payment_id | amount | status |
| ---------- | ------ | ------ |
| 1          | 500    | PAID   |

This is an excellent practical demonstration of both **atomicity and consistency**.

### **19.4 Dropping a constraint**

```
%sql

ALTER TABLE payments
DROP CONSTRAINT positive_amount;
```

This removes the associated validation rule. Run it only when you intend to change the table's data-quality contract.

### **19.5 Are primary keys and foreign keys enforced?**

This is particularly important for students coming from Oracle, SQL Server and MySQL backgrounds.

| Constraint  | Enforced on Delta tables      |
| ----------- | ----------------------------- |
| NOT NULL    | Yes                           |
| CHECK       | Yes                           |
| PRIMARY KEY | Informational                 |
| FOREIGN KEY | Informational                 |
| UNIQUE      | Informational where supported |

Databricks does not enforce primary-key uniqueness, foreign-key relationships or informational unique constraints in the same way as a conventional transactional RDBMS.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



For business-key uniqueness, implement appropriate ingestion controls, deduplication and validation checks.

Example:

```
SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;
```

### **Alternatives for data validation**

| Approach              | Use case                                   |
| --------------------- | ------------------------------------------ |
| Delta constraints     | Mandatory table-level rules                |
| PySpark validations   | Complex business logic                     |
| Lakeflow expectations | Pipeline-level data-quality enforcement    |
| Great Expectations    | Reusable data-quality tests                |
| Quarantine tables     | Preserve invalid records for investigation |

## **20. Cloning Delta tables — Deep vs. shallow clone**

### **Definition**

Cloning creates a separate table representing a particular snapshot of an existing table.

It is useful for testing, development, experimentation, migration and certain recovery workflows.

Delta supports two primary cloning approaches.

### **20.1 Deep clone**

A deep clone copies the required physical data and creates separate table metadata.

### **Deep clone architecture**

Source and deep clone have separate physical data files and separate transaction histories.

### **Notebook Lab — Deep clone**

```
%sql

CREATE TABLE orders_deep
DEEP CLONE orders;
```

Modify the clone:

```
%sql

UPDATE orders_deep
SET status = 'TEST'
WHERE order_id = 101;
```

Compare both tables:

```
%sql

SELECT
    'SOURCE' AS table_type,
    order_id,
    status
FROM orders
WHERE order_id = 101

UNION ALL

SELECT
    'DEEP CLONE',
    order_id,
    status
FROM orders_deep
WHERE order_id = 101;
```

Expected output, assuming the original MERGE state:

| table_type | order_id | status  |
| ---------- | -------- | ------- |
| SOURCE     | 101      | SHIPPED |
| DEEP CLONE | 101      | TEST    |

Changes to the clone do not modify the source table.

### **20.2 Shallow clone**

A shallow clone creates independent table metadata while initially referencing the source's existing physical data files instead of copying them.

### **Shallow clone architecture**

New writes to the clone can create independent files without modifying the source table's logical contents.

### **Notebook Lab — Shallow clone**

```
%sql

CREATE TABLE orders_shallow
SHALLOW CLONE orders;
```

```
%sql

SELECT *
FROM orders_shallow;
```

Update the clone:

```
%sql

UPDATE orders_shallow
SET status = 'TEST'
WHERE order_id = 101;
```

The source table should remain logically unchanged.

### **20.3 Deep vs. shallow clone**

| Feature                    | Deep clone                               | Shallow clone                     |
| -------------------------- | ---------------------------------------- | --------------------------------- |
| Data physically copied     | Yes                                      | No, initially                     |
| Separate table metadata    | Yes                                      | Yes                               |
| Initial storage cost       | Higher                                   | Lower                             |
| Creation time              | Generally longer                         | Generally faster                  |
| Dependency on source files | No for copied snapshot                   | Yes                               |
| Independent modifications  | Yes                                      | Yes                               |
| Typical use                | Independent test environments and copies | Temporary experiments and testing |

### **Interactive clone storage comparison**

### **Estimated initial additional data storage**

Source table size

**1,000 GB**

Number of test clones

**3**

Deep clones

# **3 TB**

Additional copied data

Shallow clones

# **≈ 0 GB**

Additional copied source data initially

Decimal units. Excludes metadata, subsequent writes and storage retained because of shared file references.

### **20.4 Important relationship between shallow cloning and VACUUM**

This is an advanced architectural distinction worth explaining.

In traditional Hive-metastore or path-based shallow-clone scenarios, removing source files through VACUUM can make the clone unreadable.

For supported Unity Catalog shallow clones, Unity Catalog tracks shared file references. Normal VACUUM operations account for those references to protect files still required by the clone.

However, dropping and eventually physically deleting the source managed table can still make its shallow clones unusable. Support and permissions also vary by environment.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



### **20.5 Cloning vs. CTAS vs. VIEW**

| Feature                         | CTAS             | Deep clone | Shallow clone | View                 |
| ------------------------------- | ---------------- | ---------- | ------------- | -------------------- |
| Physical data copied            | New query result | Yes        | No, initially | No                   |
| Separate table state            | Yes              | Yes        | Yes           | No                   |
| Source snapshot copy            | Via query        | Yes        | Yes           | Typically dynamic    |
| Copies supported table metadata | Limited          | Yes        | Yes           | No                   |
| Independent updates             | Yes              | Yes        | Yes           | Not a separate table |

Cloning does not copy the entire source transaction history. The cloned table maintains its own history, and certain metadata such as Unity Catalog tags are not automatically copied.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS



# **Part 10: Complete enterprise use case**

## **21. Design an e-commerce lakehouse using Delta Lake**

This case study connects all the concepts covered in the module and gives experienced IT professionals a practical understanding of how they work together.

### **Business requirements**

An e-commerce organization processes millions of orders per day. The company needs incremental ingestion, payment-status updates, shipment tracking, schema validation, historical reporting, failure recovery and efficient analytical queries.

### **End-to-end architecture**

### **Mapping business requirements to Delta features**

| Business requirement                 | Delta Lake feature           |
| ------------------------------------ | ---------------------------- |
| Reliable order ingestion             | ACID transactions            |
| Modify order status                  | UPDATE                       |
| Remove cancelled records             | DELETE                       |
| Incremental customer synchronization | MERGE                        |
| Recover accidental changes           | Time travel and RESTORE      |
| Investigate production incidents     | DESCRIBE HISTORY             |
| Reject malformed data                | Schema enforcement           |
| Accommodate new fields               | Schema evolution             |
| Reclaim obsolete storage             | VACUUM                       |
| Reduce small-file overhead           | OPTIMIZE                     |
| Improve selective queries            | Z-ORDER or liquid clustering |
| Synchronize downstream tables        | Change Data Feed             |
| Validate business rules              | NOT NULL and CHECK           |
| Create QA environments               | Deep or shallow clone        |

# **Part 11: Delta Lake vs. competing technologies**

## **22. Delta Lake, Apache Iceberg and Apache Hudi**

All three provide table-management capabilities for data lake environments, but their architectures and ecosystems differ.

| Feature                 | Delta Lake                                      | Apache Iceberg                           | Apache Hudi                                    |
| ----------------------- | ----------------------------------------------- | ---------------------------------------- | ---------------------------------------------- |
| Primary metadata design | Transaction log                                 | Snapshots and manifests                  | Transaction timeline                           |
| ACID support            | Yes                                             | Yes                                      | Yes                                            |
| Historical snapshots    | Yes                                             | Yes                                      | Yes                                            |
| Schema evolution        | Yes                                             | Yes                                      | Yes                                            |
| Incremental processing  | Strong                                          | Supported                                | Strong                                         |
| Primary strengths       | Databricks integration and lakehouse processing | Multi-engine analytical interoperability | Incremental ingestion and record-level updates |
| Typical maintenance     | OPTIMIZE, VACUUM                                | File rewriting, snapshot expiration      | Compaction, clustering, cleaning               |

### **Conceptual metadata architecture**

Delta Lake

\_delta_log

Table snapshot

Data files

Iceberg

Metadata JSON

Manifests

Data files

Hudi

Timeline

File groups

Data files

For Databricks training, Delta Lake is an appropriate primary format because many of the platform's data-engineering and optimization features are built around it. In a broader enterprise architecture, format selection should also account for interoperability, governance and existing processing engines.

# **Part 12: Assessment and trainer preparation**

## **23. Important interview and classroom questions**

0 of 18 answers revealed

Reveal all

1\. Is Delta Lake a file format?

2\. What happens if \_delta_log is lost?

3\. Does UPDATE modify an existing Parquet file in place?

4\. What happens if an ETL job fails before committing?

5\. What happens when two jobs update the same row?

6\. What is the difference between MERGE and overwrite?

7\. Can MERGE handle duplicate source keys?

8\. Does DELETE physically remove data immediately?

9\. Can VACUUM improve small-file performance?

10\. Is time travel possible after VACUUM?

11\. Is Z-ORDER a B-tree index?

12\. Can we combine liquid clustering and Z-ORDER?

13\. Does schema evolution accept all incompatible data types?

14\. Does Delta enforce primary key uniqueness?

15\. What is the difference between CDF and time travel?

16\. Does deep clone copy all transaction history?

17\. Is a shallow clone a permanent independent backup?

18\. Is Unity Catalog the same as Delta Lake?

## **24. Trainer's checklist**

Use this checklist to prepare your sessions and track practical demonstrations.

## **0/17 completed**

Reset

Explain why plain Parquet needs a transactional table layer

Explain transaction log, snapshots and checkpoints

Demonstrate ACID using a failed transactional insert

Create managed Delta tables

Perform INSERT, UPDATE and DELETE

Demonstrate MERGE and duplicate-source handling

Query historical versions and demonstrate RESTORE

Inspect DESCRIBE HISTORY and operation metrics

Demonstrate schema enforcement failure

Evolve a schema using mergeSchema

Execute VACUUM DRY RUN

Compare file counts before and after OPTIMIZE

Explain Z-ORDER and data skipping

Create a liquid-clustered table

Read row-level changes using CDF

Test NOT NULL and CHECK constraints

Demonstrate deep and shallow clones

## **25. Official documentation for trainer preparation**

| Topic                     | Reference                                                                                                                         |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Delta Lake introduction   | [Databricks Delta Lake](https://docs.databricks.com/aws/en/delta/?utm_source=chatgpt.com)                                         |
| Internal architecture     | [Delta Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md?utm_source=chatgpt.com)                |
| Concurrency and isolation | [Databricks isolation levels](https://docs.databricks.com/aws/en/optimizations/isolation/isolation-levels?utm_source=chatgpt.com) |
| MERGE                     | [Delta MERGE documentation](https://docs.databricks.com/aws/en/delta/merge?utm_source=chatgpt.com)                                |
| Time travel               | [Table history](https://docs.databricks.com/aws/en/tables/history?utm_source=chatgpt.com)                                         |
| Schema evolution          | [Update table schemas](https://docs.databricks.com/aws/en/tables/update-schema?utm_source=chatgpt.com)                            |
| VACUUM                    | [VACUUM documentation](https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum?utm_source=chatgpt.com)                |
| OPTIMIZE                  | [OPTIMIZE documentation](https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize?utm_source=chatgpt.com)            |
| Liquid clustering         | [Liquid clustering documentation](https://docs.databricks.com/aws/en/tables/clustering?utm_source=chatgpt.com)                    |
| CDF                       | [Change Data Feed documentation](https://docs.databricks.com/aws/en/tables/features/change-data-feed?utm_source=chatgpt.com)      |
| Constraints               | [Delta constraints](https://docs.databricks.com/aws/en/tables/constraints?utm_source=chatgpt.com)                                 |
| Cloning                   | [Clone a table](https://docs.databricks.com/aws/en/tables/operations/clone?utm_source=chatgpt.com)                                |

## **26. Final revision cheat sheet**

| Command or concept       | Remember it as                                |
| ------------------------ | --------------------------------------------- |
| Parquet                  | Physical columnar storage                     |
| `_delta_log`             | Transactional table state                     |
| ACID                     | Reliable transactions                         |
| CREATE TABLE USING DELTA | Initialize a Delta table                      |
| INSERT                   | Add records                                   |
| UPDATE                   | Modify records                                |
| DELETE                   | Logically remove records                      |
| MERGE                    | Conditional incremental changes               |
| VERSION AS OF            | Read a historical version                     |
| TIMESTAMP AS OF          | Read a historical snapshot by time            |
| RESTORE                  | Restore historical state through a new commit |
| DESCRIBE HISTORY         | Inspect transactions                          |
| Schema enforcement       | Protect the data contract                     |
| mergeSchema              | Allow controlled schema changes               |
| VACUUM                   | Remove eligible obsolete files                |
| OPTIMIZE                 | Improve physical file layout                  |
| Z-ORDER                  | Colocate related values                       |
| Liquid clustering        | Flexible data clustering                      |
| CDF                      | Consume row-level changes                     |
| Constraints              | Enforce data-quality rules                    |
| Deep clone               | Independent physical copy                     |
| Shallow clone            | Metadata copy referencing source data         |

**My recommendation for teaching this module:** Spend extra time on the transaction log, ACID, MERGE, time travel and the relationship between OPTIMIZE and VACUUM. Once students understand how physical Parquet files relate to logical table snapshots, most of the advanced Delta Lake features become considerably easier to understand.

For the practical sessions, use the single sequential notebook rather than manually executing unrelated snippets. It keeps the datasets consistent, includes intentional failure demonstrations and uses isolated tables for advanced operations.

Download the complete Databricks source notebook | Download the IPYNB version
