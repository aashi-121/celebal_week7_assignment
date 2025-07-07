# 📊 **Daily ETL Pipeline: Load Data Lake Files into SQL Tables**

## 🎯 **Project Goal**

This ETL pipeline performs the following daily operations:

- 📁 Ingest CSV files from a Data Lake folder
- 🔄 Truncate and reload data into SQL Server tables
- 🧠 Extract date metadata from filenames where required
- 🛠 Apply file-specific transformations before loading

---

## 📂 **Input Files in Data Lake**

Folder: `celebal_assignment_week_7_data/`

| File Pattern                     | Target Table      | Transformation Logic                                              |
| ------------------------------- | ----------------- | ----------------------------------------------------------------- |
| `CUST_MSTR_YYYYMMDD.csv`        | `CUST_MSTR`       | ➕ Add `date` column extracted from filename in `YYYY-MM-DD`      |
| `master_child_export-YYYYMMDD.csv` | `master_child`    | ➕ Add `date` and `date_key` columns from filename                |
| `H_ECOM_ORDER.csv`              | `H_ECOM_Orders`   | ✅ Load as-is without any transformation                          |

---

## ⚙️ **ETL Workflow Overview**

### 1️⃣ Start Spark & Setup JDBC Connection

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import re

# Initialize Spark
spark = SparkSession.builder.appName("Daily_ETL_Pipeline").getOrCreate()

# Define Data Lake Path
data_lake_path = "/mnt/datalake/container/"  # Update as needed

# JDBC SQL Server Config
jdbc_url = "jdbc:sqlserver://<server>:<port>;databaseName=<dbname>"
jdbc_props = {
    "user": "<username>",
    "password": "<password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
files = dbutils.fs.ls(data_lake_path)

for file in files:
    filename = file.name
    file_path = file.path
if filename.startswith("CUST_MSTR_"):
    match = re.search(r"CUST_MSTR_(\d{8})\.csv", filename)
    if match:
        date_str = f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:]}"
        df = spark.read.option("header", True).csv(file_path)
        df = df.withColumn("date", lit(date_str))

        spark.sql("TRUNCATE TABLE CUST_MSTR")
        df.write.jdbc(jdbc_url, "CUST_MSTR", mode="append", properties=jdbc_props)

        print(f"✔ Loaded CUST_MSTR from {filename}")
elif filename.startswith("master_child_export-"):
    match = re.search(r"master_child_export-(\d{8})\.csv", filename)
    if match:
        date_key = match.group(1)
        date_str = f"{date_key[:4]}-{date_key[4:6]}-{date_key[6:]}"
        df = spark.read.option("header", True).csv(file_path)
        df = df.withColumn("date", lit(date_str)).withColumn("date_key", lit(date_key))

        spark.sql("TRUNCATE TABLE master_child")
        df.write.jdbc(jdbc_url, "master_child", mode="append", properties=jdbc_props)

        print(f"✔ Loaded master_child from {filename}")
elif filename == "H_ECOM_ORDER.csv":
    df = spark.read.option("header", True).csv(file_path)

    spark.sql("TRUNCATE TABLE H_ECOM_Orders")
    df.write.jdbc(jdbc_url, "H_ECOM_Orders", mode="append", properties=jdbc_props)

    print(f"✔ Loaded H_ECOM_Orders from {filename}")
| File Type                          | Transformation Applied                 | SQL Table       |
| ---------------------------------- | -------------------------------------- | --------------- |
| `CUST_MSTR_YYYYMMDD.csv`           | ➕ Add `date` column                    | `CUST_MSTR`     |
| `master_child_export-YYYYMMDD.csv` | ➕ Add `date`, `date_key` columns       | `master_child`  |
| `H_ECOM_ORDER.csv`                 | ✅ Load directly without transformation | `H_ECOM_Orders` |
