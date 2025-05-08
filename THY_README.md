# ✈️ THY Flight Data Analysis with PySpark

This project presents a scalable data analysis pipeline using **Apache Spark** to process and analyze Turkish Airlines (THY) flight data. The dataset contains information about seasonal flights, their origins, destinations, and passenger counts.

---

## 📂 Dataset Overview

- **Filename:** `THY_DATA.txt`
- **Stored in:** HDFS (`/user/train/datasets/thy_data.txt`)
- **File Size:** ~32 MB
- **Format:** CSV (comma-separated)
- **Columns:**
  - `SEASON`: Season of the flight (e.g., SUMMER, WINTER)
  - `ORIGIN`: Departure airport code
  - `DESTINATION`: Arrival airport code
  - `PSGR_COUNT`: Number of passengers on the flight

---

## ⚙️ Environment & Tools

- **Apache Spark:** Running on YARN
- **Cluster Filesystem:** Hadoop Distributed File System (HDFS)
- **Main Language:** Python (PySpark)
- **Other Libraries:**
  - `findspark` – to initialize Spark
  - `pandas` – optional, for displaying small subsets locally

---

## 🚀 Steps to Reproduce

### 1. Upload Dataset to HDFS

```bash
hdfs dfs -put ~/datasets/THY_DATA.txt /user/train/datasets/
```

### 2. Initialize Spark Session

To start using Spark, you need to initialize a Spark session. Below is the code to do that:

```python
import findspark
findspark.init("/opt/manual/spark")

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("THY Analysis") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()
```
### 3. Load Data

Once the Spark session is initialized, you can load the dataset into a DataFrame:

```python
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ",") \
    .load("/user/train/datasets/THY_DATA.txt")

```

## 📊 Exploratory Data Analysis
### 1. Schema and Dimensions

To inspect the schema of the dataset and check how many records and columns are present:
```python
df.printSchema()
df.count()
df.columns
df.describe().show()

df.agg({"PSGR_COUNT": "sum"}).show()

df.groupBy("SEASON") \
  .sum("PSGR_COUNT") \
  .withColumnRenamed("sum(PSGR_COUNT)", "TOTAL_PASSENGERS") \
  .show()

```
### Top 10 Routes
To find the top 10 routes based on passenger count:

```python
df.groupBy("ORIGIN", "DESTINATION") \
  .sum("PSGR_COUNT") \
  .withColumnRenamed("sum(PSGR_COUNT)", "TOTAL_PASSENGERS") \
  .orderBy("TOTAL_PASSENGERS", ascending=False) \
  .show(10)
```

---

## 📚 Summary

This project uses **Apache Spark** to analyze Turkish Airlines (THY) flight data, performing exploratory data analysis (EDA) and leveraging distributed computing for big data processing. The project explores trends, seasonal patterns, and insights that could benefit the airline industry for optimizing operations.

The dataset used for this project includes information on flights, passenger counts, routes, and seasonal data. The analysis focuses on identifying the most popular routes, peak seasons, and overall flight performance.

---

## 🛠️ How to Run the Project

### 1. **Prerequisites**

Before running this project, ensure you have the following:

- **Apache Spark** (installed and configured correctly)
- **HDFS** or access to a distributed file system
- **Python** (with necessary dependencies such as `pyspark` and `findspark`)
  
You can install `pyspark` using pip:

```bash
pip install pyspark
```

## 🎯 Project Goals
- Big Data Handling: Showcase the power of Apache Spark to process large datasets distributed across a cluster.

- Data Exploration: Perform an exploratory data analysis (EDA) to uncover insights such as trends in passenger counts, busiest routes, and seasonal patterns.

- Optimization: Use Spark's distributed computing features to efficiently perform tasks like aggregations, joins, and data transformations on large datasets.

## 🚀 Future Enhancements
- Integration with ML: Leverage machine learning techniques for predictive analytics (e.g., predict passenger numbers for future seasons).

- Visualization: Create visualizations for the insights gathered in the EDA (e.g., bar charts, heatmaps for routes, etc.).

- Data Enrichment: Enhance the dataset with external sources, such as weather data or external flight-related information, to uncover more insights

## 📢 Acknowledgements
- Apache Spark: For distributed data processing capabilities.

- Hadoop: For providing the distributed storage environment with HDFS.

- Turkish Airlines: For providing the dataset for this project.

- Open Source Contributors: For the continued development and support of the Spark ecosystem.

