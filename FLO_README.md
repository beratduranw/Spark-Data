# 🛍️ Customer Segmentation & Omnichannel Analysis on FLO Dataset

This project is part of the **Miuul Data Engineering & Science Bootcamp**. It focuses on data preparation and analysis of customer behavior from FLO, one of the leading fashion retailers in Turkey. The aim is to explore customer interactions across online and offline platforms using **Apache Spark** and **HDFS**.

---

## 📌 Project Objectives

- Load and explore a customer behavior dataset stored in HDFS
- Perform exploratory data analysis using Spark
- Identify missing values and data quality issues
- Analyze omnichannel purchasing behaviors
- Create meaningful aggregations and customer-level KPIs

---

## 📁 Dataset Information

- **Source**: Provided by FLO through Miuul Bootcamp
- **Storage**: Uploaded to `HDFS` under the `/datasets/` directory
- **Format**: CSV
- **Delimiter**: Dataset-specific (e.g., `;`)
- **Schema Inference**: Enabled via `inferSchema=True`

---

## ⚙️ Technologies Used

| Tool / Library   | Purpose                          |
|------------------|----------------------------------|
| Apache Spark     | Distributed data processing      |
| HDFS             | Data storage                     |
| PySpark          | Python interface for Spark       |
| Python 3.x       | General programming              |
| Jupyter Notebook | Development environment          |

---

## 📊 Tasks & Steps

### 🧱 Task 1: Data Preparation & Initial Exploration

#### 🔹 Step 1: Environment Setup
- Required libraries were installed (`pyspark`)
- Dataset was moved to HDFS
- A Spark session was created with schema inference enabled

#### 🔹 Step 2: Initial Look at the Data
- Displayed the first 5 records
- Counted:
  - Number of observations (rows)
  - Number of variables (columns)
- Inspected variable data types

#### 🔹 Step 3: Missing Value Analysis
- Checked for missing values across all variables
- Documented the percentage of missingness per column

---

### 📈 Task 2: Data Analysis & Feature Engineering

#### 🔹 Step 1: Unique Customer Check
- Verified that the `master_id` column contains unique customer identifiers

#### 🔹 Step 2: Group-Based Analysis
- Analyzed data by:
  - `platform_type` (e.g., mobile, web, store)
  - `order_channel` (e.g., app, website, call center)

#### 🔹 Step 3: Omnichannel Feature Creation
Defined new customer metrics:
- `order_num_total`: Total number of purchases
- `customer_value_total`: Total monetary value of purchases

*Note:* Missing values were filled with `0`, and all values were validated to be `>= 0`.

#### 🔹 Step 4: Channel & Platform Performance
- Calculated:
  - Total number of customers
  - Average number of products purchased
  - Total and average revenue
- Grouped by:
  - `order_channel`
  - `platform_type`

---

## 📂 Project Structure

```
FLO-Omnichannel-Analysis
├── notebooks/
│   └── flo_data_analysis.ipynb         # Main analysis notebook
├── scripts/
│   └── spark_etl.py                    # Optional: PySpark ETL script
├── data/
│   └── flo_data.csv                    # Local version of dataset (original stored in HDFS)
├── README.md                           # Project documentation
```

---

## 📷 Sample Code

```python
# Load data with Spark
df = spark.read.csv("hdfs:///datasets/flo_data.csv", header=True, inferSchema=True, sep=";")

# Show initial rows
df.show(5)

# Group analysis
df.groupBy("platform_type").agg(
    {"order": "avg", "customer_value_total": "sum"}
).show()

