# DE-project-1

## 🌿 Fashion Retail Sales Performance Analysis

### 📌 Project Overview

This project is an end-to-end **Data Engineering and Analytics Pipeline** built around the *Global Fashion Retail Sales* dataset from Kaggle. It demonstrates how to collect, clean, transform, and visualize retail data using a modern cloud-native data stack.

---

### 🧰 Tech Stack

- **Terraform** – Infrastructure-as-code to provision GCP resources
- **Google Cloud Platform (GCP)** – BigQuery for warehousing, Cloud Storage for staging data
- **Docker** – Containerized environment for Spark and Kestra
- **Apache Spark** – Data cleaning and transformation
- **Kestra** – Workflow orchestration and scheduling
- **Looker Studio** – Dashboard creation and KPI visualization
- **Pandas** – Data inspection and cleaning (initially)
- **Parquet** – Format for efficient storage & querying

---

### 📂 Dataset

- **Source:** [Kaggle - Global Fashion Retail Sales](https://www.kaggle.com/datasets/ricgomes/global-fashion-retail-stores-dataset)
- **Format:** ZIP archive containing 6 CSV files
- **Used:**
  - `transactions.csv`
  - `customers.csv`
  - `products.csv`
  - `stores.csv`
  - `employees.csv`
- **Excluded:** `discount.csv`

---

### ♻️ ETL Workflow

#### 1. **Data Extraction & Upload**

- Downloaded the dataset from Kaggle
- Extracted the relevant CSV files
- Uploaded the raw files to **Google Cloud Storage (GCS)**

#### 2. **Infrastructure Setup**

- Used **Terraform** to:
  - Create a GCS bucket
  - Provision a BigQuery dataset

#### 3. **Data Processing with Spark**

- Read CSV files from GCS
- Cleaned and joined the datasets in **Apache Spark**
- Transformed the data into revenue summary tables:
  - `customer_revenue`
  - `store_revenue`
  - `employee_revenue`
  - `product_revenue`
- Output format: **Parquet**
- Loaded the final datasets into **BigQuery**

#### 4. **Workflow Orchestration**

- Created **Kestra flows** to automate:
  - Data extraction and upload
  - Spark-based transformation
  - BigQuery load jobs

---

### 📊 Dashboard & Analysis (Looker Studio)

Built an interactive dashboard to analyze key sales performance metrics:

#### Key KPIs:

- **Total Sales Quantity**
- **Total Revenue**
- **Return Rate**
- **Revenue by Store, Product, Customer, Employee**
- **Top Selling Products**
- **Monthly Revenue Trends**

> Charts and filters allow users to slice data by store, product category, employee, and date range.

---

### 📁 Folder Structure

```
├── data/                  # Raw and cleaned data files
├── dags/                  # Kestra workflows
├── terraform/             # GCP infra setup
├── notebooks/             # Data inspection & Spark scripts
├── docker/                # Docker Compose & container configs
└── README.md              # This file
```

---

### 🚀 How to Run

1. **Clone repo & set up GCP credentials**
2. Run `terraform apply` to provision GCP resources
3. Use Docker Compose to start Spark and Kestra
4. Trigger Kestra flows to process and load data
5. Connect Looker Studio to your BigQuery dataset

---

### 📌 Conclusion

This project showcases a full-stack data engineering pipeline using cloud-native tools to deliver business insights for a fashion retail store. The dashboard helps stakeholders understand sales trends, return behavior, and revenue drivers across multiple dimensions.










Uploading the dataset to Google Cloud Bucket
```bash
gsutil -m cp -r dataset/ gs://olalekan-de2753/dataset
```
code to download the Spark-bigQuery connector jar file

```
mkdir -p ./jars
curl -L -o ./jars/spark-bigquery-with-dependencies_2.12-0.30.0.jar \
https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.30.0/spark-bigquery-with-dependencies_2.12-0.30.0.jar

```

Moving scripts to gcs

```
gsutil -m cp -r spark/ gs://olalekan-de2753/script/
```
ALTER TABLE `my-de-journey.Fashion_retail_dataset.Product_Revenue` 
  ADD COLUMN Profit FLOAT64;

UPDATE 
  `my-de-journey.Fashion_retail_dataset.Product_Revenue`
SET Profit = (total_COGS - (total_sales * Production_cost))
WHERE total_COGS IS NOT NULL;
