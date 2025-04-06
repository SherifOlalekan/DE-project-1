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
- Extracted the relevant CSV files 👉 [extraction](https://github.com/SherifOlalekan/DE-project-1/blob/main/extraction.ipynb)
- Uploaded the raw files to **Google Cloud Storage (GCS)**
```bash
gsutil -m cp -r dataset/ gs://olalekan-de2753/dataset
```

#### 2. **Infrastructure Setup**

- Used **Terraform** to:
  - Create a GCS bucket
  - Provision a BigQuery dataset 👉 [terraform](https://github.com/SherifOlalekan/DE-project-1/tree/main/terraform_gcp)

#### 3. **Containerization and Workflow Orchestration**

-  Created a [docker-compose.yaml file](https://github.com/SherifOlalekan/DE-project-1/blob/main/spark_kestra_docker/docker-compose.yml) for Kestra and Spark (master and worker)
- Created **Kestra flows** to automate:
  - Ingest data from GCS, clean and upload to BigQuery 👉 [flows](https://github.com/SherifOlalekan/DE-project-1/tree/main/spark_kestra_docker/kestra_flow/data_etl.yml)
  - Spark-based transformation 👉 [pyspark scrips](https://github.com/SherifOlalekan/DE-project-1/tree/main/spark_kestra_docker/spark)
  - BigQuery load jobs

#### 4. **Data Processing**

- Read CSV files from GCS and clean with **pandas**
- Uploaded the cleaned datasets to BigQuery for Storage
- Performed transformation with **pyspark**, A Python API for Spark
- Transformed the data into revenue summary tables: 👉[revenue yaml file](https://github.com/SherifOlalekan/DE-project-1/blob/main/spark_kestra_docker/kestra_flow/revenue.yml)
  - `customer_revenue`
  - `store_revenue`
  - `employee_revenue`
  - `product_revenue`
- Loaded the final datasets into **BigQuery** with **Kestra** PySparkSubmit.
- I was also able to use the trigger function in Kestra to automate the orchestration process where:
  - The data ingest task was schedule to run on the 1st of every month at 1hr interval for the 5 datasets
  - The revenue transformation task was schdeule to run on the 2nd of every month

---

### 📊 Dashboard & Analysis (Looker Studio)

Built an interactive dashboard to analyze key sales performance metrics:

#### Key KPIs:

- **Total Sales Quantity**
- **Total Revenue**
- **Return Rate**
- **Top Selling Products**
- **Monthly Revenue Trends**

> Charts and filters allow users to slice data by store, product category, employee, and date range.
![Fashion dashboard](https://github.com/user-attachments/assets/3db2ac95-2e07-41d3-8bc4-b03aef40558f)
---

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



### ❗ Note:
Here is the [data cleaning](https://github.com/SherifOlalekan/DE-project-1/blob/main/dataset_cleaning.ipynb) and 
[Spark BiqQuery](https://github.com/SherifOlalekan/DE-project-1/blob/main/spark_kestra_docker/spark/spark_bigquery.ipynb) jupyter notebook for use outside Kestra.
Download the Spark-bigQuery connector jar file to the .jar folder
```
mkdir -p ./jars
curl -L -o ./jars/spark-bigquery-with-dependencies_2.12-0.30.0.jar \
https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.30.0/spark-bigquery-with-dependencies_2.12-0.30.0.jar

```
