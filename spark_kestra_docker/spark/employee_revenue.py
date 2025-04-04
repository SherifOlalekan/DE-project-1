#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



credentials_location = '/home/olalekan/data_Engineering_Journey/02_workflow_orchestration/keys/credk.json'

spark = SparkSession.builder \
    .appName("BigQueryAccess") \
    .config("spark.jars", "/home/olalekan/data_Engineering_Journey/05_batch_processing/code/lib/gcs-connector-hadoop3-latest.jar,/home/olalekan/DE-project-1/spark_kestra_docker/jars/spark-bigquery-with-dependencies_2.12-0.30.0.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
    .getOrCreate()


# spark.conf.set('temporaryGcsBucket', 'olalekan-de2753')

# Load in employee table from BigQuery
df_employee = spark.read \
    .format("bigquery") \
    .option("table", "my-de-journey.Fashion_retail_dataset.employees") \
    .load()


# Perform an aggregation with employee
agg_emp_df = df_transact.groupBy("EmployeeID").agg(
    F.round(F.sum("COGS_usd"), 2).alias("total_COGS"),  # Round the sum of COGS to 2 decimal places
    F.sum("Quantity").alias("total_sales")
)



# Perform an inner join with the customer table on column EmployeeID
employee_rev_df = agg_emp_df.join(df_employee, on="EmployeeID", how="inner")


# Upload to BigQuery as Employee_Revenue
employee_rev_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "olalekan-de2753") \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .mode("overwrite") \
    .save('my-de-journey.Fashion_retail_dataset.Employee_Revenue')