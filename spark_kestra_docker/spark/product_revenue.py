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


# Read from BigQuery
df_transact = spark.read \
    .format("bigquery") \
    .option("table", "my-de-journey.Fashion_retail_dataset.transactions_partitioned") \
    .load()


# Perform an aggregation on product
agg_prod_df = df_transact.groupBy("ProductID").agg(
    F.round(F.sum("COGS_usd"), 2).alias("total_COGS"),  # Round the sum of COGS to 2 decimal places
    F.sum("Quantity").alias("total_sales")
)


# Load in the product table from BigQuery
df_product = spark.read \
    .format("bigquery") \
    .option("table", "my-de-journey.Fashion_retail_dataset.products") \
    .load()



# Perform an inner join with the product table on column ProductID
product_rev_df = agg_prod_df.join(df_product, on="ProductID", how="inner")


# Removing whitespaces from column name
product_rev_df = product_rev_df.withColumnRenamed("Description EN", "Description")
product_rev_df = product_rev_df.withColumnRenamed("Production Cost", "Production_cost")

# Upload to BigQuery as Product_Revenue
product_rev_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "olalekan-de2753") \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .mode("overwrite") \
    .save('my-de-journey.Fashion_retail_dataset.Product_Revenue')