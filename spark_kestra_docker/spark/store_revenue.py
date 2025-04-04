#!/usr/bin/env python
# coding: utf-8

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



credentials_location = '/opt/keys/gcp-creds.json'

spark = SparkSession.builder \
    .appName("BigQueryAccess") \
    .config("spark.jars", "/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.30.0.jar,/opt/spark/jars/gcs-connector-hadoop3-latest.jar")\
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location) \
    .getOrCreate()


# spark.conf.set('temporaryGcsBucket', 'olalekan-de2753')


# Read from BigQuery
df_transact = spark.read \
    .format("bigquery") \
    .option("table", "my-de-journey.Fashion_retail_dataset.transactions_partitioned") \
    .load()


# Perform an aggregation on store
agg_prod_df = df_transact.groupBy("StoreID").agg(
    F.round(F.sum("COGS_usd"), 2).alias("total_COGS"),  # Round the sum of COGS to 2 decimal places
    F.sum("Quantity").alias("total_sales")
)


# Load in the store table from BigQuery
df_store = spark.read \
    .format("bigquery") \
    .option("table", "my-de-journey.Fashion_retail_dataset.stores") \
    .load()



# Perform an inner join with the store table on column ProductID
store_rev_df = agg_prod_df.join(df_store, on="StoreID", how="inner")


# Removing whitespaces from column name
store_rev_df = store_rev_df.withColumnRenamed("Number of Employees", "Employee_Count")


# Upload to BigQuery as Store_Revenue
store_rev_df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "olalekan-de2753") \
    .option("writeMethod", "direct") \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .option("writeDisposition", "WRITE_TRUNCATE") \
    .mode("overwrite") \
    .save('my-de-journey.Fashion_retail_dataset.Store_Revenue')