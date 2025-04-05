#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--inputs', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

inputs = args.inputs
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'olalekan-de2753')


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
    .option("table", inputs) \
    .load()



# Perform an inner join with the store table on column StoreID
store_rev_df = agg_prod_df.join(df_store, on="StoreID", how="inner")


# Removing whitespaces from column name
store_rev_df = store_rev_df.withColumnRenamed("Number of Employees", "Employee_Count")


# Upload to BigQuery as store_revenue
store_rev_df.write.format('bigquery') \
    .option('table', output) \
    .save()