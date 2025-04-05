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


# Read transaction data from BigQuery
df_transact = spark.read \
    .format("bigquery") \
    .option("table", "my-de-journey.Fashion_retail_dataset.transactions_partitioned") \
    .load()


# Perform an aggregation with customerID
agg_cust_df = df_transact.groupBy("CustomerID").agg(
    F.round(F.sum("COGS_usd"), 2).alias("total_COGS"),
    F.sum("Quantity").alias("total_sales")
)

# Load in customer table from Bigquery
df_customer = spark.read \
    .format("bigquery") \
    .option("table", inputs) \
    .load()


# Perform an inner join with the customer table on column CustomerID
customer_rev_df = agg_cust_df.join(df_customer, on="CustomerID", how="inner")

# Upload to BigQuery as customer_Revenue
customer_rev_df.write.format('bigquery') \
    .option('table', output) \
    .save()