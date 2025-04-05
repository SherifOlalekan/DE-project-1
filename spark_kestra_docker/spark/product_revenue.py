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


# Perform an aggregation on product
agg_prod_df = df_transact.groupBy("ProductID").agg(
    F.round(F.sum("COGS_usd"), 2).alias("total_COGS"),  # Round the sum of COGS to 2 decimal places
    F.sum("Quantity").alias("total_sales")
)


# Load in the product table from BigQuery
df_product = spark.read \
    .format("bigquery") \
    .option("table", inputs) \
    .load()



# Perform an inner join with the product table on column ProductID
product_rev_df = agg_prod_df.join(df_product, on="ProductID", how="inner")


# Removing whitespaces from column name
product_rev_df = product_rev_df.withColumnRenamed("Description EN", "Description")
product_rev_df = product_rev_df.withColumnRenamed("Production Cost", "Production_cost")

# Upload to BigQuery as Product_Revenue
product_rev_df.write.format('bigquery') \
    .option('table', output) \
    .save()