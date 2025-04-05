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

# Perform an aggregation with employeeID
agg_emp_df = df_transact.groupBy("EmployeeID").agg(
    F.round(F.sum("COGS_usd"), 2).alias("total_COGS"),  # Round the sum of COGS to 2 decimal places
    F.sum("Quantity").alias("total_sales")
)


# Load in employee table from BigQuery
df_employee = spark.read \
    .format("bigquery") \
    .option("table", inputs) \
    .load()


# Perform an inner join with the customer table on column EmployeeID
employee_rev_df = agg_emp_df.join(df_employee, on="EmployeeID", how="inner")


# Upload to BigQuery as employee_Revenue
employee_rev_df.write.format('bigquery') \
    .option('table', output) \
    .save()