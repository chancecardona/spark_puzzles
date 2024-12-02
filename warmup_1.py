#from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db

# Init spark session
spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .config("spark.executor.memory", "500mb") \
    .appName("Exercise1") \
    .getOrCreate()

products_table = spark.read.parquet("./data/products_parquet")
sales_table = spark.read.parquet("./data/sales_parquet")
sellers_table = spark.read.parquet("./data/sellers_parquet")

print(f"Number of Orders: {sales_table.count()}")
print(f"Number of Products: {products_table.count()}")
print(f"Number of Sellers: {sellers_table.count()}")

print(f"Number of products sold at least once: {sales_table.agg(countDistinct(col('product_id')))}")
#   Output which is the product that has been sold in more orders
print("Product present in more orders")
sales_table.groupBy(col("product_id")).agg(
        count("*").alias("cnt")
    ).orderBy(col("cnt").desc()).limit(1).show()
