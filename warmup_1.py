#from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db

# Init spark session
#spark = SparkSession.builder \
#    .master("local") \
#    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
#    .config("spark.executor.memory", "500mb") \
#    .appName("Exercise1") \
#    .getOrCreate()

products_table = dd.read_parquet("./data/products_parquet")
sales_table = dd.read_parquet("./data/sales_parquet")
sellers_table = dd.read_parquet("./data/sellers_parquet")

print(f"Number of Orders: {sales_table.count().compute()}")
print(f"Number of Products: {products_table.count().compute()}")
print(f"Number of Sellers: {sellers_table.count().compute()}")

print(f"Number of products sold at least once: {sales_table.groupby('product_id').size().compute()}")
#   Output which is the product that has been sold in more orders
print("Product present in more orders")
result = (sales_table
         .groupby("product_id")
         .size() # .agg in pyspark. Counts rows in each group.
         .reset_index()
         .rename(columns={0: "cnt"}) # count column is renamed to cnt
         .nlargest(1, "cnt")
         )

print(result.compute())
