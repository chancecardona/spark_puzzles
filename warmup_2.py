#from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db


products_table = dd.read_parquet("./data/products_parquet").repartition(npartitions=100)
sales_table = dd.read_parquet("./data/sales_parquet").repartition(npartitions=100)
sellers_table = dd.read_parquet("./data/sellers_parquet")

print("Distinct products sold in each date:")
print(dd.merge(products_table, sales_table, on="product_id").groupby("date")["product_id"].nunique().compute().desc())

