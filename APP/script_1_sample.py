#!/usr/bin/env python
# coding: utf-8

# In[3]:


import warnings
import shutil
import os
warnings.filterwarnings("ignore")
from pyspark.sql import SparkSession
import opendatasets as od


# In[4]:


if not os.path.exists("./archive"):
    od.download(
        "https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset")
    os.rename('stock-market-dataset', 'archive')
    
    path = "./archive"
    data_path = path + "/etfs/"
    output_path = path + "/output"


    shutil.rmtree('./archive/stocks')   
    os.rename('./archive/etfs', './archive/etfs_')
    
    
    folder_name = './archive/etfs'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


    # Source folder containing files to move
    src_folder = './archive/etfs_'

    # # Destination folder where files will be moved to
    dest_folder = './archive/etfs'

    # # Get a list of files in the source folder
    files = os.listdir(src_folder)

    # # Loop over the first 5 files in the list and move them to the destination folder
    for file in files[:5]:
        src_file = os.path.join(src_folder, file)
        dest_file = os.path.join(dest_folder, file)
        shutil.copy(src_file, dest_file)
    
    shutil.rmtree('./archive/etfs_')  


# In[71]:


path = "./archive"

import os
import concurrent.futures
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("csv-to-parquet").config("spark.driver.memory", "6g").getOrCreate()

data_path = "./archive/etfs/"

# Function to read a CSV file
def read_csv_file(symbol_row):
    symbol = symbol_row["Symbol"]
    security_name = symbol_row["Security Name"]
    csv_file_path = data_path + f"{symbol}.csv"
    if not os.path.exists(csv_file_path):
        return None
    spark = SparkSession.builder.appName("csv-to-parquet").getOrCreate()
    schema = "Date DATE, Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT, Adj_Close FLOAT, Volume INT"
    df = spark.read.format("csv").option("header", "true").schema(schema).load(csv_file_path)
    df = df.withColumn("Symbol", lit(symbol))
    df = df.withColumn("Security Name", lit(security_name))
    return df

# Load into a Spark dataframe
spark = SparkSession.builder.appName("csv-to-parquet").getOrCreate()
main_df = spark.read.csv(path + "/symbols_valid_meta.csv", header=True, inferSchema=True)
main_df = main_df.select("Symbol", "Security Name").distinct()

# Create an empty dataframe
combined_schema = "Date DATE, Open FLOAT, High FLOAT, Low FLOAT, Close FLOAT, Adj_Close FLOAT, Volume INT, Symbol STRING, Security_Name STRING"
combined_df = spark.createDataFrame([], schema=combined_schema)

# Implementing multithreading to read CSV files
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = []
    for symbol_row in main_df.rdd.collect():
        futures.append(executor.submit(read_csv_file, symbol_row))
    for future in concurrent.futures.as_completed(futures):
        df = future.result()
        if df is not None:
            combined_df = combined_df.union(df)

combined_df.write.mode("overwrite").parquet("./output.parquet")

spark.stop()


# In[72]:


spark = SparkSession.builder.appName("ReadParquetFile").getOrCreate()
df = spark.read.parquet("./output.parquet")
df.show(5)
# spark.stop()


# In[73]:


from pyspark.sql.functions import col, avg, percentile_approx
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReadParquetFile").getOrCreate()

df = spark.read.parquet("./output.parquet")

window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-29, 0)
df = df.withColumn('vol_moving_avg', avg(col('Volume')).over(window_spec))
df = df.withColumn('adj_close_rolling_med', percentile_approx(col('Adj_Close'), 0.5).over(window_spec))

# write the DataFrame to a new location
df.write.mode("overwrite").parquet("./updated_output.parquet")

# read the updated DataFrame from the new location
updated_df = spark.read.parquet("./updated_output.parquet")

# show the first 5 rows of the updated DataFrame
updated_df.show(5)


# In[74]:


# save the output to a temporary file location
df.write.mode("overwrite").parquet("./temp_output.parquet")

# delete the original file
shutil.rmtree("./output.parquet") 

# move the temporary file to the original file location
os.rename("./temp_output.parquet", "./output.parquet")

# read the updated DataFrame from the original location
updated_df = spark.read.parquet("./output.parquet")

# show the first 5 rows of the updated DataFrame
updated_df.show(5)
spark.stop()


# In[75]:


spark = SparkSession.builder.appName("ReadParquetFile").getOrCreate()
df = spark.read.parquet("./output.parquet")
df.show(5)
spark.stop()

shutil.rmtree('./archive') 