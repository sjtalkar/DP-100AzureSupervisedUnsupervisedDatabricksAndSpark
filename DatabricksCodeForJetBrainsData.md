```
import json
from collections import defaultdict


import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt


## Store files in tmp directory

from urllib.request import urlretrieve
files = ['ntb_2020_consistency.csv', 'ntb_2020_from_mooc.csv', 'ntb_2020_imports.csv', 'ntb_2020_md_stats.csv',
         'ntb_2020_text_counts.csv', 'ntb_2020_versions.csv', '2019_imports_4128764_nbs.json']
for f in files:
  url= f'https://github-notebooks-samples.s3-eu-west-1.amazonaws.com/{f}'
  dest = f"/tmp/{f}"
  urlretrieve(url,dest)
  
  
  ## Check for the file
  dbutils.fs.ls(f"file:/tmp")
  
  
  # Move the files to the data directory so that we can create tables from them
  for f in files:
  dbutils.fs.mv(f"file:/tmp/{f}",f"dbfs:/data/{f}")
  
  # Create dataframes
  df_dict ={}  
for i, f in enumerate(files):
  dfname = "df_" + f.split(".")[0]
  if   f.split(".")[1]  == 'csv':
      df_dict[dfname] = spark.read.format('csv').load(f"dbfs:/data/{f}", header=True)
      
#Create permanent tables      
for dfname in list(df_dict.keys()):
  perm_table_name = dfname
  #df_dict[dfname].createOrReplaceTempView(temp_table_name)
  df_dict[dfname].write.format("parquet").saveAsTable(perm_table_name)  
      
%sql 
show tables


## Read the tables
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.sql('''select * from  df_ntb_2020_text_counts ''')
df.show()
