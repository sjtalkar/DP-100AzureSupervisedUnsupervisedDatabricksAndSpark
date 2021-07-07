# DP-100
## Azure Supervised, Unsupervised Databricks, And Spark

NOTE: Sources for below Read Me text and pictures

https://www.youtube.com/watch?v=LoFN_Q224fQ
https://www.youtube.com/watch?v=4xsBQYdHgn8
Azure Databricks notebooks and training from MS LEarn



Foray Into Spark and Databricks 

This article is a compilation of noteworthy aspects captured when working with Azure Databricks and Spark. I was also introduced to Spark in a course in the Master of Applied DataScience program at University of Michigan.
As you may know, Spark is a Distributed computing environment. The unit of distribution is a Spark Cluster. Every Cluster has a Driver and one or more executors. Work submitted to the Cluster is split into as many independent Jobs as needed. This is how work is distributed across the Cluster's nodes. Jobs are further subdivided into tasks.
o The first level of parallelization is the Executor - a Java virtual machine running on a node, typically, one instance per node.
o The second level of parallelization is the Slot - the number of which is determined by the number of cores and CPUs of each node.

Transformations and Actions
It's not until we induce an action that a job is triggered and the data is processed. 
When loading data for instance, using Parquet file saves on Inferring Schema. Number of job when reading a Parquet files is typically 0 beacuse of the meta data available. For that matter, using JSON files saves on one job (so does not infer the schema for data types but can infer column names since those are keys).
When the data has to be physically touched - that's when an Executoe needs to rool up it's sleeve and get to work - to accomplish a Job.

For operations such as select, withColumns, map and other transforations seen below, things are kept on hold until an action demanding data is called such as show, count, collect (DANGER!!) and save.







### All that you can load
Now a Parquet data load does not benifit from it, but knowing your schema
1) column headers names type of column and nullable in advance reduces the number of jobs since you do not have to inferSchema
2) Parquet files come with the metadata that helps avoid inferring schema but CSVs and Json can benefit from the schema definition shown below


NOTE all types such as StringType, IntegerType and so on have to be imported.

from pyspark.sql.types import *

parquetSchema = StructType([
  StructField("project", StringType(), False),
  StructField("article", StringType(), False),
  StructField("requests", IntegerType(), False),
  StructField("bytes_served", IntegerType(), False)
  
])

### Use the schema defined above
df = (spark
  .read           # The DataFrameReader
  .schema(parquetSchema)           # Use the specified schema
  .parquet(path)                   # Creates a DataFrame from PARQUET after reading in the file
)

### The below fins the number of distinct articles in the files 
totalArticles = df.select('article').distinct().count() # Identify the total number of records remaining.

print("Distinct Articles: {0:,}".format(totalArticles))



All that you can avoid
Aim : Reduce the number of jobs that are spawned and are to be executed.

All that you can register
UDFs - registering for usage in an SQL query
Registering a dataframe as a view again so that it can be used in the query
### create a temporary view from the resulting DataFrame
parquetDF.createOrReplaceTempView("parquet_table")
Once registered the function or the view can be used in the SQL command
%sql
select * from parquet_table order by requests desc limit(5)

Sources: 
Microsoft Azure Learning Path
