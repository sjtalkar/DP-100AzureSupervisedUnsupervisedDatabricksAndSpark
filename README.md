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







All that you can load

All that you can avoid
Aim : Reduce the number of jobs that are spawned and are to be executed.

All that you can register
UDFs - registering for usage in an SQL query
Registering a dataframe as a view again so that it can be used in the query
# create a temporary view from the resulting DataFrame
parquetDF.createOrReplaceTempView("parquet_table")
Once registered the function or the view can be used in the SQL command
%sql
select * from parquet_table order by requests desc limit(5)

Sources: 
Microsoft Azure Learning Path
