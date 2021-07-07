# DP-100
## Azure Supervised, Unsupervised Databricks, And Spark

Foray Into Spark and Databricks 

This article is a compilation of noteworthy aspects captured when working with Azure Databricks and Spark. I was also introduced to Spark in a course in the Master of Applied DataScience program at University of Michigan.
As you may know, Spark is a Distributed computing environment. The unit of distribution is a Spark Cluster. Every Cluster has a Driver and one or more executors. Work submitted to the Cluster is split into as many independent Jobs as needed. This is how work is distributed across the Cluster's nodes. Jobs are further subdivided into tasks.
o The first level of parallelization is the Executor - a Java virtual machine running on a node, typically, one instance per node.
o The second level of parallelization is the Slot - the number of which is determined by the number of cores and CPUs of each node.
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
