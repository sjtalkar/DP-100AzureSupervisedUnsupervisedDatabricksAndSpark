## STEPS TO WORK WITH DATABRICKS ON AZURE

1. Create a compute cluster.
    - Specify the compute size and Databricks Runtime
    - One the Cluster is created, you can add additional libraries
2. %fs
   ls dbfs:/databricks-datasets/nyctaxi/tripdata/yellow
3. https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/897686883903747/2503669437642038/312541189568512/latest.html
4. Converting Spark dataframe to Pandas
    https://sparkbyexamples.com/pyspark/convert-pyspark-dataframe-to-pandas/
    
```python
from pyspark.sql.functions import col, lit, expr, when
from pyspark.sql.types import *
from datetime import datetime
import time
 
# Define schema
nyc_schema = StructType([
  StructField('Vendor', StringType(), True),
  StructField('Pickup_DateTime', TimestampType(), True),
  StructField('Dropoff_DateTime', TimestampType(), True),
  StructField('Passenger_Count', IntegerType(), True),
  StructField('Trip_Distance', DoubleType(), True),
  StructField('Pickup_Longitude', DoubleType(), True),
  StructField('Pickup_Latitude', DoubleType(), True),
  StructField('Rate_Code', StringType(), True),
  StructField('Store_And_Forward', StringType(), True),
  StructField('Dropoff_Longitude', DoubleType(), True),
  StructField('Dropoff_Latitude', DoubleType(), True),
  StructField('Payment_Type', StringType(), True),
  StructField('Fare_Amount', DoubleType(), True),
  StructField('Surcharge', DoubleType(), True),
  StructField('MTA_Tax', DoubleType(), True),
  StructField('Tip_Amount', DoubleType(), True),
  StructField('Tolls_Amount', DoubleType(), True),
  StructField('Total_Amount', DoubleType(), True)
])
 
rawDF = spark.read.format('csv').options(header=True).schema(nyc_schema).load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz")

rawDF.printSchema()

rawDF.createOrReplaceTempView("nyc_taxi_for_analysis")
```

## To use azure storage 
https://docs.microsoft.com/en-us/learn/modules/get-started-azure-databricks/3-provision-workspaces-clusters
1. Load the CSV file into Azure storage account
2. Mount the storage account into dbfs

```python
data_storage_account_name = '<data_storage_account_name>'
data_storage_account_key = '<data_storage_account_key>'

data_mount_point = '/mnt/data'

data_file_path = '/bronze/wwi-factsale.csv'

dbutils.fs.mount(
  source = f"wasbs://dev@{data_storage_account_name}.blob.core.windows.net",
  mount_point = data_mount_point,
  extra_configs = {f"fs.azure.account.key.{data_storage_account_name}.blob.core.windows.net": data_storage_account_key})

display(dbutils.fs.ls("/mnt/data"))
#this path is available as dbfs:/mnt/data for spark APIs, e.g. spark.read
#this path is available as file:/dbfs/mnt/data for regular APIs, e.g. os.listdir
```

# Azure DataBricks ML
https://microsoftlearning.github.io/dp-090-databricks-ml/Instructions/Labs/01a-introduction-to-azure-databricks.html


## Reading the loaded/imported into DBFS data
```python
df = spark.read.csv('dbfs:/FileStore/tables/nyc_taxi.csv', header=True, inferSchema=True)
display(df)

or 

df = spark.sql("SELECT * FROM nyc_taxi")
display(df)

or

%sql

select * from nyc_taxi;

## Apply a udf to a column in the dataframe
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import *

# Step 1 define a function
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import *

#Step 2 define a udf using the above function
udfCelsiusToFahrenheit = udf(lambda z: celsiusToFahrenheit(z), DoubleType())

#Use the udf
display(df.filter(col('temperature').isNotNull()) \
  .withColumn("tempC", col("temperature").cast(DoubleType())) \
  .select(col("tempC"), udfCelsiusToFahrenheit(col("tempC")).alias("tempF")))
  

udfCelsiusToFahrenheit = udf(lambda z: celsiusToFahrenheit(z), DoubleType())

display(df.filter(col('temperature').isNotNull()) \
  .withColumn("tempC", col("temperature").cast(DoubleType())) \
  .select(col("tempC"), udfCelsiusToFahrenheit(col("tempC")).alias("tempF")))
  

# The abive can also be performed using Spark SQL. You do have to register a UDF though
result_with_plain_sql  = spark.sql("select temperature as tempC, (temperature * (9.0/5.0)) + 32.0 as tempF from nyc_taxi where temperature is not null" )

spark.udf.register("udfCelsiusToFahrenheit", celsiusToFahrenheit)
result_with_sql_udf  = spark.sql("select temperature as tempC, udfCelsiusToFahrenheit(temperature) as tempF from nyc_taxi where temperature is not null" )
result.show()

```
