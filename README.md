# DP-100
## Azure Supervised, Unsupervised Databricks, And Spark

NOTE: Sources for below Read Me text and pictures
      Azure Databricks provides a large number of datasets. Access them - %fs ls “databricks-datasets” 

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


### Setting up the spark session to get things going
The Azure Databricks environment provides us with a Spark session - the object is named "spark". 
Spark contexts can be created within a Spark session to work with Resilient Distributed Datasets. TO read and load data we use the Spark session object. In a notebook you can create a Spark session with:
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('My First Spark application') \
    .getOrCreate() 

sc = spark.sparkContext





### All that you can load
Now a Parquet data load does not benifit from it, but knowing your schema
1) column headers names type of column and nullable in advance reduces the number of jobs since you do not have to inferSchema
2) Parquet files come with the metadata that helps avoid inferring schema but CSVs and Json can benefit from the schema definition shown below

The read structure is similar to that in Pandas in that you can specify delimitiers and if there is header and so on.

For instance to read an Inside AirBnB csv file that has way too many columns to define schema on (not that it cannot be laboriously performed), we can set inferSchema to True (this unfortunately ensure a Job will be created). If the records can possible broken up by a newline character, set multiline to True.
https://sparkbyexamples.com/spark/spark-read-multiline-multiple-line-csv-file/

You will also notice escape set to the double quotes character- this is to preserve the qutotes in string. The difference in amenities column with and without double quotes can be seen as:
This string with escape = '"' set

{TV,"Cable TV",Internet,Wifi,Kitchen,"Pets live on this property",Dog(s),Heating,"Family/kid friendly",Washer,Dryer,"Smoke detector","Carbon monoxide detector","First aid kit",Essentials,Shampoo,"24-hour check-in",Hangers,"Hair dryer",Iron,"Laptop friendly workspace","Self check-in",Keypad,"Private entrance","Pack ’n Play/travel crib","Room-darkening shades"}

Turns to this is double quotes is not escaped:
"{TV,""Cable TV""

In Databricks, the diplay function prettifies the Dataframe so that you can see the data in rows muc better than if you use Spark's show
["Display of Table"]("Pictures for Readme/DBDisplay.JPG")


filePath = "dbfs:/mnt/training/airbnb/sf-listings/sf-listings-2019-03-06.csv"
rawDF = spark.read.csv(filePath, header=True, inferSchema=True, multiLine=True, escape='"')
display(rawDF)



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


#### Read file from Github

%sh curl -O "https://raw.githubusercontent.com/sjtalkar/SeriallyBuildDashboard/main/data/listings_1.csv"

#### check out where the file was stored
%fs ls "file:/databricks/driver"
###### or dbutils.fs.ls("file:/databricks/driver")



##### Read in CSV to DataFrame using above path
path = 'file:/databricks/driver/listings_1.csv'
# load data using sqlContext
airbnb_df  = spark.read.csv(path, header=True, inferSchema=True, multiLine=True, escape='"')
# display in table format
display(airbnb_df)


##### Create a function to get first letter of host name ( I know a trivial task)
def firstInitialFunction(name):
  return name[0]

firstInitialFunction("Jane")

To create a udf from the function that can be applied on the dataframe column
firstInitialUDF = udf(firstInitialFunction)

Employ the UDF
from pyspark.sql.functions import col
display(airbnb_df.select(firstInitialUDF(col("host_name"))))


TO create a registered UDF from the function that can be used within a SQL query

from pyspark.sql.types import *
spark.udf.register("firstInitialRegisteredUDF", firstInitialFunction,  StringType())

Employ the registered UDF 
NOTE: convert the Dataframe into a view so that it can be used in the query!!!
airbnb_df.createOrReplaceTempView("airbnbDF")

%sql
select distinct firstInitialRegisteredUDF(host_name) 
from airbnbDF


Since UDFs can be time consuming, use pre-defined functions or vectorized UDFs
The below is a UDF defined by a "decorator" pandas_udf is a vectorized UDF versus just udf which is a line by line udf
%python
from pyspark.sql.functions import pandas_udf

##### We have a string input/output
@pandas_udf("string")
def vectorizedUDF(name):
  return name.str[0]


##### Text file can be read with read.csv

###### Define the schema to reduce jobs

from pyspark.sql.types import *

textSchema = StructType([
  StructField("firstName", StringType(), False),
  StructField("middleName", StringType(), False),
  StructField("lastName", StringType(), False),
  StructField("gender", StringType(), False),
  StructField("birthDate", TimestampType(), False),
  StructField("salary", FloatType(), False),
  StructField("ssn", StringType(), False)
    
])
exercise_df = (
               spark
              .read
              .schema(textSchema)
              .option("sep", ":")
              .option("header", "true") 
              .csv(sourceFile)
               
)

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



from pyspark.sql.types import *

textSchema = StructType([
  StructField("firstName", StringType(), False),
  StructField("middleName", StringType(), False),
  StructField("lastName", StringType(), False),
  StructField("gender", StringType(), False),
  StructField("birthDate", TimestampType(), False),
  StructField("salary", FloatType(), False),
  StructField("ssn", StringType(), False)
    
])


exercise_df = (
               spark
              .read
              .schema(textSchema)
              .option("sep", ":")
              .option("header", "true") 
              .csv(sourceFile)
               
)

def capitalizeString(textString):
  return textString.capitalize()
firstCapitalizeUDF = udf(capitalizeString)

def formatSSN(ssnText):
  #Strip off hyphens if any to bring all SSNs to the same format and then insert them
  ssnText = ssnText.replace('-', '')
  return ssnText[0:3] + "-" + ssnText[3:6] +  "-" + ssnText[6:] 
formatSSNUDF = udf(formatSSN)  

from pyspark.sql.functions import col
exercise_cap_df = exercise_df.select(
                                    firstCapitalizeUDF(col("firstName")).alias("firstName"),
                                    firstCapitalizeUDF(col("middleName")).alias("middleName"),
                                    firstCapitalizeUDF(col("lastName")).alias("lastName"),
                                    firstCapitalizeUDF(col("gender")).alias("gender"),
                                    col("birthdate"),
                                    col("salary"),
                                    formatSSNUDF(col("ssn" )).alias("ssn"))
                                    
                                    

#register the Dataframe so that a SQL can be applied on it

exercise_cap_df.createOrReplaceTempView('exercise_cap_vw')

query = """
select distinct * from exercise_cap_vw
"""
result = spark.sql(query)

result.count()

(3) Spark Jobs
Out[75]: 100000


destFile = userhome + "/people.parquet"
# In case it already exists
result.write.parquet(destFile)

## Use request REST API to get data into Databricks
```
groundwater_request_api = requests.get('https://data.cnra.ca.gov/api/3/action/datastore_search?resource_id=bfa9f262-24a1-45bd-8dc8-138bc8107266&limit=32000').json()
data_groundwater = groundwater_request_api['result']['records']
while groundwater_request_api['result']['records']:
    groundwater_request_api = requests.get('https://data.cnra.ca.gov'+groundwater_request_api['result']['_links']["next"]).json()
    data_groundwater.extend(groundwater_request_api['result']['records'])
    
    
df_groundwater = spark.createDataFrame(Row(**row) for row in data_groundwater)
perm_table_name = "table_groundwater"
df_groundwater.write.format("parquet").saveAsTable(f"MILESTONE2WATER.{perm_table_name}")      


spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

```

Sources: 
Microsoft Azure Learning Path



### AZURE Blob mounting
```
# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = r""




accountname = ""
accountkey = ""



fullname = "fs.azure.account.key." +accountname+ ".blob.core.windows.net"
accountsource = "wasbs://files@" +accountname+ ".blob.core.windows.net/NYCTaxi"
dbutils.fs.mount(
source = accountsource,
mount_point = "/mnt/NYCTaxiData",
extra_configs = {fullname : accountkey}
)
#dbutils.fs.ls("/mnt/NYCTaxiData")



# Allow SPARK to read from Blob remotely
wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set(
'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
blob_sas_token)
print('Remote blob path: ' + wasbs_path)



# SPARK read parquet, note that it won't load any data yet by now
source = spark.read.parquet(wasbs_path)
#print('Register the DataFrame as a SQL temporary view: source')
source.createOrReplaceTempView('source')



source.write.mode("Overwrite").parquet("/mnt/NYCTaxiData")
# Display top 10 rows
#print('Displaying top 10 rows: ')
#SQLSource = spark.sql('SELECT * FROM source')
#DestFilePath = dest_wasbs_path + 'dbo.NYCTaxi.parquet' 
```

## Mount

Print out the mounts 

```python
mounts = dbutils.fs.mounts()

for mount in mounts:
  print(mount.mountPoint + " >> " + mount.source)

print("-"*80)
```
Use info from above to find files within the mount
```
#And now we can use dbutils.fs.ls(..) to view the contents of that mount
files = dbutils.fs.ls("/mnt/training/")

for fileInfo in files:
  print(fileInfo.path)

print("-"*80)
```


TO create the Struct element, you have to peek at the file. You can do that using

```
%fs head /mnt/training/wikipedia/pageviews/pageviews_by_second.tsv
```

# The dataframe is loaded

### Get number of partitions in a dataframe
```
csvDF.rdd.getNumPartitions()
```

## Get the schema
```
wikiEditsDF.printSchema()
```

## Create a view out of it
```
# create a view called wiki_edits
wikiEditsDF.createOrReplaceTempView("wiki_edits")
```

### Getting Kaggle data
%sh

export KAGGLE_USERNAME='<username>'
export KAGGLE_KEY="<API key from create api key in kaggle profile>"
> The below is the API call provided in a competition dataset. You have to accept the rules otherwise it will throw a forbidden error
`kaggle competitions download -c jigsaw-toxic-comment-classification-challenge`
      
      
      

# Apache Spark  Notes: Databricks Certified Associate Developer for Apache Spark

Data and compute separation      
      
Unification of data, analytics and AI on one common platform : No redundancy of data : all types of data can be placed together 
      
Better reliability with transactions : ACID and time travel 

Advanced caching and indexing methods : accessing data very quiring data quickly due to underlying Apache Spark

Data Governance at scale with fine-grained access control lists ACLs   
       With Azure: Databricks can be connected to Key Vault
                  Store credentials in Key Vault.
                  Authorized Databricks can grab the the credential from Key Vault. 




####  wget a csv and store in dbfs
```
%sh
# Pull CSV file from url
wget -nc https://lafkkbox.blob.core.windows.net/worldtourdata/uszips.csv

# Move from databricks/driver to dbfs
mv /databricks/driver/uszips.csv /dbfs/FileStore/uszips.csv
```


#### Storing a file in xlsx format


`Note:` It has to be stored locally in /tmp/ then copied over to the ADLS storage


Once a Secret is createdin Key Vault in Azure, you can create a secret in Azure Databricks as well by appending #secrets/createScope to the Azure
Databricks url  ending with .azuredatabricks.net

For instance if you create a secret with name/title : sec and a Databricks secret scope with name sec:

<application-id> with the Application (client) ID for the Azure Active Directory application. 

<scope-name> with the Databricks secret scope name. 'sec'

<service-credential-key-name> with the name of the key containing the client secret. 'sec'

<directory-id> with the Directory (tenant) ID for the Azure Active Directory application. (application's tenant id)

<container-name> with the name of a container in the ADLS Gen2 storage account. = 'cn-databricks'

<storage-account-name> with the ADLS Gen2 storage account name. = 'datalakestorageforbricks'

<mount-name> with the name of the intended mount point in DBFS. 'adls_in_databricks'

#### Get the service credential secret with 
service_credential = dbutils.secrets.get(scope="sec",key="sec")


With the above, set configuration:

spark.conf.set("fs.azure.account.auth.type.<storage-account-name>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account-name>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account-name>.dfs.core.windows.net", <client-id-of-application>)
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account-name>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account-name>.dfs.core.windows.net", "https://login.microsoftonline.com/<direcotry-id>/oauth2/token")


You can mount the data lake and then read and write into it as /dbfs/mnt/

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": <client-id-of-application>,
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope='sec', key="sec"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token"}

##### Optionally, you can add <directory-name> to the source URI of your mount point. Directory-id is the same as tenant-id

dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-point-name>",
  extra_configs = configs)

pandas_df.to_csv('/dbfs/mnt/adls_in_databricks/pandas_dataframe_stored_adls.csv', index=False)

Convert the spark dataframe to Pandas : df.toPandas()

``` python
temp_file = '/tmp/saved_df_as_excel.xlsx'
pandas_df.to_excel(temp_file)

Check for file : dbutils.fs.ls("file:/tmp/saved_df_as_excel.xlsx")

dbutils.fs.cp('file:/tmp/saved_df_as_excel.xlsx', 'dbfs:/mnt/<mount-name-in-databricks>/store_as_excel.xlsx')
```

#### Create a StructType 
> Note do not place parenthesis Convert a print schema to json  
 ```df.schema.json```
 
 #### Azure Storage Databricks Access
 [Picture](https://github.com/sjtalkar/DP-100AzureSupervisedUnsupervisedDatabricksAndSpark/blob/69897ec923a157b40a849896d56a6f803351aa11/Screenshot%202023-04-06%20082546.jpg)
 [Data pipeline](https://blog.devgenius.io/building-an-end-to-end-data-pipeline-using-azure-databricks-part-3-ea5941622edd)
 Screenshot 2023-04-06 082546.jpg
      
