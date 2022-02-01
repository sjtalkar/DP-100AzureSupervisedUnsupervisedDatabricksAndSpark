## STEPS TO WORK WITH DATABRICKS ON AZURE

Any time you want to use Databricks Community Edition in the future, head straight to community.cloud.databricks.com and login with your username and your password.

1. Create a compute cluster.
    - Specify the compute size and Databricks Runtime
    - One the Cluster is created, you can add additional libraries
2. %fs
   ls dbfs:/databricks-datasets/nyctaxi/tripdata/yellow
3. https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/897686883903747/2503669437642038/312541189568512/latest.html
4. Converting Spark dataframe to Pandas
    https://sparkbyexamples.com/pyspark/convert-pyspark-dataframe-to-pandas/
5. Remove Databricks files from Filestore
    %fs
    rm /FileStore/tables/wellcompletionreports.csv
 6. Once CREATE TABLE FROM UI has been performed with row headers checked and Infer Schema checked. You can check the Databricks tables using
     %sql 
    show tables
  7. Rename a table
      %sql
     ALTER TABLE wellcompletionreports_2_csv RENAME TO wellcompletionreports
  8. When using Create a table from UI is used to upload a CSV file, if TIMESTAMP type is used for a date field it can result in a null.
    https://stackoverflow.com/questions/66454529/how-to-convert-string-to-date-in-databricks-sql
    
    Read this : https://stackoverflow.com/questions/40763796/convert-date-from-string-to-date-format-in-dataframes
    ```
    #### Converting single digit month and dates in date string
    from pyspark.sql.functions import isnan, when, count, col, to_date
    from pyspark.sql.types import *


    #Create a UDF for converting date into a format that will allow for to_date function to be used
    def convert_to_date_format(this_date):
      try:
        date_string_split = this_date.split("/")
        finaldate = date_string_split[2]+"-" + date_string_split[0].zfill(2)  + "-"+  date_string_split[1].zfill(2)
        print(finaldate)
      except:
        finaldate = None

      return finaldate

    udfConvertDateFormat = udf(convert_to_date_format, StringType())
    
    from pyspark.sql.functions import isnan, when, count, col, to_date
    #Add a column to df
    df = df.withColumn("PERMITDATEFORMAT", to_date(udfConvertDateFormat(col("PERMITDATE"))))

   
    # The below does convert the single digit month/date format to date but a write error is encountered upon storing into a table 
     spark.sql("""
                    SELECT TO_DATE(CAST(UNIX_TIMESTAMP(PERMITDATE, 'MM/dd/yyyy') AS TIMESTAMP)) AS PERMITDATEFORMAT FROM wellcompletionreports"""
        ).show()
```

9. Switching between Databricks tables and Spark Dataframes
      https://datamajor.net/convert-dataframe-into-table-in-spark/
      
   10. This works but the create table does not
   11.   SELECT
    WCRNUMBER,
    PERMITNUMBER,
    COUNTYNAME,
    WELLLOCATION,
    CITY,
    PLANNEDUSEFORMERUSE,
    DRILLERNAME,
    DECIMALLATITUDE,
    DECIMALLONGITUDE,
    GROUNDSURFACEELEVATION,
    TO_DATE(CAST(UNIX_TIMESTAMP(PERMITDATE, 'MM/dd/yyyy') AS TIMESTAMP)) AS PERMITDATEFORMAT,
    TO_DATE(CAST(UNIX_TIMESTAMP(DATEWORKENDED, 'MM/dd/yyyy') AS TIMESTAMP)) AS DATEWORKENDEDFORMAT,
    TO_DATE(CAST(UNIX_TIMESTAMP(RECEIVEDDATE, 'MM/dd/yyyy') AS TIMESTAMP)) AS RECEIVEDDATEFORMAT,
    TOTALDRILLDEPTH,
    TOPOFPERFORATEDINTERVAL,
    BOTTOMOFPERFORATEDINTERVAL,
    CASINGDIAMETER,
    STATICWATERLEVEL,
    WELLYIELD,
    WELLYIELDUNITOFMEASURE
  FROM wellcompletionreports

```python
# Read the csv
val csvFile = "/databricks-datasets/flights/departuredelays.csv"
val tempDF = (spark.read         
   .option("sep", ",")
   .option("header", "true")  
   .option("inferSchema", "true")           
   .csv(csvFile)
)

#Convert dataframe to view
tempDF.createOrReplaceTempView("AirportCodes")


#Save it as a table
tempDF.write.saveAsTable("tbl_AirportCodes")

#Reverse procedure 
val df_ResAirportCodes = spark.read.table("Tbl_AirportCodes")

```

    
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




#Using Windowing functions in Spark
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, row_number, monotonically_increasing_id

display(df.orderBy('tripDistance', ascending=False) \
  .withColumn('rowno', row_number().over(Window.orderBy(monotonically_increasing_id()))))
  
# Using SQL
result = spark.sql("select *,  row_number() over (order by tripDistance desc) as rowno from nyc_taxi order by tripDistance desc")
result.show(7)
  
  
#Check for presence of null value in each column
display(df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]))


#Above in SQL
- First create the SQL using Python
["sum(case when " +  col + " is null then 1 else 0 end) as " + col  for col in df.columns]

#Use the generated SQL after removing quotes
result = spark.sql("""
select sum(case when passengerCount is null then 1 else 0 end) as passengerCount,
 sum(case when tripDistance is null then 1 else 0 end) as tripDistance,
 sum(case when hour_of_day is null then 1 else 0 end) as hour_of_day,
 sum(case when day_of_week is null then 1 else 0 end) as day_of_week,
 sum(case when month_num is null then 1 else 0 end) as month_num,
 sum(case when normalizeHolidayName is null then 1 else 0 end) as normalizeHolidayName,
 sum(case when isPaidTimeOff is null then 1 else 0 end) as isPaidTimeOff,
 sum(case when snowDepth is null then 1 else 0 end) as snowDepth,
 sum(case when precipTime is null then 1 else 0 end) as precipTime,
 sum(case when precipDepth is null then 1 else 0 end) as precipDepth,
 sum(case when temperature is null then 1 else 0 end) as temperature,
 sum(case when totalAmount is null then 1 else 0 end) as totalAmount
from nyc_taxi
""")
result.show()



```

### Predictive modeling is largely based on statistical relationships between fields in the data. To design a good model, you need to understand how the data points relate to one another.

## Featurization
Cleaning data and adding features creates the inputs for machine learning models, which are only as strong as the data they are fed. This notebook examines the process of featurization including common tasks such as:

- Handling missing data
                Null values refer to unknown or missing data as well as irrelevant responses. Strategies for dealing with this scenario include:

                - Dropping these records: Works when you do not need to use the information for downstream workloads
                - Adding a placeholder (e.g. -1): Allows you to see missing data later on without violating a schema
                - Basic imputing: Allows you to have a "best guess" of what the data could have been, often by using the mean of non-missing data
                - Advanced imputing: Determines the "best guess" of what data should be using more advanced strategies such as clustering machine learning algorithms or oversampling techniques such as SMOTE.
                - 
- Feature Engineering
- Scaling Numeric features
- Encoding Categorical Features


## Feature engineering
##### Naturally Cyclical features
```python
def get_sin_cosine(value, max_value):
  sine =  np.sin(value * (2.*np.pi/max_value))
  cosine = np.cos(value * (2.*np.pi/max_value))
  return (sine.tolist(), cosine.tolist())

schema = StructType([
    StructField("sine", DoubleType(), False),
    StructField("cosine", DoubleType(), False)
])

get_sin_cosineUDF = udf(get_sin_cosine, schema)

print("UDF get_sin_cosineUDF defined.")


engineeredDF = imputedDF.withColumn("udfResult", get_sin_cosineUDF(col("hour_of_day"), lit(24))).withColumn("hour_sine", col("udfResult.sine")).withColumn("hour_cosine", col("udfResult.cosine")).drop("udfResult").drop("hour_of_day")
display(engineeredDF)

```
https://ianlondon.github.io/blog/encoding-cyclical-features-24hour-time/

## Vectorizing in PySpark
``` python
from pyspark.ml.feature import Imputer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoder
from pyspark.ml import Pipeline

numerical_cols = ["passengerCount", "tripDistance", "snowDepth", "precipTime", "precipDepth", "temperature", "hour_sine", "hour_cosine"]
categorical_cols = ["day_of_week", "month_num", "normalizeHolidayName", "isPaidTimeOff"]
label_column = "totalAmount"

stages = []

inputCols = ["passengerCount"]
outputCols = ["passengerCount"]
imputer = Imputer(strategy="median", inputCols=inputCols, outputCols=outputCols)
stages += [imputer]

assembler = VectorAssembler().setInputCols(numerical_cols).setOutputCol('numerical_features')
scaler = MinMaxScaler(inputCol=assembler.getOutputCol(), outputCol="scaled_numerical_features")
stages += [assembler, scaler]

for categorical_col in categorical_cols:
    # Category Indexing with StringIndexer
    stringIndexer = StringIndexer(inputCol=categorical_col, outputCol=categorical_col + "_index", handleInvalid="skip")
    encoder = OneHotEncoder(inputCols=[stringIndexer.getOutputCol()], outputCols=[categorical_col + "_classVector"])
    # Add stages.  These are not run here, but will run all at once later on.
    stages += [stringIndexer, encoder]
    
print("Created stages in our featurization pipeline to scale the numerical features and to encode the categorical features.")

assemblerInputs = [c + "_classVector" for c in categorical_cols] + ["scaled_numerical_features"]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
stages += [assembler]
print("Used a VectorAssembler to combine all the feature columns into a single vector column named features.")


partialPipeline = Pipeline().setStages(stages)
pipelineModel = partialPipeline.fit(dataset)
preppedDataDF = pipelineModel.transform(dataset)

display(preppedDataDF)


from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol=label_column)

lrModel = lr.fit(trainingData)

print(lrModel)

summary = lrModel.summary
print("RMSE score: {} \nMAE score: {} \nR2 score: {}".format(summary.rootMeanSquaredError, summary.meanAbsoluteError, lrModel.summary.r2))
print("")
print("β0 (intercept): {}".format(lrModel.intercept))
i = 0
for coef in lrModel.coefficients:
  i += 1
  print("β{} (coefficient): {}".format(i, coef))
  
  
  from pyspark.ml.evaluation import RegressionEvaluator

predictions = lrModel.transform(testData)
evaluator = RegressionEvaluator(
    labelCol=label_column, predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
evaluator = RegressionEvaluator(
    labelCol=label_column, predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)
print("MAE on test data = %g" % mae)
evaluator = RegressionEvaluator(
    labelCol=label_column, predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print("R2 on test data = %g" % r2)


p_df = predictions.select(["totalAmount",  "prediction"]).toPandas()
true_value = p_df.totalAmount
predicted_value = p_df.prediction

plt.figure(figsize=(10,10))
plt.scatter(true_value, predicted_value, c='crimson')
plt.yscale('log')
plt.xscale('log')

display(predictions.select(["totalAmount",  "prediction"]).describe())


p1 = max(max(predicted_value), max(true_value))
p2 = min(min(predicted_value), min(true_value))
plt.plot([p1, p2], [p1, p2], 'b-')
plt.xlabel('True Values', fontsize=15)
plt.ylabel('Predictions', fontsize=15)
plt.axis('equal')
plt.show()

```

## How to download a spark dataframe to local computer as a CSV

[Save CSV to local computer](https://towardsdatascience.com/databricks-how-to-save-files-in-csv-on-your-local-computer-3d0c70e6a9ab)
- Save the dataframe to the dbfs Filestore by creating a folder
```
full_df.coalesce(1).write.format('com.databricks.spark.csv').option('header', "true").save("dbfs:/FileStore/JetBrainsDF/full_jetbrains_df.csv")

dbutils.fs.ls("dbfs:/FileStore/JetBrainsDF/full_jetbrains_df.csv")

```

- Go to Compute => Create Table => DBFS
- Click on the file in DBFS folder. Copy the browser URL and the path at the bottom of the table
- https://adb-8268979028189023.3.azuredatabricks.net/**?o=8268979028189023**#tables/new/dbfs
- /FileStore/**JetBrainsDF/full_jetbrains_df.csv/part-00000-tid-3693693608208467924-89eccd7f-da0f-4e5a-8633-7bbaf60461dc-258-1-c000.csv**
- Combine them and paste in a new browser window and the CSV will be downloaded
- https://adb-8268979028189023.3.azuredatabricks.net/files/JetBrainsDF/full_jetbrains_df.csv/**part-00000**-tid-3693693608208467924-89eccd7f-da0f-4e5a-8633-7bbaf60461dc-258-1-c000.csv?o=8268979028189023
- Note that only the part-0000 contains full data . Due to coalesce (1) not partitioning occurs .
- Note that the stored CSV can be used to recreate the dataframe
```# File location and type
file_location = "/FileStore/JetBrainsDF/full_jetbrains_df.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
recreate_df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(recreate_df)
```




## DELTA LAKE
> Delta Lake is a robust storage solution designed specifically to work with Apache Spark. It was created as part of a new data management paradigm. In the past, organizations commonly used data warehouses to store their data. While advances in data warehouses have allowed them to handle larger and larger data sizes, many organizations also have to work with unstructured high-velocity data. Data warehouses are not suited for these types of use cases, and they're definitely not cost efficient. As this unstructured high-velocity data became more and more popular, many organizations moved to data lakes. Data lakes are a single system to store many different types of data, and support different types of analytics products and workloads. However, data lakes lack a few key features of data warehouses. They don't enforce data quality, or support ACID transactions, which are a set of database properties that guarantee data validity, during mishaps. What we end up with are two options. The compliant, reliable, and incomplete data warehouse, or the complete, unreliable, and non-compliant data lake. You can probably guess where we're going with this in your right. Delta Lake is the best of both of these worlds. Delta Lake is a data lake house. It has all of the features of a data warehouse, and the low cost Cloud storage solutions, of the data lake. Next, we'll show you how to use Delta Lake to store your data on Databricks, which will be really useful as you progress through the data science projects in this course. As we mentioned earlier, Delta Lake is built directly into Databricks.



## Reading files from S3 (public) into databricks
import json
from urllib.request import urlretrieve


files = ['ntb_2020_consistency.csv', 'ntb_2020_from_mooc.csv', 'ntb_2020_imports.csv', 'ntb_2020_md_stats.csv',
         'ntb_2020_text_counts.csv', 'ntb_2020_versions.csv', '2019_imports_4128764_nbs.json']
for f in files:
  url= f'https://github-notebooks-samples.s3-eu-west-1.amazonaws.com/{f}'
  dest = f"/tmp/{f}"
  urlretrieve(url,dest)

#### Check if the file exists
dbutils.fs.ls("file:/tmp/ntb_2020_consistency.csv")

## Move the files to DBFS
for f in files:
  dbutils.fs.mv(f"file:/tmp/{f}",f"dbfs:/data/{f}")
  
## Check if files exist in DBFS
for f in files:
  print(dbutils.fs.ls(f"dbfs:/data/{f}"))
  
## Load it into dataframes

df_dict ={}  
for i, f in enumerate(files):
  dfname = "df_" + f.split(".")[0]
  df_dict[dfname] = spark.read.format('csv').load(f"dbfs:/data/{f}")
dbutils.fs.mv("file:/tmp/ntb_2020_consistency.csv","dbfs:/data/ntb_2020_consistency.csv")
df = spark.read.format('csv').load("dbfs:/data/ntb_2020_consistency.csv")
display (df)



![Algorithm selection](https://github.com/sjtalkar/DP-100AzureSupervisedUnsupervisedDatabricksAndSpark/blob/main/Fig11.png)

## Tree based models vs Linear regression
- Non-Linearity in the data
- Doesn't assume linear relation
- Scaling not required
- Better Accuracy
- Better for categorical independent variables
- Handles feature collinearity better
- They are not sensitive to outliers or the variance in the data. 
- One hot encoding is inefficient for tree based algorithms. For categorical variables it is better to use Label encoding. But the danger in this is that:
     If categorical data is not truly  ordinal the tree will end up making splits that do not make sense, because the
    model will interpret these numbers as ranked

## Tree pre-pruning
Pre-pruning criteria can include:
    ● Maximum tree depth
        ○ limiting how deep the tree grows (how many levels of splitting)
    ● Minimum node size
        ○ requiring that each node have a minimum number of data points
        in order to split it further
    ● Minimum leaf size
        ○ Requiring at least a certain number of data points in each leaf
    ● Maximum features
        ○ maximum number of features to consider at each split
        ○ Introduces randomness

### Tuning hyper-parameters by paying attention to bias and variance
1. Set max_depth, check for high bias     (poor training score) / low bias  (good training score) 
2.                          high variance (poor test score)    / low variance (good test score)

## Aggregating Bootstrapped Results

We learned about the bootstrap method in the last video. In general,
bootstrap aggregating (bagging) is a method for reducing the variance of a
model: it is especially useful when applied to decision trees that often suffer
from high variance - involves taking many sampled/bootstrapped training
data sets, building separate models on each, and averaging the resulting
predictions, to obtain a model with lower variance.

How do we actually aggregate the results of different models into one output
or result? For categorical there are two methods - with “hard voting”, we take
the majority vote of all the models and assign the unknown data point to that
class. Another method, called soft voting, is where we consider the
probabilities of each class returned by all the models, average these
probabilities and keep the class with the highest average probability.

For numerical, average the results of each individual model and apply that to
the unknown data point to get the result of the ensemble model.

We mentioned boosting as another type of ensemble model in a previous
video. Different from bagging - instead of combining multiple independent
trees in parallel, it iteratively combines trees sequentially into one model. The
trees are not independent - each tree attempts to correct the errors of the
previous tree, by giving more weight to observations in the dataset that were
incorrectly predicted by the previous models in the sequence. The resulting
ensemble model will have less underfitting and bias than the individual weak
learner models. One commonly used boosting technique is Gradient Boosting
Trees. Another is called xgboost. These are often used to win data science
competitions.

 Bagging combines models in parallel, while boosting combines models sequentially.
 
 
 The individual trees in any ensemble algorithm can be either shallow (not
many depths of decisions) or deep (lots of depths). Shallow trees have less
variance but higher bias, so they are generally a better choice for boosting or
sequential methods that decrease bias. Conversely, Deep trees have low bias
but high variance and are good for bagging or parallel methods that result in
lower variance.


#### Interpretation of the Standard Error

The simplest interpretation uses a t-statistic. Suppose the data at
hand come from a distribution for which we believe the population mean, E[Yi], takes on a
particular value, μ (read this Greek letter as “mu”). This value constitutes a working
hypothesis. A t-statistic for the sample mean under the working hypothesis that E[Yi] = μ is
constructed as    

t(mu) = (Sample mean - population mean)/standard error 

The working hypothesis is a reference point that is often called the null hypothesis. When
the null hypothesis is μ = 0, the t-statistic is the ratio of the sample mean to its estimated
standard error.

One miraculous statistical fact is that if E[Yi] is indeed equal to μ,
then—as long as the sample is large enough—the quantity t(μ) has a sampling distribution
that is very close to a bell-shaped standard normal distribution, sketched in Figure 1.1.
This property, which applies regardless of whether Yi itself is normally distributed, is
called the Central Limit Theorem (CLT). The CLT allows us to make an empirically
informed decision as to whether the available data support or cast doubt on the hypothesis
that E[Yi] equals μ.

The CLT is an astonishing and powerful result. Among other things, it implies that the
(large-sample) distribution of a t-statistic is independent of the distribution of the
underlying data used to calculate it.x

The standard normal distribution has a mean of 0 and standard deviation of 1. With any
standard normal variable, values larger than ±2 are highly unlikely. In fact, realizations
larger than 2 in absolute value appear only about 5% of the time. Because the t-statistic is
close to normally distributed, we similarly expect it to fall between about ±2 most of the
time. Therefore, it’s customary to judge any t-statistic larger than about 2 (in absolute
value) as too unlikely to be consistent with the null hypothesis used to construct it. When
the null hypothesis is μ = 0 and the t-statistic exceeds 2 in absolute value, we say the
sample mean is significantly different from zero. Otherwise, it’s not. Similar language is
used for other values of μ as well.


MASTER STEVEFU: In a nutshell, please, Grasshopper.
GRASSHOPPER: Causal comparisons compare like with like. In assessing the
effects of college choice, we focus on students with similar characteristics.
MASTER STEVEFU: Each is different in a thousand ways. Must all ways be similar?
GRASSHOPPER: Good comparisons eliminate systematic differences between those
who chose one path and those who choose another, when such differences are
associated with outcomes.
MASTER STEVEFU: How is this accomplished?
GRASSHOPPER: The method of matching sorts individuals into groups with the
same values of control variables, like measures of ability and family
background. Matched comparisons within these groups are then averaged to get
a single overall effect.
MASTER STEVEFU: And regression?
GRASSHOPPER: Regression is an automated matchmaker. The regression estimate
of a causal effect is also an average of within-group comparisons



Regression is a way to make other things equal, but
equality is generated only for variables included as controls on the right-hand side of the
model. Failure to include enough controls or the right controls still leaves us with selection
bias. The regression version of the selection bias generated by inadequate controls is
called omitted variables bias (OVB), and it’s one of the most important ideas in the
’metrics canon.

Galton
seems to have been uninterested in regression as a control strategy. The use of regression
for statistical control was pioneered by George Udny Yule, a student of statistician Karl
Pearson, who was Galton’s protégé.
