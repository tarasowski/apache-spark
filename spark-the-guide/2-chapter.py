# The SparkSession
# You control your application through a driver process called the SparkSession

from pyspark.sql import SparkSession


spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

myRange = spark.range(1000).toDF('number')
# This is a data frame with one column containing 1,000 rows with values from 0 to 999.
myRange.show()

# DataFrames
# A DataFrame is the most common Structured API and simply represents a table of data with rows and columns.
# You can think of a DataFrame as a spreadsheet with named columns
# The difference between it is that a spreadsheet sits on one computer, whereas a Spark DataFrame can span thousands of computers

# Schema
# The list that defines the columns and the types with those columns is called the schema.

# Partitions
# To allow every executor to perform work in parallel, Spark breaks up the data into chunks called partitions.
# A partition is a collection of rows that sit on one physical machine in your cluster


# Transformations
# To "change" a DataFrame, you need to instruct Spark how you would like to modify it to do what you want. These instructions are called transformations.
# Narrow dependency: each input partition contribute to only one output partition -> no shuffling
# Wide dependency: will have input partitions contributing to many output partitions. It's called shuffle whereby Spark will excahnge partitions across the cluster.
# With narrow transfromations, Spark will automatically perform an operation called pipelining, meaning that if we specify multple filters on DataFrames, 
# they'll be performed in-memory. The same cannot be said for shuffles. When we perform a shuffle, Spark writes the results to disk

divisBy2 = myRange.where('number % 2 = 0') # filter transfomartion


# Actions
# Transformations allow us to build up a logical transfromation plan. To trigger the computation, we need run an action.
# There are 3 kinds of actions:
#   - Actions to view data in the console
#   - Actions to collect data to native objects in the respective language
#   - Actions to write to output data sources

num = divisBy2.count() # count() is an action to collect data to native objects in the respective language 

# Spark job that runs:
# 1. Our filter transformation (a narrow transformation),
# 2. then an aggregation (a wide transformation) that performs the counts on a per partition basis and 
# 3. then a collect, which brings our results to a native object in the respective language.
#   collect() is basically hidden from us away in DF's, on RDD we need to use collect()


# Schema inference: Spark peeked at only a couple of rows of data to try to guess what types each column should be
flightData2015 = spark\
        .read\
        .option('inferSchema', 'true')\
        .option('header', 'true')\
        .csv('./Spark-The-Definitive-Guide/data/flight-data/csv/2015-summary.csv')

tk3 = flightData2015.take(3) # take() is an action to collect data to native objects in the respective language
print(tk3)

# Call explain() on any DataFrame object to see the DataFrame's lineage (or how Spark will execute this query)
# You can read explain plans from top to bottom, the top being the end result, and the bottom being the source(s) of data
# Take a look at the first keywords:
#   - sort
#   - exchange
#   - filescan
# That's because the sort of our data is actually a wide transformation because rows will need to be compared with one another.
flightData2015.sort('count').explain()

# By default, when we perform a shuffle, Spark outputs 200 shuffle partitions. Let's set this value to 5 to reduce the number of the output partitons from the shuffle:

spark.conf.set('spark.sql.shuffle.partitions', '5')
tk2 = flightData2015.sort('count').take(2)
print(tk2)

# DataFrames and SQL
# Spark can run the same transformations, regardless of the language, in the exact same way. You can express your logic in SQL or DataFrames (R, Python, Scala, Java)
# With Spark SQL you can register any DF as a table or view and query it using pure SQL. There is no performance difference between SQL or DataFrame code

flightData2015.createOrReplaceTempView('flight_data_2015')

# No, we'll use the spark.sql function that returns a new DataFrame
# spark is our SparkSession variable

sqlWay = spark.sql("""
        select DEST_COUNTRY_NAME, count(1)
        from flight_data_2015
        group by dest_country_name
        """)

dataFrameWay = flightData2015\
        .groupBy('DEST_COUNTRY_NAME')\
        .count()

# These plans compile to the exact same underlying plan!

sqlWay.explain()
dataFrameWay.explain()

from pyspark.sql.functions import max, desc

mxcSQL = spark.sql('select max(count) from flight_data_2015').take(1)
mxcDF = flightData2015.select(max('count')).take(1)

# This is how you can access the data from a list of row() objects
# take() is an action to collect data to native objects in python
print(mxcSQL[0][0], mxcDF[0][0])


maxSql = spark.sql("""
        select DEST_COUNTRY_NAME, sum(count) as destination_total
        from flight_data_2015
        group by DEST_COUNTRY_NAME
        order by sum(count) desc
        limit 5
        """)

maxSql.show()

maxDF = flightData2015\
        .groupBy('dest_country_name')\
        .sum('count')\
        .withColumnRenamed('sum(count)', 'destination_total')\
        .sort(desc('destination_total'))\
        .limit(5)\

maxDF.explain()

# Now there are seven steps that take us all the way back to the source data.
# This execution plan is a directed acyclic graph (DAG) of transformations, each resulting in a new immutable DataFrame, on which we call an action to generate a result
# CSV Fiel - read -> DataFrame - groupBy -> Grouped DataFrame - sum -> DataFrame - rename column -> DataFrame - sort -> DataFrame - limit -> DataFrame - collect -> Array(..)
# 1. Step: read the data
# 2. Step: grouping, we end up with a RelationalGroupedDataset, which is a fance name for DataFrame that has a grouping specified but needs the user to specify an aggregation before it can be queried further.
# 3. Step: specification of the aggregation, we use the sum aggregation method
# 4. Step: is renaming
# 5. Step: sorts the data that if we were to take results off the top of the DataFrame, they would have the largest values in the destination_total colmn
# 6. Step: we specify the limit for only top 5 results
# 7. Step: IS OUR ACTION. Now we begin the process of collecting the results of our DataFrame, and Spark will give us back a list or array in the language we're executing.

# We don't need to collect the data. We can also write it out to any data source that Spark supports. Store data in PostgreSQL or write them out to another file
# There are basically no instances in modern Spark, for which you should be using RDDs instead of the structured APIs beyond manipulating some very raw unprocessed and unstructured data
