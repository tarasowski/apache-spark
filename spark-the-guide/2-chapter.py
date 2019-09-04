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

divisBy2 = myRange.where('number % 2 = 0')


# Actions
# Transformations allow us to build up a logical transfromation plan. To trigger the computation, we need run an action.
# There are 3 kinds of actions:
#   - Actions to view data in the console
#   - Actions to collect data to native objects in the respective language
#   - Actions to write to output data sources

num = divisBy2.count() # return int see part 3

# Spark job that runs:
# 1. Our filter transformation (a narrow transformation),
# 2. then an aggregation (a wide transformation) that performs the counts on a per partition basis and 
# 3. then a collect, which brings our results to a native object in the respective language.
