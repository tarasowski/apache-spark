# Overview Spark


# Structured Streaming
# With Structured Streaming, you can take the same operations that you perform in batch mode using Spark's structured APIs and run them in a streaming fashion.

from pyspark.sql import SparkSession


spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

# Because we're running on local machine, it's a good practice to set the numbe rof shuffle partitions to something that's going to be a better fit for local mode.
spark.conf.set('spark.sql.shuffle.partitions', '5')

staticDataFrame = spark.read.format('csv')\
        .option('header', 'true')\
        .option('inferSchema', 'true')\
        .load('./Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv')


# We are working here with time series. A time series is a series of data points indexed in time order.

staticDataFrame.createOrReplaceTempView('retail_data')
staticSchema = staticDataFrame.schema # prints the schema definition


from pyspark.sql.functions import window, column, desc, col, date_format

staticDataFrame\
        .selectExpr(
                'CustomerId',
                '(UnitPrice * Quantity) as total_cost',
                'InvoiceDate')\
        .groupBy(
                col('CustomerId'), window(col('InvoiceDate'), '1 day'))\
        .sum('total_cost')\
        .sort(desc('sum(total_cost)'))\
        .show(5)


streamingDataFrame = spark.readStream\
        .schema(staticSchema)\
        .option('maxFilesPerTrigger', '1')\
        .format('csv')\
        .option('header', 'true')\
        .load('./Spark-The-Definitive-Guide/data/retail-data/by-day/*.csv')

stream = streamingDataFrame.isStreaming # returns True

staticDataFrame.printSchema() # prints the schema metadata

# Lower-Level APIs

# Everything in Spark is built on top of RDDs.
# DataFrames are built on top of RDDs and compile down to these lower-level tools for fast execution
# RDDs are lower level than DataFrames because they reveal physical execution characteristics (like partitions) to end users.
# There are basically no instances in modern Spark, for which you should be using RDDs instead of the structured APIs beyond manipulating some very raw unprocessed and unstructured data
