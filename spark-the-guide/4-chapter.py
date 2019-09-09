from pyspark.sql import SparkSession


spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()


df = spark.range(500).toDF('number')

# Spark is effectively a programming language of its own.
# Spark uses an engine called Catalyst that maintains its own type information
# Spark types map directly to the different language APIS that Spark maintains and there exists a lookup table for each of these e.g. Python
# For example, the following code does not perform addition in Python; it performs addition purely in Spark
df.select(df['number'] + 10).show(10)
df.selectExpr('number + 10').show(10)


# Columns
# Columns represent a simple type like an integer or string, a complex type line an array or map, or null value

# Rows
# A row is nothing more than a record of data. Each record in a DataFrame must be of type Row.
# We can create these rows manually from SQL, from RDD, from data sources, or manually from scratch

print(spark.range(2).collect())
