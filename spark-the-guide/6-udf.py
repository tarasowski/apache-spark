# User Defined Functions
# UDF can take and return one or more columns as input
# UDF are just functions that operate on the data, record by record
# Workflow:
#   1. you create an udf
#   2. pass that into Spark
#   3. execute code using UDF

from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

udfExampleDF = spark.range(5).toDF('num')
udfExampleDF.createOrReplaceTempView("udfTable")

def power3(double_value):
    return double_value ** 3

# Now that we've created these functions and tested them, we need to register them with Spark so that we can use them on all of our worker machines
# Spark will serialize the function on the driver and transfer it over the network to all executor proceses.
# Python functions are costly, because serializing the data to Python is expensive

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType, DoubleType
power3udf = udf(power3)

udfExampleDF.select(power3udf(col("num"))).show(2)

# We can also register the function as a Spark SQL function. 
# By registering an udf as a Spark SQL we can use it within SQL as well as across languages

spark.udf.register("power3", power3, IntegerType())
udfExampleDF.selectExpr("power3(num)").show(2)

# To ensure that our functions work correctly we can specify a return type
# Spark manages its own type information, which does not align exactly with Python's types.
# The best practice to define the return type of your function when you define it

# If you specify the type that doesn't align with the actual type returned by the function, Spark will not throw an error but will just return null to designate a failure

# it going to return null because of the wrong return type
# we return integer, but here the return type is DoubleType
spark.udf.register("power3py", power3, DoubleType())
udfExampleDF.selectExpr("power3py(num)").show(2)

# the same here, the return type of power3py doesn't match the return type from the udf
spark.sql("select power3(num), power3py(num) from udfTable").show(2)

# Whe you want to optionally retur a value from a UDF, you should return None in Python and an Option type in Scala


