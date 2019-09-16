# https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4
# Sparks splits data into partitions and executes computations on the partitions in parallel

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import col
from pyspark.sql import types as t
import sys
from pyspark.sql.window import Window
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import Row

def show_partition_id(df):
    return df.select(*df.columns, spark_partition_id().alias("partition_id")).show()

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

numbersDF = spark.range(10).toDF('number')
print("The number of partitions after range(10)", numbersDF.rdd.getNumPartitions()) # 7

# numbersDF.write.csv("./spark_output/numbers")
# Each partition is a separate CSV file when you write a DF to disc
# The write method creates 7 files
# Partition 1: 0
# Partition 2: 1
# Partition 3: 2, 3
# Partition 4: 4
# Partition 5: 5, 6
# Partition 6: 7
# Partition 7: 8, 9

# coalesce
# The coalesce method reduces the number of partitions in a DF
numbersDF2 = numbersDF.coalesce(2)
print("The number of partitions after coalesce(10)", numbersDF2.rdd.getNumPartitions()) # 2
print("numbersDF2")
show_partition_id(numbersDF2)

#numbersDF2.write.csv("./spark_output/numbers2")
# The write method creates 2 files
# Partition 1: 0, 1, 2, 3 
# Partition 2: 4, 5, 6, 7, 8, 9
# The coalesce algorithm moves the data from Partition 2 and 3 to Partition 1 and moved the data from Partition 4, 5, 6, 7 to **Partition 2**

# you cannot increate the partitions with coalesce e.g. coalesce(10)
# The coalesce algorithm changes the number of nodes by moving data from some partitions to existing partitions
# The algorithm cannot increase the numbers of partitions, it can combine data to smaller number of nodes


# repartition
# The repartition method can be used to either increase or decrease the number of partitions in a DF
homerDF = numbersDF.repartition(2)
print("The number of partitions after repartition(2)", homerDF.rdd.getNumPartitions())
print("homerDF partitions")
show_partition_id(homerDF)
#homerDF.write.csv("./spark_output/numbers_homer")
# The write methods creates 2 files
# Partition 1: 0, 1, 3, 4, 5, 7, 8 
# Partition 2: 2, 6, 9
# The Partition 1 contains data from Partition 1,  3, 4, 5, 6, 7
# The Partition 2 contans data from Partition 2, 5, 7
# The repartition algorithm does a full data shuffle and equally distributes the data among the partitions. It does not attempt to minimize data movement like the coalesce algorithm


# increasing partitions
# The repartition method can be used to increase the number of partitions
bartDF = numbersDF.repartition(6)
print("bartDF partitions")
print("The number of partitions after repartition(6)", bartDF.rdd.getNumPartitions())


show_partition_id(bartDF)
# Partition 1: 2, 7
# Partition 2: 0 
# Partition 3: 8 
# Partition 4: 4, 5, 9 
# Partition 5: 1, 6 
# Partition 6: 3 

# Differences between coalesce and repartition
# The repartition algorithm does a full shuffle of the data and creates equal sized partitions of data.
# Coalesce combines existing partitions to avoid a full shuffle

# repartition by column
# Let's use the following data to examine how a DF can be repartitioned by a particular column

l = [Row(10, "blue"), Row(13, "red"), Row(15, "blue"), Row(99, "red"), Row(67, "blue")]
people = spark.createDataFrame(l)
people = people.select(col("_1").alias("age"), col("_2").alias("color"))

people.show()

# repartition by column
# colorDF contains different partitions for each color
# Partitioning by a column is similar to indexing a column in a relational database
colorDF = people.repartition("color")
print("The number of partition after repartition('color'): ", colorDF.rdd.getNumPartitions()) # 200
print(show_partition_id(colorDF))
# Partition 00091
# 13, red
# 99, red
# Partition 00168
# 10, blue
# 15, blue
# 67, blue


# Spark doesnt adjust the number of partitions when a large DF is filtered
# Data late 2B rows of data split into 13,000 partitions
# Data puddle is 2,000 rows of data and still split into 13,000 partitions -> many of the partitions will be empty
# We should repartition in order to optimize the read / write performance e.g. data_puddle.repartition(4)
# Why 4 partitions for the data puddle?
#   - The data is million times smaller, so we reduce the number of partitions by a millions 13,000 / 1,000,000 = 1 partition (rounded up). We use 4 so data puddle can leverage the parallelism of Spark
#   - You can determine the number of partitions by multiplying the number of CPUs in the cluster by 2, 3, or 4
#   - On my local machine 7 cpus

# Why to choose repartition instead of coalesce?
# For small files repartition method returns equal sized text files, which are most more efficient for downstream consumers
# Performance improvement
#   - it took 241 seconds to count the rows in the puddle when data wasn't repartitioned (on a 5 node cluster)
#   - it only took 2 seconds to count the data puddle when the data was partitioned


# The number of partitions set to 3 or 4 times the number of CPU cores in your cluster!!!
