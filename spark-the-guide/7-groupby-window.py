# Aggregations
# Aggregation is the act of collecting something together
# In an aggregation you will specify a key or grouping and an aggregation function that specifies how you should transform one or more columns
# This function must produce one result for each group, given multiple input values
# You use aggregation to summarize numerical data usually by means of some grouping:
#   - This might be a summation
#   - A product
#   - Simple counting
# Also w/ Spark you can aggregate any kind of value into an array, list or map

# Spark allows to create following groupings types:
#   - The simplest grouping is to just summarize a complete DataFrame by performing an aggregation in a select statement
#   - A group by allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns
#   - A window gives you the ability to specify one or more keys as well as one or more aggregation functions to transform the value columns
#   - A grouping set which you can use to aggregate at multiple different levels
#   - A rollup makes it possible for you so specify one or more keys as well as one or more aggregation functions to transforma the value columns
#   - A cube allows you to specify one or more keys as well as one or more aggregation functions to transform the value columns
# Each grouping returns a RelationalGroupedDataset on which we specify our aggregations

from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("./Spark-The-Definitive-Guide/data/retail-data/all/*.csv")\
        .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")
df.show(5)


# The simplest aggregation is count
# Eagerly evaluated and is a method on a DF
df.count()

# Lazy evaluated and is not a method on DF
df.selectExpr("count(*)").show()

# All aggregations are available as functions
# An aggregate function performs a calculation one or more values and returns a single value

from pyspark.sql.functions import count, countDistinct, col, first, last, min, max, sum

df.select(count(col("StockCode"))).show()
df.selectExpr("count(StockCode)").show()
spark.sql("select count(StockCode) from dfTable").show()


# Get the first and last values from a DF
df.select(first(col("StockCode"))).show()
df.select(last(col("StockCode"))).show()
df.selectExpr(
        "first(StockCode) as first",
        "last(StockCode) as last"
        ).show()

# To extract min / max and get the sum from DF 

df.select(min(col("Quantity")), max(col("Quantity"))).show()
df.selectExpr(
        "min(Quantity) as min",
        "max(Quantity) as max",
        "sum(Quantity) as sum"
        ).show()

from pyspark.sql.functions import avg, expr

# To get avg

df.select(
        count(col("Quantity")).alias("total_transactions"),
        sum(col("Quantity")).alias("total_purchases"),
        avg(col("Quantity")).alias("avg_purchases"),
        expr("mean(Quantity)").alias("mean_purchases"))\
    .selectExpr(
            "total_purchases/total_transactions",
            "avg_purchases",
            "mean_purchases").show()

# Aggregation to Complex Types
# A set is a collection of distinct objects
from pyspark.sql.functions import collect_set, collect_list

# You can use this to pass to pass the entire collection in a user-defined function 
df.agg(collect_set("Country"), collect_list("Country")).show()

df.selectExpr(
        "collect_set(Country)",
        "collect_list(Country)"
        ).show()

# Grouping
# The above aggregations are DF-level aggregations.
# 1. We specify the column(s) on which we would like to group  --> The first step returns a RelationalGroupedDataset
# 2. Then we specify aggregation(s) --> The second step returns a DataFrame

df.groupBy("InvoiceNo", "CustomerId").count().show()

spark.sql("""
        select InvoiceNo, CustomerId, count(*) from dfTable group by InvoiceNo, CustomerId
        """).show()

# Grouping with Expressions

df.groupBy("InvoiceNo").agg(
        count("Quantity").alias("quan"),
        expr("count(Quantity) as quan2")).show()

# Grouping with Maps
df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"), expr("stddev_pop(Quantity)"))\
  .show()


# Window Functions
# A group-by takes data, and every row can go only into one grouping. 
# A window function calculates a return value for every row of a table based on a group of rows, called a frame.
# Each row can fall into one or more frames
# Spark supports three kinds of window functions: ranking functions, analytics functions, and aggregate functions


# We will add a date column that will convert our invoice date into a column that contains only date information
from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")


# 1. The first step to a window function is to create a window specification. 
#   - The partition by described how we will be breaking up our group
#   - The ordering determines the ordering within a given partition
#   - The frame specification (the rowsBetween statement) states wich rows will be included in the frame based on its reference to the current input row

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank, rank, max

windowSpec = Window\
        .partitionBy("CustomerId", "date")\
        .orderBy(desc("Quantity"))\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# 2. Now we want to use an aggregation function to learn more about specific customer.
#   - An example might be establishing the maximum purchase quantity over all time

purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)


# 3. Now we can perform a select to view the calculated window values

dfWithDate.where("CustomerId is not null").orderBy("CustomerId")\
          .select(
                  col("CustomerId"),
                  col("date"),
                  col("Quantity"),
                  purchaseRank.alias("quantityRank"),
                  purchaseDenseRank.alias("quantityDenseRank"),
                  maxPurchaseQuantity.alias("maxPurchaseQuantity"))\
          .show()

# Grouping Sets
# An aggregation across multiple groups. We achieve this by using grouping sets
# Grouping sets are a low-level tool for combining sets of aggregations together
# They give you the ability to create arbitrary aggregation in their group-by statements

# Example: Get the total quantity of all stock codes and customers
# Grouping sets depend on null values for aggregation levels. If you do not filter-out null values, you will get incorrect results
# This applies to cubes, rollups, and grouping sets

dfNoNull = dfWithDate.drop() # drops all columns with null values
dfNoNull.createOrReplaceTempView("dfNoNull")

spark.sql("""
        select CustomerId, stockCode, sum(Quantity)
        from dfNoNull
        group by CustomerId, stockCode
        order by CustomerId desc, stockCode desc
        """).show()

# Grouping Sets operator is only available in SQL. To perform the same in DataFrames, you use the rollups and cube operators
spark.sql("""
        select CustomerId, stockcode, sum(Quantity)
        from dfNoNull
        group by CustomerId, stockCode grouping sets((CustomerId, stockCode), ())
        order by CustomerId desc, stockCode desc
        """).show()

# Pivot
# Pivot makes it possible for you to convert a row into a column
# The DF will now have a column for every combination of country, numeric variable, and a column specifying the date.
# For example for USA we have the following columns: USA_sum(Quantity), USA_sum(UnitPrice), USA_sum(CustomerId). This represents one for each numeric column in our dataset
pivoted = dfWithDate.groupBy("date").pivot("Country").sum()
pivoted.createOrReplaceTempView("pivot")


# Pivot example doesn't work needs to be checked

