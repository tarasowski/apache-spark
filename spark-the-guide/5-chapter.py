from pyspark.sql import SparkSession


spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

df = spark.read.format('json').load('./Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json')
df.printSchema()

# A schema defines the column names and types of DataFrame.
# We can let a data source define the schema (schema-on-read) or we can explicitly define it ourselves
# Warning: When using Spark for production ETL, it is often a good idea to define your schema manually.

static_schema = df.schema
print(static_schema)
# StructType(List(StructField(DEST_COUNTRY_NAME,StringType,true),StructField(ORIGIN_COUNTRY_NAME,StringType,true),StructField(count,LongType,true)))

# If the types in the data (at runtime) do not match the schema, Spark will throw an error
# The example below shows how to create and enforce a specific schema


from pyspark.sql.types import StructField, StructType, StringType, LongType

# We cannot set types via the per-language types because Spark maintains its own type information

my_manual_schema = StructType([
        StructField('DEST_COUNTRY_NAME', StringType(), True),
        StructField('ORIGIN_COUNTRY_NAME', StringType(), True),
        StructField('count', LongType(), False, metadata={'hello': 'world'}) # metadata can be used for machine learning
        ])

df = spark.read.format('json').schema(my_manual_schema)\
        .load('./Spark-The-Definitive-Guide/data/flight-data/json/2015-summary.json')

df.printSchema()

# Columns and Expressions
# You can select, manipulate, and remove columns from DataFrames and these operations are represented as expressions
# To Spark, columns are logical constructs that simply represent a value computed on a per-record basis by means of an expression.
# This means to have a real value for a column, we need to have a row; and to have a row, we need to have a DataFrame

# SQL Basics
# SQL expression is a combination of one or more values, operators and SQL functions that results in to a value.
# SQL Expressions can be classified into categories: Boolean, Numeric, Date, Interval, Condition value expression
# Regardless of its complexity, an expression must reduce to a single value
# Combining multiple value expression into a single expression is possible, as long as the component value expressions reduce to value that have compatible data types

# Columns
# There are a lot of different ways to construct or refer to columns.


from pyspark.sql.functions import col, column, expr, sum

col('someColumnName')
column('someColumnName')

# Columns are not resolved until we compare the column names with those we are maintinaing in the catalog.
# Column and talbe resolution happens in the analyzer phase

# Explicit column references
# If you need to refer to a specific DataFrame's column, you can use the col method on the specific DataFrame
df['count']
df.select(col('count'))


# Expressions
# An expression is a set of transformations on one or more values in a record in a DataFrame. Think of it like a function that takes as input one or more column names, resolves them, and the potentially applies more expressions to create a single value for each record in the dataset
# In the simplest case, an expression, created via the expr function is just a DataFrame column reference expr('someCol') is equivalent to col('someCol')
# Expression evaluates to a single value, therefore we'll just get count inside the select function
df.select(expr('count')).show(5)

print(expr('count'))

# expr('someCol - 5') is the same transformation as performing col('someCol') - 5, or even expr('someCol') - 5
# This might be confusing, but remember a couple of key points:
#   - Columns are just expression - they evaluate to a value if we do expr('count') it evaluates to a value for each row
#   - Columns and transformations of those columns compile to the same logical plan as parsed expression

(((col('someCol') + 5) * 200) - 6) < col('otherCol')

# You can write your expressions as DataFrame code or as SQL expressions and get the same performance characteristics.
df.select(expr('(((count + 5) * 200) - 6)')).show(5)
df.select((((col('count') + 5) * 200) - 6)).show(5)


from pyspark.sql import Row

# Accessing data in rows: you can specify the position that you would like
myRow = Row('Hello', None, 1)

r1 = myRow[0]
print(r1) # hello

df.createOrReplaceTempView('dfTable')

# Create DataFrame on the fly

my_schema = StructType([
        StructField('some', StringType(), True),
        StructField('col', StringType(), True),
        StructField('names', LongType(), False, metadata={'hello': 'world'})
        ])

myDF = spark.createDataFrame([myRow], my_schema)
myDF.show()

# select method when you're working with columns or expressions
# selectExpr method when you're working with expressions in strings
# some transformations are not specified as methods on columns; therefore, there exists a group of functions found in the functions package
# with these three tools, you should be able to solve the vast majority of transformation challenges

df.select('DEST_COUNTRY_NAME').show(2)

df.select('DEST_COUNTRY_NAME', 'ORIGIN_COUNTRY_NAME').show(2)
# select dest_country_name, origin_country_name from dfTable limit 2


# You can refer to columns in a number of different ways; you can use them interchangeably

from pyspark.sql.functions import expr, col, column

df.select(
        expr('dest_country_name'),
        col('dest_country_name'),
        column('dest_country_name')
        ).show(2)

#One common error is attempting to mix Column objects and strings. For example the following code
# df.select(col('dest_country_name'), 'dest_country_name')

# expr is the most flexible reference that we can use. It can refer to a plain column or a string manipulation of a column
df.select(expr('dest_country_name as destination')).show(2)
# select dest_country_name as destination from dfTable limit 2

# You can further manipulate the result of your expression as another expression
# It changes the column name back to it's original
df.select(expr('dest_country_name as destination').alias('dest_country_name')).show(2)

# We can treat selectExpr as a simple way to build up complex expression that create new DataFrame.
# In fact, we can add any valid non-aggregating SQL statement, and as long as the columns resolve it will be valid

df.selectExpr(
        '*',
        '(dest_country_name = origin_country_name) as withinCountry')\
                .show(2)
# select *, (dest_country_name = origin_country_name) as withinCountry from dfTable limit 2

# With selectExpr, we can also specify aggregations over the entire DataFrame (summarization)

df.selectExpr(
        'avg(count)',
        'count(distinct(dest_country_name))')\
                .show(2)

# Literals
# Sometimes, we need to pass explicit values into Spark that are just a value (rather than a new column)
# The way we do this is through literals.
# This is basically a translation from a given programming language's literal value to one that Spark understands
# Literals are expressions and you can use them in the same way

# What is a litreal in compuer programming
# A litrals is a notation for representing a fixed value in source code
#   - A value is something that can be manipulated by a computer program
# In contrast to literals, variables or constants are symbols that can take on one of a class of fixed values, the constant being constrained not to change. 
# Literals are often used to initialize variables, for example 1 is an integer literal and the three letters string 'cat' is a string litral
# int a = 1
# string s = 'cat'


from pyspark.sql.functions import lit
df.select(expr('*'), lit(1).alias('One')).show(2)
# select * , 1 as One from dfTable limit 2
# In SQL, literals are just the specific value


# Reserverd Characters and Keywords
# There are reserved characters like spaces or dashes in column names
# Handling these means escaping column names

# Escaping means reducing the ambiguity
h = "Hello World"
h_ = "Hello \"World\""
# any quote that is preceded by a slash is escaped, and understood to be part of the value of the string
h__ = 'Hello "World"'

# SQL has certain keywords it watches for that we cannot use in our queries without causing some confusion
# Suppose we had a table of values where a column was named "Select", and we wanted to select that
# select select from myTable
# Now we introduced some ambiguity into our query. Within our query, we can reduce that ambiguity by using back-ticks:
# select `select` from myTable


# Changing a Column's Type (cast)
# Cast converts from one type to another
df.withColumn('count2', col('count').cast('long'))
# select * cast(count as long) as count2 from dfTable


# Filtering Rows
# To filter rows, we create an expression that evaluates to true or false
# You then filter out the words with an expression that is equal to false
# The most common way is to create an expression as a String or build an expression by using a set of column manipulations
df.where('count < 2').show(2)
# select * from dfTable where count < 2 limit 2
df.where(col('count') < 2).show(2)


# Concatenating and Appending Rows (Union)
# DataFrames are immutable. This means users canot append to DF because that would be changing it.
# To append to a DF, you must union the original DataFrame along with the new DF.
# Union = concatenation of two DataFrames
# To union two DFs, you must be sure that they have the same schema and number of columns;

schema = df.schema
newRows = [
        Row('New Country', 'Other Country', 5),
        Row('New Country2', 'Other Country 3', 1)
        ]
parallelizeRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizeRows, schema)


df.union(newDF)\
        .where('count = 1')\
        .where(col('origin_country_name') != 'United States')\
        .show()


# Sorting Rows
# There are two equivalent operations to do this sort and orderBy that work the exact same way
# They both accept column expressions and strings as well as multiple columns. 

df.sort('count').show(5)
df.orderBy('count', 'dest_country_name').show(5)
df.orderBy(col('count'), col('dest_country_name')).show(5)

df.orderBy(expr('count desc')).show(2)
df.orderBy(col('count').desc(), col('dest_country_name').asc()).show(2)


# Repartition and Coaleasce
# Repartiton will incur a full shuffle of the data, regardless of whether one is necessary. 
# This means you should typicall only repartition when the future number of partitions is greater than your current number of partitions
nP = df.rdd.getNumPartitions()
print(nP) # 1
df.repartition(5)

# If you know that you're going to be filtering by a certain column often, it can be worth repartitioning based on that column
# specify the number of partitions e.g. 5
df.repartition(5, col('dest_country_name'))


# Coalese
# coalesce means come together to form one mass or whole
# Coalesce on the other hand, will not incur a full shuffle and will try to combine partitons
# This operation will shuffle your data into five partitions based on the destination country name, and then coalesce them (without a full shuffle)
df.repartition(5, col('dest_country_name')).coalesce(2)


# Collecting Rows to the Driver
# Spark maintains the state of the cluster in the driver.
# There are times when you'll want to collect some of your data to the driver in order to manipulate it on your local machine
# However, we used several different methods for doing so that are effectively all the same. collect gets all data from the entire DataFrame, take selects the first N rows, and show prints out a number of rows

collectDF = df.limit(10)
collectDF.take(5)
collectDF.show()
collectDF.show(5, False)
collectDF.collect()
