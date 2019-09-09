from pyspark.sql import SparkSession


spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

df = spark.read.format('csv')\
        .option('header', 'true')\
        .load('./Spark-The-Definitive-Guide/data/retail-data/by-day/2010-12-01.csv')

df.printSchema()
df.createOrReplaceTempView('dfTable')

# Convert to Spark Types
# Convert native python types to Spark types. We do this by using the lit function
# This function converts a type in another language to its corresponding Spark representation

from pyspark.sql.functions import lit

df.select(lit(5), lit('five'), lit(5.0)).show(5)
# select 5, 'five', 5.0


# Working w/ Booleans
# Boolean statement consist of four elements: and, or , true and false
# These statements are often used as conditional requirements for when a row of data must either pass the test or else it will be filtered out

from pyspark.sql.functions import col, instr, expr
df.where(col('InvoiceNo') != 536365)\
        .select('InvoiceNo', 'Description')\
        .show(5, False)

df.where('InvoiceNo <> 536365')

priceFilter = col('UnitPrice') > 600
descripFilter = instr(df.Description, 'POSTAGE') >= 1 # like a find() in string function that returns 0 or 1
df.where(df.StockCode.isin('DOT')).where(priceFilter | descripFilter).show()

# Boolean expresions are not just reserved to filters. To filter a DF, you can also specify a Boolean column

DOTCodeFilter = col('StockCode') == 'DOT'
priceFilter_ = col('UnitPrice') > 600
descripFilter_ = instr(col('Description'), 'POSTAGE') >=1
df.withColumn('isExpensive', DOTCodeFilter & (priceFilter_ | descripFilter_))\
        .where('isExpensive')\
        .select('unitPrice', 'isExpensive')\
        .show(5)


# If fact, it's often easier to just express filters as SQL statements than using the programmatic DF interface

df.withColumn('isExpensive', expr('NOT UnitPrice <= 250'))\
        .where('isExpensive')\
        .select('Description', 'UnitPrice')\
        .show(5)

# Expression vs. Statement
# An expression can be evaluated, and as such returns a single value. It is only one possible part of a statement
# A statement is a collection of elements such as identifiers, reserved keywords, data types, functions, expressions to make the smallest possible unit of code. A statement can be executed

# Expression vs. Statement
# An expression returns a single value
#   Column names, variables, constants, functions and formulas using any of the preceding are all expressions
#   Certain subqueries that return only a single value may also be considered expressions
# A statement defines control of flow, data manipulation, or data definition operation

# SQL Expressions
# An expression is a combination of one or more values, operators, and SQL functions that evaluates to a value
# An expression generally assumes the datatype of its components

# The simple expression evaluates to 4 and has datatype NUMBER (the same datatype as its components)
r = 2 * 2

# The following expression is an example of a more complex expression that uses both functions and operators.
# The expression adds seven days to the current date, removes the time component from the sum, and convers the result to CHAR datatype:
# TO_CHAR(TRUNC(SYSDATE+7))

# You can use expression in:
#   - The select list of select statment
#   - A condition of the WHERE clause and HAVING clause
#   - The connect by, start with, and order by clause
#   - The values clause of the insert statement
#   - The set clause of the update statement

# For example, you could use an expression in place of the quoted string 'Smith':
# SET last_name = 'Smith';
# This SET clause has the expression INITCATP(last_name) instead of the quoted string 'Smith':
# SET last_name = INITCATP(last_name);

# Expressions have several forms:
# expr::= simple_expression | compound_expression | case_expression | cursor_expression | datetime_expression | function_expression | interval_expression | variable_expression


# Working w/ Numbers
# Image that we found out that we mis-recorded the quantity in our retail dataset and the true quantity is equal to (the current quantity * the unit price) hoch 2 + 5
# We can multiply our columns together because they were both numerical
from pyspark.sql.functions import expr, pow, round, bround
fabricatedQuantity = pow(col('Quantity') * col('UnitPrice'), 2) + 5
df.select(expr('CustomerId'), fabricatedQuantity.alias('realQunatity')).show(2)


df.selectExpr(
        'CustomerId',
        '(Power((Quantity * UnitPrice), 2.0) + 5) as realQuantity').show(2)
# select customerId, (power((Quantity * Unitprice), 2.0) + 5) as realQuantity from dfTable

df.select(round(col('UnitPrice'), 1).alias('rounded'), col('UnitPrice')).show(5)
df.selectExpr('round(UnitPrice, 1) as rounded', 'UnitPrice').show(5)

# You can use either DF API methods or just SQL Expressions
df.select(round(lit('2.5')), bround(lit('2.5'))).show(2)
df.selectExpr('round(2.5)', 'bround(2.5)').show(2)


# Unique ID
# We can add a unique ID to each row by using the function monotonically_increasing_id
# The function generates a unique value for each row, starting with 0

from pyspark.sql.functions import monotonically_increasing_id

df.select(monotonically_increasing_id()).show(2)


# Working with String

from pyspark.sql.functions import initcap, lower, upper
df.select(initcap(col('Description'))).show(2)
# select initcap(Description) from dfTable

df.select(col('Description'),
        lower(col('Description')),
        upper(col('Description'))).show(2)

df.selectExpr(
        'Description',
        'lower(Description)',
        'upper(lower(Description))').show(2)

# select description, lower(Description), upper(lower(Description)) from dfTable


from pyspark.sql.functions import ltrim, rtrim, rpad, lpad, trim

df.select(
        ltrim(lit('         HELLO           ')).alias('ltrim'),
        rtrim(lit('         HELLO           ')).alias('rtrim'),
        trim(lit('         HELLO           ')).alias('trim'),
        lpad(lit('HELLO'), 3, ' ').alias('lp'),
        rpad(lit('HELLO'), 10, ' ').alias('rp')).show(2)

df.selectExpr(
        'ltrim(         "HELLO"           ) as ltrim',
        'rtrim(         "HELLO"           ) as rtrim',
        'trim(         "HELLO"           )as trim',
        'lpad("HELLO", 3, " ") as lp',
        'rpad("HELLO", 3, " ")as rp').show(2)

# select 
#   ltrim('     HELLO       '),
#   rtrim('     HELLO       '),
#   trim('      HELLO       '),
#   lpad('HELLO', 3, ' '),
#   rpad('HELLO', 10, ' ')

# Rather than extracting values, we simply want to check for their existence. We can do this with the contains method on each column. 
# In Python and SQL, we can use the instr function

from pyspark.sql.functions import instr

cB = instr(col('Description'), 'BLACK') >= 1
cW = instr(col('Description'), 'WHITE') >= 1

df.withColumn('hasSimpleColor', cB|cW)\
        .where('hasSimpleColor')\
        .select('Description').show(3, False)

from pyspark.sql.functions import expr, locate

simple_colors = ['black', 'white', 'red', 'green', 'blue']

def color_location(column, color_string):
    return locate(color_string.upper(), column)\
            .cast('boolean')\
            .alias('is_' + color_string)

selectedColumns = [color_location(df.Description, c) for c in simple_colors]
selectedColumns.append(expr('*'))

df.select(*selectedColumns).where(expr('is_white OR is_red'))\
        .select('Description', 'is_white').show(3, False)

# Working w/ Dates and Timestamps
# There are dates which focus exclusively on calendar dates, and timestamps, which include both data and time
# Spark wtih make a best effort to correctly identify column types, including dates and timestamps when we enable inferSchema
# Spark can be a bit particular about what format you have at any given point in time
# It's important to be explicit when parsing or converting to ensure that there are not issues in doing so
# It's important to be explicit when parsing or converting to ensure that there are not issue sin doing so

from pyspark.sql.functions import current_date, current_timestamp, date_add, date_sub, datediff, months_between, to_date

dateDF = spark.range(10).alias('number')\
        .withColumn('today', current_date())\
        .withColumn('now', current_timestamp())
dateDF.createOrReplaceTempView('dateTable')
dateDF.printSchema()

dateDF.select(date_sub(col('today'), 5), date_add(col('today'), 5)).show(1)

dateDF.selectExpr(
        'date_sub(today, 5)',
        'date_add(today, 5)'
        ).show(1)
# select date_sub(today, 5), date_add(today, 5) from dateTable

# Another task is to take a look at the difference between two dates. We can do this with datediff function
dateDF.withColumn('week_ago', date_sub(col('today'), 7))\
        .select(datediff(col('week_ago'), col('today'))).show(1)

dateDF.withColumn('week_ago', date_sub(col('today'), 7))\
        .selectExpr('datediff(week_ago, today)').show(1)

spark.sql('select datediff(week_ago, today) from (select *, date_sub(today, 7) as week_ago from dateTable)')


dateDF.select(
        to_date(lit('2016-01-01')).alias('start'),
        to_date(lit('2017-05-22')).alias('end'))\
        .select(months_between(col('start'), col('end'))).show(1)


dateDF.selectExpr(
        'to_date("2016-01-01") as start',
        'to_date("2017-05-22") as end')\
        .selectExpr(
                'months_between(start, end)',
                'datediff("2016-01-01", "2017-01-01")'
                ).show(1)

# We introduces the to_date function. The to_date function allows you to convert a string to a date, optionally with a specified format
# We specify our format in the Java SimpleDateFormat

spark.range(5).withColumn('date', lit('2017-01-01'))\
        .select(to_date(col('date'))).show(1)


spark.range(5).selectExpr('to_date("2017-01-01") as date').show(1)

# Spark will not throw an error if it cannot parse the date; rather, it will just return null

dateDF.select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11"))).show(1)
# first column returns null because it cannot parse the date properly

dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
        to_date(lit("2017-12-11"), dateFormat).alias('date'),
        to_date(lit("2017-20-12"), dateFormat).alias('date2'))

cleanDateDF.createOrReplaceTempView('dateTable2')

cleanDateDF.selectExpr(
        'to_date(date, "yyyy-dd-MM")',
        'to_date(date2, "yyyy-dd-MM")',
        'to_date(date)'
        ).show(1)

# to_timestamp, always requires a format to be specified

from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col('date'), dateFormat)).show()

cleanDateDF.selectExpr(
        'to_timestamp(date, "yyyy-dd-MM")',
        'to_timestamp(date2, "yyyy-dd-MM")').show(1)

spark.sql('select to_timestamp(date, "yyyy-dd-MM"), to_timestamp(date2, "yyyy-dd-MM") from dateTable2').show(1)


cleanDateDF.filter(col('date2') > lit('2017-12-12')).show()


# Working with Nulls in Data
# As a best practice, you should always use nulls to represent missing or empty data in your DFs.
# Spark can optimize working with null values more than it can if you use empty string or other values
# There are two things you can do with null values: you can explicitly drop nulls or you can fill them with a value
# Spark includes a function to allow you to select the first non-null value from a set of columns by using the coalesce function

from pyspark.sql.functions import coalesce

df.select(coalesce(col('Description'), col('CustomerId'))).show()

# ifnull -> return the second value if the first is null
# nullif -> returns null if the two values are equal or else return the second value
# nvl -> returns the second value if the first in null, but defaults to the first
# nvl2 -> returns the second value if the first is not null, otherwise, it will return the last specified value (else_value)

df.selectExpr(
        'ifnull(null, "return value")',
        'nullif("value", "value")',
        'nvl(null, "return_value")',
        'nvl2("not_null", "return_value", "else_value")'
        ).show()

