# Joins
# Spark's ability to talk to different data means that you gain the ability to tap into a variety of data sources across your company.

# Join Expression
# A join brings together two sets of data, the left and the right, by comparing the value of one or more keys of the left and right 
# and evaluating the result of a join expression that determines whether Spark should bring togehter the left set of data with the right set of data
# The most common join expression, an equi-join, compares whether the specified keys in your left and right datasets are equal
# If they are equal, Spark will combine the left and right datasets

# Data set
# A data set is a collection of data. In the case of tabular data, a data set corresponds to one or more database tables, where every column of a table represents a particular variable, and each row correspondends to a given record of the data set.
# A collection of related sets of information that is composed of separate elements but can be manipulated as a unit by a computer


# Join Types
# The join expression determines whether two rows should join
# The join type determines what should be in the result set

from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

person = spark.createDataFrame([
        (0, "Bill Chambers", 0, [100]),
        (1, "Matei Zaharia", 1, [500, 250, 100]),
        (2, "Michael Armbrust", 1, [250, 100])])\
        .toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
        (0, "Masters", "School of Information", "UC Berkeley"),
        (2, "Masters", "EECS", "UC Berkely"), 
        (1, "Ph.D", "EECS", "UC Berkeley")])\
        .toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
        (500, "Vice President"),
        (250, "PMC Member"),
        (100, "Contributor")])\
        .toDF("id", "status")

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

person.show()
graduateProgram.show()
sparkStatus.show()

# Inner Joins
# Inner joins evaluate the keys in both of the DFs or tables and include (and join together) only the rows that evaluate to true

joinExpression = person["graduate_program"] == graduateProgram["id"]

# We can also specify this explicitly by passing in a third parameter, the joinType
joinType = "inner"


# Inner joins are the default jon, so we just need to specify our lef DF and join the right in the join expression

person.join(graduateProgram, joinExpression, joinType).show()

spark.sql("""
        select *
        from person -- left
        inner join graduateProgram -- right table but also we explicitly define the join type 
        on person.graduate_program = graduateProgram.id
        """).show()

# Outer Joins
# Outer joins evaluate the keys in both of the DF or tables and includes (and join togther) the rows that evaluate to true or false
# If there is no equivalent row in either the left or right DF, Spark will insert null

joinType = "outer"


person.join(graduateProgram, joinExpression, joinType).show()

spark.sql("""
        select *
        from person
        full outer join graduateProgram
        on person.graduate_program = graduateProgram.id
        """)

# Left Outer Joins
# Left outer joins evaluate the keys in both of the DFs or tables and include all rows from the left DF as well as any rows in the right DF that have a match in the left DF. If there is no equivalent row in the right DF, Spark will insert null

joinType = "left_outer"

graduateProgram.join(person, joinExpression, joinType).show()

spark.sql("""
        select *
        from graduateProgram
        left outer join person
        on person.graduate_program = graduateProgram.id
        """)

# Right Outer Joins
# Right outer joins evaluate the keys in both of the DFs or tables and includes all rows form the right DF as well as any rows in the left DF that have a match in the right DF
# If there is no equivalent row in the left DF, Spark will insert null

joinType = "right_outer"

person.join(graduateProgram, joinExpression, joinType).show()

# Left Semi Joins
# Semi joins do not include any values form the right DF. They only compare values to see if the value exists in the second DF.
# They only compare values to see if the value exists in the second DF. If the value does exits, those rows will be kept in the result, even if there are duplicate keys in the left DF
# This of left semi joins as filters on a DF, as opposed to the function of a conventional join


joinType = "left_semi"

graduateProgram.join(person, joinExpression, joinType).show()


gradProgram2 = graduateProgram.union(spark.createDataFrame([
        (0, "Masters", "Duuplicated Row", "Duplicated School")]))

gradProgram2.createOrReplaceTempView("gradProgram2")

gradProgram2.join(person, joinExpression, joinType).show()

# Left Anti Joins
# Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually include any values from the right DF
# They only compare values to see if the value exists in the second DF
# Rather then keeping the values that exists in the second DF, they keep only the values that do not have a corresponding key in the second DF
# Think of anti joins as a NOT IN SQL-style filter

joinType = "left_anti"

graduateProgram.join(person, joinExpression, joinType).show()


# Cross (Cartesian) Joins
# Cross-joins in simplest terms are inner joins that do not specify a predicate
# Cross joins will join every row in the left DF to ever single row in the right DF
# This will cause an absolute explosion in the number of rows
# If you have 1,000 rows in each DF, the cross-join of these will result in 1,000,000 rows (1,000 x 1,000)

joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()

spark.sql("""
            select *
            from graduateProgram
            cross join person
            on graduateProgram.id = person.graduate_program
        """).show(20)
        
# If you truy intend to have a cross-join, you can call that out explicitly

person.crossJoin(graduateProgram).show()


# Joins on Complex Types

from pyspark.sql.functions import expr

person.withColumnRenamed("id", "personId")\
      .join(sparkStatus, expr("array_contains(spark_status, id)")).show()
