# Exploring and Analysing Data

* Use RDD only if the use case demands it
* If your data is unstructured used for RDD
* In other situations use DataFrames

## SparkSession vs. SparkContext
* SparkSession is a simple entry point
* SparkSession encapsulates SparkContext
  * Sqlcontext for sql queries
  * Hivecontext if you run Spark on top of Hive
* SparkSession avoids the trap which context you have to use
* Single point of entry


```py
from pyspark.sql import SparkSession # entry point into our Spark application

spark = SparkSession.builder\
                    .appName('Analyzing London crime data')\
                    .getOrCreate()

data = spark.read\
            .format('csv')\
            .option('header', 'true')\
            .load('./london_crime_by_lsoa.csv')

data.printSchema()
data.count()
data.limit(5).show() # it creates a new data frame, everything is immutable
data.dropna() #drops data that has n/a
data = data.drop('lsoa_code') # removes a columns from a data frame
data.show(5)

total_boroughs = data.select('borough')\
                     .distinct() # very expensive operation

total_boroughs.show()
total_boroughs.count()

hackney_data = data.filter(data['borough'] == 'Hackney')
hackney_data.show(5)

data_2015_2016 = data.filter(data['year'].isin(['2015', '2016']))
data_2015_2016.show(5) # filters by year

data_2015_2016.sample(fraction=0.1).show() # shows a sample of the dataframe

data_2014_onwards = data.filter(data['year'] >= 2014)
data_2014_onwards.sample(fraction=0.1).show(5)

# aggregations
# logically group all crime data on per-borough basis
borough_crime_count = data.groupBy('borough')\
                           .count()

borough_crime_count.show(5)

borough_crime_count_sum = data.groupBy('borough')\
                           .agg({'value': 'sum'})\
                            .withColumnRenamed('sum(value)', 'convictions')
# we perform the aggregation on the value column and changes the name of the new
new created column sum(value) to convictions
borough_crime_count_sum.show(5)

total_borough_convictions = borough_crime_count_sum.agg({'convictions': 'sum'})
total_borough_convictions.show()

total_convictions = total_borough_convictions.collect()[0][0]
# we use total_convictions variable to store the output and use it later in the code
print(total_convictions)

import pyspark.sql.functions a func
``` 
