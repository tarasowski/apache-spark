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
import pyspark.sql.functions as func

borough_percentage_contribution = borough_conviction_sum.withColumn(
    '% contribution',
    # we're using func.roung() method to round to 2 decimal numbers
    func.round(borough_conviction_sum['convictions'] / total_convictions * 100, 2)
)

borough_percentage_contribution.printSchema()

borough_percentage_contribution.orderBy(borough_percentage_contribution[2].desc()).show(10)

conviction_monthly = data.filter(data['year'] == 2014)\
                            .groupBy('month')\
                            .agg({'value': 'sum'})\
                            .withColumnRenamed('sum(value)', 'convictions')
# group all data per month basis
conviction_monthly.show(5)
total_conviction_monthly = conviction_monthly.agg({'convictions': 'sum'})\
                                                .collect()[0][0]
total_conviction_monthly = conviction_monthly.withColumn(
        'percent',
        func.round(conviction_monthly['convictions']/total_conviction_monthly * 100, 2)
)

# shows the columns in the resulting data frame
# ['month', 'convictions', 'percent']
total_conviction_monthly.columns

total_conviction_monthly.orderBy(total_conviction_monthly['percent'].desc()).show()
crimes_category = data.groupBy('major_category')\
                        .agg({'value': 'sum'})\
                        .withColumnRenamed('sum(value)', 'convictions')
crimes_category.orderBy(crimes_category['convictions'].desc()).show()

year_df = data.select('year')
# create a new dataframe only with year column
year_df.agg({'year': 'min'}).show()
#2008
year_df.agg({'year': 'max'}).show()
# 2016
year_df.describe().show()
data.crosstab('borough', 'major_category')\
    .select('borough_major_category', 'Burglary', 'Drugs', 'Fraud or Forgery', 'Robbery')\
    .show()

get_ipython().magic('matplotlib inline')
import matplotlib.pyplot as plt
plt.style.use('ggplot')


def describe_year(year):
    yearly_details = data.filter(data['year'] == year)\
                            .groupBy('borough')\
                            .agg({'value': 'sum'})\
                            .withColumnRenamed('sum(value)', 'convictions')
    borough_list = [x[0] for x in yearly_details.toLocalIterator()]
    convictions_list = [x[1] for x in yearly_details.toLocalIterator()]
    # toLocalIterator() is available on dataframes

    plt.figure(figsize=(33, 10))
    plt.bar(borough_list, convictions_list)

    plt.table('Crime for the year: ' + year, fontsize=30)
    plt.xlabel('Boroughs', fontsize=30)
    plt.ylabel('Convictions', fontsize=30)

    plt.xticks(rotation=30, fontsize=30)
    plt.yticks(fontsize=30)
    plt.autoscale()
    plt.show()

describe_year('2014')
``` 

## Accumulators and Broadcast Variables

* Different processes running on your cluster can share variables by using:
  * Broadcast variables: every worker node has only 1 read-only copy
    * hold 1 copy for 1 compute node
    * distributed efficiently by Spark
    * all nodes in cluster distribute
    * no shuffling data. Shuffling is splitting data to be sent the data to multiple nodes to process the data
    * will be cached in-memory on each node
    * so, can be large, but not too large
    * whenever tasks across stages need same data
    * share dataset in with all nodes: training data in ML, static lookup tables
    * 1 copy per worker node
  * Accumulators: shared variables can be broadcasted to worker noed
    * are read/write variables can be modified on worker nodes
    * added associatively and commutatively (A + B = B + A)
    * associacitvity: A + (B + C) = (A + B) + C
    * Spark natively support for accumulators Long, Double, Collections
    * Counters or sums for distributed processes (global sum / global counter)
    * Wokers can only modify state, only the driver program can read the value

* Spark is written in Scala, and heavily utilizes closures

* In Scala functions are first class citizens:
  * Can be stored in a variables
  * Can be passed to function
  * Can be returned from functions
