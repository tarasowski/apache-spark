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

```py

from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .appName('Analyzing soccer players')\
                    .getOrCreate()

players = spark.read\
                .format('csv')\
                .option('header', 'true')\
                .load('./player.csv')
    
players.printSchema()

player_attributes = spark.read\
                            .format('csv')\
                            .option('header', 'true')\
                            .load('./player_attributes.csv')

player_attributes.printSchema()
player_attributes.count(), players.count() # creates a tuple (183978, 11060)

player_attributes.select('player_api_id')\
                    .distinct()\
                    .count() # 11060

players = players.drop('id', 'player_fifa_api_id')
# drop id and player_fifa_api_id from the data frame
players.columns
# ['player_api_id', 'player_name', 'birthday', 'height', 'weight']

player_attributes = player_attributes.drop(
    'id',
    'player_fifa_api_id',
    'preferred_foot',
    'attacking_work_rate',
    'defensive_work_rate',
    'crossing',
    'jumping',
    'sprint_speed',
    'balance',
    'aggression',
    'short_passing',
    'potential'

)

player_attributes.columns

player_attributes = player_attributes.dropna()
players = players.dropna()
# clean up dataset by those rows which contain missing information
player_attributes.count(), players.count()
# (181265, 11060)

from pyspark.sql.functions import udf

# specify a functionw hich acts on columns in a DataFrame
# lambda function extract the year
year_extract_udf = udf(lambda date: date.split('-')[0])

player_attributes = player_attributes.withColumn(
    'year',
    year_extract_udf(player_attributes['date'])
)

player_attributes = player_attributes.drop('date')
# since we don't need a date column anymore we go ahead and drop it from the dataframe
player_attributes.columns

# find the best striker based on specific attributes

pa_2016 = player_attributes.filter(player_attributes['year'] == 2016)
pa_2016.count()
# 14103

pa_2016.select(pa_2016['player_api_id'])\
        .distinct()\
        .count()
# the number of unique players is 5586 for 2016

pa_striker_2016 = pa_2016.groupBy('player_api_id')\
                            .agg({
                            'finishing': 'avg',
                            'shot_power': 'avg',
                            'acceleration': 'avg'
})

pa_striker_2016.show(5)
pa_striker_2016.columns
# ['player_api_id', 'avg(finishing)', 'avg(acceleration)', 'avg(shot_power)']

pa_striker_2016 = pa_striker_2016.withColumnRenamed('avg(finishing)', 'finishing')\
                                    .withColumnRenamed('avg(acceleration)', 'acceleration')\
                                    .withColumnRenamed('avg(shot_power)', 'shot_power')

pa_striker_2016.columns
# ['player_api_id', 'finishing', 'acceleration', 'shot_power']

weight_finishing = 1
weight_shot_power = 2
weight_acceleration = 1

total_weight = weight_finishing + weight_shot_power + weight_acceleration
print(total_weight)

strikers = pa_striker_2016.withColumn('striker_grade',
                                     (pa_striker_2016['finishing'] * weight_finishing + \
                                      pa_striker_2016['shot_power'] * weight_shot_power + \
                                      pa_striker_2016['acceleration'] * weight_acceleration / total_weight)
                                     )

strikers = strikers.drop('finishing', 'acceleration', 'shot_power')
# drop columns which are not needed anymore
# it makes the distributed processing faster because it has to operate on less data

strikers = strikers.filter(strikers['striker_grade'] > 70)\
                    .sort(strikers['striker_grade'].desc())

strikers.show(10)

striker_details = players.join(strikers, players['player_api_id'] == strikers['player_api_id'])
# this operation performs an inner join
# another way to specify a join operation
# players.join(strikers, ['player_api_id'])

striker_details.columns
# ['player_api_id', 'player_name', 'birthday', 'height','weight','player_api_id', 'striker_grade']

striker_details.show(5)
strikers.count(), players.count()
# (5346, 11060)

# perform join operations by broadcasting dataframes to share the data across tasks
from pyspark.sql.functions import broadcast

# since the striker dataframe is smaller than players dataframe the strikers dataframe should be broadcast
striker_details = players.select('player_api_id', 'player_name')\
                        .join(broadcast(strikers), ['player_api_id'], 'inner')

# in order to perform joins we need to transfer the contents of the dataframe to another dataframe
# therefore we broadcast the smaller dataframe to all nodes
# also we ensure that there is only 1 copy per node by using the broadcast

striker_details = striker_details.sort(striker_details['striker_grade'].desc())
striker_details.show()

players.count(), player_attributes.count()
# (11060, 181265)

# get all the information that we need into a single dataframe
# to make join more efficient we need to select only the columns we are interested in
# players table is smaller so we can broadcast it to all nodes that is just 1 copy per worker node
# this copy will be stored in the node's cache
players_heading_acc = player_attributes.select('player_api_id', 'heading_accuracy')\
                                        .join(broadcast(players),
                                             player_attributes['player_api_id'] == players['player_api_id'])


players_heading_acc.columns
# ['player_api_id', 'heading_accuracy', 'player_api_id', 'player_name', 'birthday', 'height', 'weight']

# we need now to share count variables across all nodes
# this is where accumulators come in

short_count = spark.sparkContext.accumulator(0)
medium_low_count = spark.sparkContext.accumulator(0)
medium_high_count = spark.sparkContext.accumulator(0)
tail_count = spark.sparkContext.accumulator(0)

def count_players_by_height(row):
    height = float(row.height)

    if (height <= 175):
        short_count.add(1)
    elif (height <= 183 and height > 175):
        medium_low_count.add(1)
    elif (height <= 195 and height > 183):
        medium_high_count.add(1)
    elif (height > 195):
        tail_count.add(1)



# to apply the function to each player record we use the foreach method
# the lambda function will be applied to all rows and is distributed across multiple nodes
players_heading_acc.foreach(lambda x: count_players_by_height(x))

all_players = [
    short_count.value,
    medium_low_count.value,
    medium_high_count.value,
    tail_count.value
]

all_players
# [18977, 97399, 61518, 3371]

# another set of accumulators
# heading accuracy above a treshold
short_ha_count = spark.sparkContext.accumulator(0)
medium_low_ha_count = spark.sparkContext.accumulator(0)
medium_high_ha_count = spark.sparkContext.accumulator(0)
tail_ha_count = spark.sparkContext.accumulator(0)

def count_players_by_height_and_heading_accuracy(row, threshold_score):

    height = float(row.height)
    ha = float(row.heading_accuracy)

    if (ha <= threshold_score):
        return
    if (height <= 175):
        short_ha_count.add(1)
    elif (height <= 183 and height > 175):
        medium_low_ha_count.add(1)
    elif (height <= 195 and height > 183):
        medium_high_ha_count.add(1)
    elif (height > 195):
        tail_ha_count.add(1)


players_heading_acc.foreach(lambda x: count_players_by_height_and_heading_accuracy(x, 60))

all_players_above_threshold = [short_ha_count.value,
                              medium_low_ha_count.value,
                              medium_high_ha_count.value,
                              tail_ha_count.value
                             ]
# the player records are bucketed into short_ha_count, medium_low etc...
all_players_above_threshold
# [7306, 82896, 40270, 3146]

# percentage of players with a high heading accuracy bucketed by height

percentage_values = [short_ha_count.value / short_count.value * 100,
                              medium_low_ha_count.value / medium_low_count.value * 100,
                              medium_high_ha_count.value /  medium_high_count.value * 100,
                              tail_ha_count.value / tail_count.value * 100
                             ]

percentage_values
# [38.499235917162885, 85.10970338504502, 65.46051562144413, 93.32542272322752]

```

# Saving data to CSV, JSON

```py
# now we are going to save the information that is present only in 2 columns
# coalesce function repartitions the dataframe into a single partition to write out a single file
# Important: individual records in a dataframe will be split across multiple nodes in a spark cluster
# use coalesce() function with an argument to repartition the dataframe into a single partition
# if coalesce() were not used, the number of files written out would be equal to the number of partitions in the dataframe
# you can change the argument to as many partitions as you want and each partition will write a single file
pa_2016.select('player_api_id', 'overall_rating')\
        .coalesce(1)\
        .write\
        .option('header', 'true')\
        .csv('players_overall.csv')

# the output will be only 1 file thanks to the coalesce function

pa_2016.select('player_api_id', 'overall_rating')\
        .write\
        .json('players_overall.json')

# the output are 6 json files, since we had 6 partitions, means 1 file for each partition
``` 

## Joins

```md
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .appName('Analyzing soccer players')\
                    .getOrCreate()


valuesA = [('John', 100000), ('James', 150000), ('Emily', 65000), ('Nina', 200000)]
tableA = spark.createDataFrame(valuesA, ['name', 'salary'])

+-----+------+
| name|salary|
+-----+------+
| John|100000|
|James|150000|
|Emily| 65000|
| Nina|200000|
+-----+------+

valuesB = [('James', 2), ('Emily', 3), ('Darth Vader', 5), ('Princess Leia', 5)]
tableB = spark.createDataFrame(valuesB,['name', 'employee_id'])

+-------------+-----------+
|         name|employee_id|
+-------------+-----------+
|        James|          2|
|        Emily|          3|
|  Darth Vader|          5|
|Princess Leia|          5|
+-------------+-----------+

inner_join = tableA.join(tableB, tableA['name'] == tableB['name'])
# if you don't specify the join operation spark assumes it's an inner join
inner_join.show()
# contains only James, Emily

+-----+------+-----+-----------+
| name|salary| name|employee_id|
+-----+------+-----+-----------+
|James|150000|James|          2|
|Emily| 65000|Emily|          3|
+-----+------+-----+-----------+

left_join = tableA.join(tableB, tableA['name'] == tableB['name'], how='left')
left_join.show()

+-----+------+-----+-----------+
| name|salary| name|employee_id|
+-----+------+-----+-----------+
|James|150000|James|          2|
| John|100000| null|       null|
|Emily| 65000|Emily|          3|
| Nina|200000| null|       null|
+-----+------+-----+-----------+

right_join = tableA.join(tableB, tableA['name'] == tableB['name'], how='right')
right_join.show()

+-----+------+-------------+-----------+
| name|salary|         name|employee_id|
+-----+------+-------------+-----------+
|James|150000|        James|          2|
| null|  null|Princess Leia|          5|
|Emily| 65000|        Emily|          3|
| null|  null|  Darth Vader|          5|
+-----+------+-------------+-----------+

full_outer_join = tableA.join(tableB, tableA['name'] == tableB['name'], how='full')
full_outer_join.show()

+-----+------+-------------+-----------+
| name|salary|         name|employee_id|
+-----+------+-------------+-----------+
|James|150000|        James|          2|
| John|100000|         null|       null|
| null|  null|Princess Leia|          5|
|Emily| 65000|        Emily|          3|
| Nina|200000|         null|       null|
| null|  null|  Darth Vader|          5|
+-----+------+-------------+-----------+
``` 
