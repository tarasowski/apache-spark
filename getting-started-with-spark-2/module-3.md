# Querying Data Using Spark SQL

* Spark SQL enables querying of DataFrames as database tables
* Before you start query them you need to register them as temporary per-session
  and global tables (available across all spark sessions)
* Spark 2.x has the catalyst optmimizer which makes SQL queries fast
* Tables schema can be inferred or explicitly specified of your sql tables
* Advanced window operations are also supported


```md
from pyspark.sql import SparkSession

# entry point into our spark application
spark = SparkSession.builder\
                    .appName('Analyzing airline data')\
                    .getOrCreate()


from pyspark.sql.types import Row
# use Row object to setup data for the SQL tables

from datetime import datetime

# use sparkContext.parallelize function to set up RDD of Row objects

record = spark.sparkContext.parallelize([Row(id = 1,
                                            name = 'Jill',
                                            active = True,
                                            clubs = ['chess', 'hockey'],
                                            subjects = {'math': 80, 'english': 56},
                                            enrolled = datetime(2014, 8, 1, 14, 1, 5)),
                                        Row(id = 2,
                                           name = 'George',
                                           active = False,
                                           clubs = ['chess', 'soccer'],
                                           subjects = {'math': 60, 'english': 96},
                                           enrolled = datetime(2015, 3, 21, 8, 2, 5))
                                        ])
record_df = record.toDF()
record_df.show() # RDD converted to a DataFrame

+------+---------------+-------------------+---+------+--------------------+
|active|          clubs|           enrolled| id|  name|            subjects|
+------+---------------+-------------------+---+------+--------------------+
|  true|[chess, hockey]|2014-08-01 14:01:05|  1|  Jill|[english -> 56, m...|
| false|[chess, soccer]|2015-03-21 08:02:05|  2|George|[english -> 96, m...|
+------+---------------+-------------------+---+------+--------------------+

# in order to run sql queries on the dataframe we need first register dataframe as a table
record_df.createOrReplaceTempView('records')
# creates a table that is per-session, not shared across Spark sessions
# as soon as the session ends the table will disapear as well

all_records_df = spark.sql('select * from records')
all_records_df.show()
# all dataframe operations can be performed on the result of the sql query

+------+---------------+-------------------+---+------+--------------------+
|active|          clubs|           enrolled| id|  name|            subjects|
+------+---------------+-------------------+---+------+--------------------+
|  true|[chess, hockey]|2014-08-01 14:01:05|  1|  Jill|[english -> 56, m...|
| false|[chess, soccer]|2015-03-21 08:02:05|  2|George|[english -> 96, m...|
+------+---------------+-------------------+---+------+--------------------+

all_records_df_ = spark.sql('select id, clubs[1] as club, subjects["english"] as subject from records')
all_records_df_.show()

+---+------+-------+
| id|  club|subject|
+---+------+-------+
|  1|hockey|     56|
|  2|soccer|     96|
+---+------+-------+

all_records_df_log = spark.sql('select id, not active from records')
all_records_df_log.show()

+---+------------+
| id|(NOT active)|
+---+------------+
|  1|       false|
|  2|        true|
+---+------------+

all_records_df_where = spark.sql('select * from records where active')
all_records_df_where.show()

+------+---------------+-------------------+---+----+--------------------+
|active|          clubs|           enrolled| id|name|            subjects|
+------+---------------+-------------------+---+----+--------------------+
|  true|[chess, hockey]|2014-08-01 14:01:05|  1|Jill|[english -> 56, m...|
+------+---------------+-------------------+---+----+--------------------+

all_records_df_gt = spark.sql('select * from records where subjects["english"] > 90')
all_records_df_gt.show()

+------+---------------+-------------------+---+------+--------------------+
|active|          clubs|           enrolled| id|  name|            subjects|
+------+---------------+-------------------+---+------+--------------------+
| false|[chess, soccer]|2015-03-21 08:02:05|  2|George|[english -> 96, m...|
+------+---------------+-------------------+---+------+--------------------+


# if you want to make the sql table to be accessbile to all spark sessions on the cluster
# register it as a global table
record_df.createGlobalTempView('global_records')

global_all = spark.sql('select * from global_temp.global_records')
# make sure to add global_temp prefix in order to access your table
global_all.show()

al_all.show()

+------+---------------+-------------------+---+------+--------------------+
|active|          clubs|           enrolled| id|  name|            subjects|
+------+---------------+-------------------+---+------+--------------------+
|  true|[chess, hockey]|2014-08-01 14:01:05|  1|  Jill|[english -> 56, m...|
| false|[chess, soccer]|2015-03-21 08:02:05|  2|George|[english -> 96, m...|
+------+---------------+-------------------+---+------+--------------------+


``` 
