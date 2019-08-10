from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql import functions as f
from pyspark.sql import types as t
import sys
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
save_location = './'
df = spark.read.json('output.jsonl').registerTempTable('clicks')

#df.printSchema()

query = """ with usid as
    (
                select body_cid, geo_city, is_new_session, timestamp,
                sum(is_new_session) over (order by body_cid, received_at_apig) as global_session_id,
                sum(is_new_session) over (partition by body_cid order by received_at_apig) as user_session_id,
                *
                from (
                    select *,
                    case when received_at_apig - last_event >= (60000 * 30)
                    or last_event is null
                    then 1 else 0 end as is_new_session
                from (
                    select body_cid, 
                        received_at_apig, 
                        from_unixtime(cast(received_at_apig/1000 as bigint)) as timestamp, 
                        lag(received_at_apig, 1) 
                        over (partition by body_cid order by received_at_apig) as last_event, 
                        geo_city, * from clicks
                        ) last
                        ) final
    )
    select * from usid
       where not body_t='adtiming'
      
    """

query_ = """ select body_dl, body_t from clicks where body_t='adtiming'

"""

sqlDF = spark.sql(query)
utm = sqlDF.rdd.map(lambda p: 'Name:' + p['body_cid'])
for name in utm:
    print(name)
#utm.show(10, False)

#sqlDF.toPandas().to_csv('export.csv', header=True)

