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

# https://dwbi.org/pages/75/methods-of-incremental-loading-in-data-warehouse
customers = [
        Row(1, "John", "Individual", "22-Mar-2012"),
        Row(2, "Ryan", "Individual", "22-Mar-2012"),
        Row(3, "Bakers", "Corporate", "23-Mar-2012"),
        ]

sales = [
        Row(1, 1, "White sheet (A4)", 100, 4.00, "22-Mar-2012"),
        Row(2, 1, "James Clip (Box)", 1, 2.50, "22-Mar-2012"),
        Row(3, 2, "Whiteboard Maker", 1, 2.00, "22-Mar-2012"),
        Row(4, 3, "Letter Envelop", 200, 75.00, "23-Mar-2012"),
        Row(5, 1, "Paper Clip", 12, 4.00, "23-Mar-2012"),
        ]

batch = [
        Row(1, "22-Mar-2012", "Success"),
        ]

customersDF = spark.createDataFrame(customers, schema=["customer_id", "customer_name", "type", "entry_date"])
salesDF = spark.createDataFrame(sales, schema=["id", "customer_id", "product_description", "qty", "revenue", "sales_date"])
batchDF = spark.createDataFrame(batch, schema=["batch_id", "loaded_untill", "status"])

customersDF.createOrReplaceTempView("customers")
salesDF.createOrReplaceTempView("sales")
batchDF.createOrReplaceTempView("batch")

_23_march_customers = spark.sql("""
        select t.*
        from customers t
        where t.entry_date > (select nvl(
                                            max(b.loaded_untill),
                                            to_date("01-01-1900", "MM-DD-YYYY")
                                            )
                              from batch b
                              where b.status = "Success")
        """)

_23_march_sales = spark.sql("""
        select t.*
        from sales t
        where t.sales_date > (select nvl(
                                        max(b.loaded_untill),
                                        to_date("01-01-1900", "MM-DD-YYYY")
                                        )
                              from batch b
                              where b.status = "Success")
        """)

print("customers table")
_23_march_customers.show()
print("sales table")
_23_march_sales.show()



# Incremental Data Load Patterns
# https://www.youtube.com/watch?v=INuucWEg3sY

# 1) Stage / left Outer Join (moving to another server, make a staging and left join, check null on right table, you know this data is new)
# 2) Control Table
#   Load | Cust  | Table | Date 
#   Id   | Table | Id | Date
# 3) Change Data Capture


# Source based incremental loading
# https://support.timextender.com/hc/en-us/articles/115001301963-How-incremental-loading-works
# The source table have a reliable natural or surrogate key and reliable incremental field such as "ModifiedDateTime" or "TimeStamp"
