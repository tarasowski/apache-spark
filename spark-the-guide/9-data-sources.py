from pyspark.sql import SparkSession


spark = SparkSession\
        .builder\
        .appName('Python Guide')\
        .config('spark.executor.memory', '6GB')\
        .getOrCreate()

# Spark has 6 core data sources and hundreds of external data sources written by the community
# Core: CSV, JSON, Parquet, ORC, JDBC/ODBC connections, Plain-text files
# Community-created: Cassandra, HBase, MongoDB, Redshift, XML and many others

# Read API Structure
# The core structuture for reading data
# DataFrameReader.format(..).option("key", "value").schema(...).load()


# Basics of Reading Data
# Note: There is a lot of shorthand notation in the Spark community, and the data source read API is no exception. We try to be consistent and will use the above mentioned one.
# The foundation for reading data in Spark is the DataFrameReader. We access this through the SparkSession via the read attribute: spark.read
# After we have a DataFrame reader, we specify several values:
# The format
# The schema
# The read mode
# A series of options
# The format, options, and schema each return a DataFrameReader that can undergo further transformations and are all optional (see Either/Task from FP)

# Overall layout
# spark.read.format("csv")\
#   .option("mode", "FAILFAST")\
#   .option("inferSchema", "true")\
#   .option("path", "path/to/file(s)")
#   .load()

# Read modes
# permissive - sets all fields to null when it encounters a corrupted record and places all corrupted records in a string column called _corrupt_records (default)
# dropMalformed - drops the rows that contains malformed records
# failFast - fails immediately upon encountering malformed records



# Write API Structure
# The core structure for writing data is as follows:
# DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...)
# partitionBy, bucketBy, sortBy work only for file-based data sources - Parquet, JSON, ORC, AVRO
# Because we always need to write out some given data source, we access the DataFrameWriter on a per-DataFrame basis via the write attribute:
# dataframe.write.format("csv")
#   .option("mode", "OVERWRITE")
#   .option("dataFormat", "yyy-MM-dd")
#   .option("path", "path/to/file(s)")
#   .save()

# Save modes
# append - Appends the output files to the list of files that already exist at the location
# overwrite - Will completely overwrite any data that already exists there
# errorIfExists - throws an error and fails the write if data or file already exists at the specfied location (default)
# ignore - If data or files at the location do nothign with the current DataFrame

# CSV Files
# Very tricky, therefore it has many options
# The options give you the ability to work around issues like certain characters needing to be escaped - for example, commas inside of columns when the file is also comma-delimited or null values
# Read/write | Key  | Potential value | Description
# Both          sep     ,               The single character that is used as separator for each field and value
# Both          header  true, false     Is the first line a header?
# Read          escape  any string \    The character Spark should use to escape other characters in the file
# Read          inferSchema true, false Should spark infer the schema?
# Both          nullValue   ""          Declares what character represents a null value in the file
# Both          nanValue    NaN         Declares what character represents a NaN or missing character
# Both          codec       gzip, none  Declares what compression codec Spark should use to read or write the file
# Both          dateFormat  yyy-MM-dd   Declares the date format for any columns that are data type
# Both          timestampFormat         Does the same as date format only for timestamps
# Read          escapeQuotes            Declares whether Spark should escape quotes that are found in lines


# Reading CSV Files
# To read a CSV file, like any other format, we must first create a DataFrameReader for that specific format
# spark.read.format("csv")\
#   .option("header", "true")\
#   .option("mode", "FAILFAST")\
#   .option("inferSchema", "true")\ #false is default
#   .load("some/path/to/file.csv")


# If a file is doesn't conform the schema Spark will fail only at job execution time rather than DataFrame definition time, even if, e.g. we point to a file that does not exist.
# This is due to lazy evaluation.


# Writing CSV Files
# There are a variety of options see above for writing data when we write CSV files
# For instance, we can take our CSV file and write it out as a TSV file:
# csvFile.write.format("csv").mode("overwrite").option("sep", "\t").save("/tmp/my-tsv-file.tsv")



##############
# JSON Files
# In Spark, when we refer to JSON files, we refer to line-delimited JSON files
# Line-delimited JSON is actually much more stable format because it allows you to append a file with a new record, rather than having to read in an entire file and then write it out

# JSON Options
# Read/write    Key         Potential values        Default     Desc    
# Both          coded       None, bzip2, deflate    none        Declares what compression codec Spark
# Both          dateFormat  Any String              yyyy-MM-dd  Declares the date format
# Both          timestampFormat Any string          ......      Declares the timestamp format
# Read          primitiveAsString   true, false     false       Infers all primitive values as string types


# Reading JSON Files
# spark.read.format("json").option("mode", "FAILFAST")\
#   .option("inferSchema", "true")\
#   .load("/data/flight-data/json/2010-summary.json")

# Writing JSON Files
# When you write a file the data sources does not mater. Therefore we can reuse the CSV DAtaFrame that we created earlier to be the source for our JSON file
# The entire DF will be written as a folder
#   - One file per partition will be written out
# csvFile.write.format("json").mode("overwrite").save("/tmp/my-sjon-file.json")


#################
# Parquet Files
# Is the default file format for Spark, highly optimized for analytics workloads
# The recommendation is writing data out to Parquet for long-term storage because reading from a Parquet file will always be more efficient thant JSON or CSV
# It supports complex types e.g. array, struct, or map, won't work with CSV, because CSV doesn't support complex types
# spark.read.format("parquet")


# Reading Parquet Files
# Parquet has very few options because it enforces its own schema when storing data
# We can the schema if we have strict requirements for our DF
# Oftentimes this is not necessary because we can use schema on read, which is similar to the inferSchema with CSV files
# However, with Parquet files this method is more powerful because the schema is built into the file itself (so no inferece needed)
# spark.read.format("parquet")\
#   .load("/data/flight-data/parquet/2010-summary.parquet")


# Parquet Options
# Parquet has only two options because it has well-defined specification that aligns closely with the concepts in Spark
# coded -> none, uncompressed, bzip2, snappy ->  Declares what compression coded Spark should use to read or write the file
# mergeSchema -> true, false -> You can incrementally add columns to newly written Parquet files in the same table/folder

# Writing Parquet Files
# csvFile.write.format("parquet").mode("overwrite")\
#   .save("/tmp/my-parquest-file.parqut")


###################
# ORC Files
# What is the difference between ORC and Parquet?
# For the most part, they're similar; the funamendal difference is that Parquet is further optimized for use with Spark, whereas ORC is further optimized for Hive
# For AWS Athena use ORC if you want the fastest performance
# For AWS Glue Jobs use Parquet if you want the fastest performance


# Reading ORC files
# spark.read.format("orc").load("/data/flight/orc/2010-summary.orc")

# Writing ORC files
# csvFile.write.format("orc").mode("overwrite").save("/tmp/my-orc-export.orc") 

##################
# SQL Databases
# SQL datasources are one of the more powerful connectors because there are a variety of systems to which you can connect (as longs as that system speaks SQL)
# You can connect to MySQL, PostgreSQL, Oracle db, SQLite
# You need to consider:
#   - authentication
#   - connectivity - whether the network of Spark cluster is connected to the network of your db
# In this example we'll use SQLite, because SQLite database is just a file


# Read & Write from DBs
# To read and write from the databases, you need to do wto things, include the Java Database Connectivity (JDBC) driver for your particular db on the spark classpath and provide the proper JAR for the driver itself
# To read and write from PostgreSQL, you might run something like this:
# ./bin/spark-shell \
#   --driver-class-path postgresql-9.4.1207.jar \
#   --jars postgresql-9.4.1207.jar


# Reading from SQL DBs
#driver = "org.sqlite.JDBC"
#path = "./Spark-The-Definitive-Guide/data/flight-data/jdbc/my-sqlite.db"
#url = "jdbc:sqlite:" + path
#tablename = "flight_info"

# Testing the connection
#df = spark.read.format("jdbc").option("url", url).option("dbtable", tablename).option("driver", driver).load()

# Reading Text Files
#spark.read.textFile("./Spark-The-Definitive-Guide/data/csv/2010-summary.csv")\
#    .selectExpr("split(value, ',') as rows").show()

# Advanced I/O Concepts
# Splittable File Types and Compression
# Certain file formats are fundamentally "splittable". This can improve speed because it makes it possible for for Spark to avoid reading an entire file
# and access only the parts of the file necessary to satisfy your query.
# Not all compression schemes are splittable. We recommend Parquet with gzip compression

# Reading Data in Parallel
# Multiple executors cannot read from the same file at the same time, but they can read different files at the same time
# This means when you read from a folder with multiple files in it, each one of those files will become a partition in your DataFrame and be read in by available executors in parallel

# Writing Data in Parallel
# The number of files or data written is dependent on the numer of partition the DataFrame has at the time you write out the data
# By default, one file is written per partition of the data!
# csvFile.reparition(5).write.fomrat("csv").save("/tmp/multiple.csv")
# we'll end up with file files inside of that folder:
# tmp/multiple.csv/part-00000-xxxxx.csv
# tmp/multiple.csv/part-00001-xxxxx.csv
# tmp/multiple.csv/part-00002-xxxxx.csv


# Partitioning
# This is the lowest-hanging optimization that you can use that you can use when you have a table that readers frequently filter by before manipulating
# Date is particularly common for a partition because downstream, often we want to look at only the previous week's data. This can speedups for readers
# Partitioning is a tool that allows you to control what data is stored (and wheere) as you write it
# When you write a file to a partitioned directory (or table), you basically encode a column as a folder
# It allows you to skip lots of data when you go to read it in later, allowing you to read in only the data relevant to your problem instead of having to scan the complete dataset
# csfFile.limit(10).write.mode("overwrite").partitionBy("DEST_COUNTRY_NAME")\
#        .save("/tmp/partitioned-files.parquet")
# You'll get list of folders: 
#DEST_COUNTRY_NAME=Costa Rica/ 
#DEST_COUNTRY_NAME=Egypt/ 
#DEST_COUNTRY_NAME=Senegal/


# Bucketing
# Bucketing is another file organization approach whith which you can control the data that is specifically written to each file
# This can help avoid shuffles later when you go to read the data because data with the same bucket ID will all be grouped together into one physical partition
# This means that the data is prepartitioned according to how you expect to use that data later on, meaning you can avoid expensive shuffles when joining or aggregating


# Managing File Size
# Spark does not do well with small files. You might hear this referred to as the "small file problem"
# The opposite is also true: you don't want files that are too large either, because it becomes inefficient to have to read entire blocks of data when you need only a few rows
# From Spark 2.2 you can use the "maxRecordsPerFile" option and specify a number of your choosing. This allows you to better control file sizes by controlling the number of records that are written each file
# df.write.option("maxRecordsPerFile", 5000), Spark will ensure that files will contain at most 5,000 records

