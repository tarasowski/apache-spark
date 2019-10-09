##############
# Loading a jsonl (without consistent fields) file into spark? 

#1. my code expect things and json is not confirm to expectations 
#2. create a schema that the code expects to work with
#3. I need to read the file in and modify it according to the schema

# myschema = [StructField(name="key", nullable=False, default=...)...]

# spark.context.textFile(...)
#      .map(lambda z: json.loads(z)...)
#      .map(...)
#      .filter(...)
#      .toDF(myschema)

# everything that cannot fit into a schema, put into a field that is called "unparsable"

############
# Ports and Adapters for Pipeline
# create an adapter to run the code from elsewhere
# provide the partition outside of the file --file_path
# pass environment variables into a script, don't hardcore it such I did it with the date

#def pipeline(date_partition):
#  # returns nothing
#  pass
#
#class DuplicatorJob:
#  date_partition = None
#
#  def __init__(self, date_partition):
#    self.date_partition = date_partition
#    pass
#
#  # Runs the complete pipline
#  # Called from the glue script
#  def run(self):
#    spark = SparkSession......getOrCreate()
#    input1 = spark.read.... # Read from glue / meta store for self.date_partition - find the s3 path for date_partition of input1
#    input2 = spark.read....
#    out = process(input1, input2)
#    out.write.parquet(...)
#    # Save output to glue or metastore
#    # for self.date_partition  
#    pass
#
#
#  # Code that is unit tested
#  def process(self, input1, input2):
#    # return output
#    pass


# Team structure and job structure

#JOB NR.1: Performs parsing (ip, browser), combines data into fields
#Process pr1nm -> "SKU1"
#Process pr2nm -> "SKU2"
#Process pr3nm -> "SKU3"

#JOB NR.2: Does aggregation on data


#########
# How to keep state?
# Pipelines are stateless. Should be stateless. Luigi, Airflow, Composer, Data Pipelines from AWS, to express the dependency definition.
# Find a separate tool that does the orchestration
# The job becomes like a function, it gets the parameters from outside
# The job runs only if the arguments are given to the job
