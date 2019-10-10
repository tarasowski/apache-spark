############
# How to break up a large Spark script?

#1. The pipeline should be stateless, orchestration tools should manage the state?
# The pipeline should be completely stateless. A pipeline should have configurable input/output.
# so I can run the pipeline with today's data bases on the arguments provided. So you can put the data into some dummy bucket so local development is rectricted to the env and current pipeline

#2. Severless state management?
# If glue jobs fails it should notify an sns failure topic, which will be consumed by a lambda which should react to that failure
# Also the lambda function should store the state of the job in dynamodb, so you know the amount of retries that needs to be performed
# E.g if attempt #1 fails it sends a message to an sns que and lambda should know if the second attempt should be started
# If the data processing was successful it goes to a lambda function that triggers another pipeline that is dependent on the previous pipeline
# By using boto3 and aws glue api you can create the orchestration by ourself, basically rebuild it and not using Luigi, Compose or Data Pipeline

#3. How to break things up?
# If you want to break anything up you need to create INTERFACES (separation of concerns)
# Move all filtering login into 1 file that only deals with the particular logic
# But if you're not reusing the code there is no much need to breakup the code.
# Breaking up the code helps to test your code better

#4. How tests should be designed?
# Testing SQL is very hard
# Basically you need to wrap the SQL into a function and create some sample input and sample output data. (e.g. max. 25 records)
# https://github.com/anish749/spark2-etl-examples/blob/master/src/main/scala/org/anish/spark/etl/ProcessData.scala an example how to break up the code for testing

def process_data(input_dataframe):
  sparksSession = input_dataframe.sparkSession
  input_dataframe.registerTempTable(...)
  out = sparkSession.sql("""
  SELECT abs from .. group by ...
  """)
  return out


# Test case

def test_process():
  spark = SparkSession.....getOrCreate()

  mock_data = spark.read.format.... \
    .load("test/data/test_agg_data_input.csv") # 20 rows

  expected_output = spark.read.for.... \
    .load("test/data/test_agg_expected_output.csv")

  actual_output = process_data(mock_data)

  Utils.sameElements(actual_output, expected_output)
  Utils.sameSchema(actual_output, expected_output)

#5. Constant values in the code
# https://github.com/pipes/google-analytics-to-s3/blob/efc3ac17ca824a84baa9a7892c0a9cedecbdbd99/functions/sessionization/spark.py#L201
# Instead of a fuction you can put it into a dict

#6. Good test coverage
# Most of the ETL projects have test coverage of 0%
# Test #1: create a test case for pipe([]) to test for NoneType exceptons
# Test #2: write test to check the assumption that session_ids have only 1 body_id
# Test #3: https://github.com/pipes/google-analytics-to-s3/blob/efc3ac17ca824a84baa9a7892c0a9cedecbdbd99/functions/sessionization/spark.py#L598 combine into 1 function and test
# Test #4: https://github.com/pipes/google-analytics-to-s3/blob/efc3ac17ca824a84baa9a7892c0a9cedecbdbd99/functions/sessionization/spark.py#L969 separate logic from IO (IO doesn't need to be tested here)
# see the example how we worked with David and his approach from removing the IO part


###########
# First principles of Apache Spark
# 1. DataFrame API (or SQL)
# 2. MapReduce paradigm -> basic principle of distributed computing
# 3. Shuffling and query planning


##############
# The best way to develop Spark locally?
# 1. Create a sample data
# 2. Develop the code on local macine
# 3. scp (secure copy) data to ERM and run the script there
# 4. shutdown the EMR cluster



##############
# Expectation of the code?

# f make an assumption that 3 values will be passed as a parameter
# instead of passing 3 values you can pass a list or dict -> this is an another assumption
def f(x,y,z):
    pass


