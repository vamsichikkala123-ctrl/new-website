import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
    col
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_PATH",
        "TARGET_PATH"
    ]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read source
df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(args["SOURCE_PATH"])
)

# Remove duplicate records
df = df.dropDuplicates()

# Remove rows where all columns are null
df = df.na.drop(how="all")

# Add metadata
df = (
    df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("file_name", input_file_name())
    .withColumn("batch_id", lit("20260802"))
)

# Write Bronze
(
    df.write
      .mode("append")
      .format("parquet")
      .partitionBy("batch_id")
      .save(args["TARGET_PATH"])
)

job.commit()

print("sucessfully completed task")