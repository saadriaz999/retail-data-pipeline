import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

# get runtime arguments
args = getResolvedOptions(sys.argv, ["input_path"])

input_path = args["input_path"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# read only the new file
raw_df = spark.read.option("multiLine", True).json(input_path)

# handle nested array
if "array" in raw_df.columns:
    raw_df = raw_df.select(F.explode("array").alias("sales")).select("sales.*")

# remove duplicates within this batch
clean_df = raw_df.dropDuplicates(["order_id"])

clean_df = clean_df.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)

clean_df = clean_df.withColumn(
    "date",
    F.to_date(F.col("date_id").cast("string"), "yyyyMMdd")
).drop("date_id")

clean_df = clean_df.withColumn("year", F.year("date"))
clean_df = clean_df.withColumn("month", F.month("date"))

# write partitioned
clean_df.write \
    .mode("append") \
    .partitionBy("year", "month") \
    .parquet("s3://retail-processed-dataset/fact_sales/")
