import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# 1. READ RAW JSON
# Your JSON is an array, so multiLine=True
raw_df = spark.read.option("multiLine", True).json(
    "s3://retail-raw-dataset/"
)

print("Raw schema:")
raw_df.printSchema()


# 2. EXPLODE ARRAY (important if nested)
if "array" in raw_df.columns:
    raw_df = raw_df.select(F.explode("array").alias("sales")).select("sales.*")


# 3. DATA CLEANING
clean_df = raw_df.dropDuplicates(["order_id"])

clean_df = clean_df.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)


# 4. ADD PARTITIONS
clean_df = clean_df.withColumn("year", F.year("date"))
clean_df = clean_df.withColumn("month", F.month("date"))
clean_df = clean_df.withColumn("day", F.dayofmonth("date"))


# 5. WRITE PARQUET (SILVER LAYER)
clean_df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet("s3://retail-processed-dataset/fact_sales/")


print("Glue job completed successfully")
