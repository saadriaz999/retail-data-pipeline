import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

raw_df = spark.read.option("multiLine", True).json(
    "s3://retail-raw-dataset/"
)

if "array" in raw_df.columns:
    raw_df = raw_df.select(F.explode("array").alias("sales")).select("sales.*")

clean_df = raw_df.dropDuplicates(["order_id"])

clean_df = clean_df.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)

clean_df = clean_df.withColumn(
    "date",
    F.to_date(F.col("date_id").cast("string"), "yyyyMMdd")
)
clean_df = clean_df.drop("date_id")

clean_df = clean_df.withColumn("year", F.year("date"))
clean_df = clean_df.withColumn("month", F.month("date"))
clean_df = clean_df.withColumn("day", F.dayofmonth("date"))

clean_df.write \
    .mode("append") \
    .partitionBy("year", "month") \
    .parquet("s3://retail-processed-dataset/fact_sales/")
