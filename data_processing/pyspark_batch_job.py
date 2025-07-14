from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("NYCTaxiBatch").getOrCreate()

df = spark.read.csv("s3://nyc-taxi-raw/*.csv", header=True, inferSchema=True)

df_cleaned = df.dropna(subset=["passenger_count", "fare_amount"])
df_cleaned.write.parquet("s3://nyc-taxi-processed/cleaned/", mode="overwrite")
