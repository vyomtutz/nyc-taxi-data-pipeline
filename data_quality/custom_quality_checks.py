from pyspark.sql import SparkSession

# Start Spark
spark = SparkSession.builder.appName("NYCTaxiQualityGate").getOrCreate()

# Read processed data from S3 or local (example)
df = spark.read.parquet("s3://nyc-taxi-processed/cleaned/")

# ---------- Quality Checks ----------

# 1️⃣ Check for nulls in critical columns
null_count = df.filter(
    df["passenger_count"].isNull() | df["fare_amount"].isNull()
).count()

if null_count > 0:
    raise Exception(f"❌ Quality gate failed: Found {null_count} null rows in critical columns.")

# 2️⃣ Check if fare amount is within valid range
invalid_fare_count = df.filter((df["fare_amount"] < 0) | (df["fare_amount"] > 1000)).count()

if invalid_fare_count > 0:
    raise Exception(f"❌ Quality gate failed: Found {invalid_fare_count} rows with invalid fare amount.")

# 3️⃣ Check if there are duplicate ride IDs (assuming 'ride_id' column exists)
duplicates = df.groupBy("ride_id").count().filter("count > 1").count()

if duplicates > 0:
    raise Exception(f"❌ Quality gate failed: Found {duplicates} duplicate ride IDs.")

# ---------- Pass ----------
print("✅ All quality gates passed successfully! 🚦")

# Stop Spark
spark.stop()
