from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder.appName("MergeDirtyCSVs").getOrCreate()

# Input and Output GCS Paths
input_path = "gs://sreeja-bucket/input/*.csv"
 # Added *.csv to correctly read all CSV files
output_path = "gs://sreeja-bucket/merged_transactions_clean/"

# Read all CSVs from the input path
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# Drop rows where all values are null
df_clean = df.dropna(how="all").dropDuplicates()

# Save cleaned & merged data
df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

# Stop the Spark session
spark.stop()
