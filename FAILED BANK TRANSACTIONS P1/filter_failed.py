from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FilterFailedTransactions").getOrCreate()

# Load cleaned data from GCS or local
df = spark.read.option("header", "true").csv("gs://sreeja-bucket/merged_transactions_clean/*.csv")

# Filter failed transactions
failed_df = df.filter(df["status"] == "FAILURE")

# Write to Cloud SQL (adjust connection details accordingly)
failed_df.write     .format("jdbc")     .option("url", "jdbc:mysql://35.238.221.80/banking")     .option("driver", "com.mysql.cj.jdbc.Driver")     .option("dbtable", "failed_transactions")     .option("user", "sreeja")     .option("password", "12345")     .mode("append")     .save()
