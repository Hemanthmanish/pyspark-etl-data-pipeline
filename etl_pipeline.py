from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

df_clean = df.dropna()

sales_summary = df_clean.groupBy("region").sum("revenue")

sales_summary.show()
