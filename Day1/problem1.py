from pyspark.sql import SparkSession
from pyspark.sql.functions import col , to_date

spark = SparkSession.Builder.appName("DetectMissingDates").getOrCreate()

sales = [("2025-01-01", 100),("2025-01-02", 150),("2025-01-04", 120),("2025-01-06", 200)]

sales_df2 = spark.createDataFrame(sales,["date","sales"])

sales_df2 = sales_df2.withColumn("date",to_date("date"))