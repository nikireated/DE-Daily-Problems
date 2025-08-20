from pyspark.sql import SparkSession
from pyspark.sql.functions import col , to_date,sequence, min, max,explode, expr

spark = SparkSession.builder.appName("DetectMissingDates").getOrCreate()

sales = [("2025-01-01", 100),("2025-01-02", 150),("2025-01-04", 120),("2025-01-06", 200)]

sales_df1 = spark.createDataFrame(sales,["date","sales"])

sales_df1 = sales_df1.withColumn("date",to_date("date"))

# sales_df1.show()
# sales_df1.printSchema()

sales_df2 = sales_df1.agg(min("date").alias("startdate"),max("date").alias("enddate")).first() #returns a single Row
startdate = sales_df2["startdate"]   # Python datetime.date
enddate   = sales_df2["enddate"]

#
all_dates_df = spark.createDataFrame(
    [(startdate, enddate)], ["startdate", "enddate"]
).withColumn(
    "all_dates", sequence(col("startdate"), col("enddate"), expr("INTERVAL 1 DAY"))
).withColumn(
    "date", explode("all_dates")
).select("date")

# #creates df with start & end date
# #
all_dates_df.show()

missing_dates_df = all_dates_df.join(sales_df1, on="date", how="left_anti").withColumnRenamed("date", "missing_date")
missing_dates_df.show()

spark.stop()