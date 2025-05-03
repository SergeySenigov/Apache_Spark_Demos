from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = (SparkSession.builder.appName("rate_stream")
         .master("local")
         .config("spark.sql.adaptive.enabled", False)
         .config("spark.default.parallelism", 5)
         .getOrCreate())
# spark.conf.set("spark.sql.adaptive.enabled", False)
# spark.conf

df_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 2) \
    .load()

df_stream = df_stream.withColumn('tag', lit('Rate stream with 2 rows per sec processing every 2nd sec, "append" mode'))

query = (df_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option('truncate', 'False') \
    .trigger(processingTime='2 second') \
    .start())

spark.streams.awaitAnyTermination()
