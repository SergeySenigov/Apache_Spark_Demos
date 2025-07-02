from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = (SparkSession.builder.appName("rate_stream_continuous")
         .master("local")
         .getOrCreate())

df_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load()

query = (df_stream\
    .filter(df_stream.value % 2 == 0)\
    # .withColumn('mod3', df_stream.value % 3) \
    .withColumn('tag', lit('Rate stream with 10 rows per sec in continuous mode')) \
    .select('value', 'tag') \
    # .select('value', 'mod3', 'tag') \
    # .groupBy('mod3')\
    # .count() \
    .writeStream \
    .option('truncate', 'False') \
    .format("console") \
    .outputMode('update') \
    .trigger(continuous="1 second") \
    # .trigger(processingTime='2 second') \
    .start())

spark.streams.awaitAnyTermination()
