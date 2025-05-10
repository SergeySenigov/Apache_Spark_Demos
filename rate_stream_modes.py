from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr, count, sum, max, window


spark = (SparkSession.builder.appName("rate_stream")
         .master("local")
         .config("spark.sql.adaptive.enabled", False)
         .config("spark.sql.shuffle.partitions", 2)
         .getOrCreate())

df_stream = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 2) \
    .load()

df_stream = df_stream.withColumn('value', expr("case when value%100 = 0 and value>=100 then value-100 else value end"))

df_stream = df_stream.withColumn('num_of_30', expr("floor(value/30) + 1"))
df_stream = df_stream.groupBy('num_of_30').agg(
                max('value').alias('max_value'),
                sum(expr(" case when value%10 = 0 then 1 else 0 end")).alias('num_of_10')
        ) 

outputMode = "update" # "complete"

if outputMode == "complete":
    df_stream = df_stream.orderBy('num_of_30')

query = (df_stream \
    .writeStream \
    .outputMode(outputMode) \
    .format("console") \
    .option('truncate', 'False') \
    .trigger(processingTime='2 second') \
    .start())

spark.streams.awaitAnyTermination()
