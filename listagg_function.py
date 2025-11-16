from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark = SparkSession.builder.appName("listagg_function").getOrCreate()


df = spark.createDataFrame(
  [
    ('Lexus',   'Japan'), 
    ('Mazda',   'Japan'), 
    (None, None), 
    ('BMW',     'Germany'), 
    ('MB',      'Germany'), 
    ('GM',      'USA'),
    ('Peugeot', 'France')
  ], 
 ['CarModel', 'Country'])
 
df.show(truncate=False)

df.select(F.listagg('CarModel', ', ').alias('CarModelsList')).show(truncate=False)

df.select(F.listagg_distinct('Country', ', ').alias('CountryDistinctList')).show(truncate=False)

spark.stop()