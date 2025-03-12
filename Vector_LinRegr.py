from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import LinearRegression
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

if __name__ == "__main__":
    conf = SparkConf().setAppName('Vector_LinRegr').setMaster('local') 
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    data = [(-13, Vectors.dense([-6.0]),),
            (-12, Vectors.dense([-5.0]),),
            ( -8, Vectors.dense([-4.0]),),
            ( -1, Vectors.dense([-3.0]),),
            ( -0, Vectors.dense([-2.0]),),
            ( 10, Vectors.dense([-1.0]),),
            (  5, Vectors.dense([0.0]),),
            ( 15, Vectors.dense([1.0]),),
            ( 20, Vectors.dense([2.0]),),
            ( 25, Vectors.dense([3.0]),),
            ( 23, Vectors.dense([4.0]),),
            ( 60, Vectors.dense([5.0]),), 
            ( 65, Vectors.dense([6.0]),) ]
    
    print('')
    print('type of feature values list :', type(data[0][1]))
    print('')

    schema = StructType([StructField('labels', IntegerType(), True), StructField('features', VectorUDT(), True)])

    df_data = spark.createDataFrame(data, schema=schema)
    df_data.show()
    
    print('\nwith loss=\'squaredError\'')
    linreg = LinearRegression(maxIter=5, labelCol='labels', featuresCol='features', \
                            fitIntercept=True, loss='squaredError', solver='auto', regParam = 0.0)
    linregModel = linreg.fit(df_data)
    print("w=%s, intersect=%s" % (str(linregModel.coefficients), str(linregModel.intercept)))

    print('\nwith loss=\'huber\'')
    linreg = LinearRegression(maxIter=5, labelCol='labels', featuresCol='features', \
                            fitIntercept=True, loss='huber', solver='auto')
    linregModel = linreg.fit(df_data)
    print("w=%s, intersect=%s" % (str(linregModel.coefficients), str(linregModel.intercept)))
    print('')
