
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.config("appName", "PythonBisectingKMeansExample").getOrCreate()

    bkm = BisectingKMeans(k=3, featuresCol = 'features', predictionCol = 'cluster')  

    data = [
        (Vectors.dense([0.0, 0.0]), ), (Vectors.dense([1.0, 1.0]), ),
        (Vectors.dense([1.0, 8.0]), ), (Vectors.dense([0.0, 9.0]), ),
        (Vectors.dense([8.0, 1.0]), ), (Vectors.dense([9.0, 0.0]), )]
        
    df = spark.createDataFrame(data, ["features",])
    
    model = bkm.fit(df)
    print("\nCluster Centers:", model.clusterCenters())
    print("Training clusters:")
    model.summary.predictions.show(truncate=False)
    
    data_predict = [
        (Vectors.dense([4.0, 3.0]), ), (Vectors.dense([6.0, 7.0]), ),
        (Vectors.dense([6.0, 4.0]), ), (Vectors.dense([9.0, 9.0]), )  ]

    df_predict = spark.createDataFrame(data_predict, ["features",])

    print("New predictions:")
    for d in data_predict:
        predicted_cluster = model.predict(d[0])
        print(d[0], ', predicted cluster', predicted_cluster)
    
    print("\nNew predictions (the same with transform()):")
    model.setPredictionCol("predicted cluster")
    model.transform(df_predict).show(truncate=False)