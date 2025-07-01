
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark import SparkConf


if __name__ == "__main__":

    conf = SparkConf().setAppName('PythonBisectingKMeansExample').setMaster('local')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Create a BisectingKMeans model with k=3
    bkm = BisectingKMeans(k=3, featuresCol = 'features', predictionCol = 'cluster')  

    # Create a DataFrame with the training data
    data = [
        (Vectors.dense([0.0, 0.0]), ), (Vectors.dense([1.0, 1.0]), ),
        (Vectors.dense([1.0, 8.0]), ), (Vectors.dense([0.0, 9.0]), ),
        (Vectors.dense([8.0, 1.0]), ), (Vectors.dense([9.0, 0.0]), )]
        
    df = spark.createDataFrame(data, ["features",])
    
    # Fit the model to the data
    model = bkm.fit(df)

    # Print the cluster centers and the predictions for each data point
    print("\nTraining clusters:")
    model.summary.predictions.show(truncate=False)

    print("Cluster Centers:")
    for c in model.clusterCenters():
        print(c)
    
    float_range = [i * 0.5 for i in range(-10, 27)]

    data_predict = []
    for x in float_range:
        for y in float_range:
            data_predict.append((Vectors.dense([x, y]), ))
    
    df_predict = spark.createDataFrame(data_predict, ["features",])
    
    # Print the new predictions using the transform() method
    print("\nNew predictions using with transform() method:")
    model.setPredictionCol("predicted cluster")
    df_predicted = model.transform(df_predict)
    

    # l = df_predicted.collect()
    # for i in l:
    #     print(str(i[0][0]).replace(".", ","), ";", str(i[0][1]).replace(".", ","), ";", i[1], sep="")
