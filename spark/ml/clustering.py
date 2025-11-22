import pandas as pd
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame


def clustering_flights_onTime_delayed(df:DataFrame, numCluster:int, columns:list,columns_clustering:list):
    """
  Questa funzione ritorna un dizionario con i seguenti elementi:

    - "clusteredData": un DataFrame contenente i dati raggruppati (clustered_data_pd).
    - "silhouette": il valore del coefficiente di silhouette per la qualità del clustering.
    - "centroids": i centri (centroidi) dei cluster.

    Esempio di ritorno:

    {
        "clusteredData": <DataFrame con i dati raggruppati>,

        "silhouette": <Valore del coefficiente di silhouette>,

        "centroids": <Array contenente i centroidi dei cluster>
    }
    """

    assembler = VectorAssembler(
        inputCols=columns_clustering,
        outputCol="features")
    df_features = assembler.transform(df)

    kmeans = KMeans(k=numCluster, seed=1, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(df_features)

    df_clusters = model.transform(df_features)

    evaluator = ClusteringEvaluator(predictionCol="cluster", featuresCol="features")
    silhouette = evaluator.evaluate(df_clusters)

    centroids = model.clusterCenters()
    df_centroids = pd.DataFrame(centroids, columns=["x", "y"])

    columns_df_pandas= columns + ["cluster"]
    clustered_data_pd = df_clusters.select(*columns_df_pandas).toPandas()
    clustered_data_pd['cluster'] = clustered_data_pd['cluster'].astype(str)

    return {"clusteredData": clustered_data_pd, "silhouette": silhouette, "centroids": df_centroids}


