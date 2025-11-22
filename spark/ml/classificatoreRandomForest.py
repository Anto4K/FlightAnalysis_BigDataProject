from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame


def train_randomForest_model(df: DataFrame):
    # Colonne categoriche e numeriche
    categorical_columns = ["OriginStateName", "DestStateName"]
    numerical_columns = ["Quarter", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime", "CRSArrTime", "AirTime","CRSElapsedTime",
                          "TaxiOut", "WheelsOff", "WheelsOn", "TaxiIn", "Distance"]

    # Suddivisione del dataset in training e test
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    # Applica StringIndexer separatamente per il train e il test
    indexers = {}
    for col in categorical_columns:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index")
        # Fit solo sul train
        fitted_indexer = indexer.fit(train_df)
        train_df = fitted_indexer.transform(train_df)
        test_df = fitted_indexer.transform(test_df)  # Trasformazione sul test

        # Salva l'indexer per eventuali usi futuri
        indexers[col] = fitted_indexer

    # Nuove colonne indicizzate
    indexed_columns = [f"{col}_index" for col in categorical_columns]

    # Prepara il VectorAssembler per combinare le feature
    assembler_inputs = numerical_columns + indexed_columns
    assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")

    # Random Forest Classifier
    rf = RandomForestClassifier(featuresCol="features", labelCol="target", numTrees=40, maxDepth=10, maxBins=55)

    # Pipeline (solo per Assembler e RF)
    pipeline = Pipeline(stages=[assembler, rf])

    # Fit della pipeline sul dataset di training
    model = pipeline.fit(train_df)

    # Applicazione del modello ai dati di test
    result_df = model.transform(test_df)

    # Mostra un'anteprima del risultato
    result_df.select("prediction", "target", "features").show()

    return result_df