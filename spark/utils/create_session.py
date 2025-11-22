from pyspark.sql import SparkSession


def create_session():
    spark = (SparkSession.builder.appName("ProgettoSpark").master("local[*]")
             .config("spark.executor.memory", "4g").config("spark.driver.memory", "4g")
             .config("spark.memory.fraction", "0.8").config("spark.memory.storageFraction", "0.3")
             .getOrCreate())
    return spark