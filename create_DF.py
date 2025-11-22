from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

csv_fp1 = "spark/data/voli_1.csv"
csv_fp2 = "spark/data/voli_2.csv"
csv_fp3 = "spark/data/voli_3.csv"
csv_fp4 = "spark/data/voli_4.csv"
csv_fp5 = "spark/data/voli_5.csv"
csv_fp6 = "spark/data/voli_6.csv"
csv_fp7 = "spark/data/voli_7.csv"
csv_fp8 = "spark/data/voli_8.csv"
csv_fp9 = "spark/data/voli_9.csv"
csv_fp10 = "spark/data/voli_10.csv"
csv_fp11 = "spark/data/voli_11.csv"
csv_fp12 = "spark/data/voli_12.csv"

csv_files = [csv_fp1, csv_fp2, csv_fp3, csv_fp4, csv_fp5, csv_fp6, csv_fp7, csv_fp8, csv_fp9, csv_fp10, csv_fp11,
                csv_fp12]


def create_all_dataframe (spark)-> DataFrame:
    # Leggi e unisci i file CSV
    df = None
    for csv_file in csv_files:
        delimiter = ","
        if df is None:
            df = spark.read.options(delimiter=delimiter).csv(csv_file, header=True, inferSchema=True,
                                                             dateFormat='yyyy-MM-dd')
        else:
            df = df.union(spark.read.options(delimiter=delimiter).csv(csv_file, header=True, inferSchema=True,
                                                                      dateFormat='yyyy-MM-dd'))

    return df


def create_month_dataframe(spark:SparkSession, month:int)-> DataFrame:

    delimiter = ","
    df = spark.read.options(delimiter=delimiter).csv(csv_files[month-1], header=True, inferSchema=True,
                                                     dateFormat='yyyy-MM-dd')
    return df