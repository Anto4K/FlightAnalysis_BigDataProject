from pyspark.sql import DataFrame

#numero totali di voli in partenza da questo aeroporto.
#numero totali di voli in arrivo a questo aeroporto.


def num_voli_partenza_e_arrivo_citta(df: DataFrame, citta: str):

    '''

    :param df:
    :param citta:
    :return: num_voli_partenza, num_voli_arrivo
    '''

    counts = df.filter((df["OriginCityName"] == citta) | (df["DestCityName"] == citta)) \
              .groupBy(
                  (df["OriginCityName"] == citta).alias("is_departure"),
                  (df["DestCityName"] == citta).alias("is_arrival")
              ).count()

    num_voli_partenza = counts.filter(counts["is_departure"] == True).agg({"count": "sum"}).collect()[0][0] or 0
    num_voli_arrivo = counts.filter(counts["is_arrival"] == True).agg({"count": "sum"}).collect()[0][0] or 0

    return num_voli_partenza, num_voli_arrivo
