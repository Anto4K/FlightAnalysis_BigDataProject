from pyspark.sql import DataFrame
from pyspark.sql.functions import column, count


def destinazione_numvoli_citta(df:DataFrame, citta:str):

    df_citta= df.filter(df["OriginCityName"] == citta)
    df_citta_dest_num_voli=df_citta.groupby(df["DestCityName"]).agg(count(("*")).alias("NumeroVoli")).orderBy(column("NumeroVoli").desc())
    top_airports = df_citta_dest_num_voli.collect()
    return {row["DestCityName"]: row["NumeroVoli"] for row in top_airports}
