
from pyspark.sql import DataFrame


def mese_voli_settimana(df:DataFrame):
    df_giornosett_volo = (df.groupby("DayOfWeek").count()).sort("DayOfWeek")
    return [row["count"] for row in df_giornosett_volo.collect()]