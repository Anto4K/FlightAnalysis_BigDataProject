#query che restituisce il numero di voli arrivati a destinazione con ritardo in 4 range diversi [15,30,45,60+]
from typing import List
from pyspark.sql import DataFrame


def calcolaVoli(df : DataFrame)-> List:
    dfRitardo = df.filter(df["Cancelled"] == 0).filter(df["Diverted"] == 0).filter(df["ArrDelayMinutes"] > 0)
    range_0_15 = dfRitardo.filter(dfRitardo["ArrivalDelayGroups"] == 0).count()
    range_15_30 = dfRitardo.filter(dfRitardo["ArrivalDelayGroups"] == 1).count()
    range_30_45 = dfRitardo.filter(dfRitardo["ArrivalDelayGroups"] == 2).count()
    range_45_60 = dfRitardo.filter(dfRitardo["ArrivalDelayGroups"] == 3).count()
    range_over_60 = dfRitardo.filter(dfRitardo["ArrivalDelayGroups"] > 3).count()
    return [range_0_15,range_15_30,range_30_45,range_45_60,range_over_60]

def rangeRitardi(df : DataFrame)-> List:
    ris = calcolaVoli(df)
    return ris
