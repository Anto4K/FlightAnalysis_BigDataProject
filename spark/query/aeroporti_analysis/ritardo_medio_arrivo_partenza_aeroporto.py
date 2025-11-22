from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg

#ritardo medio in partenza per i voli da questo aeroporto (rispetto ad ogni mese)
#ritardo medio in arrivo per i voli diretti a questo aeroporto (rispetto ad ogni mese).


def ritardo_medio_citta_mensile(df:DataFrame):

    avg_ritardi_part = df.dropna(subset=['ArrDelayMinutes']) \
        .filter(col('ArrDelayMinutes') > 0) \
        .agg(avg("ArrDelayMinutes")).collect()[0][0]

    avg_ritardi_dest = df.dropna(subset=['DivArrDelay']) \
        .agg(avg("DivArrDelay")).collect()[0][0]

    avg_ritardi_part = avg_ritardi_part if avg_ritardi_part is not None else 0
    avg_ritardi_dest = avg_ritardi_dest if avg_ritardi_dest is not None else 0

    return round((avg_ritardi_part + avg_ritardi_dest) / 2, 2)

