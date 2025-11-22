from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, column, col


#VISUALIZZARE PER OGNI MESE IL NUMERO DEI VOLI TOTALI MEDIA DEI RITARDI, MEDIA DISTANZA PERCORSA,MEDIA MINUTI DI VOLO


def calcola_media(df1: DataFrame):
    """
    Calcola medie ottimizzate per ritardi, distanze e minuti di volo.
    Ritorna un dizionario con le medie necessarie.
    """
    # Filtri pre-calcolati per evitare duplicazioni
    df_non_dirottati = df1.filter(col('Diverted') == 0)
    df_dirottati = df1.filter(col('Diverted') != 0)

    medie = {}

    # Calcolo dei ritardi medi
    medie['average_delay_diretti'] = df1.dropna(subset=['ArrDelayMinutes']) \
        .filter(col('ArrDelayMinutes') > 0) \
        .agg(avg("ArrDelayMinutes")).collect()[0][0]

    medie['average_delay_diverted'] = df1.dropna(subset=['DivArrDelay']) \
        .agg(avg("DivArrDelay")).collect()[0][0]

    # Calcolo delle distanze medie
    medie['average_distance_diretti'] = df_non_dirottati.agg(avg("Distance")).collect()[0][0]
    medie['average_distance_diverted'] = df_dirottati.agg(avg("DivDistance")).collect()[0][0]

    # Calcolo del tempo medio di volo
    medie['average_flight_minutes_diretti'] = df1.agg(avg("ActualElapsedTime")).collect()[0][0]
    medie['average_flight_minutes_diverted'] = df1.agg(avg("DivActualElapsedTime")).collect()[0][0]

    return medie

def mese_num_voli_medie_ritardo_distanza_minutiVolo(df: DataFrame):


    num_voli_tot = df.count()
    medie = calcola_media(df)
    media_ritardi = round((medie['average_delay_diretti'] + medie['average_delay_diverted']) / 2, 2)
    media_distanza_percorsa = round((medie['average_distance_diretti'] + medie['average_distance_diverted']) / 2, 2)
    media_minuti_volo = round((medie['average_flight_minutes_diretti'] + medie['average_flight_minutes_diverted']) / 2, 2)

    return [num_voli_tot, media_ritardi, media_distanza_percorsa, media_minuti_volo]







