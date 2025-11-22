from datetime import datetime
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from create_DF import create_month_dataframe, create_all_dataframe
from spark.ml.clustering import clustering_flights_onTime_delayed
from spark.ml.metodiAux import build_train_dataframe
from spark.query.aeroporti_analysis.citta_voli_aeroporti import citta_voli_aeroporti
from spark.query.aeroporti_analysis.destinazioni_maxfrequenti_aeroporto import destinazione_numvoli_citta
from spark.query.aeroporti_analysis.numero_voli_partenza_e_arrivo_aeroporto import num_voli_partenza_e_arrivo_citta
from spark.query.aeroporti_analysis.ritardo_medio_arrivo_partenza_aeroporto import ritardo_medio_citta_mensile
from spark.query.dashboard_analysis.info_voli import informazioni_mensili
from spark.query.route.ricerca_voli import get_flight
from spark.query.statistiche_annuali.aeroporti_trafficati import citta_piu_trafficate
from spark.query.statistiche_annuali.mese_voli_settimana import mese_voli_settimana
from spark.utils.create_session import create_session
from spark.utils.preprocessing import convert_Time




spark_session=create_session()

def build_month_dataframe(mese:int):
    return create_month_dataframe(spark_session,mese)

def build_all_dataframe():
    return create_all_dataframe(spark_session)


def query_numero_partenze_e_arrivi_citta(citta:str):
    return num_voli_partenza_e_arrivo_citta(build_all_dataframe(), citta)


def query_ritardo_medio_partenza_arrivo_citta(citta:str) ->list[list[float]]:
    '''
    :param citta:
    :return: -prima lista: ritardi medi partenze; - seconda lista: ritardi medi in arrivi
    '''
    ritardi_medi = [[], []]
    for i in range(1, 13):
        df= build_month_dataframe(i)
        df_mese_from_city = df.filter((df["OriginCityName"] == citta))
        df_mese_to_city = df.filter((df["DestCityName"] == citta))

        ritardi_medi[0].append(ritardo_medio_citta_mensile(df_mese_from_city))
        ritardi_medi[1].append(ritardo_medio_citta_mensile(df_mese_to_city))

    return ritardi_medi

def query_destinazione_numvoli_citta(aeroporto:str):
    return destinazione_numvoli_citta(build_all_dataframe(), aeroporto)


def query_citta_numvoli_aeroporto(citta: str):
    return citta_voli_aeroporti(build_all_dataframe(),citta)


def query_get_volo(data: datetime.date, origine: str, destinazione: str, ora : datetime.time):
    return get_flight(convert_Time(build_all_dataframe()),data,origine,destinazione,ora)

def query_mesi_stato_voli():
    """
    :return: una lista di quattro elementi (ognuna di 12 valori) : [[orario][cancellati][dirottati][ritardo]]
    """
    numero_stato_voli = [[], [], [], []]
    for i in range(1, 13):
        df = build_month_dataframe(i)
        stato_voli_mese= informazioni_mensili(df)
        numero_stato_voli[0].append(stato_voli_mese[0])
        numero_stato_voli[1].append(stato_voli_mese[1])
        numero_stato_voli[2].append(stato_voli_mese[2])
        numero_stato_voli[3].append(stato_voli_mese[3])
    return numero_stato_voli

def query_mesi_voli_settimana():
    NUM_GIORNI_SETTIMANA = 7
    giorni_settimana_numvoli = [[] for i in range(NUM_GIORNI_SETTIMANA)]

    for i in range(1, 13):
        df = build_month_dataframe(i)
        lista_numvoli_settimana = mese_voli_settimana(df)

        for j in range(NUM_GIORNI_SETTIMANA):
            giorni_settimana_numvoli[j].append(lista_numvoli_settimana[j])

    return giorni_settimana_numvoli


def query_citta_num_voli() -> pd.DataFrame:
    return citta_piu_trafficate(build_all_dataframe())

def preprocessing_for_classification() -> DataFrame:
    return build_train_dataframe(build_all_dataframe())


#CLUSTERING

columns = ["DepDelay", "ArrDelay", "flight_duration"]
columns_clustering = ["DepDelay", "ArrDelay"]

def preprocessing_clustering(df:DataFrame)->DataFrame:
    df_new = df.filter(
        (col("Cancelled") == 0) &  # Volo non cancellato
        (col("Diverted") == 0)  # Volo non dirottato
    )
    df_new = df_new.withColumn("flight_duration", df["AirTime"] + df["ArrDelay"])
    df_clustering = df_new.select(columns)

    return df_clustering


def clusteringFlights(df:DataFrame,numCluster:int):

    return clustering_flights_onTime_delayed(df,numCluster,columns, columns_clustering)




