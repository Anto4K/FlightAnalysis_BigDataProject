from pyspark.sql import DataFrame


def citta_voli_aeroporti(df:DataFrame,citta:str):

    #Prendo per quella citta aeroporto numvoli in partenza
    df_citta_aeroporti_partenza= df.filter((df["OriginCityName"]==citta)).select("Origin").groupby("Origin").count()
    dizionario_voli_partenze = {row['Origin']: row['count'] for row in df_citta_aeroporti_partenza.collect()}

    #Prendo per quella citta aeroporti numvoli in arrivo
    df_citta_aeroporti_arrivi= df.filter((df["DestCityName"]==citta)).select("Dest").groupby("Dest").count()
    dizionario_voli_arrivi = {row['Dest']: row['count'] for row in df_citta_aeroporti_arrivi.collect()}

    dizionario_combinato={}

    for citta in set(dizionario_voli_partenze.keys()).union(dizionario_voli_arrivi.keys()):
        partenze = dizionario_voli_partenze.get(citta, 0)
        arrivi = dizionario_voli_arrivi.get(citta, 0)
        dizionario_combinato[citta] = [partenze, arrivi]

    return dizionario_combinato

