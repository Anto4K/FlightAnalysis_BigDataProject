from typing import List
from pyspark.sql import DataFrame

#query che restituisce i voli senza ritardo,cancellati, con ritardo e dirottati di un particolare mese

def calcolaVoliInOrario(df: DataFrame)-> int:
    return df.filter((df["Cancelled"] == 0) & (df["Diverted"] == 0) & (df["ArrDelayMinutes"] == 0)).count()

def calcolaVoliCancellati(df: DataFrame)-> int:
    return df.filter(df["Cancelled"] != 0).count()

def calcolaVoliDirottati(df: DataFrame)-> int:
    return df.filter(df["Diverted"] != 0).count()

def calcolaVoliConRitardo(df: DataFrame)->int:
    return df.filter((df["Cancelled"] == 0) & (df["Diverted"] == 0) & (df["ArrDelayMinutes"] > 0)).count()

def informazioni_mensili(df: DataFrame) -> List:
    num_voli_in_orario = calcolaVoliInOrario(df)
    num_voli_cancellati = calcolaVoliCancellati(df)
    num_voli_dirottati = calcolaVoliDirottati(df)
    num_voli_con_ritardo = calcolaVoliConRitardo(df)
    return [num_voli_in_orario,num_voli_cancellati,num_voli_dirottati,num_voli_con_ritardo]

