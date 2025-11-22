import pandas as pd
from pandas import DataFrame
from spark.query.aeroporti_analysis.numero_voli_partenza_e_arrivo_aeroporto import num_voli_partenza_e_arrivo_citta
from spark.utils.utils import get_coordinates_city


def citta_piu_trafficate(df: DataFrame) -> DataFrame:
    citta = ["Atlanta, GA", "Dallas/Fort Worth, TX", "Denver, CO", "Chicago, IL", "Charlotte, NC","Orlando, FL", "Las Vegas, NV", "Phoenix, AZ", "New York, NY", "Seattle, WA"]

    dati = []
    for citta_nome in citta:
        coor = get_coordinates_city(citta_nome)
        if coor is not None:
            latitudine = coor[0]
            longitudine = coor[1]
            val1, val2 = num_voli_partenza_e_arrivo_citta(df, citta_nome)
            num = val1 + val2

            dati.append({"citta": citta_nome, "lat": latitudine, "lon": longitudine, "num": num})

    return pd.DataFrame(dati)
