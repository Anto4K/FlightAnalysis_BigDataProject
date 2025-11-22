import locale
import os

import airportsdata
import pandas as pd
from geopy import Nominatim
from datetime import datetime, timedelta


def mese_da_numero(numero):
    match numero:
        case 1:
            return "gennaio"
        case 2:
            return "febbraio"
        case 3:
            return "marzo"
        case 4:
            return "aprile"
        case 5:
            return "maggio"
        case 6:
            return "giugno"
        case 7:
            return "luglio"
        case 8:
            return "agosto"
        case 9:
            return "settembre"
        case 10:
            return "ottobre"
        case 11:
            return "novembre"
        case 12:
            return "dicembre"

def mese_da_nome(mese):
    match mese.lower():
        case "gennaio":
            return 1
        case "febbraio":
            return 2
        case "marzo":
            return 3
        case "aprile":
            return 4
        case "maggio":
            return 5
        case "giugno":
            return 6
        case "luglio":
            return 7
        case "agosto":
            return 8
        case "settembre":
            return 9
        case "ottobre":
            return 10
        case "novembre":
            return 11
        case "dicembre":
            return 12
        case _:
            return "Mese non valido"


def mese_precedente(mese_corrente):
    # Impostare la locale in italia
    locale.setlocale(locale.LC_TIME, 'it_IT.UTF-8')
    # Creare una data_mese fittizia nel mese corrente (1° giorno del mese corrente)
    data_corrente = datetime.strptime(f"01 {mese_corrente} 2023", "%d %B %Y")
    # Sottrarre un giorno per ottenere l'ultimo giorno del mese precedente
    data_precedente = data_corrente - timedelta(days=1)
    # Restituire il nome del mese precedente
    return data_precedente.strftime("%B")


def getListaCitta():
    file_path = os.path.join(os.path.dirname(__file__), 'lista_nomi_aeroporti.txt')
    with open(file_path, 'r') as file:
        lista_letta = list(eval(file.read()))
    return lista_letta

def getSortedListaCitta():
    return sorted(getListaCitta())

def get_coordinates_city(address):
    """
    :param address:
    :return: [latitude, longitude]
    """
    geolocator = Nominatim(user_agent="myGeocoderApp")
    location = geolocator.geocode(address,timeout=10)
    if location:
        return [location.latitude, location.longitude]
    else:
        return None

def get_airport_coordinates(code) -> dict:

    airports = airportsdata.load("IATA")
    airport=airports.get(code.upper())
    if airport:
        return {
            "name": airport["name"],
            "lat": airport["lat"],
            "lon": airport["lon"]
        }
    else:
        return None


def getDataframeCordinateCitta():
    file_path = os.path.join(os.path.dirname(__file__), 'citta_lat_long.csv')
    df = pd.read_csv(file_path)
    df.set_index("citta", inplace=True)
    return df

def getCordinateCitta_asDict():
    df=getDataframeCordinateCitta()
    dict = df.to_dict(orient='index')
    for city, coords in dict.items():
        dict[city] = [coords['lat'], coords['lon']]
    return dict