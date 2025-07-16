import requests

API_KEY = "6b1465f1-214c-47ef-91f5-f73e1b54b918"

# -------------------------------
# SECCIÓN: CONSUMOS API ARKHAM
# -------------------------------

def get_data_arkham(url):
    try:
        headers = {
            "API-Key": API_KEY
        }

        response = requests.request("GET", url, headers=headers)#, params=querystring) 
        response.raise_for_status()
        datos = response.json()

        return datos

    except requests.RequestException as e:
        print("Error al realizar la solicitud a la API:", e)
        return None

def get_data_transfers(token_name):
    url = "https://api.arkm.com/transfers"
    params = {
        "tokens": token_name,
        "flow": "all", # in, out, self, all
        "timeLast": "30d", # 24h, 7d, 30d, 90d, 180d
        "usdGte": 1000000, # 1000000
        "sortKey": "usd",
        "sortDir": "desc",
        "limit": 1650
    }
    headers = {
        "accept": "application/json",
        "API-Key": API_KEY
    }

    response = requests.get(url, params=params, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code}: {response.text}")

    return response.json().get("transfers", [])
 
import time 

def get_info_tx_hash(hash, intentos=10):
    url = f"https://api.arkm.com/tx/{hash}"
    headers = {
        "accept": "application/json",
        "API-Key": API_KEY
    }

    for intento in range(intentos):
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json() 

        elif response.status_code == 429:
            espera = 2 ** intento  # backoff exponencial: 1, 2, 4, 8, 16, etc.
            print(f"[{hash}] Error 429: Esperando {espera} segundos (intento {intento + 1}/{intentos})...")
            time.sleep(espera)
            continue

        else:
            raise Exception(f"[{hash}] Error {response.status_code}: {response.text}")

    print(f"[{hash}] Falló después de {intentos} intentos consecutivos por límite.")
    return None


# -------------------------------
# SECCIÓN: CONSUMOS API BITCOIN-DATA.COM
# -------------------------------

def get_btc_data(value):
    """
    Consulta desde bitcoin-data.com
    """
    url = f"https://bitcoin-data.com/v1/{value}"
    headers = {
        "accept": "application/hal+json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(f"Error al obtener datos de {value}:", e)
        return None
 
    """
    Consulta el HASHRATE de BTC desde bitcoin-data.com
    Devuelve una lista con fecha y HASHRATE.
    """
    url = "https://bitcoin-data.com/v1/hashrate"
    headers = {
        "accept": "application/hal+json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print("Error al obtener el HASHRATE:", e)
        return None