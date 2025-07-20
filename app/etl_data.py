# etl_top_holders_eth.py
import pandas as pd
from datetime import datetime
import app.exportar as exportar
import app.get_data_arkham as get_data_arkham 
from collections import defaultdict

def procesar_top_holders(data):
    fecha_extraccion = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    token_name = data.get("token", {}).get("name")
    token_symbol = data.get("token", {}).get("symbol")

    filas = []
    for red, holders in data["addressTopHolders"].items(): 
        if holders is not None:
            for h in holders: 
                fila = {
                    "url_address": "https://intel.arkm.com/explorer/address/"+h["address"].get("address"),
                    "address": h["address"].get("address"),
                    "chain": h["address"].get("chain"),
                    "balance": h.get("balance"),
                    "usd": h.get("usd"),
                    "pctOfCap": h.get("pctOfCap"),
                    "isUserAddress": h["address"].get("isUserAddress"),
                    "contract": h["address"].get("contract"),
                    "arkhamEntity_name": h["address"].get("arkhamEntity", {}).get("name"),
                    "arkhamEntity_type": h["address"].get("arkhamEntity", {}).get("type"),
                    "arkhamEntity_website": h["address"].get("arkhamEntity", {}).get("website"),
                    "arkhamLabel_name": h["address"].get("arkhamLabel", {}).get("name"),
                    "arkhamLabel_chainType": h["address"].get("arkhamLabel", {}).get("chainType"),
                    "fecha_extraccion": fecha_extraccion,
                    "network": red,
                    "token_name": token_name,
                    "token_symbol": token_symbol
                }
                filas.append(fila)

    return pd.DataFrame(filas)

def procesar_top_flow(data, token_name):
    """
    Procesa una lista de movimientos de holders con in/out value en USD y token.
    Args:
        data (list): Lista de dicts, cada uno representando una dirección y sus métricas.
        token_name (str): Nombre del token (ej. Ethereum).
        token_symbol (str): Símbolo del token (ej. ETH).
    Returns:
        pd.DataFrame: Datos estructurados para análisis.
    """
    from datetime import datetime
    import pandas as pd

    fecha_extraccion = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    filas = []

    for h in data:
        addr = h.get("address", {})
        fila = {
            "url_address": "https://intel.arkm.com/explorer/address/"+addr.get("address"),
            "address": addr.get("address"),
            "chain": addr.get("chain"),
            "contract": addr.get("contract"),
            "isUserAddress": addr.get("isUserAddress"),
            "arkhamEntity_name": addr.get("arkhamEntity", {}).get("name"),
            "arkhamEntity_type": addr.get("arkhamEntity", {}).get("type"),
            "arkhamEntity_website": addr.get("arkhamEntity", {}).get("website"),
            "arkhamLabel_name": addr.get("arkhamLabel", {}).get("name"),
            "arkhamLabel_chainType": addr.get("arkhamLabel", {}).get("chainType"),
            "inUSD": h.get("inUSD"),
            "outUSD": h.get("outUSD"),
            "inValue": h.get("inValue"),
            "outValue": h.get("outValue"), 
            "fecha_extraccion": fecha_extraccion,
            "token_name": token_name
        }
        filas.append(fila)

    return pd.DataFrame(filas)

def procesar_pricetoken(data): 
    filas = []
    price = data.get("price", 0)
    price24hAgo = data.get("price24hAgo", 0)
    Change24hAgo =  price - price24hAgo
    Change24hAgoPct = 1-(price24hAgo/price)
    fecha_extraccion = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    fila = { 
        "fecha_extraccion": fecha_extraccion, 
        "fecha_extraccion_texto": "Update data: "+fecha_extraccion +" (UTC)", 
        "token_name": data.get("name", ""),
        "token_symbol": data.get("symbol", ""), 
        "price": price,
        "price24hAgo": price24hAgo,
        "Change24hAgo": Change24hAgo,
        "Change24hAgoPct": Change24hAgoPct,
        "volume24h": data.get("volume24h", 0),
        "allTimeHigh": data.get("allTimeHigh", 0),
        "allTimeLow": data.get("allTimeLow", 0),
        "circulatingSupply": data.get("circulatingSupply", 0),
        "totalSupply": data.get("totalSupply", 0)
    }
    filas.append(fila)
    return pd.DataFrame(filas)

def procesar_volume(data, token_name):
    filas = []
    for h in data:
        fila = {
            "inUSD": h.get("inUSD"), 
            "outUSD": h.get("outUSD"),
            "differentUSD": h.get("inUSD") - h.get("outUSD"), 
            "inValue": h.get("inValue"),
            "outValue": h.get("outValue"),
            "differentValue": h.get("inValue") - h.get("outValue"), 
            "time": h.get("time"),
            "token_name": token_name

        }
        filas.append(fila)

    return pd.DataFrame(filas)
 
def procesar_transfers(data, token_name):
    registros = []
    lista_tx_hash_btc = []

    for tx in data:
        txid = tx.get("transactionHash") or tx.get("txid")
        chain = tx.get("chain")
        token_id = tx.get("tokenId")
        token_name_cap = token_id.capitalize() if token_id else token_name.capitalize()

        if token_name.lower() == "bitcoin":
            if txid in lista_tx_hash_btc:
                continue

            detalle = get_data_arkham.get_info_tx_hash(txid)
            lista_tx_hash_btc.append(txid)

            if not detalle or "bitcoin" not in detalle:
                continue

            btc_tx = detalle["bitcoin"]
            timestamp = btc_tx["blockTimestamp"]
            block = btc_tx["blockHeight"]
            block_hash = btc_tx["blockHash"]
            token_symbol = "BTC"
            token_decimals = 8
            token_address = None
            token_name_api = "bitcoin"
            tx_type = None

            inputs = btc_tx["inputs"]
            outputs = btc_tx["outputs"]

            input_addresses = [i.get("address", {}) for i in inputs]
            input_labels = [addr.get("arkhamEntity", {}).get("name") or addr.get("arkhamLabel", {}).get("name") for addr in input_addresses if addr]
            unique_labels = list(set(l for l in input_labels if l))

            if unique_labels:
                from_entity_label = unique_labels[0]
            else:
                from_entity_label = None

            extra_inputs = len(inputs) - 1
            from_addr_summary = input_addresses[0].get("address") if input_addresses else None
            if extra_inputs > 0:
                from_addr_summary += f" (+{extra_inputs})"

            values = [(o.get("value", 0), o) for o in outputs]
            if len(values) < 2:
                continue

            _, output_major = sorted(values, key=lambda x: -x[0])[0]
            _, output_minor = sorted(values, key=lambda x: -x[0])[1]

            salidas_inciertas = [
                o for o in outputs
                if not o.get("address", {}).get("arkhamEntity") and not o.get("address", {}).get("arkhamLabel")
            ]

            if len(salidas_inciertas) > 1:
                total_valor = sum(o.get("value", 0) for o in salidas_inciertas)
                total_usd = sum(o.get("usd", 0) for o in salidas_inciertas)

                primera = salidas_inciertas[0]
                direccion_base = primera.get("address", {}).get("address")
                direccion_final = f"{direccion_base} (+{len(salidas_inciertas) - 1})"

                flow_direction = "Uncertain"
                interpretacion = "Flujo incierto"
                interpretation = "Uncertain flow"

                registros.append({
                    "token_name": token_name_cap,
                    "tx_hash": txid,
                    "timestamp": timestamp,
                    "block": block,
                    "block_hash": block_hash,
                    "chain": chain,
                    "unit_value": total_valor,
                    "historical_usd": total_usd,
                    "token_name_api": token_name_api,
                    "token_symbol": token_symbol,
                    "token_address": token_address,
                    "token_decimals": token_decimals,
                    "tx_type": tx_type,
                    "from_address": from_addr_summary,
                    "from_entity": from_entity_label,
                    "from_type": None,
                    "from_label": from_entity_label,
                    "from_is_user": None,
                    "from_contract": None,
                    "from_deposit_id": None,
                    "to_address": direccion_final,
                    "to_entity": None,
                    "to_type": None,
                    "to_label": None,
                    "to_is_user": None,
                    "to_contract": None,
                    "to_deposit_id": None,
                    "flow_direction": flow_direction,
                    "interpretacion": interpretacion,
                    "interpretation": interpretation
                })
            else:
                for output_entry in [output_major, output_minor]:
                    value = output_entry.get("value")
                    usd = output_entry.get("usd")
                    to_data = output_entry.get("address", {})
                    to_addr = to_data.get("address")
                    to_entity = to_data.get("arkhamEntity", {}).get("name")
                    to_type = to_data.get("arkhamEntity", {}).get("type")
                    to_label = to_data.get("arkhamLabel", {}).get("name")
                    to_is_user = to_data.get("isUserAddress")

                    is_probable_change = output_entry == output_minor and not to_entity and not to_label

                    if is_probable_change:
                        flow_direction = "Possible_change"
                        interpretacion = "Probable cambio"
                        interpretation = "Possible change"
                    elif from_entity_label and to_entity:
                        if from_entity_label != to_entity:
                            flow_direction = "outflow_exchange"
                            interpretacion = "Posible acumulación"
                            interpretation = "Possible accumulation"
                        else:
                            flow_direction = "internal_exchange"
                            interpretacion = "Movimiento interno"
                            interpretation = "Internal movement"
                    elif to_entity:
                        flow_direction = "inflow_exchange"
                        interpretacion = "Posible venta"
                        interpretation = "Possible sale"
                    else:
                        flow_direction = "neutral_wallets"
                        interpretacion = "Trans. entre billeteras"
                        interpretation = "Trans. between wallets"

                    registros.append({
                        "token_name": token_name_cap,
                        "tx_hash": txid,
                        "timestamp": timestamp,
                        "block": block,
                        "block_hash": block_hash,
                        "chain": chain,
                        "unit_value": value,
                        "historical_usd": usd,
                        "token_name_api": token_name_api,
                        "token_symbol": token_symbol,
                        "token_address": token_address,
                        "token_decimals": token_decimals,
                        "tx_type": tx_type,
                        "from_address": from_addr_summary,
                        "from_entity": from_entity_label,
                        "from_type": None,
                        "from_label": from_entity_label,
                        "from_is_user": None,
                        "from_contract": None,
                        "from_deposit_id": None,
                        "to_address": to_addr,
                        "to_entity": to_entity,
                        "to_type": to_type,
                        "to_label": to_label,
                        "to_is_user": to_is_user,
                        "to_contract": None,
                        "to_deposit_id": None,
                        "flow_direction": flow_direction,
                        "interpretacion": interpretacion,
                        "interpretation": interpretation
                    })

        else:
            timestamp = tx.get("blockTimestamp")
            block = tx.get("blockNumber") or tx.get("blockHeight")
            block_hash = tx.get("blockHash")
            usd = tx.get("historicalUSD")
            unit_value = tx.get("unitValue")
            token_name_api = tx.get("tokenName")
            token_symbol = tx.get("tokenSymbol")
            token_address = tx.get("tokenAddress")
            token_decimals = tx.get("tokenDecimals")
            tx_type = tx.get("type")

            from_data = tx.get("fromAddress", {})
            from_addr = from_data.get("address")
            from_entity = from_data.get("arkhamEntity", {}).get("name")
            from_type = from_data.get("arkhamEntity", {}).get("type")
            from_label = from_data.get("arkhamLabel", {}).get("name")
            from_is_user = from_data.get("isUserAddress")
            from_contract = from_data.get("contract")
            from_deposit_id = from_data.get("depositServiceID")

            to_data = tx.get("toAddress", {})
            to_addr = to_data.get("address")
            to_entity = to_data.get("arkhamEntity", {}).get("name")
            to_type = to_data.get("arkhamEntity", {}).get("type")
            to_label = to_data.get("arkhamLabel", {}).get("name")
            to_is_user = to_data.get("isUserAddress")
            to_contract = to_data.get("contract")
            to_deposit_id = to_data.get("depositServiceID")

            is_from_cex = from_type == "cex"
            value_to_type = to_type or to_label
            is_to_cex = value_to_type == "cex"

            if not is_from_cex and is_to_cex:
                flow_direction = "inflow_exchange"
                interpretacion = "Posible venta"
                interpretation = "Possible sale"
            elif is_from_cex and not is_to_cex:
                flow_direction = "outflow_exchange"
                interpretacion = "Posible acumulación"
                interpretation = "Possible accumulation"
            elif is_from_cex and is_to_cex:
                flow_direction = "internal_exchange"
                interpretacion = "Movimiento interno"
                interpretation = "Internal movement"
            else:
                flow_direction = "neutral_wallets"
                interpretacion = "Trans. entre billeteras"
                interpretation = "Trans. between wallets"

            registros.append({
                "token_name": token_name_cap,
                "tx_hash": txid,
                "timestamp": timestamp,
                "block": block,
                "block_hash": block_hash,
                "chain": chain,
                "unit_value": unit_value,
                "historical_usd": usd,
                "token_name_api": token_name_api,
                "token_symbol": token_symbol,
                "token_address": token_address,
                "token_decimals": token_decimals,
                "tx_type": tx_type,
                "from_address": from_addr,
                "from_entity": from_entity,
                "from_type": from_type,
                "from_label": from_label,
                "from_is_user": from_is_user,
                "from_contract": from_contract,
                "from_deposit_id": from_deposit_id,
                "to_address": to_addr,
                "to_entity": to_entity,
                "to_type": to_type,
                "to_label": to_label,
                "to_is_user": to_is_user,
                "to_contract": to_contract,
                "to_deposit_id": to_deposit_id,
                "flow_direction": flow_direction,
                "interpretacion": interpretacion,
                "interpretation": interpretation
            })

    df_resultado = pd.DataFrame(registros)
    print(f"Token evaluado: {token_name}")
    print(f"Total registros generados: {len(df_resultado)}")
    return df_resultado

def procesar_btc_price(data):
    """
    Procesa el precio histórico de BTC desde bitcoin-data.com
    """
    filas = []
    for item in data:
        fila = {
            "d": item.get("d"),  # mantener formato original
            "unixTs": item.get("unixTs"),
            "btcPrice": item.get("btcPrice")
        }
        filas.append(fila)
    return pd.DataFrame(filas)

def procesar_mvrv_zscore(data):
    """
    Procesa los valores MVRV-Z de BTC desde bitcoin-data.com
    """
    filas = []
    for item in data:
        fila = {
            "d": item.get("d"),  # mantener formato original
            "unixTs": item.get("unixTs"),
            "mvrvZscore": item.get("mvrvZscore")
        }
        filas.append(fila)
    return pd.DataFrame(filas)

def procesar_nupl(data):
    """
    Procesa los valores NUPL de BTC desde bitcoin-data.com
    """
    filas = []
    for item in data:
        fila = {
            "d": item.get("d"),  # mantener formato original
            "unixTs": item.get("unixTs"),
            "nupl": item.get("nupl")
        }
        filas.append(fila)
    return pd.DataFrame(filas)

def procesar_sopr(data):
    """
    Procesa los valores SOPR de BTC desde bitcoin-data.com
    """
    filas = []
    for item in data:
        fila = {
            "d": item.get("d"),  # fecha
            "unixTs": item.get("unixTs"),
            "sopr": item.get("sopr")
        }
        filas.append(fila)
    return pd.DataFrame(filas)

def procesar_hashrate(data):
    """
    Procesa los valores HASHRATE de BTC desde bitcoin-data.com
    """
    filas = []
    for item in data:
        fila = {
            "d": item.get("d"),  # mantener formato original
            "unixTs": item.get("unixTs"),
            "hashrate": item.get("hashrate")
        }
        filas.append(fila)
    return pd.DataFrame(filas)


def procesar_miner_outflow(data):
    """
    Procesa los valores de ouflow de mineros, es decir, salidas de wallets de mineros a otras direcciones
    """
    filas = []
    for item in data:
        fila = {
            "d": item.get("d"),   
            "unixTs": item.get("unixTs"),
            "outFlows": item.get("outFlows")
        }
        filas.append(fila)
    return pd.DataFrame(filas)

def procesar_miner_reserves(data):
    """
    Procesa los valores de ouflow de mineros, es decir, salidas de wallets de mineros a otras direcciones
    """
    filas = []
    for item in data:
        fila = {
            "d": item.get("d"),   
            "unixTs": item.get("unixTs"),
            "reserves": item.get("reserves")
        }
        filas.append(fila)
    return pd.DataFrame(filas)


""" info defilama """
def procesar_tvl_aave(data, token): 
    """
    Procesa la información de protocolos AAVE con categoría 'Lending' y devuelve un DataFrame.
    
    Parámetros:
        json_data (list or str): Lista de diccionarios (JSON parseado) o string JSON.
        
    Retorna:
        pd.DataFrame: DataFrame con información detallada de los protocolos AAVE relevantes.
    """
    nombres_aave = {"AAVE V1", "AAVE V2", "AAVE V3", "AAVE Arc"}
    protocolos_filtrados = []

    for p in data:
        if p.get("name") in nombres_aave and p.get("category") == "Lending":
            protocolo = {
                "id": p.get("id"),
                "name": p.get("name"),
                "description": p.get("description"),
                "url": p.get("url"),
                "symbol": p.get("symbol"),
                "category": p.get("category"),
                "chain": p.get("chain"),
                "chains": p.get("chains"),
                "tvl": p.get("tvl"),
                "borrowed": p.get("chainTvls", {}),
                "change_1h": p.get("change_1h"),
                "change_1d": p.get("change_1d"),
                "change_7d": p.get("change_7d"),
                "audit_note": p.get("audit_note"),
                "audits": p.get("audits"),
                "logo": p.get("logo"),
                "twitter": p.get("twitter"),
                "methodology": p.get("methodology"),
                "listedAt": p.get("listedAt"),
                "parentProtocol": p.get("parentProtocol"),
                "module": p.get("module"),
                "forkedFrom": p.get("forkedFrom"),
                "hallmarks": p.get("hallmarks"),
                "slug": p.get("slug"),
                "mcap": p.get("mcap"),
            }
            protocolos_filtrados.append(protocolo)

    df = pd.DataFrame(protocolos_filtrados)
    return df  

def procesar_y_exportar(nombre_salida, token_list):
    configuracion = ENDPOINTS[nombre_salida]
    salidas = []

    for token in token_list:
        url = configuracion["url"](token)
        if nombre_salida == "transfers":
            datos = get_data_arkham.get_data_transfers(token)
        else:
            url = configuracion["url"](token)
            datos = get_data_arkham.get_data_arkham(url)
        
        if datos is not None:
            df = configuracion["procesar"](datos, token)
            salidas.append(df)

    if salidas:
        df_concatenado = pd.concat(salidas, ignore_index=True)
        exportar.exportar_datos(df_concatenado, nombre_salida)

def procesar_y_exportar_btc(nombre_salida, token_list=["bitcoin"]):
    configuracion = ENDPOINTS_BTC[nombre_salida]
    salidas = []

    for token in token_list: # solo BTC
        if nombre_salida == "btc_price":
            datos = get_data_arkham.get_btc_data("btc-price")
        elif nombre_salida == "btc_mvrvz":
            datos = get_data_arkham.get_btc_data("mvrv-zscore")
        elif nombre_salida == "btc_nupl":
            datos = get_data_arkham.get_btc_data("nupl")
        elif nombre_salida == "btc_sopr":
            datos = get_data_arkham.get_btc_data("sopr")
        elif nombre_salida == "btc_hashrate":
            datos = get_data_arkham.get_btc_data("hashrate")
        else:
            print("-")

        if datos is not None:
            df = configuracion["procesar"](datos, token)
            salidas.append(df)

    if salidas:
        df_concatenado = pd.concat(salidas, ignore_index=True)
        exportar.exportar_datos(df_concatenado, nombre_salida)

# Diccionario que mapea tipo de extracción a sus parámetros
ENDPOINTS = {
    "transfers": {
        "url": lambda token: token,  # porque el token es parte del parámetro, no de la URL
        "procesar": lambda datos, token: procesar_transfers(datos, token)
    },
    "top_holders": {
        "url": lambda token: f"https://api.arkm.com/token/holders/{token}",
        "procesar": lambda datos, token: procesar_top_holders(datos)
    },
    "top_flow": {
        "url": lambda token: f"https://api.arkm.com/token/top_flow/{token}?timeLast=30d",
        "procesar": lambda datos, token: procesar_top_flow(datos, token)
    },
    "trending": {
        "url": lambda token: f"https://api.arkm.com/token/trending/{token}",
        "procesar": lambda datos, token: procesar_pricetoken(datos)
    },
    "top_volume": {
        "url": lambda token: f"https://api.arkm.com/token/volume/{token}?timeLast=30d&granularity=24h",
        "procesar": lambda datos, token: procesar_volume(datos, token)
    },
    "tvl_aave":{
        "url": lambda token: f"https://api.llama.fi/protocols",
        "procesar": lambda datos, token: procesar_tvl_aave(datos, token)
    }
}

ENDPOINTS_BTC = {
     "btc_price": { 
        "procesar": lambda datos: procesar_btc_price(datos)
    },
    "btc_mvrvz": { 
        "procesar": lambda datos: procesar_mvrv_zscore(datos)
    },
    "btc_nupl": { 
        "procesar": lambda datos: procesar_nupl(datos)
    },
    "btc_sopr": { 
        "procesar": lambda datos: procesar_sopr(datos)
    },
    "btc_hashrate": { 
        "procesar": lambda datos: procesar_hashrate(datos)
    },
    "btc_miner_outflows": { 
        "procesar": lambda datos: procesar_miner_outflow(datos)
    },
    "btc_miner_reserves": { 
        "procesar": lambda datos: procesar_miner_reserves(datos)
    }
}

if __name__ == "__main__":
    #Rate limit exceeded, 20 per hour. Please try again later or change to another plan.
    for tipo in ENDPOINTS_BTC:
        procesar_y_exportar_btc(tipo)

    # arkham
    tokens = ["bitcoin", "ethereum", "chainlink", "aave"]
    for tipo in ENDPOINTS:
        procesar_y_exportar(tipo, tokens)