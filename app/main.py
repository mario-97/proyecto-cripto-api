import asyncio
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import pandas as pd
import app.etl_data as etl
import app.exportar_github as exportar   
from pytz import utc  

app = FastAPI()
scheduler = AsyncIOScheduler()

# Lista tokens para las tareas recurrentes
TOKENS = ["bitcoin", "ethereum", "chainlink", "aave"]

# Función para exportar y subir a Drive
async def exportar_y_subir(nombre_salida, df):
    # subir a Google Drive
    exportar.exportar_datos(df, nombre_salida)

# JOB para BTC cada 4 horas
def job_btc():
    print(f"[{datetime.utcnow()}] Ejecutando job BTC cada 1 horas")
    for tipo in etl.ENDPOINTS_BTC:
        dfs = []
        for token in ["bitcoin"]:  # solo bitcoin
            if tipo == "btc_price":
                datos = etl.get_data_arkham.get_btc_data("btc-price")
            elif tipo == "btc_mvrvz":
                datos = etl.get_data_arkham.get_btc_data("mvrv-zscore")
            elif tipo == "btc_nupl":
                datos = etl.get_data_arkham.get_btc_data("nupl")
            elif tipo == "btc_sopr":
                datos = etl.get_data_arkham.get_btc_data("sopr")
            elif tipo == "btc_hashrate":
                datos = etl.get_data_arkham.get_btc_data("hashrate")
            else:
                datos = None
            
            if datos:
                df = etl.ENDPOINTS_BTC[tipo]["procesar"](datos, token)
                dfs.append(df)
        if dfs:
            df_concat = pd.concat(dfs, ignore_index=True)
            exportar.exportar_datos(df_concat, tipo) 

# JOB para transfers cada 5 minutos
def job_transfers():
    print(f"[{datetime.utcnow()}] Ejecutando job transfers cada 5 minutos")
    tipo = "transfers"
    dfs = []
    for token in TOKENS:
        datos = etl.get_data_arkham.get_data_transfers(token)
        if datos:
            df = etl.ENDPOINTS[tipo]["procesar"](datos, token)
            dfs.append(df)
    if dfs:
        df_concat = pd.concat(dfs, ignore_index=True)
        exportar.exportar_datos(df_concat, tipo)      

# Scheduler setup
@app.on_event("startup")
async def startup_event():
    scheduler.add_job(job_btc, 'interval', hours=1, id='btc_job')
    scheduler.add_job(job_transfers, 'interval', minutes=10, id='transfers_job')
    scheduler.add_job(job_otros_endpoints, 'interval', minutes=3, id='otros_endpoints_job')  
    scheduler.start()

# JOB para todos los otros endpoints cada 1 minuto (excepto transfers y btc)
def job_otros_endpoints():
    print(f"[{datetime.utcnow()}] Ejecutando job otros endpoints cada 5 minuto")
    for tipo in etl.ENDPOINTS:
        if tipo in ["transfers"]:
            continue  # evitar repetir lo que ya maneja otro job
        dfs = []
        for token in TOKENS:
            try:
                url = etl.ENDPOINTS[tipo]["url"](token)
                datos = etl.get_data_arkham.get_data_arkham(url)
                if datos:
                    df = etl.ENDPOINTS[tipo]["procesar"](datos, token)
                    dfs.append(df)
            except Exception as e:
                print(f"Error procesando {tipo} - {token}: {e}")
        if dfs:
            df_concat = pd.concat(dfs, ignore_index=True)
            exportar.exportar_datos(df_concat, tipo)


@app.get("/jobs")
def listar_jobs():
    jobs = scheduler.get_jobs()
    lista = []
    for job in jobs:
        next_run = job.next_run_time
        next_run_utc = next_run.astimezone(utc) if next_run else None
        lista.append({
            "id": job.id,
            "next_run_time": next_run_utc.strftime("%Y-%m-%d %H:%M:%S UTC") if next_run_utc else None,
            "trigger": str(job.trigger)
        })
    return lista
 