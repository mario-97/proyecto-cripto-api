import os
import tempfile
import base64
import requests
import pandas as pd
from datetime import datetime

from app.utils.config import GITHUB_TOKEN, GITHUB_REPO, GITHUB_BRANCH, GITHUB_SUBFOLDER

HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

def subir_archivo_a_github(filepath, filename):
    url_api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_SUBFOLDER}/{filename}"
    
    with open(filepath, "rb") as file:
        content_encoded = base64.b64encode(file.read()).decode("utf-8")
    
    # Verificar existencia
    response = requests.get(url_api, headers=HEADERS)
    sha = response.json().get("sha") if response.status_code == 200 else None

    data = {
        "message": f"Upload {filename}",
        "content": content_encoded,
        "branch": GITHUB_BRANCH
    }
    if sha:
        data["sha"] = sha

    put_resp = requests.put(url_api, headers=HEADERS, json=data)

    if put_resp.status_code in [200, 201]:
        print(f"✅ Subido: {filename}")
        return True
    else:
        print(f"❌ Error al subir {filename}: {put_resp.status_code} {put_resp.text}")
        return False

def registrar_log(nombre_base, formato, status="guardado"):
    log_local_path = os.path.join("/tmp", "logs_actualizacion.csv")
    log_github_filename = "logs_actualizacion.csv"

    nueva_fila = {
        "fecha_hora": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "archivo": nombre_base,
        "formato": formato,
        "status": status
    }

    if os.path.exists(log_local_path):
        df_log = pd.read_csv(log_local_path)
        df_log = pd.concat([df_log, pd.DataFrame([nueva_fila])], ignore_index=True)
    else:
        df_log = pd.DataFrame([nueva_fila])

    df_log.to_csv(log_local_path, index=False)
    subir_archivo_a_github(log_local_path, log_github_filename)

def exportar_datos(df, nombre_base):
    with tempfile.TemporaryDirectory() as tmp_dir:
        formatos = {
            "csv": os.path.join(tmp_dir, f"{nombre_base}.csv"),
            "xlsx": os.path.join(tmp_dir, f"{nombre_base}.xlsx"),
            "parquet": os.path.join(tmp_dir, f"{nombre_base}.parquet")
        }

        for formato, ruta in formatos.items():
            try:
                if formato == "csv":
                    df.to_csv(ruta, index=False)
                elif formato == "xlsx":
                    df.to_excel(ruta, index=False)
                elif formato == "parquet":
                    df.to_parquet(ruta, index=False)

                exito = subir_archivo_a_github(ruta, os.path.basename(ruta))
                registrar_log(nombre_base, formato, "guardado" if exito else "error")

            except Exception as e:
                registrar_log(nombre_base, formato, "error")
                print(f"❌ Error en {formato}: {e}")

    print(f"🎯 Exportación completa para {nombre_base}")

