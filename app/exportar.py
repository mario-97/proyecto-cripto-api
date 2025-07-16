
import os
def exportar_datos(df, nombre_base):
    os.makedirs("D:\dashboard_arkham\data", exist_ok=True)
    
    ruta_excel = os.path.join("D:\dashboard_arkham\data", f"{nombre_base}.xlsx")
    ruta_parquet = os.path.join("D:\dashboard_arkham\data", f"{nombre_base}.parquet")

    df.to_excel(ruta_excel, index=False)
    df.to_parquet(ruta_parquet, index=False)

    print(f"Datos exportados a: {ruta_excel} y {ruta_parquet}")