import io
import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from indexer import procesar_pregunta, autocompletar

# ============================================================
# FASTAPI + CORS
# ============================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# CARGA UNIVERSAL DE EXCEL (XLS + XLSX)
# ============================================================

def cargar_excel_mas_reciente():
    carpeta = "data"

    if not os.path.exists(carpeta):
        raise Exception("La carpeta /data no existe en Render.")

    archivos = [f for f in os.listdir(carpeta) if f.lower().endswith((".xls", ".xlsx"))]

    if not archivos:
        raise Exception("No hay archivos Excel (.xls o .xlsx) en /data")

    # Ordenar por fecha de modificación
    archivos.sort(key=lambda x: os.path.getmtime(os.path.join(carpeta, x)), reverse=True)
    archivo = archivos[0]
    ruta = os.path.join(carpeta, archivo)

    with open(ruta, "rb") as f:
        contenido = f.read()

    nombre = archivo.lower()

    # XLSX → openpyxl
    if nombre.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(contenido), engine="openpyxl")

    # XLS → xlrd
    elif nombre.endswith(".xls"):
        try:
            df = pd.read_excel(io.BytesIO(contenido), engine="xlrd")
        except ImportError:
            raise Exception(
                "Falta xlrd para leer archivos .xls. "
                "Agregá 'xlrd==2.0.1' a requirements.txt"
            )

    else:
        raise Exception(f"Formato no soportado: {archivo}")

    return df, archivo


# Cargar Excel al iniciar el servidor
df, archivo_fuente = cargar_excel_mas_reciente()
print(f"Excel cargado correctamente: {archivo_fuente}")


# ============================================================
# ENDPOINT PRINCIPAL /query
# ============================================================

@app.post("/query")
async def query(data: dict):
    try:
        pregunta = data.get("question", "")
        resultado = procesar_pregunta(df, pregunta)
        return resultado

    except Exception as e:
        return {
            "tipo": "mensaje",
            "mensaje": "Ocurrió un error procesando la consulta.",
            "voz": "Ocurrió un error procesando la consulta.",
            "error": str(e)
        }


# ============================================================
# AUTOCOMPLETE
# ============================================================

@app.get("/autocomplete")
async def autocomplete_endpoint(q: str):
    try:
        columnas = {
            "descripcion": None,
            "marca": None,
            "rubro": None,
            "color": None,
            "codigo": None,
            "talle": None
        }
        sugerencias = autocompletar(df, columnas, q)
        return {"sugerencias": sugerencias}

    except Exception as e:
        return {"sugerencias": [], "error": str(e)}


# ============================================================
# DASHBOARD GLOBAL (OPCIONAL)
# ============================================================

@app.get("/dashboard/global")
async def dashboard_global():
    try:
        # Stock total
        if "Stock" in df.columns:
            stock_total = int(pd.to_numeric(df["Stock"], errors="coerce").sum())
        else:
            stock_total = 0

        # Artículos únicos
        if "Codigo" in df.columns:
            articulos = df["Codigo"].nunique()
        else:
            articulos = len(df)

        return {
            "stock_total": stock_total,
            "articulos": articulos,
            "rubros": {},
            "marcas": {},
            "talles": {}
        }

    except Exception as e:
        return {"error": str(e)}
