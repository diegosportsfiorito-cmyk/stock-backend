# ===== MAIN.PY FINAL Y VERIFICADO =====

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from indexer import procesar_pregunta
from drive import listar_archivos_en_carpeta, descargar_archivo_por_id
import pandas as pd
import io

# ID de la carpeta de Google Drive
FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

# ------------------------------------------------------------
# CARGAR EXCEL MÁS RECIENTE
# ------------------------------------------------------------

def cargar_excel_mas_reciente():
    archivos = listar_archivos_en_carpeta(FOLDER_ID)

    if not archivos:
        raise Exception("No se encontraron archivos en la carpeta de Drive.")

    # Ordenar por fecha de modificación
    archivos_ordenados = sorted(
        archivos,
        key=lambda x: x["modifiedTime"],
        reverse=True
    )

    archivo = archivos_ordenados[0]
    contenido = descargar_archivo_por_id(archivo["id"])

    df = pd.read_excel(io.BytesIO(contenido))
    return df, archivo

# Cargar Excel al iniciar el servidor
df, archivo_fuente = cargar_excel_mas_reciente()

# ------------------------------------------------------------
# FASTAPI
# ------------------------------------------------------------

app = FastAPI()

# CORS para permitir acceso desde tu frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Podés restringirlo si querés
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelo de entrada
class QueryRequest(BaseModel):
    question: str

# ------------------------------------------------------------
# ENDPOINT PRINCIPAL
# ------------------------------------------------------------

@app.post("/query")
async def query(req: QueryRequest):
    pregunta = req.question.strip()

    try:
        resultado = procesar_pregunta(df, pregunta)

        # Agregar fuente del Excel
        resultado["fuente"] = {
            "id": archivo_fuente["id"],
            "name": archivo_fuente["name"],
            "mimeType": archivo_fuente["mimeType"],
            "modifiedTime": archivo_fuente["modifiedTime"]
        }

        return resultado

    except Exception as e:
        print("ERROR EN /query:", e)
        return {
            "tipo": "mensaje",
            "mensaje": "Ocurrió un error procesando la consulta.",
            "voz": "Ocurrió un error procesando la consulta.",
            "error": str(e),
        }

# ------------------------------------------------------------
# ENDPOINT DE PRUEBA
# ------------------------------------------------------------

@app.get("/")
async def root():
    return {
        "status": "OK",
        "message": "Stock IA Backend funcionando.",
        "archivo_cargado": archivo_fuente["name"]
    }

# ===== FIN MAIN.PY =====
