# ===== INICIO BLOQUE MAIN =====

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from indexer import procesar_pregunta
from drive import listar_archivos_en_carpeta, descargar_archivo_por_id
import pandas as pd
import io

# ID de la carpeta de Google Drive
FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

# ============================================================
# CARGAR EL ÚLTIMO ARCHIVO DEL DRIVE
# ============================================================

def cargar_excel_mas_reciente():
    archivos = listar_archivos_en_carpeta(FOLDER_ID)

    if not archivos:
        raise Exception("No se encontraron archivos en la carpeta de Drive.")

    # Ordenar por fecha de modificación (descendente)
    archivos_ordenados = sorted(
        archivos,
        key=lambda x: x["modifiedTime"],
        reverse=True
    )

    archivo = archivos_ordenados[0]  # el más reciente
    contenido = descargar_archivo_por_id(archivo["id"])

    df = pd.read_excel(io.BytesIO(contenido))
    return df, archivo

# Cargar Excel al iniciar
df, archivo_fuente = cargar_excel_mas_reciente()

# ============================================================
# FASTAPI
# ============================================================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    question: str

# ============================================================
# ENDPOINT PRINCIPAL
# ============================================================

@app.post("/query")
async def query(req: QueryRequest):
    pregunta = req.question.strip()

    try:
        resultado = procesar_pregunta(df, pregunta)

        # Agregar info de la fuente
        resultado["fuente"] = archivo_fuente

        return resultado

    except Exception as e:
        print("ERROR EN /query:", e)
        return {
            "tipo": "mensaje",
            "mensaje": "Ocurrió un error procesando la consulta.",
            "voz": "Ocurrió un error procesando la consulta.",
            "error": str(e),
        }

# ============================================================
# ENDPOINT DE PRUEBA
# ============================================================

@app.get("/")
async def root():
    return {"status": "OK", "message": "Stock IA Backend funcionando."}

# ===== FIN BLOQUE MAIN =====
