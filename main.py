import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from ai_engine import ask_ai

# Cargar variables de entorno
load_dotenv()

app = FastAPI()

# CORS para permitir tu frontend (ajustá el origen si querés restringir)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # o ["https://diegosportsfiorito-cmyk.github.io"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ruta del archivo Excel
EXCEL_PATH = os.getenv("STOCK_EXCEL_PATH", "stock.xlsx")

# Cargar Excel al iniciar
try:
    df_stock = pd.read_excel(EXCEL_PATH)
    print(f"✅ Excel cargado desde: {EXCEL_PATH}")
    print(f"✅ Filas leídas: {len(df_stock)}")
    if "Valorizado LISTA1" in df_stock.columns:
        total_valorizado = df_stock["Valorizado LISTA1"].sum()
        print(f"✅ Control total Valorizado LISTA1: {total_valorizado}")
except Exception as e:
    print("🔥 ERROR AL CARGAR EL EXCEL:", e)
    df_stock = None


class IAQuery(BaseModel):
    pregunta: str


@app.get("/")
def root():
    return {"status": "ok", "message": "Backend de stock con IA activo"}


@app.get("/health")
def health():
    if df_stock is None:
        raise HTTPException(status_code=500, detail="No se pudo cargar el Excel de stock")
    return {
        "status": "ok",
        "filas": len(df_stock),
        "columnas": list(df_stock.columns),
    }


@app.post("/api/ia-query")
async def ia_query(body: IAQuery):
    """
    Endpoint que recibe una pregunta y la pasa al motor de IA (DeepSeek).
    """
    if df_stock is None:
        raise HTTPException(status_code=500, detail="No hay datos de stock cargados")

    prompt = body.pregunta

    try:
        # Por ahora le pasamos solo la pregunta.
        # Si querés, en una versión siguiente le agregamos contexto del Excel.
        respuesta = ask_ai(prompt)
        return {"respuesta": respuesta}
    except Exception as e:
        print("🔥 ERROR EN IA_QUERY:", e)
        raise HTTPException(status_code=500, detail="Error interno del servidor")
