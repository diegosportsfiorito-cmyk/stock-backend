# ===== INICIO BLOQUE MAIN =====

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from indexer import procesar_pregunta
import pandas as pd

# Cargar Excel una sola vez al iniciar
df = pd.read_excel("stock.xlsx")   # <-- Ajustá el nombre si es otro

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelo de entrada
class QueryRequest(BaseModel):
    question: str

# Endpoint principal
@app.post("/query")
async def query(req: QueryRequest):
    pregunta = req.question.strip()

    try:
        resultado = procesar_pregunta(df, pregunta)

        return resultado

    except Exception as e:
        print("ERROR EN /query:", e)
        return {
            "tipo": "mensaje",
            "mensaje": "Ocurrió un error procesando la consulta.",
            "voz": "Ocurrió un error procesando la consulta.",
            "error": str(e),
        }

# Endpoint de prueba
@app.get("/")
async def root():
    return {"status": "OK", "message": "Stock IA Backend funcionando."}

# ===== FIN BLOQUE MAIN =====
