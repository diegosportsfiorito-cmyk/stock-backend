import os
import json
import pandas as pd
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from indexer import Indexer   # Usa el INDEXER v4.0 que te pasé
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ============================================================
# CONFIG FASTAPI
# ============================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Podés restringirlo si querés
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# MODELO DE REQUEST
# ============================================================
class QueryRequest(BaseModel):
    question: str
    solo_stock: bool = False

# ============================================================
# CARGA DESDE GOOGLE DRIVE
# ============================================================
def load_excel_from_drive():
    print(">>> INDEXER v4.0 CARGADO <<<")

    SERVICE_ACCOUNT_FILE = "service_account.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)

    # ID del archivo Excel en Drive
    FILE_ID = os.getenv("DRIVE_FILE_ID")

    print(f"📂 Cargando desde Drive: {FILE_ID}")

    request = service.files().get_media(fileId=FILE_ID)
    file = request.execute()

    with open("stock.xlsx", "wb") as f:
        f.write(file)

    df = pd.read_excel("stock.xlsx")

    print("Columnas normalizadas:", df.columns.tolist())

    # Info del archivo
    metadata = service.files().get(fileId=FILE_ID, fields="id, name, mimeType, modifiedTime").execute()
    print("Excel cargado. Fuente:", metadata)

    return df, metadata

# ============================================================
# CARGAR EXCEL AL INICIAR
# ============================================================
df, metadata = load_excel_from_drive()
indexer = Indexer(df)

# ============================================================
# ENDPOINT PRINCIPAL /query
# ============================================================
@app.post("/query")
async def query_stock(req: QueryRequest):
    try:
        question = req.question.strip()
        solo_stock = req.solo_stock

        print(f">>> Consulta recibida: {question}")

        result = indexer.query(question, solo_stock)

        # Agregar metadata de fuente
        result["fuente"] = metadata

        return result

    except Exception as e:
        print("ERROR en /query:", e)
        return {
            "tipo": "lista",
            "items": [],
            "voz": "Error procesando la consulta.",
            "fuente": metadata
        }

# ============================================================
# ENDPOINT /autocomplete
# ============================================================
@app.get("/autocomplete")
async def autocomplete(q: str):
    try:
        q = q.lower().strip()

        # Tomamos la columna "texto" del indexer
        textos = indexer.df["texto"].tolist()

        sugerencias = sorted(
            {t for t in textos if q in t}  # coincidencias parciales
        )

        # Limitar a 10 sugerencias
        sugerencias = sugerencias[:10]

        return {"sugerencias": sugerencias}

    except Exception as e:
        print("ERROR en /autocomplete:", e)
        return {"sugerencias": []}

# ============================================================
# ROOT
# ============================================================
@app.get("/")
async def root():
    return {"status": "OK", "message": "Backend Stock IA PRO v4.0 listo."}
