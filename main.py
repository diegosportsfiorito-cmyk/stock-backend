import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from indexer import Indexer   # ✔ Import correcto del INDEXER v4.0

from googleapiclient.discovery import build
from google.oauth2 import service_account

# ============================================================
# FASTAPI CONFIG
# ============================================================
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],        # Podés restringirlo si querés
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
# CARGA DESDE GOOGLE DRIVE (ARCHIVO MÁS RECIENTE)
# ============================================================
def load_excel_from_drive():
    print(">>> INDEXER v4.0 CARGADO <<<")

    SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)

    # ⭐ ID de la carpeta donde están los Excel
    FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

    print(f"📁 Buscando archivos en carpeta: {FOLDER_ID}")

    # ⭐ Listar archivos dentro de la carpeta
    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc"
    ).execute()

    files = results.get("files", [])

    if not files:
        raise Exception("No se encontraron archivos Excel en la carpeta.")

    # ⭐ Tomar el archivo más reciente
    newest = files[0]
    FILE_ID = newest["id"]

    print(f"📂 Archivo más reciente: {newest['name']} ({FILE_ID})")

    # ⭐ Descargar el archivo
    request = service.files().get_media(fileId=FILE_ID)
    file = request.execute()

    with open("stock.xlsx", "wb") as f:
        f.write(file)

    df = pd.read_excel("stock.xlsx")

    print("Columnas normalizadas:", df.columns.tolist())

    metadata = newest

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

        textos = indexer.df["texto"].tolist()

        sugerencias = sorted({t for t in textos if q in t})
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
    return {
        "status": "OK",
        "message": "Backend Stock IA PRO v4.0 listo."
    }
