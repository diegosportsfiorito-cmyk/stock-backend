import os
import pandas as pd
import numpy as np

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from indexer import Indexer
from style_manager import load_style, save_style
from apply_style import apply_style

from googleapiclient.discovery import build
from google.oauth2 import service_account


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
# MODELOS
# ============================================================
class QueryRequest(BaseModel):
    question: str
    solo_stock: bool = False


class StyleRequest(BaseModel):
    style: str
    admin_key: str


# ============================================================
# CARGA DE EXCEL DESDE GOOGLE DRIVE (XLS + XLSX)
# ============================================================
def load_excel_from_drive():
    SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)

    FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

    # 🔥 Buscar XLSX y XLS
    query = (
        f"'{FOLDER_ID}' in parents and ("
        "mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
        "or mimeType='application/vnd.ms-excel'"
        ")"
    )

    results = service.files().list(
        q=query,
        fields="files(id, name, modifiedTime, mimeType)",
        orderBy="modifiedTime desc"
    ).execute()

    files = results.get("files", [])

    if not files:
        raise Exception("No se encontró ningún archivo Excel (.xls o .xlsx) en Google Drive.")

    newest = files[0]
    FILE_ID = newest["id"]

    request = service.files().get_media(fileId=FILE_ID)
    file = request.execute()

    # Guardar archivo temporal
    filename = "stock_file"
    if newest["mimeType"] == "application/vnd.ms-excel":
        filename += ".xls"
    else:
        filename += ".xlsx"

    with open(filename, "wb") as f:
        f.write(file)

    # 🔥 Leer XLS o XLSX automáticamente
    df = pd.read_excel(filename, engine="xlrd" if filename.endswith(".xls") else None)

    # LIMPIAR NaN / inf / None
    df = df.replace([np.inf, -np.inf, np.nan], 0)

    return df, newest


# ============================================================
# AUTOCOMPLETE
# ============================================================
@app.get("/autocomplete")
async def autocomplete(q: str = Query("")):
    if not q.strip():
        return {"suggestions": []}

    df, _ = load_excel_from_drive()
    indexer = Indexer(df)

    suggestions = indexer.autocomplete(q)
    suggestions = [str(s) for s in suggestions]

    return {"suggestions": suggestions}


# ============================================================
# QUERY — ACEPTA GET Y POST
# ============================================================
@app.get("/query")
@app.post("/query")
async def query_stock(
    q: str = None,
    question: str = None,
    solo_stock: bool = False,
    req: QueryRequest = None
):
    # Compatibilidad POST
    if req:
        question = req.question
        solo_stock = req.solo_stock

    # Compatibilidad GET (frontend usa q=)
    if question is None and q is not None:
        question = q

    if question is None:
        return {"error": "Falta parámetro 'q' o 'question'."}

    question = question.strip()

    df, metadata = load_excel_from_drive()
    indexer = Indexer(df)

    result = indexer.query(question, solo_stock)

    # LIMPIAR NaN / inf / None
    def clean(obj):
        if isinstance(obj, float):
            if np.isnan(obj) or np.isinf(obj):
                return 0
        if obj is None:
            return 0
        return obj

    if "items" in result:
        for item in result["items"]:
            for k, v in item.items():
                item[k] = clean(v)

    style = load_style()
    result = apply_style(style, result, question)

    result["fuente"] = metadata
    result["style"] = style

    return result


# ============================================================
# ROOT
# ============================================================
@app.get("/")
async def root():
    return {
        "status": "OK",
        "message": "Backend Stock IA PRO v5.0 listo (XLS + XLSX)."
    }
