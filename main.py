import os
import pandas as pd
import numpy as np

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from indexer import Indexer
from style_manager import load_style
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


# ============================================================
# CARGA DE EXCEL DESDE GOOGLE DRIVE
# ============================================================
def load_excel_from_drive():
    SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)

    FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

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
        raise Exception("No se encontró ningún archivo Excel en Google Drive.")

    newest = files[0]
    FILE_ID = newest["id"]

    request = service.files().get_media(fileId=FILE_ID)
    file = request.execute()

    filename = "stock_file.xlsx"
    with open(filename, "wb") as f:
        f.write(file)

    df = pd.read_excel(filename)

    # Normalizar valores europeos: "22.000,00" → 22000.00
    def normalize_number(x):
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".")
        try:
            return float(x)
        except:
            return 0

    df["LISTA"] = df["LISTA"].apply(normalize_number)
    df["Valorizado LISTA"] = df["Valorizado LISTA"].apply(normalize_number)
    df["Cantid"] = df["Cantid"].apply(normalize_number)

    df = df.replace([np.inf, -np.inf, np.nan], 0)

    return df, newest


# ============================================================
# AGRUPACIÓN REAL (UNA FILA POR TALLE)
# ============================================================
def group_rows(df):
    grouped = {}

    for _, row in df.iterrows():
        codigo = str(row.get("Artículo", "")).strip()
        if not codigo:
            continue

        if codigo not in grouped:
            grouped[codigo] = {
                "codigo": codigo,
                "descripcion": str(row.get("Descripción", "")).strip(),
                "marca": str(row.get("Marca", "")).strip(),
                "rubro": str(row.get("Rubro", "")).strip(),
                "color": str(row.get("Color", "")).strip(),
                "precio": float(row.get("LISTA", 0)),
                "talles": [],
                "valorizado": 0,
            }

        talle = str(row.get("Talle", "")).strip()
        stock = float(row.get("Cantid", 0))
        valorizado = float(row.get("Valorizado LISTA", 0))

        grouped[codigo]["talles"].append({"talle": talle, "stock": stock})
        grouped[codigo]["valorizado"] += valorizado

    return list(grouped.values())


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
# QUERY — GET y POST
# ============================================================
@app.get("/query")
@app.post("/query")
async def query_stock(
    q: str = None,
    question: str = None,
    solo_stock: bool = False,
    req: QueryRequest = None
):
    if req:
        question = req.question
        solo_stock = req.solo_stock

    if question is None and q is not None:
        question = q

    if question is None:
        return {"error": "Falta parámetro 'q' o 'question'."}

    question = question.strip().upper()

    df, metadata = load_excel_from_drive()
    indexer = Indexer(df)

    # ============================================================
    # 1) COINCIDENCIA EXACTA POR ARTÍCULO
    # ============================================================
    exact_rows = df[df["Artículo"].astype(str).str.upper() == question]

    if len(exact_rows) > 0:
        items = group_rows(exact_rows)
        result = {"tipo": "lista", "items": items}

    else:
        # ============================================================
        # 2) SI NO HAY EXACTA → FUZZY SEARCH
        # ============================================================
        fuzzy = indexer.query(question, solo_stock)

        if "items" in fuzzy:
            fuzzy_df = pd.DataFrame(fuzzy["items"])
            items = group_rows(fuzzy_df)
            fuzzy["items"] = items

        result = fuzzy

    # ============================================================
    # LIMPIEZA
    # ============================================================
    def clean(obj):
        if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
            return 0
        return obj if obj is not None else 0

    if "items" in result:
        for item in result["items"]:
            item["precio"] = clean(item.get("precio"))
            item["valorizado"] = clean(item.get("valorizado"))
            for t in item["talles"]:
                t["stock"] = clean(t.get("stock"))

    # Estilo
    style = load_style()
    result = apply_style(style, result, question)

    result["fuente"] = metadata
    result["style"] = style

    return result


# ============================================================
# CATALOGO COMPLETO
# ============================================================
@app.get("/catalogos")
async def get_catalogos():
    df, _ = load_excel_from_drive()

    marcas = sorted(set(df["Marca"].astype(str).str.strip()))
    rubros = sorted(set(df["Rubro"].astype(str).str.strip()))
    talles = sorted(set(df["Talle"].astype(str).str.strip()))

    return {
        "marcas": marcas,
        "rubros": rubros,
        "talles": talles
    }


# ============================================================
# ROOT
# ============================================================
@app.get("/")
async def root():
    return {
        "status": "OK",
        "message": "Backend Stock IA PRO adaptado a Excel real (una fila por talle, LISTA y Valorizado normalizados)."
    }
