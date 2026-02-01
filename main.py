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
# NORMALIZACIÓN DE COLUMNAS
# ============================================================
def normalize_columns(df):
    def clean(col):
        col = str(col)
        col = col.replace("\xa0", " ")
        col = col.replace("\t", " ")
        col = col.replace("\r", "")
        col = col.replace("\n", "")
        col = col.strip()
        col = col.upper()
        col = col.replace("  ", " ")
        return col

    df.columns = [clean(c) for c in df.columns]
    return df


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

    df = normalize_columns(df)

    # Detectar columnas reales
    col_precio = "PRECIO PUBLICO"
    col_valorizado = "PRECIO VALORIZADO"
    col_stock = "CANTIDAD"

    if col_precio not in df.columns:
        raise Exception(f"No existe columna '{col_precio}' en el Excel.")

    if col_valorizado not in df.columns:
        raise Exception(f"No existe columna '{col_valorizado}' en el Excel.")

    if col_stock not in df.columns:
        raise Exception(f"No existe columna '{col_stock}' en el Excel.")

    # Normalizar números europeos
    def normalize_number(x):
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".")
        try:
            return float(x)
        except:
            return 0

    df[col_precio] = df[col_precio].apply(normalize_number)
    df[col_valorizado] = df[col_valorizado].apply(normalize_number)
    df[col_stock] = df[col_stock].apply(normalize_number)

    df = df.replace([np.inf, -np.inf, np.nan], 0)

    return df, newest, col_precio, col_valorizado, col_stock


# ============================================================
# AGRUPACIÓN REAL (UNA FILA POR TALLE)
# ============================================================
def group_rows(df, col_precio, col_valorizado, col_stock):
    grouped = {}

    for _, row in df.iterrows():
        codigo = str(row.get("ARTICULO", "")).strip()
        if not codigo:
            continue

        if codigo not in grouped:
            grouped[codigo] = {
                "codigo": codigo,
                "descripcion": str(row.get("DESCRIPCION", "")).strip(),
                "marca": str(row.get("MARCA", "")).strip(),
                "rubro": str(row.get("RUBRO", "")).strip(),
                "color": str(row.get("COLOR", "")).strip(),
                "precio": float(row.get(col_precio, 0)),
                "talles": [],
                "valorizado": 0,
            }

        talle = str(row.get("TALLE", "")).strip()
        stock = float(row.get(col_stock, 0))
        valorizado = float(row.get(col_valorizado, 0))

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

    df, _, _, _, _ = load_excel_from_drive()
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

    df, metadata, col_precio, col_valorizado, col_stock = load_excel_from_drive()
    indexer = Indexer(df)

    # EXACT MATCH
    exact_rows = df[df["ARTICULO"].astype(str).str.upper() == question]

    if len(exact_rows) > 0:
        items = group_rows(exact_rows, col_precio, col_valorizado, col_stock)
        result = {"tipo": "lista", "items": items}

    else:
        fuzzy = indexer.query(question, solo_stock)

        if "items" in fuzzy:
            fuzzy_df = pd.DataFrame(fuzzy["items"])
            items = group_rows(fuzzy_df, col_precio, col_valorizado, col_stock)
            fuzzy["items"] = items

        result = fuzzy

    # LIMPIEZA
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
    df, _, _, _, _ = load_excel_from_drive()

    marcas = sorted(set(df["MARCA"].astype(str).str.strip()))
    rubros = sorted(set(df["RUBRO"].astype(str).str.strip()))
    talles = sorted(set(df["TALLE"].astype(str).str.strip()))

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
        "message": "Backend Stock IA PRO adaptado a columnas reales."
    }
