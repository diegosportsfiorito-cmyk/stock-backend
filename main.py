# main.py
import os
from typing import List

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ============================================================
# CONFIG
# ============================================================

EXCEL_PATH = os.getenv("EXCEL_PATH", "data/stock.xlsx")

# Columnas reales de tu Excel
COL_CODIGO = "Artículo"
COL_DESC = "Descripción"
COL_TALLE = "Talle"
COL_STOCK = "Cantidad"
COL_PRECIO = "LISTA1"
COL_VALORIZADO = "Valorizado LISTA1"

# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(title="ORB STOCK Backend")

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


class TalleItem(BaseModel):
    talle: str
    stock: int


class ItemResponse(BaseModel):
    codigo: str
    descripcion: str
    marca: str
    rubro: str
    color: str
    precio: float
    valorizado: float
    talles: List[TalleItem]


class QueryResponse(BaseModel):
    items: List[ItemResponse]


# ============================================================
# CARGA DE EXCEL
# ============================================================

def load_excel() -> pd.DataFrame:
    if not os.path.exists(EXCEL_PATH):
        raise Exception(f"No se encontró el archivo Excel en: {EXCEL_PATH}")

    df = pd.read_excel(EXCEL_PATH)

    required = [
        COL_CODIGO,
        COL_DESC,
        COL_TALLE,
        COL_STOCK,
        COL_PRECIO,
        COL_VALORIZADO,
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise Exception(f"Faltan columnas en el Excel: {missing}")

    return df


# ============================================================
# LÓGICA DE BÚSQUEDA
# ============================================================

def procesar(df: pd.DataFrame, query: str, solo_stock: bool):
    q = query.strip().upper()

    df2 = df.copy()
    df2["__desc"] = df2[COL_DESC].astype(str).str.upper()
    df2["__cod"] = df2[COL_CODIGO].astype(str).str.upper()

    mask = df2["__desc"].str.contains(q, na=False) | df2["__cod"].str.contains(q, na=False)
    df2 = df2[mask]

    if solo_stock:
        df2 = df2[df2[COL_STOCK] > 0]

    if df2.empty:
        return []

    items = []

    for (codigo, descripcion), grupo in df2.groupby([COL_CODIGO, COL_DESC]):
        talles = [
            TalleItem(talle=str(row[COL_TALLE]), stock=int(row[COL_STOCK]))
            for _, row in grupo.iterrows()
        ]

        precio = float(grupo[COL_PRECIO].max())
        valorizado = float(grupo[COL_VALORIZADO].sum())

        item = ItemResponse(
            codigo=str(codigo),
            descripcion=str(descripcion),
            marca="",
            rubro="",
            color="",
            precio=precio,
            valorizado=valorizado,
            talles=talles,
        )

        items.append(item)

    return items


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
def root():
    return {"status": "ok", "message": "ORB STOCK backend activo"}


@app.post("/query", response_model=QueryResponse)
async def query_stock(req: QueryRequest):
    df = load_excel()
    items = procesar(df, req.question, req.solo_stock)
    return QueryResponse(items=items)
