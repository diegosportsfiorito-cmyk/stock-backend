# main.py
import os
import io
from typing import List

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

# ============================================================
# CONFIG
# ============================================================

DRIVE_FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

COL_CODIGO = "Artículo"
COL_DESC = "Descripción"
COL_TALLE = "Talle"
COL_STOCK = "Cantidad"
COL_PRECIO = "LISTA1"
COL_VALORIZADO = "Valorizado LISTA1"

# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(title="STOCK IA PRO Backend")

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
# CARGA DE EXCEL DESDE GOOGLE DRIVE
# ============================================================

def load_excel_from_drive() -> pd.DataFrame:
    archivos = listar_archivos_en_carpeta(DRIVE_FOLDER_ID)

    excel_files = [f for f in archivos if f["name"].lower().endswith(".xlsx")]
    excel_files.sort(key=lambda x: x["modifiedTime"], reverse=True)

    file_id = excel_files[0]["id"]
    contenido = descargar_archivo_por_id(file_id)
    buffer = io.BytesIO(contenido)

    df = pd.read_excel(buffer)

    required = [
        COL_CODIGO, COL_DESC, COL_TALLE,
        COL_STOCK, COL_PRECIO, COL_VALORIZADO
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise Exception(f"Faltan columnas en el Excel: {missing}")

    return df


# ============================================================
# LÓGICA DE BÚSQUEDA INTELIGENTE
# ============================================================

def procesar(df: pd.DataFrame, query: str, solo_stock: bool):
    q = query.strip().upper()

    df2 = df.copy()
    df2["__desc"] = df2[COL_DESC].astype(str).str.upper()
    df2["__cod"] = df2[COL_CODIGO].astype(str).str.upper()

    # ============================================================
    # 1) Coincidencia EXACTA en ARTÍCULO (código)
    # ============================================================
    exact_code = df2[df2["__cod"] == q]
    if not exact_code.empty:
        df2 = exact_code
    else:
        # ============================================================
        # 2) Coincidencia en DESCRIPCIÓN por relevancia
        # ============================================================

        # Exacta de palabra
        mask_exact_word = df2["__desc"].str.split().apply(lambda words: q in words)

        # Prefijo
        mask_prefix = df2["__desc"].str.contains(rf"\b{q}", regex=True, na=False)

        # Parcial
        mask_partial = df2["__desc"].str.contains(q, na=False)

        # Orden de prioridad
        df2 = df2[mask_exact_word | mask_prefix | mask_partial]

        # Ordenar por relevancia
        df2["__score"] = (
            mask_exact_word.astype(int) * 3 +
            mask_prefix.astype(int) * 2 +
            mask_partial.astype(int) * 1
        )
        df2 = df2.sort_values("__score", ascending=False)

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

        items.append(
            ItemResponse(
                codigo=str(codigo),
                descripcion=str(descripcion),
                marca="",
                rubro="",
                color="",
                precio=precio,
                valorizado=valorizado,
                talles=talles,
            )
        )

    return items


# ============================================================
# ENDPOINT
# ============================================================

@app.post("/query", response_model=QueryResponse)
async def query_stock(req: QueryRequest):
    df = load_excel_from_drive()
    items = procesar(df, req.question, req.solo_stock)
    return QueryResponse(items=items)
