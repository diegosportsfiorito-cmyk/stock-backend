# main.py
import io
from typing import List, Optional

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

# Opcionales (si existen en el Excel)
COL_MARCA = "Marca"
COL_RUBRO = "Rubro"
COL_COLOR = "Color"

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

    # NUEVOS CAMPOS PARA FILTROS GLOBALES
    filtros_globales: bool = False
    marca: Optional[str] = None
    rubro: Optional[str] = None
    talle_desde: Optional[int] = None
    talle_hasta: Optional[int] = None


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
# LÓGICA DE BÚSQUEDA INTELIGENTE + FILTROS GLOBALES
# ============================================================

def aplicar_filtros_globales(df: pd.DataFrame, req: QueryRequest) -> pd.DataFrame:
    """
    Aplica filtros sobre el TOTAL del Excel:
    - marca
    - rubro
    - rango de talles
    sin tocar todavía la lógica de búsqueda por texto.
    """
    df2 = df.copy()

    if not req.filtros_globales:
        return df2

    # Marca
    if req.marca and COL_MARCA in df2.columns:
        df2 = df2[df2[COL_MARCA].astype(str) == str(req.marca)]

    # Rubro
    if req.rubro and COL_RUBRO in df2.columns:
        df2 = df2[df2[COL_RUBRO].astype(str) == str(req.rubro)]

    # Rango de talles
    if (req.talle_desde is not None or req.talle_hasta is not None) and COL_TALLE in df2.columns:
        # Intentamos convertir a numérico para poder comparar rangos
        df2["__talle_num"] = pd.to_numeric(df2[COL_TALLE], errors="coerce")

        if req.talle_desde is not None:
            df2 = df2[df2["__talle_num"] >= req.talle_desde]

        if req.talle_hasta is not None:
            df2 = df2[df2["__talle_num"] <= req.talle_hasta]

    return df2


def procesar(df: pd.DataFrame, req: QueryRequest):
    """
    Mantiene tu lógica original de búsqueda,
    pero ahora recibe el objeto completo `req` para usar filtros globales
    y soporta consultas solo por filtros (question vacío).
    """
    # 1) Aplicar filtros globales sobre el TOTAL del Excel
    df2 = aplicar_filtros_globales(df, req)

    # Si después de filtros globales no queda nada, devolvemos vacío
    if df2.empty:
        return []

    q = req.question.strip().upper()

    # Campos auxiliares para búsqueda
    df2["__desc"] = df2[COL_DESC].astype(str).str.upper()
    df2["__cod"] = df2[COL_CODIGO].astype(str).str.upper()

    # ============================================================
    # CASO 1: SOLO FILTROS (question vacío)
    # ============================================================
    if not q:
        if req.solo_stock:
            df2 = df2[df2[COL_STOCK] > 0]
        if df2.empty:
            return []
    else:
        # ============================================================
        # CASO 2: BÚSQUEDA POR TEXTO + FILTROS
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

            # Filtrar por alguna coincidencia
            df2 = df2[mask_exact_word | mask_prefix | mask_partial]

            if df2.empty:
                return []

            # Ordenar por relevancia
            df2["__score"] = (
                mask_exact_word.astype(int) * 3 +
                mask_prefix.astype(int) * 2 +
                mask_partial.astype(int) * 1
            )
            df2 = df2.sort_values("__score", ascending=False)

        # Filtro solo stock
        if req.solo_stock:
            df2 = df2[df2[COL_STOCK] > 0]

        if df2.empty:
            return []

    items = []

    # Agrupamos por código + descripción (como antes)
    for (codigo, descripcion), grupo in df2.groupby([COL_CODIGO, COL_DESC]):
        talles = [
            TalleItem(talle=str(row[COL_TALLE]), stock=int(row[COL_STOCK]))
            for _, row in grupo.iterrows()
        ]

        precio = float(grupo[COL_PRECIO].max())
        valorizado = float(grupo[COL_VALORIZADO].sum())

        # Intentamos leer marca/rubro/color si existen en el Excel
        if COL_MARCA in grupo.columns:
            marca = str(grupo[COL_MARCA].iloc[0])
        else:
            marca = ""

        if COL_RUBRO in grupo.columns:
            rubro = str(grupo[COL_RUBRO].iloc[0])
        else:
            rubro = ""

        if COL_COLOR in grupo.columns:
            color = str(grupo[COL_COLOR].iloc[0])
        else:
            color = ""

        items.append(
            ItemResponse(
                codigo=str(codigo),
                descripcion=str(descripcion),
                marca=marca,
                rubro=rubro,
                color=color,
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
    items = procesar(df, req)
    return QueryResponse(items=items)
