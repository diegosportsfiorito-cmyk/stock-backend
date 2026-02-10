import io
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

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
    expose_headers=["*"],
    max_age=3600,
)

# ============================================================
# MODELOS
# ============================================================

class QueryRequest(BaseModel):
    question: str
    solo_stock: bool = False

    filtros_globales: bool = False
    marca: Optional[str] = None
    rubro: Optional[str] = None

    # camelCase
    talleDesde: Optional[int] = None
    talleHasta: Optional[int] = None

    # nuevos filtros
    soloUltimo: bool = False
    soloNegativo: bool = False


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
    archivos = listar_archivos_en_carpeta("1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-")

    excel_files = [f for f in archivos if f["name"].lower().endswith(".xlsx")]
    excel_files.sort(key=lambda x: x["modifiedTime"], reverse=True)

    file_id = excel_files[0]["id"]
    contenido = descargar_archivo_por_id(file_id)
    buffer = io.BytesIO(contenido)

    df = pd.read_excel(buffer)

    df.columns = [
        "Marca",
        "Rubro",
        "Artículo",
        "Descripción",
        "Color",
        "Talle",
        "Cantidad",
        "LISTA1",
        "Valorizado LISTA1"
    ]

    return df


# ============================================================
# FILTROS GLOBALES
# ============================================================

def aplicar_filtros_globales(df: pd.DataFrame, req: QueryRequest) -> pd.DataFrame:
    df2 = df.copy()

    if not req.filtros_globales:
        return df2

    if req.marca:
        df2 = df2[df2["Marca"].astype(str) == str(req.marca)]

    if req.rubro:
        df2 = df2[df2["Rubro"].astype(str) == str(req.rubro)]

    if req.talleDesde is not None or req.talleHasta is not None:
        df2["__talle_num"] = pd.to_numeric(df2["Talle"], errors="coerce")

        if req.talleDesde is not None:
            df2 = df2[df2["__talle_num"] >= req.talleDesde]

        if req.talleHasta is not None:
            df2 = df2[df2["__talle_num"] <= req.talleHasta]

    return df2


# ============================================================
# PROCESAMIENTO PRINCIPAL
# ============================================================

def procesar(df: pd.DataFrame, req: QueryRequest):

    df2 = aplicar_filtros_globales(df, req)

    if df2.empty:
        return []

    q = req.question.strip().upper()

    # Si hay filtros especiales, ignoramos question
    if req.soloUltimo or req.soloNegativo:
        q = ""

    df2["__desc"] = df2["Descripción"].astype(str).str.upper()
    df2["__cod"] = df2["Artículo"].astype(str).str.upper()

    # SOLO FILTROS (sin question)
    if not q:
        if req.solo_stock:
            df2 = df2[df2["Cantidad"] > 0]

        if df2.empty:
            return []

    else:
        exact_code = df2[df2["__cod"] == q]
        if not exact_code.empty:
            df2 = exact_code
        else:
            mask_exact_word = df2["__desc"].str.split().apply(lambda words: q in words)
            mask_prefix = df2["__desc"].str.contains(rf"\b{q}", regex=True, na=False)
            mask_partial = df2["__desc"].str.contains(q, na=False)

            df2 = df2[mask_exact_word | mask_prefix | mask_partial]

            if df2.empty:
                return []

            df2["__score"] = (
                mask_exact_word.astype(int) * 3 +
                mask_prefix.astype(int) * 2 +
                mask_partial.astype(int) * 1
            )

            df2 = df2.sort_values("__score", ascending=False)

        if req.solo_stock:
            df2 = df2[df2["Cantidad"] > 0]

        if df2.empty:
            return []

    # ============================================================
    # FILTROS ESPECIALES: ULTIMO / NEGATIVO
    # ============================================================

    items = []

    for (codigo, descripcion), grupo in df2.groupby(["Artículo", "Descripción"]):

        total_stock = int(grupo["Cantidad"].sum())

        if req.soloUltimo and total_stock != 1:
            continue

        if req.soloNegativo and total_stock >= 0:
            continue

        talles = [
            TalleItem(talle=str(row["Talle"]), stock=int(row["Cantidad"]))
            for _, row in grupo.iterrows()
        ]

        precio = float(grupo["LISTA1"].max())
        valorizado = float(grupo["Valorizado LISTA1"].sum())

        marca = str(grupo["Marca"].iloc[0])
        rubro = str(grupo["Rubro"].iloc[0])
        color = str(grupo["Color"].iloc[0])

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

    # 🔥 LOG AGREGADO — imprime exactamente lo que envía el frontend
    print("=== REQUEST RECIBIDO ===")
    print(req.dict())
    print("========================")

    df = load_excel_from_drive()
    items = procesar(df, req)
    return QueryResponse(items=items)
