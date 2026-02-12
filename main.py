import io
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
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
# ENDPOINT: PING (OBLIGATORIO PARA EL FRONTEND)
# ============================================================

@app.get("/ping")
async def ping():
    return {"status": "ok"}

# ============================================================
# MODELOS
# ============================================================

class QueryRequest(BaseModel):
    question: str
    solo_stock: bool = False

    filtros_globales: bool = False
    marca: Optional[str] = None
    rubro: Optional[str] = None

    talleDesde: Optional[int] = None
    talleHasta: Optional[int] = None

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
# CACHE GLOBAL
# ============================================================

df_global: Optional[pd.DataFrame] = None
last_file_id: Optional[str] = None


# ============================================================
# CARGA INTELIGENTE DESDE GOOGLE DRIVE
# ============================================================

def load_excel_smart() -> pd.DataFrame:
    global df_global, last_file_id

    try:
        archivos = listar_archivos_en_carpeta("1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-")
        excel_files = [f for f in archivos if f.get("name", "").lower().endswith(".xlsx")]
        excel_files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)

        if not excel_files:
            if df_global is not None:
                print(">>> WARNING: No se encontraron .xlsx en Drive, usando cache en memoria")
                return df_global
            raise RuntimeError("No se encontraron archivos .xlsx en la carpeta de Drive")

        newest = excel_files[0]

        if last_file_id == newest.get("id") and df_global is not None:
            return df_global

        file_id = newest.get("id")
        if not file_id:
            raise RuntimeError("El archivo más reciente no tiene ID válido")

        contenido = descargar_archivo_por_id(file_id)
        buffer = io.BytesIO(contenido)

        df = pd.read_excel(buffer)

        expected_cols = [
            "Marca",
            "Rubro",
            "Artículo",
            "Descripción",
            "Color",
            "Talle",
            "Cantidad",
            "LISTA1",
            "Valorizado LISTA1",
        ]

        if len(df.columns) < len(expected_cols):
            raise RuntimeError(
                f"El Excel no tiene la cantidad esperada de columnas. "
                f"Esperadas: {len(expected_cols)}, encontradas: {len(df.columns)}"
            )

        df = df.iloc[:, : len(expected_cols)]
        df.columns = expected_cols

        df_global = df
        last_file_id = file_id

        print(">>> Excel actualizado desde Google Drive:", newest.get("name"))
        return df_global

    except Exception as e:
        print(">>> ERROR en load_excel_smart:", repr(e))
        if df_global is not None:
            print(">>> Usando df_global en cache como fallback")
            return df_global
        raise


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

def procesar(df: pd.DataFrame, req: QueryRequest) -> List[ItemResponse]:
    df2 = aplicar_filtros_globales(df, req)

    if df2.empty:
        return []

    q = (req.question or "").strip().upper()

    if req.soloUltimo or req.soloNegativo:
        q = ""

    df2["__desc"] = df2["Descripción"].astype(str).str.upper()
    df2["__cod"] = df2["Artículo"].astype(str).str.upper()

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
            desc_series = df2["__desc"].fillna("")

            mask_exact_word = desc_series.str.split().apply(lambda words: q in words)
            mask_prefix = desc_series.str.contains(rf"\b{q}", regex=True, na=False)
            mask_partial = desc_series.str.contains(q, na=False)

            df2 = df2[mask_exact_word | mask_prefix | mask_partial]

            if df2.empty:
                return []

            df2["__score"] = (
                mask_exact_word.astype(int) * 3
                + mask_prefix.astype(int) * 2
                + mask_partial.astype(int) * 1
            )

            df2 = df2.sort_values("__score", ascending=False)

        if req.solo_stock:
            df2 = df2[df2["Cantidad"] > 0]

        if df2.empty:
            return []

    items: List[ItemResponse] = []

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

        precio_raw = grupo["LISTA1"].max()
        valorizado_raw = grupo["Valorizado LISTA1"].sum()

        try:
            precio = float(precio_raw) if pd.notna(precio_raw) else 0.0
        except:
            precio = 0.0

        try:
            valorizado = float(valorizado_raw) if pd.notna(valorizado_raw) else 0.0
        except:
            valorizado = 0.0

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
# ENDPOINT: CATALOGO
# ============================================================

@app.get("/catalog")
async def get_catalog():
    try:
        df = load_excel_smart()

        marcas = sorted(set(df["Marca"].astype(str)))
        rubros = sorted(set(df["Rubro"].astype(str)))

        resumen = {
            "archivo": "Google Drive",
            "fecha": "Automático",
            "marcas": len(marcas),
            "rubros": len(rubros),
            "articulos": int(len(df)),
            "stock_total": int(df["Cantidad"].sum()),
            "stock_negativo": int((df["Cantidad"] < 0).sum()),
        }

        items = []
        for _, row in df.iterrows():
            try:
                items.append(
                    {
                        "marca": str(row["Marca"]),
                        "rubro": str(row["Rubro"]),
                        "codigo": str(row["Artículo"]),
                        "descripcion": str(row["Descripción"]),
                        "color": str(row["Color"]),
                        "talle": str(row["Talle"]),
                        "stock": int(row["Cantidad"]),
                        "precio": float(row["LISTA1"]) if pd.notna(row["LISTA1"]) else 0.0,
                        "valorizado": float(row["Valorizado LISTA1"])
                        if pd.notna(row["Valorizado LISTA1"])
                        else 0.0,
                    }
                )
            except Exception as e:
                print(">>> WARNING: fila inválida en /catalog:", repr(e))

        return {"items": items, "resumen": resumen}

    except Exception as e:
        print(">>> ERROR en /catalog:", repr(e))
        raise HTTPException(status_code=500, detail="Error al cargar catálogo")


# ============================================================
# ENDPOINT: QUERY PRINCIPAL
# ============================================================

@app.post("/query", response_model=QueryResponse)
async def query_stock(req: QueryRequest):
    try:
        print("=== REQUEST RECIBIDO ===")
        print(req.dict())
        print("========================")

        df = load_excel_smart()
        items = procesar(df, req)
        return QueryResponse(items=items)

    except Exception as e:
        print(">>> ERROR en /query:", repr(e))
        raise HTTPException(status_code=500, detail="Error al procesar la consulta")
