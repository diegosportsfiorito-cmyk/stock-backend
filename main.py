import io
import json
import datetime
from typing import List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import jwt

from drive_service import listar_archivos_en_carpeta, descargar_archivo_por_id

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
# MIDDLEWARE JWT
# ============================================================

SECRET_KEY = "CAMBIAR_ESTA_CLAVE_POR_UNA_SEGURA"

@app.middleware("http")
async def verificar_token(request: Request, call_next):
    if request.url.path in ["/", "/ping", "/login"]:
        return await call_next(request)

    auth = request.headers.get("Authorization")
    if not auth:
        raise HTTPException(status_code=401, detail="Token requerido")

    token = auth.replace("Bearer ", "")

    try:
        request.state.user = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido o expirado")

    return await call_next(request)

# ============================================================
# ROOT / HEALTHCHECK
# ============================================================

@app.get("/")
async def root():
    return {"status": "ok", "service": "stock-backend"}

@app.get("/ping")
async def ping():
    return {"status": "ok"}

# ============================================================
# MODELOS
# ============================================================

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
last_file_name: Optional[str] = None

# ============================================================
# CARGA INTELIGENTE DESDE GOOGLE DRIVE (CON FIX DE COLUMNAS)
# ============================================================

def load_excel_smart() -> pd.DataFrame:
    global df_global, last_file_id, last_file_name

    try:
        folder_id = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"
        archivos = listar_archivos_en_carpeta(folder_id)

        excel_files = [
            f for f in archivos
            if f.get("name", "").lower().endswith(".xlsx")
        ]

        excel_files.sort(key=lambda x: x.get("modifiedTime", ""), reverse=True)

        if not excel_files:
            if df_global is not None:
                return df_global
            raise RuntimeError("No se encontraron archivos .xlsx")

        newest = excel_files[0]
        file_id = newest.get("id")
        last_file_name = newest.get("name")

        if last_file_id == file_id and df_global is not None:
            return df_global

        contenido = descargar_archivo_por_id(file_id)
        buffer = io.BytesIO(contenido)

        df = pd.read_excel(buffer)

        # ============================================================
        # FIX CRÍTICO: FORZAR 9 COLUMNAS EXACTAS
        # ============================================================
        df = df.iloc[:, :9]
        df.columns = [
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

        df_global = df
        last_file_id = file_id
        return df_global

    except Exception:
        if df_global is not None:
            return df_global
        raise
# ============================================================
# LOGIN
# ============================================================

def cargar_usuarios() -> list:
    folder_id = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"
    archivos = listar_archivos_en_carpeta(folder_id)

    json_files = [
        f for f in archivos
        if f.get("name", "").lower() == "usuarios.json"
    ]

    if not json_files:
        raise RuntimeError("No se encontró usuarios.json")

    file_id = json_files[0]["id"]
    contenido = descargar_archivo_por_id(file_id)
    return json.loads(contenido.decode("utf-8"))

@app.post("/login")
async def login(request: Request):
    data = await request.json()
    username = data.get("username")
    password = data.get("password")

    usuarios = cargar_usuarios()

    for u in usuarios:
        if u["username"] == username and u["password"] == password:
            payload = {
                "username": username,
                "role": u["role"],
                "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=12)
            }
            token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
            return {"token": token, "role": u["role"]}

    raise HTTPException(status_code=401, detail="Credenciales inválidas")

# ============================================================
# FILTROS
# ============================================================

def aplicar_filtros_globales(df: pd.DataFrame, filtros: dict) -> pd.DataFrame:
    df2 = df.copy()

    if filtros.get("marca"):
        df2 = df2[df2["Marca"] == filtros["marca"]]

    if filtros.get("rubro"):
        df2 = df2[df2["Rubro"] == filtros["rubro"]]

    if filtros.get("talleDesde") is not None or filtros.get("talleHasta") is not None:
        df2["__talle_num"] = pd.to_numeric(df2["Talle"], errors="coerce")

        if filtros.get("talleDesde") is not None:
            df2 = df2[df2["__talle_num"] >= filtros["talleDesde"]]

        if filtros.get("talleHasta") is not None:
            df2 = df2[df2["__talle_num"] <= filtros["talleHasta"]]

    return df2

# ============================================================
# PROCESAMIENTO PRINCIPAL (CORREGIDO)
# ============================================================

def procesar(df: pd.DataFrame, filtros: dict) -> List[ItemResponse]:
    df2 = aplicar_filtros_globales(df, filtros)

    if df2.empty:
        return []

    question = (filtros.get("question") or "").strip().upper()

    df2["__desc"] = df2["Descripción"].astype(str).str.upper()
    df2["__cod"] = df2["Artículo"].astype(str).str.upper()

    if question:
        exact = df2[df2["__cod"] == question]
        if not exact.empty:
            df2 = exact
        else:
            mask = df2["__desc"].str.contains(question, na=False)
            df2 = df2[mask]

    items = []

    for (codigo, descripcion), grupo in df2.groupby(["Artículo", "Descripción"]):
        cantidades = pd.to_numeric(grupo["Cantidad"], errors="coerce").fillna(0).astype(int)
        precios = pd.to_numeric(grupo["LISTA1"], errors="coerce").fillna(0).astype(float)

        talles = [
            TalleItem(talle=str(t), stock=int(s))
            for t, s in zip(grupo["Talle"], cantidades)
        ]

        valorizado = float((cantidades * precios).sum())
        precio_ref = float(precios.iloc[0]) if len(set(precios.tolist())) == 1 else 0.0

        items.append(
            ItemResponse(
                codigo=str(codigo),
                descripcion=str(descripcion),
                marca=str(grupo["Marca"].iloc[0]),
                rubro=str(grupo["Rubro"].iloc[0]),
                color=str(grupo["Color"].iloc[0]),
                precio=precio_ref,
                valorizado=valorizado,
                talles=talles,
            )
        )

    return items
# ============================================================
# ENDPOINT: CATALOGO (ROBUSTO)
# ============================================================

@app.get("/catalog")
async def get_catalog(request: Request):
    role = request.state.user["role"]

    df = load_excel_smart()

    resumen = {
        "archivo": last_file_name or "No informado",
        "fecha": "Automático",
        "marcas": df["Marca"].nunique(),
        "rubros": df["Rubro"].nunique(),
        "articulos": len(df),
        "stock_total": int(df["Cantidad"].sum()),
        "stock_negativo": int((df["Cantidad"] < 0).sum()),
    }

    items = []
    for _, row in df.iterrows():
        try:
            marca = str(row.get("Marca", "") or "")
            rubro = str(row.get("Rubro", "") or "")
            codigo = str(row.get("Artículo", "") or "")
            descripcion = str(row.get("Descripción", "") or "")
            color = str(row.get("Color", "") or "")
            talle = str(row.get("Talle", "") or "")

            stock = int(pd.to_numeric(row.get("Cantidad", 0), errors="coerce") or 0)
            precio = float(pd.to_numeric(row.get("LISTA1", 0), errors="coerce") or 0)
            valorizado = float(pd.to_numeric(row.get("Valorizado LISTA1", 0), errors="coerce") or 0)

            if role != "admin":
                valorizado = 0.0

            items.append({
                "marca": marca,
                "rubro": rubro,
                "codigo": codigo,
                "descripcion": descripcion,
                "color": color,
                "talle": talle,
                "stock": stock,
                "precio": precio,
                "valorizado": valorizado,
            })

        except Exception as e:
            print(">>> WARNING: fila inválida en /catalog:", repr(e))
            continue

    return {"items": items, "resumen": resumen}

# ============================================================
# ENDPOINT: QUERY
# ============================================================

@app.post("/query", response_model=QueryResponse)
async def query_stock(request: Request):
    role = request.state.user["role"]
    raw = await request.json()

    filtros = {
        "question": raw.get("question"),
        "marca": raw.get("marca"),
        "rubro": raw.get("rubro"),
        "talleDesde": raw.get("talleDesde"),
        "talleHasta": raw.get("talleHasta"),
    }

    df = load_excel_smart()
    items = procesar(df, filtros)

    if role != "admin":
        for item in items:
            item.valorizado = 0.0

    return QueryResponse(items=items)
