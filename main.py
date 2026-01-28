import io
import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from indexer import procesar_pregunta, autocompletar

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
# CARGA UNIVERSAL DE EXCEL (XLS + XLSX) + NORMALIZACIÓN
# ============================================================

def cargar_excel_mas_reciente():
    carpeta = "data"

    # Crear carpeta si no existe (Render no la crea)
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)
        print("⚠️ Carpeta /data creada automáticamente. No había archivos Excel.")
    
    # Buscar archivos Excel
    archivos = [f for f in os.listdir(carpeta) if f.lower().endswith((".xls", ".xlsx"))]

    if not archivos:
        print("⚠️ No hay archivos Excel en /data. El backend arrancará igual con DF vacío.")
        return pd.DataFrame(), "SIN_ARCHIVO"

    # Ordenar por fecha de modificación
    archivos.sort(key=lambda x: os.path.getmtime(os.path.join(carpeta, x)), reverse=True)
    archivo = archivos[0]
    ruta = os.path.join(carpeta, archivo)

    with open(ruta, "rb") as f:
        contenido = f.read()

    nombre = archivo.lower()

    # XLSX → openpyxl
    if nombre.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(contenido), engine="openpyxl")

    # XLS → xlrd
    elif nombre.endswith(".xls"):
        try:
            df = pd.read_excel(io.BytesIO(contenido), engine="xlrd")
        except ImportError:
            raise Exception(
                "Falta xlrd para leer archivos .xls. "
                "Agregá 'xlrd==2.0.1' a requirements.txt"
            )
    else:
        raise Exception(f"Formato no soportado: {archivo}")

    # ========================================================
    # NORMALIZACIÓN DE ENCABEZADOS (SEGÚN TU XLS REAL)
    # Encabezados típicos:
    # Descripción | Descripción | Artículo | Descripción | Descripción | Talle | Cantidad | LISTA1 | Valorizado LISTA1
    # ========================================================

    columnas_originales = [str(c).strip().lower() for c in df.columns]

    mapping = {
        "descripción": "descripcion",
        "descripcion": "descripcion",
        "artículo": "codigo",
        "articulo": "codigo",
        "talle": "talle",
        "cantidad": "stock",
        "lista1": "precio",
        "valorizado lista1": "valorizado",
        "valorizado": "valorizado",
    }

    columnas_finales = []
    contador_desc = 1

    for col in columnas_originales:
        if col in mapping:
            columnas_finales.append(mapping[col])
        elif col in ("descripción", "descripcion"):
            if contador_desc == 1:
                columnas_finales.append("descripcion")
            else:
                columnas_finales.append(f"descripcion_extra_{contador_desc}")
            contador_desc += 1
        else:
            columnas_finales.append(col.replace(" ", "_"))

    df.columns = columnas_finales

    # ========================================================
    # LIMPIEZA DE COLUMNAS CRÍTICAS
    # ========================================================

    if "codigo" in df.columns:
        df["codigo"] = df["codigo"].astype(str).str.strip()

    if "descripcion" in df.columns:
        df["descripcion"] = df["descripcion"].astype(str).str.strip()

    if "talle" in df.columns:
        df["talle"] = df["talle"].astype(str).str.strip()

    if "stock" in df.columns:
        df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0)

    if "precio" in df.columns:
        df["precio"] = pd.to_numeric(df["precio"], errors="coerce").fillna(0)

    if "valorizado" in df.columns:
        df["valorizado"] = pd.to_numeric(df["valorizado"], errors="coerce").fillna(0)

    print("Columnas normalizadas:", df.columns.tolist())
    return df, archivo


# Cargar Excel al iniciar el servidor
df, archivo_fuente = cargar_excel_mas_reciente()
print(f"Excel cargado correctamente: {archivo_fuente}")


# ============================================================
# ENDPOINT PRINCIPAL /query
# ============================================================

@app.post("/query")
async def query(data: dict):
    try:
        pregunta = data.get("question", "")

        if df.empty:
            return {
                "tipo": "mensaje",
                "mensaje": "No hay datos de stock cargados en el servidor.",
                "voz": "No hay datos de stock cargados en el servidor.",
                "fuente": archivo_fuente
            }

        resultado = procesar_pregunta(df, pregunta)

        if isinstance(resultado, dict):
            resultado.setdefault("fuente", {})
            resultado["fuente"]["name"] = archivo_fuente
        else:
            resultado = {
                "tipo": "mensaje",
                "mensaje": "Respuesta no válida del indexer.",
                "voz": "Ocurrió un error procesando la consulta.",
                "fuente": {"name": archivo_fuente}
            }

        return resultado

    except Exception as e:
        return {
            "tipo": "mensaje",
            "mensaje": "Ocurrió un error procesando la consulta.",
            "voz": "Ocurrió un error procesando la consulta.",
            "error": str(e),
            "fuente": {"name": archivo_fuente}
        }


# ============================================================
# AUTOCOMPLETE
# ============================================================

@app.get("/autocomplete")
async def autocomplete_endpoint(q: str):
    try:
        if df.empty:
            return {"sugerencias": [], "error": "No hay datos cargados."}

        columnas = {
            "descripcion": "descripcion" if "descripcion" in df.columns else None,
            "marca": "marca" if "marca" in df.columns else None,
            "rubro": "rubro" if "rubro" in df.columns else None,
            "color": "color" if "color" in df.columns else None,
            "codigo": "codigo" if "codigo" in df.columns else None,
            "talle": "talle" if "talle" in df.columns else None
        }

        sugerencias = autocompletar(df, columnas, q)
        return {"sugerencias": sugerencias}

    except Exception as e:
        return {"sugerencias": [], "error": str(e)}


# ============================================================
# DASHBOARD GLOBAL
# ============================================================

@app.get("/dashboard/global")
async def dashboard_global():
    try:
        if df.empty:
            return {
                "stock_total": 0,
                "articulos": 0,
                "fuente": archivo_fuente
            }

        stock_total = int(df["stock"].sum()) if "stock" in df.columns else 0
        articulos = df["codigo"].nunique() if "codigo" in df.columns else len(df)

        return {
            "stock_total": stock_total,
            "articulos": articulos,
            "fuente": archivo_fuente
        }

    except Exception as e:
        return {"error": str(e), "fuente": archivo_fuente}
