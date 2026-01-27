# ===== MAIN.PY v3.0 PRO COMPLETO =====

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from indexer import procesar_pregunta, autocompletar, detectar_columna_descripcion
from drive import listar_archivos_en_carpeta, descargar_archivo_por_id
import pandas as pd
import io

# Carpeta de Google Drive
FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

# ------------------------------------------------------------
# CARGAR EXCEL MÁS RECIENTE
# ------------------------------------------------------------

def cargar_excel_mas_reciente():
    archivos = listar_archivos_en_carpeta(FOLDER_ID)
    if not archivos:
        raise Exception("No se encontraron archivos en Drive.")

    archivos_ordenados = sorted(
        archivos,
        key=lambda x: x["modifiedTime"],
        reverse=True
    )

    archivo = archivos_ordenados[0]
    contenido = descargar_archivo_por_id(archivo["id"])
    df = pd.read_excel(io.BytesIO(contenido))

    return df, archivo


# Cargar Excel al iniciar el servidor
df, archivo_fuente = cargar_excel_mas_reciente()

# ------------------------------------------------------------
# RENOMBRAR COLUMNAS SEGÚN ORDEN FIJO DEL EXCEL
# ------------------------------------------------------------

df = df.rename(columns={
    df.columns[0]: "Marca",
    df.columns[1]: "Rubro",
    df.columns[2]: "Codigo",
    df.columns[3]: "Descripcion",
    df.columns[4]: "Color",
    df.columns[5]: "Talle",
    df.columns[6]: "Stock",
    df.columns[7]: "Precio",
    df.columns[8]: "Valorizado"
})

# ------------------------------------------------------------
# PREPROCESAMIENTO GLOBAL (ACELERA TODO)
# ------------------------------------------------------------

def norm(x):
    try:
        return str(x).strip().lower()
    except:
        return ""

df["Marca_norm"] = df["Marca"].apply(norm)
df["Rubro_norm"] = df["Rubro"].apply(norm)
df["Descripcion_norm"] = df["Descripcion"].apply(norm)
df["Color_norm"] = df["Color"].apply(norm)
df["Talle_norm"] = df["Talle"].apply(norm)
df["Codigo_norm"] = df["Codigo"].apply(norm)

# Columna de búsqueda rápida
df["__search"] = (
    df["Marca_norm"] + " " +
    df["Rubro_norm"] + " " +
    df["Descripcion_norm"] + " " +
    df["Color_norm"] + " " +
    df["Talle_norm"] + " " +
    df["Codigo_norm"]
)

# Convertir números una sola vez
df["Stock"] = pd.to_numeric(df["Stock"], errors="coerce").fillna(0)
df["Precio"] = pd.to_numeric(df["Precio"], errors="coerce").fillna(0)
df["Valorizado"] = pd.to_numeric(df["Valorizado"], errors="coerce").fillna(0)

# ------------------------------------------------------------
# MÉTRICAS GLOBALES PARA EL DASHBOARD
# ------------------------------------------------------------

metricas_globales = {
    "stock_total": int(df["Stock"].sum()),
    "articulos": int(df["Codigo"].nunique()),
    "rubros": df.groupby("Rubro")["Stock"].sum().sort_values(ascending=False).to_dict(),
    "marcas": df.groupby("Marca")["Stock"].sum().sort_values(ascending=False).to_dict(),
    "talles": df.groupby("Talle")["Stock"].sum().sort_values(ascending=False).to_dict(),
    "colores": df.groupby("Color")["Stock"].sum().sort_values(ascending=False).to_dict(),
}

# ------------------------------------------------------------
# FASTAPI
# ------------------------------------------------------------

app = FastAPI()

# CORS para permitir acceso desde tu frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelo de entrada
class QueryRequest(BaseModel):
    question: str

# ------------------------------------------------------------
# ENDPOINT PRINCIPAL /query
# ------------------------------------------------------------

@app.post("/query")
async def query(req: QueryRequest):
    pregunta = req.question.strip()

    try:
        resultado = procesar_pregunta(df, pregunta)

        # Agregar información del archivo fuente
        resultado["fuente"] = {
            "id": archivo_fuente["id"],
            "name": archivo_fuente["name"],
            "mimeType": archivo_fuente["mimeType"],
            "modifiedTime": archivo_fuente["modifiedTime"]
        }

        return resultado

    except Exception as e:
        print("ERROR EN /query:", e)
        return {
            "tipo": "mensaje",
            "mensaje": "Ocurrió un error procesando la consulta.",
            "voz": "Ocurrió un error procesando la consulta.",
            "error": str(e),
        }

# ------------------------------------------------------------
# ENDPOINT AUTOCOMPLETADO /autocomplete
# ------------------------------------------------------------

@app.get("/autocomplete")
async def autocomplete(q: str):
    try:
        columnas = {
            "descripcion": "Descripcion",
            "marca": "Marca",
            "rubro": "Rubro",
            "color": "Color",
            "codigo": "Codigo",
            "talle": "Talle",
        }

        sugerencias = autocompletar(df, columnas, q)
        return {"sugerencias": sugerencias}

    except Exception as e:
        print("ERROR EN /autocomplete:", e)
        return {"sugerencias": []}

# ------------------------------------------------------------
# ENDPOINT DASHBOARD GLOBAL
# ------------------------------------------------------------

@app.get("/dashboard/global")
async def dashboard_global():
    return metricas_globales

# ------------------------------------------------------------
# ENDPOINT DE PRUEBA
# ------------------------------------------------------------

@app.get("/")
async def root():
    return {
        "status": "OK",
        "message": "Stock IA Backend funcionando.",
        "archivo_cargado": archivo_fuente["name"]
    }

# ===== FIN MAIN.PY =====
