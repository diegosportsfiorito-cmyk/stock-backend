from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd
import requests
import io
import time
from ai_engine import ask_ai
from intent_engine import detect_intent

# ---------------------------------------------------------
# 1) Crear app y configurar CORS
# ---------------------------------------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------
# 2) Configuración de Google Sheets
# ---------------------------------------------------------
SHEET_EXPORT_URL = (
    "https://docs.google.com/spreadsheets/d/1YCC5hCn-rXaSQaI3a0EeTyzq7rBTc7sN/export"
    "?format=xlsx&gid=469792711"
)

df_cache = None
last_load_time = 0
CACHE_TTL_SECONDS = 60


# ---------------------------------------------------------
# 3) Cargar Excel con control de calidad
# ---------------------------------------------------------
def load_sheet():
    global df_cache, last_load_time
    now = time.time()

    if df_cache is not None and (now - last_load_time) < CACHE_TTL_SECONDS:
        return df_cache

    resp = requests.get(SHEET_EXPORT_URL)
    resp.raise_for_status()
    data = io.BytesIO(resp.content)

    df = pd.read_excel(data)

    df.columns = [str(c).strip().lower() for c in df.columns]

    col_map = {}
    for c in df.columns:
        if "art" in c and "ículo" in c:
            col_map["articulo"] = c
        elif "descr" in c and "color" not in c:
            col_map["descripcion"] = c
        elif "color" in c:
            col_map["color"] = c
        elif "talle" in c:
            col_map["talle"] = c
        elif "cant" in c:
            col_map["cantidad"] = c
        elif "precio" in c:
            col_map["precio"] = c

    df = df.fillna("")
    df = df.replace({float("inf"): 0, float("-inf"): 0})

    total_filas = len(df)
    if total_filas < 10000:
        raise ValueError(f"Excel incompleto: solo {total_filas} filas cargadas")

    df_cache = (df, col_map)
    last_load_time = now
    return df_cache


# ---------------------------------------------------------
# 4) Funciones de consulta
# ---------------------------------------------------------
def buscar_por_articulo_talle(articulo=None, talle=None):
    df, col_map = load_sheet()
    q = df.copy()

    if articulo and "articulo" in col_map:
        col = col_map["articulo"]
        q = q[q[col].astype(str).str.contains(str(articulo), case=False, na=False)]

    if talle and "talle" in col_map:
        col = col_map["talle"]
        q = q[q[col].astype(str) == str(talle)]

    if q.empty:
        return "No se encontró stock para esa búsqueda."

    resultados = []
    for _, row in q.iterrows():
        art = row[col_map.get("articulo", "")]
        desc = row[col_map.get("descripcion", "")]
        talle_val = row[col_map.get("talle", "")]
        cant = row[col_map.get("cantidad", "")]
        precio = row[col_map.get("precio", "")]
        resultados.append(
            f"Artículo {art} | {desc} | Talle {talle_val} | Cantidad {cant} | Precio {precio}"
        )

    return "\n".join(resultados)


def analisis_basico():
    df, col_map = load_sheet()
    res = {}

    if "cantidad" in col_map:
        col_cant = col_map["cantidad"]
        total_items = df[col_cant].sum(numeric_only=True)
        negativos = df[df[col_cant] < 0]
        sin_stock = df[df[col_cant] == 0]
        con_stock = df[df[col_cant] > 0]

        res["total_unidades"] = float(total_items)
        res["items_con_stock"] = int(len(con_stock))
        res["items_sin_stock"] = int(len(sin_stock))
        res["items_stock_negativo"] = int(len(negativos))

    if "precio" in col_map and "cantidad" in col_map:
        col_precio = col_map["precio"]
        col_cant = col_map["cantidad"]
        df["valorizado_calc"] = df[col_precio] * df[col_cant]
        res["valorizado_total"] = float(df["valorizado_calc"].sum(numeric_only=True))

    return res


# ---------------------------------------------------------
# 5) Interpretación de consulta
# ---------------------------------------------------------
def interpretar_consulta(texto: str):
    texto_l = texto.lower()

    articulo = None
    talle = None

    import re

    m_art = re.search(r"([0-9]{2,}[0-9\-]*)", texto_l)
    if m_art:
        articulo = m_art.group(1)

    m_talle = re.search(r"talle\s+([0-9./]+)", texto_l)
    if m_talle:
        talle = m_talle.group(1)

    if "análisis" in texto_l or "analisis" in texto_l or "resumen" in texto_l:
        a = analisis_basico()
        partes = []
        if "total_unidades" in a:
            partes.append(f"Total de unidades en stock: {a['total_unidades']}")
        if "items_con_stock" in a:
            partes.append(f"Items con stock: {a['items_con_stock']}")
        if "items_sin_stock" in a:
            partes.append(f"Items sin stock: {a['items_sin_stock']}")
        if "items_stock_negativo" in a:
            partes.append(f"Items con stock negativo: {a['items_stock_negativo']}")
        if "valorizado_total" in a:
            partes.append(f"Valorizado total estimado: {a['valorizado_total']}")
        return "\n".join(partes) if partes else "No pude generar el análisis."

    if articulo or talle:
        return buscar_por_articulo_talle(articulo, talle)

    return "No entendí la consulta. Probá indicando artículo y/o talle."


# ---------------------------------------------------------
# 6) Endpoint IA optimizado
# ---------------------------------------------------------
@app.post("/api/ia-query")
async def ia_query(request: Request):
    try:
        body = await request.json()
        pregunta = body.get("pregunta", "")

        df, col_map = load_sheet()
        intent = detect_intent(pregunta)

        # -------------------------
        # STOCK POR CÓDIGO
        # -------------------------
        if intent["intent"] == "stock_por_codigo":
            codigo = intent["codigo"]
            talle = intent["talle"]

            q = df.copy()
            col_art = col_map.get("articulo")
            col_talle = col_map.get("talle")

            if col_art:
                q = q[q[col_art].astype(str).str.contains(codigo, case=False, na=False)]

            if talle and col_talle:
                q = q[q[col_talle].astype(str) == str(talle)]

            if q.empty:
                return {"respuesta": "No se encontró stock para ese artículo o talle."}

            q_small = q.head(20)

            cols = [
                col_map.get("articulo"),
                col_map.get("descripcion"),
                col_map.get("talle"),
                col_map.get("cantidad"),
                col_map.get("precio")
            ]
            cols = [c for c in cols if c]
            q_small = q_small[cols]

            q_small = q_small.fillna("")
            q_small = q_small.replace({float("inf"): 0, float("-inf"): 0})

            prompt = f"""
            Datos filtrados del Excel (máx 20 filas):
            {q_small.to_json(orient='records')}

            Pregunta del usuario:
            {pregunta}

            Respondé como un analista experto en stock.
            """

            return {"respuesta": ask_ai(prompt)}

        # -------------------------
        # PRECIO POR CÓDIGO
        # -------------------------
        if intent["intent"] == "precio_por_codigo":
            codigo = intent["codigo"]
            q = df[df[col_map["articulo"]].astype(str).str.contains(codigo)]
            q_small = q.head(20)

            cols = [
                col_map.get("articulo"),
                col_map.get("descripcion"),
                col_map.get("precio")
            ]
            cols = [c for c in cols if c]
            q_small = q_small[cols]

            q_small = q_small.fillna("")

            prompt = f"""
            Datos filtrados del Excel (máx 20 filas):
            {q_small.to_json(orient='records')}

            Pregunta del usuario:
            {pregunta}
            """

            return {"respuesta": ask_ai(prompt)}

        # -------------------------
        # ANÁLISIS GLOBAL
        # -------------------------
        if intent["intent"] == "analisis_global":
            stats = analisis_basico()

            prompt = f"""
            Estadísticas globales del Excel:
            {stats}

            Pregunta del usuario:
            {pregunta}
            """

            return {"respuesta": ask_ai(prompt)}

        # -------------------------
        # CONSULTA GENERAL
        # -------------------------
        df_sample = df.head(20)

        cols = [
            col_map.get("articulo"),
            col_map.get("descripcion"),
            col_map.get("talle"),
            col_map.get("cantidad"),
            col_map.get("precio")
        ]
        cols = [c for c in cols if c]
        df_sample = df_sample[cols]

        df_sample = df_sample.fillna("")

        prompt = f"""
        Muestra del Excel (primeras 20 filas):
        {df_sample.to_json(orient='records')}

        Pregunta del usuario:
        {pregunta}

        Respondé como un analista experto.
        """

        return {"respuesta": ask_ai(prompt)}

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)},
            headers={"Access-Control-Allow-Origin": "*"},
        )