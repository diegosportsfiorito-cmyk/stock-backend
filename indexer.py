# indexer.py
import io
import re
from typing import List, Dict
import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

# ============================================================
# MAPA DE ALIAS PARA COLUMNAS OPCIONALES
# ============================================================

ALIAS = {
    "marca": ["marca", "brand"],
    "rubro": ["rubro", "categoria", "cat"],
    "grupo": ["grupo", "grupo_articulo"],
    "proveedor": ["proveedor", "supplier"],
}

# ============================================================
# DETECCIÓN AUTOMÁTICA DE COLUMNA CÓDIGO
# ============================================================

def detectar_columna_codigo(df):
    # 1) Buscar por nombre conocido
    posibles = ["articulo", "artículo", "codigo", "cod", "id", "sku"]
    for col in df.columns:
        if col.lower().replace(" ", "") in posibles:
            return col

    # 2) Heurística: columna con más valores tipo código
    mejor_col = None
    mejor_score = 0

    for col in df.columns:
        score = 0
        for val in df[col].head(20):
            s = str(val).strip()
            if 2 <= len(s) <= 20 and re.match(r"^[A-Za-z0-9\-]+$", s):
                score += 1
        if score > mejor_score:
            mejor_score = score
            mejor_col = col

    return mejor_col

# ============================================================
# NORMALIZACIÓN
# ============================================================

def normalizar_encabezados(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "", regex=False)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    return df

def parse_numero(valor):
    if valor is None:
        return 0.0
    s = str(valor).strip()
    if s == "":
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def normalizar_talle(valor: str) -> str:
    if isinstance(valor, str) and "/" in valor:
        partes = valor.split("/")
        if len(partes) == 2 and partes[0].isdigit() and partes[1].isdigit():
            base = int(partes[0])
            siguiente = int(partes[1]) + 1
            return f"{base}-{siguiente}"
    return str(valor).strip()

def normalizar_color(valor: str) -> str:
    if not isinstance(valor, str):
        return ""
    return valor.replace("/", "-").strip().upper()

def normalizar_descripcion(valor: str) -> str:
    if not isinstance(valor, str):
        return ""
    return " ".join(valor.strip().split()).title()

# ============================================================
# FILTRADO INTELIGENTE
# ============================================================

def extraer_codigo(pregunta: str) -> str:
    m = re.search(r"\b[A-Za-z0-9\-]{3,}\b", pregunta)
    return m.group(0) if m else ""

def filtrar_por_texto(df, pregunta):
    p = pregunta.lower()
    mask = df.apply(lambda row: p in str(row).lower(), axis=1)
    return df[mask]

def filtrar_por_columnas_opcionales(df, pregunta):
    p = pregunta.lower()
    filtros = []

    for campo, alias in ALIAS.items():
        col = None
        for posible in alias:
            if posible in df.columns:
                col = posible
                break

        if col:
            mask = df[col].astype(str).str.lower().str.contains(p)
            filtros.append(df[mask])

    if filtros:
        return pd.concat(filtros).drop_duplicates()

    return pd.DataFrame()

# ============================================================
# LECTURA DEL EXCEL
# ============================================================

def extraer_contenido_excel(file_bytes, nombre_archivo, pregunta):
    try:
        excel_file = io.BytesIO(file_bytes)
        xls = pd.ExcelFile(excel_file)

        df_total = []

        for sheet in xls.sheet_names:
            df = xls.parse(sheet, dtype=str).fillna("")
            df = normalizar_encabezados(df)

            for col in df.columns:
                df[col] = df[col].astype(str).str.strip()

            df_total.append(df)

        df = pd.concat(df_total, ignore_index=True)

        # DETECTAR COLUMNA CÓDIGO
        col_codigo = detectar_columna_codigo(df)
        if not col_codigo:
            return "No se pudo detectar la columna de código."

        # FILTRADO
        codigo = extraer_codigo(pregunta)
        if codigo:
            df_filtrado = df[df[col_codigo].astype(str).str.strip().str.lower() == codigo.lower()]
        else:
            df_filtrado = filtrar_por_columnas_opcionales(df, pregunta)
            if df_filtrado.empty:
                df_filtrado = filtrar_por_texto(df, pregunta)

        if df_filtrado.empty:
            return "No se encontraron artículos relevantes."

        # AGRUPACIÓN
        grupos = {}
        for _, row in df_filtrado.iterrows():
            codigo = str(row.get(col_codigo, "")).strip()
            if not codigo:
                continue

            if codigo not in grupos:
                grupos[codigo] = {
                    "descripcion": normalizar_descripcion(row.get("descripcion_original", "")),
                    "color": normalizar_color(row.get("color", "")),
                    "marca": row.get(detectar_columna_codigo(df), ""),
                    "rubro": row.get("rubro", ""),
                    "grupo": row.get("grupo", ""),
                    "stock_total": 0,
                    "talles": {},
                }

            talle = normalizar_talle(row.get("talle", ""))
            stock = int(parse_numero(row.get("stock", "0")))
            grupos[codigo]["stock_total"] += stock
            grupos[codigo]["talles"][talle] = grupos[codigo]["talles"].get(talle, 0) + stock

        # LIMITADOR
        grupos = dict(list(grupos.items())[:10])

        # ARMADO DEL TEXTO
        partes = []
        for codigo, info in grupos.items():
            talles = "\n".join([f"  - {t}: {s} unidades" for t, s in info["talles"].items()])
            partes.append(
                f"Artículo: {codigo}\n"
                f"Descripción: {info['descripcion']}\n"
                f"Color: {info['color']}\n"
                f"Stock total: {info['stock_total']}\n"
                f"Talles:\n{talles}\n"
                "-------------------------\n"
            )

        return "\n".join(partes)

    except Exception as e:
        return f"ERROR LECTURA EXCEL: {e}"

# ============================================================
# CONTEXTO PARA LA IA
# ============================================================

def obtener_contexto_para_pregunta(pregunta: str) -> List[Dict]:
    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)
    contextos = []

    for archivo in archivos:
        nombre = archivo["name"]
        file_id = archivo["id"]
        mod_time = archivo.get("modifiedTime")

        if not nombre.lower().endswith(".xlsx"):
            continue

        try:
            file_bytes = descargar_archivo_por_id(file_id)
            contenido = extraer_contenido_excel(file_bytes, nombre, pregunta)

            contextos.append({
                "archivo": nombre,
                "fecha": mod_time or "Fecha desconocida",
                "contenido": contenido
            })

        except Exception as e:
            print(f"ERROR procesando archivo {nombre}: {e}")

    return contextos
