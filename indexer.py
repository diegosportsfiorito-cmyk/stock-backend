import io
import re
from typing import List, Dict, Tuple, Optional
import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

# ============================================================
# CACHÉ EN MEMORIA
# ============================================================

CACHE_DF = None
CACHE_ARCHIVO_ID = None
CACHE_MODTIME = None
CACHE_ARCHIVO_META = None

# ============================================================
# NORMALIZACIÓN
# ============================================================

def normalizar_texto(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i")
    s = s.replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    s = re.sub(r"[^a-z0-9/ ]", "", s)
    return s

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

def normalizar_descripcion(valor: str) -> str:
    if not isinstance(valor, str):
        return ""
    return " ".join(valor.strip().split()).title()

def normalizar_talle(valor: str) -> str:
    if not isinstance(valor, str):
        return ""
    valor = valor.strip()
    if "/" in valor:
        return valor
    return valor

# ============================================================
# DETECCIÓN DE COLUMNAS SIN ENCABEZADOS
# ============================================================

def detectar_columnas_por_contenido(df: pd.DataFrame) -> Dict[str, str]:
    """
    Detecta columnas aunque NO haya encabezados.
    Usa patrones de contenido para identificar:
    - código
    - descripción
    - color
    - talle
    - precio público
    - precio costo
    - stock
    """

    columnas = {c: normalizar_texto(str(c)) for c in df.columns}
    mapeo = {}

    # 1) Detectar código (Artículo)
    for col in df.columns:
        sample = str(df[col].iloc[0]).strip()
        if re.match(r"^[A-Za-z0-9\-]{3,}$", sample):
            mapeo["codigo"] = col
            break

    # 2) Detectar descripción
    for col in df.columns:
        col_norm = normalizar_texto(str(col))
        if "descripcion" in col_norm:
            mapeo["descripcion"] = col
            break

    # fallback por contenido
    if "descripcion" not in mapeo:
        for col in df.columns:
            sample = str(df[col].iloc[0]).lower()
            if len(sample) > 8 and " " in sample:
                mapeo["descripcion"] = col
                break

    # 3) Detectar color
    for col in df.columns:
        col_norm = normalizar_texto(str(col))
        if "color" in col_norm:
            mapeo["color"] = col
            break

    # 4) Detectar talle
    for col in df.columns:
        col_norm = normalizar_texto(str(col))
        if "talle" in col_norm:
            mapeo["talle"] = col
            break

    # fallback por contenido
    if "talle" not in mapeo:
        for col in df.columns:
            sample = str(df[col].iloc[0])
            if re.match(r"^\d{1,2}(/?\d{1,2})?$", sample):
                mapeo["talle"] = col
                break

    # 5) Detectar precios
    for col in df.columns:
        col_norm = normalizar_texto(str(col))
        if "lista1" in col_norm or "precio1" in col_norm:
            mapeo["precio_publico"] = col
        if "lista0" in col_norm or "precio0" in col_norm or "costo" in col_norm:
            mapeo["precio_costo"] = col

    # 6) Detectar stock
    for col in df.columns:
        col_norm = normalizar_texto(str(col))
        if "stock" in col_norm:
            mapeo["stock"] = col
            break

    return mapeo

# ============================================================
# LECTURA DEL EXCEL (CON O SIN ENCABEZADOS)
# ============================================================

def cargar_excel_inteligente(file_bytes) -> pd.DataFrame:
    excel_file = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(excel_file)

    df_total = []

    for sheet in xls.sheet_names:
        df = xls.parse(sheet, header=None, dtype=str).fillna("")
        df_total.append(df)

    if not df_total:
        return pd.DataFrame()

    df = pd.concat(df_total, ignore_index=True)

    # Detectar si la primera fila es encabezado
    primera_fila = df.iloc[0].tolist()
    encabezado_probable = any(re.search(r"[A-Za-z]", str(x)) for x in primera_fila)

    if encabezado_probable:
        df.columns = [normalizar_texto(c) for c in primera_fila]
        df = df[1:]
    else:
        df.columns = [f"col_{i}" for i in range(len(df.columns))]

    df = df.reset_index(drop=True)

    # PRINT DE COLUMNAS DETECTADAS (lo que pediste)
    print("COLUMNAS DETECTADAS:", df.columns.tolist())

    return df

# ============================================================
# CARGA CON CACHÉ
# ============================================================

def obtener_ultimo_excel_con_cache():
    global CACHE_DF, CACHE_ARCHIVO_ID, CACHE_MODTIME, CACHE_ARCHIVO_META

    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)
    archivos = sorted(archivos, key=lambda a: a.get("modifiedTime", ""), reverse=True)

    for archivo in archivos:
        nombre = archivo["name"]
        file_id = archivo["id"]
        mod_time = archivo.get("modifiedTime")

        if not nombre.lower().endswith(".xlsx"):
            continue

        if CACHE_DF is not None and CACHE_ARCHIVO_ID == file_id and CACHE_MODTIME == mod_time:
            return CACHE_DF, CACHE_ARCHIVO_META

        try:
            file_bytes = descargar_archivo_por_id(file_id)
            df = cargar_excel_inteligente(file_bytes)

            CACHE_DF = df
            CACHE_ARCHIVO_ID = file_id
            CACHE_MODTIME = mod_time
            CACHE_ARCHIVO_META = archivo

            return df, archivo

        except Exception as e:
            print(f"ERROR cargando archivo {nombre}: {e}")
            continue

    return None, None

# ============================================================
# BÚSQUEDA POR DESCRIPCIÓN
# ============================================================

def buscar_por_descripcion(df, col_desc, pregunta):
    tokens = normalizar_texto(pregunta).split()
    desc = df[col_desc].astype(str).str.lower()

    mask = True
    for t in tokens:
        mask = mask & desc.str.contains(t, na=False)

    resultados = df[mask].copy()
    if resultados.empty:
        return resultados

    resultados["score"] = resultados[col_desc].str.count("|".join(tokens))
    return resultados.sort_values("score", ascending=False)

# ============================================================
# AGRUPACIÓN
# ============================================================

def agrupar_por_modelo(df, col_desc, col_talle, col_stock):
    grupos = []

    for desc, grupo in df.groupby(col_desc):
        talles = sorted({str(t).strip() for t in grupo[col_talle]}) if col_talle else []
        stock_total = int(sum(parse_numero(v) for v in grupo[col_stock])) if col_stock else None

        grupos.append({
            "descripcion": desc,
            "talles": talles,
            "stock_total": stock_total,
            "items": grupo.to_dict(orient="records")
        })

    return sorted(grupos, key=lambda x: x["stock_total"] or 0, reverse=True)

# ============================================================
# BÚSQUEDA PRINCIPAL
# ============================================================

def buscar_articulo_en_archivos(pregunta: str):
    df, archivo = obtener_ultimo_excel_con_cache()
    if df is None:
        return None, None

    columnas = detectar_columnas_por_contenido(df)

    col_codigo = columnas.get("codigo")
    col_desc = columnas.get("descripcion")
    col_talle = columnas.get("talle")
    col_stock = columnas.get("stock")
    col_publico = columnas.get("precio_publico")
    col_costo = columnas.get("precio_costo")

    # Normalizar descripción
    if col_desc:
        df[col_desc] = df[col_desc].astype(str).apply(normalizar_descripcion)

    # 1) Búsqueda por código
    if col_codigo:
        tokens = re.findall(r"[A-Za-z0-9\-]{2,}", pregunta)
        for t in tokens:
            df_art = df[df[col_codigo].astype(str).str.upper() == t.upper()]
            if not df_art.empty:
                fila = df_art.iloc[0]

                info = {
                    "codigo": t.upper(),
                    "descripcion": fila[col_desc] if col_desc else "",
                    "precio_publico": parse_numero(fila[col_publico]) if col_publico else None,
                    "precio_costo": parse_numero(fila[col_costo]) if col_costo else None,
                    "stock_total": int(sum(parse_numero(v) for v in df_art[col_stock])) if col_stock else None,
                    "talles": sorted({normalizar_talle(x) for x in df_art[col_talle]}) if col_talle else [],
                }

                return info, archivo

    # 2) Búsqueda por descripción
    if col_desc:
        encontrados = buscar_por_descripcion(df, col_desc, pregunta)
        if not encontrados.empty:
            grupos = agrupar_por_modelo(encontrados, col_desc, col_talle, col_stock)
            return {"lista_completa": grupos}, archivo

    return None, archivo
