import io
import re
from typing import Dict, Tuple, Any, Optional
import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

# ============================================================
# CACHÉ
# ============================================================

CACHE_DF: Optional[pd.DataFrame] = None
CACHE_ARCHIVO_ID: Optional[str] = None
CACHE_MODTIME: Optional[str] = None
CACHE_ARCHIVO_META: Optional[Dict[str, Any]] = None

# ============================================================
# NORMALIZACIÓN Y STOPWORDS
# ============================================================

STOPWORDS = {
    "que", "hay", "tengo", "quiero", "busco", "de", "para", "el", "la",
    "los", "las", "un", "una", "unos", "unas", "me", "por", "en", "del",
    "al", "cuanto", "cuantos", "cuantas", "cual", "cuales",
    "dime", "decime", "mostrame", "mostra",
    "tenemos", "stock", "ver", "lista", "mostrar",
    "codigo", "modelo", "articulo"
}

def normalizar_texto(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i")
    s = s.replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    s = re.sub(r"[^a-z0-9/ .-]", "", s)
    s = " ".join(s.split())
    return s

def parse_numero(valor):
    if valor is None:
        return 0.0
    s = str(valor).strip()
    if s == "":
        return 0.0
    s = s.replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

def normalizar_descripcion(valor: str) -> str:
    if not isinstance(valor, str):
        return ""
    return " ".join(valor.strip().split()).title()

# ============================================================
# LECTURA DEL EXCEL
# ============================================================

def cargar_excel_inteligente(file_bytes) -> pd.DataFrame:
    excel_file = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(excel_file)

    df_total = []

    for sheet in xls.sheet_names:
        df = xls.parse(sheet, header=0, dtype=str).fillna("")
        df_total.append(df)

    if not df_total:
        return pd.DataFrame()

    df = pd.concat(df_total, ignore_index=True)
    df = df.dropna(how="all")

    # Normalizar nombres de columnas
    cols_norm = []
    for c in df.columns:
        c_norm = normalizar_texto(str(c))
        cols_norm.append(c_norm)
    df.columns = cols_norm

    print("COLUMNAS DETECTADAS:", df.columns.tolist())

    return df

# ============================================================
# DETECCIÓN DE COLUMNAS
# ============================================================

def detectar_columnas_por_contenido(df: pd.DataFrame) -> Dict[str, str]:
    columnas = {}

    desc_cols = [c for c in df.columns if "descripcion" in c]

    rubro_col = None
    marca_col = None
    desc_principal_col = None

    if len(desc_cols) >= 3:
        rubro_col = desc_cols[0]
        marca_col = desc_cols[1]
        desc_principal_col = desc_cols[2]
    elif len(desc_cols) == 2:
        rubro_col = desc_cols[0]
        marca_col = desc_cols[1]
    elif len(desc_cols) == 1:
        desc_principal_col = desc_cols[0]

    if len(desc_cols) >= 3:
        max_len = -1
        best_col = desc_principal_col
        for c in desc_cols:
            textos = df[c].astype(str)
            longitudes = textos.apply(lambda x: len(x.strip()))
            prom = longitudes.mean()
            if prom > max_len:
                max_len = prom
                best_col = c
        desc_principal_col = best_col

    if rubro_col:
        columnas["rubro"] = rubro_col
    if marca_col:
        columnas["marca"] = marca_col
    if desc_principal_col:
        columnas["descripcion"] = desc_principal_col

    for c in df.columns:
        if "articulo" in c or "codigo" in c:
            columnas["codigo"] = c
            break

    for c in df.columns:
        if "talle" in c:
            columnas["talle"] = c
            break

    for c in df.columns:
        if "cantidad" in c or "stock" in c:
            columnas["stock"] = c
            break

    for c in df.columns:
        if "lista1" in c:
            columnas["precio_publico"] = c
            break

    for c in df.columns:
        if "valorizado" in c:
            columnas["valorizado"] = c
            break

    for c in df.columns:
        if "color" in c:
            columnas["color"] = c
            break

    return columnas

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
    tokens = [
        t for t in normalizar_texto(pregunta).split()
        if t not in STOPWORDS and len(t) > 2
    ]

    if not tokens:
        return pd.DataFrame()

    desc = df[col_desc].astype(str).str.lower()

    mask = False
    for t in tokens:
        mask = mask | desc.str.contains(t, na=False)

    resultados = df[mask].copy()
    if resultados.empty:
        return resultados

    resultados["score"] = resultados[col_desc].str.lower().apply(
        lambda x: sum(t in x for t in tokens)
    )

    return resultados.sort_values("score", ascending=False)

# ============================================================
# ORDEN DE TALLES (FIX DEFINITIVO)
# ============================================================

def _orden_talle(talle_str: str):
    s = str(talle_str).strip().lower()

    if "/" in s:
        try:
            return float(s.split("/")[0])
        except:
            return 9999

    try:
        return float(s)
    except:
        pass

    orden_texto = {
        "xs": 10001,
        "s": 10002,
        "m": 10003,
        "l": 10004,
        "xl": 10005,
        "xxl": 10006,
        "unico": 10007,
    }

    return orden_texto.get(s, 10050)

# ============================================================
# AGRUPACIÓN POR MODELO
# ============================================================

def agrupar_por_modelo(
    df,
    col_desc,
    col_talle,
    col_stock,
    col_marca=None,
    col_rubro=None,
    col_precio=None,
    col_color=None,
    col_codigo=None
):
    if col_talle:
        df[col_talle] = df[col_talle].astype(str).str.strip()

    grupos = []

    for desc, grupo in df.groupby(col_desc):
        talles = []
        if col_talle:
            for t, g_t in grupo.groupby(col_talle):
                stock_t = int(sum(parse_numero(v) for v in g_t[col_stock])) if col_stock else 0
                talles.append({"talle": str(t).strip(), "stock": stock_t})
            talles = sorted(talles, key=lambda x: _orden_talle(x["talle"]))

        stock_total = int(sum(parse_numero(v) for v in grupo[col_stock])) if col_stock else None

        precio = None
        if col_precio:
            precio = parse_numero(grupo[col_precio].iloc[0])

        valorizado_total = None
        if col_precio and col_stock and stock_total is not None:
            valorizado_total = int(stock_total * precio)

        marca = grupo[col_marca].iloc[0] if col_marca else ""
        rubro = grupo[col_rubro].iloc[0] if col_rubro else ""
        color = grupo[col_color].iloc[0] if col_color else ""
        codigo = grupo[col_codigo].iloc[0] if col_codigo else ""

        grupos.append({
            "tipo": "producto",
            "descripcion": normalizar_descripcion(desc),
            "codigo": str(codigo),
            "marca": marca,
            "rubro": rubro,
            "precio": precio,
            "stock_total": stock_total,
            "talles": talles,
            "valorizado_total": valorizado_total,
            "color": color,
            "items": grupo.to_dict(orient="records")
        })

    return sorted(grupos, key=lambda x: x["stock_total"] or 0, reverse=True)
                    if col_talle:
                    df_art[col_talle] = df_art[col_talle].astype(str).str.strip()

                stock_total = int(sum(parse_numero(v) for v in df_art[col_stock])) if col_stock else None

                talles = []
                if col_talle and col_stock:
                    for tl, g in df_art.groupby(col_talle):
                        stock_t = int(sum(parse_numero(v) for v in g[col_stock]))
                        talles.append({"talle": str(tl).strip(), "stock": stock_t})
                    talles = sorted(talles, key=lambda x: _orden_talle(x["talle"]))

                info = {
                    "tipo": "producto",
                    "codigo": t.upper(),
                    "descripcion": fila[col_desc] if col_desc else "",
                    "precio": parse_numero(fila[col_publico]) if col_publico else None,
                    "stock_total": stock_total,
                    "talles": talles,
                    "marca": fila[col_marca] if col_marca else "",
                    "rubro": fila[col_rubro] if col_rubro else "",
                    "color": fila[col_color] if col_color else "",
                }
                return info, archivo

    # ============================================================
    # RESÚMENES POR MARCA / RUBRO
    # ============================================================

    if contexto["modo"] == "marca_resumen" and contexto["marca"]:
        resumen = resumen_por_marca(
            df, columnas,
            contexto["marca"],
            contexto["pedir_valorizado"],
            contexto["pedir_cantidad"]
        )
        if resumen:
            return resumen, archivo

    if contexto["modo"] == "rubro_resumen" and contexto["rubro"]:
        resumen = resumen_por_rubro(
            df, columnas,
            contexto["rubro"],
            contexto["pedir_valorizado"],
            contexto["pedir_cantidad"]
        )
        if resumen:
            return resumen, archivo

    # ============================================================
    # LISTADOS POR MARCA / RUBRO / TALLE
    # ============================================================

    df_filtrado = df.copy()

    if contexto["marca"] and col_marca:
        df_filtrado = df_filtrado[df_filtrado[col_marca] == contexto["marca"]]

    if contexto["rubro"] and col_rubro:
        df_filtrado = df_filtrado[df_filtrado[col_rubro] == contexto["rubro"]]

    if contexto["talle"] and col_talle:
        df_filtrado[col_talle] = df_filtrado[col_talle].astype(str).str.strip()
        df_filtrado = df_filtrado[df_filtrado[col_talle] == contexto["talle"]]

    if contexto["modo"] in {"marca_listado", "rubro_listado", "talle_listado"}:
        if df_filtrado.empty or not col_desc:
            return None, archivo

        grupos = agrupar_por_modelo(
            df_filtrado,
            col_desc,
            col_talle,
            col_stock,
            col_marca,
            col_rubro,
            col_publico,
            col_color,
            col_codigo
        )
        return {"tipo": "lista", "items": grupos}, archivo

    # ============================================================
    # BÚSQUEDA POR DESCRIPCIÓN
    # ============================================================

    if col_desc:
        encontrados = buscar_por_descripcion(df_filtrado, col_desc, pregunta)
        if not encontrados.empty:
            grupos = agrupar_por_modelo(
                encontrados,
                col_desc,
                col_talle,
                col_stock,
                col_marca,
                col_rubro,
                col_publico,
                col_color,
                col_codigo
            )
            return {"tipo": "lista", "items": grupos}, archivo

    # ============================================================
    # SIN RESULTADOS
    # ============================================================

    return None, archivo
