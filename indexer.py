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
    "al", "cuanto", "cuantos", "cuantas", "cual", "cuales", "hay",
    "dime", "decime", "mostrame", "mostra", "mostrame", "mostrame",
    "tenemos", "hay", "stock", "ver", "lista", "mostrar"
}

def normalizar_texto(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip().lower()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i")
    s = s.replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    s = re.sub(r"[^a-z0-9/ .-]", "", s)
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

    # Normalizar nombres de columnas
    cols_norm = []
    for c in df.columns:
        c_norm = normalizar_texto(str(c))
        cols_norm.append(c_norm)
    df.columns = cols_norm

    print("COLUMNAS DETECTADAS:", df.columns.tolist())

    return df

# ============================================================
# DETECCIÓN DE COLUMNAS (RUBRO, MARCA, DESCRIPCIÓN, ETC.)
# ============================================================

def detectar_columnas_por_contenido(df: pd.DataFrame) -> Dict[str, str]:
    """
    Adaptado a tu Excel:
    - varias columnas 'descripcion'
    - rubro = primera descripcion
    - marca = segunda descripcion
    - descripcion principal = tercera descripcion (o la de textos más largos)
    - articulo = codigo
    - talle, cantidad, lista1, valorizado lista1
    """
    columnas = {}

    # 1) Detectar todas las columnas "descripcion"
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

    # Si hay más de 3 descripciones, elegimos como principal la de textos más largos
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

    # 2) Código (articulo)
    for c in df.columns:
        if "articulo" in c:
            columnas["codigo"] = c
            break

    # 3) Talle
    for c in df.columns:
        if "talle" in c:
            columnas["talle"] = c
            break

    # 4) Cantidad (stock)
    for c in df.columns:
        if "cantidad" in c or "stock" in c:
            columnas["stock"] = c
            break

    # 5) Precio público (lista1)
    for c in df.columns:
        if "lista1" in c:
            columnas["precio_publico"] = c
            break

    # 6) Valorizado lista1
    for c in df.columns:
        if "valorizado" in c:
            columnas["valorizado"] = c
            break

    # 7) Color (si existe)
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
# BÚSQUEDA POR DESCRIPCIÓN (PRODUCTOS)
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
# AGRUPACIÓN POR MODELO
# ============================================================

def agrupar_por_modelo(df, col_desc, col_talle, col_stock, col_marca=None, col_rubro=None, col_precio=None, col_color=None):
    grupos = []

    for desc, grupo in df.groupby(col_desc):
        talles = []
        if col_talle:
            talles = []
            for t, g_t in grupo.groupby(col_talle):
                stock_t = int(sum(parse_numero(v) for v in g_t[col_stock])) if col_stock else 0
                talles.append({"talle": str(t).strip(), "stock": stock_t})
            talles = sorted(talles, key=lambda x: x["talle"])

        stock_total = int(sum(parse_numero(v) for v in grupo[col_stock])) if col_stock else None
        precio = None
        if col_precio:
            # asumimos mismo precio en todas las filas del modelo
            precio = parse_numero(grupo[col_precio].iloc[0])

        valorizado_total = None
        if col_precio and col_stock:
            valorizado_total = int(stock_total * precio) if stock_total is not None else None

        marca = grupo[col_marca].iloc[0] if col_marca else ""
        rubro = grupo[col_rubro].iloc[0] if col_rubro else ""
        color = grupo[col_color].iloc[0] if col_color else ""

        grupos.append({
            "tipo": "producto",
            "descripcion": normalizar_descripcion(desc),
            "codigo": str(grupo.iloc[0].get("codigo", "")),
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

# ============================================================
# INTERPRETACIÓN DE LA PREGUNTA (RUBRO, MARCA, TALLE, VALORIZADO, CANTIDAD)
# ============================================================

def interpretar_pregunta(pregunta: str, df: pd.DataFrame, columnas: Dict[str, str]) -> Dict[str, Any]:
    q_norm = normalizar_texto(pregunta)
    tokens = q_norm.split()

    col_rubro = columnas.get("rubro")
    col_marca = columnas.get("marca")
    col_talle = columnas.get("talle")

    rubro_detectado = None
    marca_detectada = None
    talle_detectado = None
    pedir_valorizado = False
    pedir_cantidad = False

    # Detectar intención de valorizado
    if any(w in q_norm for w in ["valorizado", "valor", "facturacion", "importe", "monto"]):
        pedir_valorizado = True

    # Detectar intención de cantidad total
    if any(w in q_norm for w in ["cantidad", "pares", "cuantos", "cuantas"]):
        pedir_cantidad = True

    # Detectar talle
    # patrones tipo "talle 35" o "35" o "27/8"
    m_talle = re.search(r"talle\s+(\d{1,2}(?:/\d{1,2})?)", q_norm)
    if m_talle:
        talle_detectado = m_talle.group(1)
    else:
        m_talle2 = re.search(r"\b(\d{1,2}(?:/\d{1,2})?)\b", q_norm)
        if m_talle2:
            talle_detectado = m_talle2.group(1)

    # Detectar rubro y marca por matching con valores del Excel
    if col_rubro:
        rubros_unicos = df[col_rubro].dropna().unique().tolist()
        rubros_norm = {normalizar_texto(r): r for r in rubros_unicos}
        for t in tokens:
            if t in rubros_norm:
                rubro_detectado = rubros_norm[t]
                break
            # también por "contiene"
            for rn, r_orig in rubros_norm.items():
                if t in rn and len(t) > 3:
                    rubro_detectado = r_orig
                    break
            if rubro_detectado:
                break

    if col_marca:
        marcas_unicas = df[col_marca].dropna().unique().tolist()
        marcas_norm = {normalizar_texto(m): m for m in marcas_unicos}
        for t in tokens:
            if t in marcas_norm:
                marca_detectada = marcas_norm[t]
                break
            for mn, m_orig in marcas_norm.items():
                if t in mn and len(t) > 3:
                    marca_detectada = m_orig
                    break
            if marca_detectada:
                break

    modo = "general"
    if marca_detectada and (pedir_valorizado or pedir_cantidad):
        modo = "marca_resumen"
    elif rubro_detectado and (pedir_valorizado or pedir_cantidad):
        modo = "rubro_resumen"
    elif marca_detectada:
        modo = "marca_listado"
    elif rubro_detectado:
        modo = "rubro_listado"
    elif talle_detectado:
        modo = "talle_listado"

    return {
        "modo": modo,
        "rubro": rubro_detectado,
        "marca": marca_detectada,
        "talle": talle_detectado,
        "pedir_valorizado": pedir_valorizado,
        "pedir_cantidad": pedir_cantidad,
        "tokens": tokens,
    }

# ============================================================
# RESÚMENES POR MARCA / RUBRO
# ============================================================

def resumen_por_marca(df, columnas, marca, pedir_valorizado, pedir_cantidad):
    col_marca = columnas.get("marca")
    col_stock = columnas.get("stock")
    col_val = columnas.get("valorizado")
    col_talle = columnas.get("talle")

    df_m = df[df[col_marca] == marca].copy()
    if df_m.empty:
        return None

    stock_total = None
    valorizado_total = None

    if pedir_cantidad and col_stock:
        stock_total = int(sum(parse_numero(v) for v in df_m[col_stock]))

    if pedir_valorizado and col_val:
        valorizado_total = int(sum(parse_numero(v) for v in df_m[col_val]))

    talles = []
    if col_talle and col_stock:
        for t, g in df_m.groupby(col_talle):
            stock_t = int(sum(parse_numero(v) for v in g[col_stock]))
            talles.append({"talle": str(t).strip(), "stock": stock_t})
        talles = sorted(talles, key=lambda x: x["talle"])

    return {
        "tipo": "marca_resumen",
        "marca": marca,
        "stock_total": stock_total,
        "valorizado_total": valorizado_total,
        "talles": talles,
    }

def resumen_por_rubro(df, columnas, rubro, pedir_valorizado, pedir_cantidad):
    col_rubro = columnas.get("rubro")
    col_stock = columnas.get("stock")
    col_val = columnas.get("valorizado")
    col_talle = columnas.get("talle")

    df_r = df[df[col_rubro] == rubro].copy()
    if df_r.empty:
        return None

    stock_total = None
    valorizado_total = None

    if pedir_cantidad and col_stock:
        stock_total = int(sum(parse_numero(v) for v in df_r[col_stock]))

    if pedir_valorizado and col_val:
        valorizado_total = int(sum(parse_numero(v) for v in df_r[col_val]))

    talles = []
    if col_talle and col_stock:
        for t, g in df_r.groupby(col_talle):
            stock_t = int(sum(parse_numero(v) for v in g[col_stock]))
            talles.append({"talle": str(t).strip(), "stock": stock_t})
        talles = sorted(talles, key=lambda x: x["talle"])

    return {
        "tipo": "rubro_resumen",
        "rubro": rubro,
        "stock_total": stock_total,
        "valorizado_total": valorizado_total,
        "talles": talles,
    }

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
    col_color = columnas.get("color")
    col_marca = columnas.get("marca")
    col_rubro = columnas.get("rubro")

    # Normalizar descripción principal
    if col_desc:
        col_desc = str(col_desc)
        df[col_desc] = df[col_desc].astype(str).apply(normalizar_descripcion)

    # Interpretar la pregunta (rubro, marca, talle, valorizado, cantidad)
    contexto = interpretar_pregunta(pregunta, df, columnas)

    # 1) Búsqueda por código (si hay código en la pregunta)
    if col_codigo:
        tokens_cod = re.findall(r"[A-Za-z0-9\-]{2,}", pregunta)
        for t in tokens_cod:
            df_art = df[df[col_codigo].astype(str).str.upper() == t.upper()]
            if not df_art.empty:
                fila = df_art.iloc[0]
                stock_total = int(sum(parse_numero(v) for v in df_art[col_stock])) if col_stock else None
                talles = []
                if col_talle and col_stock:
                    for tl, g in df_art.groupby(col_talle):
                        stock_t = int(sum(parse_numero(v) for v in g[col_stock]))
                        talles.append({"talle": str(tl).strip(), "stock": stock_t})
                    talles = sorted(talles, key=lambda x: x["talle"])

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

    # 2) Resúmenes por marca / rubro (valorizado / cantidad)
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

    # 3) Listados por marca / rubro / talle
    df_filtrado = df.copy()

    if contexto["marca"] and col_marca:
        df_filtrado = df_filtrado[df_filtrado[col_marca] == contexto["marca"]]

    if contexto["rubro"] and col_rubro:
        df_filtrado = df_filtrado[df_filtrado[col_rubro] == contexto["rubro"]]

    if contexto["talle"] and col_talle:
        df_filtrado = df_filtrado[df_filtrado[col_talle].astype(str) == contexto["talle"]]

    # Si hay filtros de marca/rubro/talle, agrupamos y devolvemos lista
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
            col_color
        )
        return {"tipo": "lista", "lista_completa": grupos}, archivo

    # 4) Búsqueda por descripción (consulta general)
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
                col_color
            )
            return {"tipo": "lista", "lista_completa": grupos}, archivo

    return None, archivo
