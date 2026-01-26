# ===== INDEXER UNIVERSAL + AUTOCOMPLETADO =====

import pandas as pd
import re
import math

# ------------------------------------------------------------
# NORMALIZACIÓN
# ------------------------------------------------------------

def normalizar(texto):
    if not isinstance(texto, str):
        texto = str(texto)
    return texto.strip().lower()

def parse_numero(valor):
    if pd.isna(valor):
        return 0
    if isinstance(valor, (int, float)):
        return valor
    v = str(valor).replace(".", "").replace(",", ".")
    try:
        return float(v)
    except:
        return 0

# ------------------------------------------------------------
# LIMPIEZA ANTI-NAN PARA JSON
# ------------------------------------------------------------

def limpiar_nan_en_valor(v):
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v

def limpiar_nan_en_objeto(obj):
    if isinstance(obj, dict):
        return {k: limpiar_nan_en_objeto(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [limpiar_nan_en_objeto(v) for v in obj]
    return limpiar_nan_en_valor(obj)

# ------------------------------------------------------------
# EXTRAER KEYWORDS
# ------------------------------------------------------------

def extraer_keywords(pregunta):
    p = normalizar(pregunta)
    tokens = re.split(r"\W+", p)
    stopwords = ["que","hay","tengo","de","el","la","los","las","un","una",
                 "stock","en","para","con","por","sobre","del","al","y","o","a"]
    return [t for t in tokens if t and t not in stopwords]

# ------------------------------------------------------------
# DETECTAR COLUMNA DE DESCRIPCIÓN
# ------------------------------------------------------------

def detectar_columna_descripcion(df):
    keywords = [
        "pantufla","pantuflas","pantu",
        "zapatilla","zapatillas","zapa",
        "remera","camiseta","buzo","campera",
        "pantalon","pantalón","short",
        "botin","botines","media","medias",
        "guante","guantes","bolsa","boxeo",
        "mochila","plush","avengers","marvel","slide","bermuda"
    ]

    columnas_prohibidas = ["rubro","marca","color","talle","cantidad","stock","precio","lista","valoriz"]

    cols_norm = {c: c.lower().strip() for c in df.columns}
    puntajes = {}

    for col in df.columns:
        nombre = cols_norm[col]
        if any(p in nombre for p in columnas_prohibidas):
            continue

        serie = df[col].astype(str).str.lower()
        puntaje = sum(serie.str.contains(kw, na=False).sum() for kw in keywords)
        puntajes[col] = puntaje

    if puntajes and max(puntajes.values()) > 0:
        return max(puntajes, key=puntajes.get)

    return max(df.columns, key=lambda c: df[c].astype(str).str.len().mean())

# ------------------------------------------------------------
# AGRUPACIÓN POR MODELO
# ------------------------------------------------------------

def _orden_talle(t):
    try:
        return int(re.sub(r"\D", "", str(t)))
    except:
        return 9999

def agrupar_por_modelo(df, col_desc, col_talle, col_stock,
                       col_marca, col_rubro, col_publico, col_color, col_codigo):

    grupos = []

    for desc, g in df.groupby(col_desc):
        talles = []
        if col_talle and col_stock:
            for tl, gg in g.groupby(col_talle):
                stock_t = int(sum(parse_numero(v) for v in gg[col_stock]))
                talles.append({"talle": str(tl).strip(), "stock": stock_t})

        precio_val = parse_numero(g[col_publico].iloc[0]) if col_publico else None
        if precio_val == 0 and pd.isna(g[col_publico].iloc[0]):
            precio_val = None

        grupos.append({
            "descripcion": desc,
            "marca": g[col_marca].iloc[0] if col_marca else "",
            "rubro": g[col_rubro].iloc[0] if col_rubro else "",
            "precio": precio_val,
            "talles": sorted(talles, key=lambda x: _orden_talle(x["talle"])),
            "codigo": g[col_codigo].iloc[0] if col_codigo else "",
            "color": g[col_color].iloc[0] if col_color else ""
        })

    return grupos

# ------------------------------------------------------------
# BÚSQUEDA UNIVERSAL
# ------------------------------------------------------------

def buscar_universal(df, keywords, columnas):
    df_filtrado = df.copy()

    for kw in keywords:
        mask = False
        for col in columnas.values():
            if col:
                mask = mask | df[col].astype(str).str.lower().str.contains(kw)
        df_filtrado = df_filtrado[mask]

    return df_filtrado

# ------------------------------------------------------------
# AUTOCOMPLETADO INTELIGENTE
# ------------------------------------------------------------

def generar_diccionario_autocompletado(df, columnas):
    palabras = set()

    columnas_relevantes = [
        columnas["descripcion"],
        columnas["marca"],
        columnas["rubro"],
        columnas["color"],
        columnas["codigo"],
        columnas["talle"]
    ]

    for col in columnas_relevantes:
        if col:
            for v in df[col].astype(str).tolist():
                tokens = re.split(r"\W+", str(v).lower())
                for t in tokens:
                    if len(t) >= 2:
                        palabras.add(t)

    return sorted(list(palabras))

def autocompletar(df, columnas, texto):
    texto = normalizar(texto)
    if len(texto) < 2:
        return []

    dicc = generar_diccionario_autocompletado(df, columnas)

    sugerencias = [p for p in dicc if p.startswith(texto)]

    return sugerencias[:12]

# ------------------------------------------------------------
# PROCESAR PREGUNTA
# ------------------------------------------------------------

def procesar_pregunta(df, pregunta):
    pregunta_norm = normalizar(pregunta)
    keywords = extraer_keywords(pregunta)

    columnas = {
        "rubro": next((c for c in df.columns if "rubro" in c.lower()), None),
        "marca": next((c for c in df.columns if "marca" in c.lower()), None),
        "codigo": next((c for c in df.columns if "art" in c.lower() or "código" in c.lower() or "codigo" in c.lower()), None),
        "color": next((c for c in df.columns if "color" in c.lower()), None),
        "talle": next((c for c in df.columns if "talle" in c.lower()), None),
        "stock": next((c for c in df.columns if "cant" in c.lower() or "stock" in c.lower()), None),
        "publico": next((c for c in df.columns if "lista" in c.lower() or "precio" in c.lower()), None),
        "valorizado": next((c for c in df.columns if "valoriz" in c.lower()), None),
    }

    col_desc = detectar_columna_descripcion(df)
    columnas["descripcion"] = col_desc

    # ===== DETECCIÓN INTELIGENTE DE MARCA Y RUBRO =====
    if not columnas["marca"]:
        posibles_marcas = ["adidas","nike","reebok","atomik","kioshi","prominent",
                           "maraton","limited","authentic","puma","fila","topper"]
        for col in df.columns:
            if df[col].astype(str).str.lower().isin(posibles_marcas).any():
                columnas["marca"] = col
                break
        if not columnas["marca"]:
            columnas["marca"] = df.columns[0]

    if not columnas["rubro"]:
        posibles_rubros = ["ojotas","calzado","indumentaria","zapatilla","bermuda",
                           "remera","buzo","campera","pantalon","pantalón","short"]
        for col in df.columns:
            if df[col].astype(str).str.lower().isin(possibles_rubros).any():
                columnas["rubro"] = col
                break
        if not columnas["rubro"] and len(df.columns) > 1:
            columnas["rubro"] = df.columns[1]

    # --------------------------------------------------------
    # AUTOCOMPLETADO
    # --------------------------------------------------------
    if pregunta_norm.startswith("auto:"):
        texto = pregunta_norm.replace("auto:", "").strip()
        sugerencias = autocompletar(df, columnas, texto)
        return limpiar_nan_en_objeto({
            "tipo": "autocomplete",
            "sugerencias": sugerencias
        })

    # --------------------------------------------------------
    # BÚSQUEDA POR CÓDIGO EXACTO
    # --------------------------------------------------------
    if columnas["codigo"]:
        df[columnas["codigo"]] = df[columnas["codigo"]].astype(str).str.strip()
        codigo_mayus = pregunta_norm.upper()
        if codigo_mayus in df[columnas["codigo"]].values:
            fila = df[df[columnas["codigo"]] == codigo_mayus].iloc[0]
            return limpiar_nan_en_objeto({
                "tipo": "producto",
                "data": {
                    "descripcion": fila[col_desc],
                    "marca": fila[columnas["marca"]] if columnas["marca"] else "",
                    "rubro": fila[columnas["rubro"]] if columnas["rubro"] else "",
                    "precio": parse_numero(fila[columnas["publico"]]) if columnas["publico"] else None,
                    "stock_total": parse_numero(fila[columnas["stock"]]) if columnas["stock"] else None,
                    "talles": [],
                    "color": fila[columnas["color"]] if columnas["color"] else "",
                    "codigo": fila[columnas["codigo"]]
                },
                "voz": f"Encontré el artículo {fila[col_desc]}",
                "fuente": {}
            })

    # --------------------------------------------------------
    # BÚSQUEDA POR TALLE
    # --------------------------------------------------------
    match_talle = re.search(r"\b(\d{1,2})\b", pregunta_norm)
    if match_talle and columnas["talle"]:
        talle_buscado = match_talle.group(1)
        df_talle = df[df[columnas["talle"]].astype(str).str.contains(talle_buscado)]

        if not df_talle.empty:
            grupos = agrupar_por_modelo(
                df_talle,
                col_desc,
                columnas["talle"],
                columnas["stock"],
                columnas["marca"],
                columnas["rubro"],
                columnas["publico"],
                columnas["color"],
                columnas["codigo"]
            )
            return limpiar_nan_en_objeto({
                "tipo": "lista",
                "items": grupos,
                "voz": f"Encontré {len(grupos)} modelos en talle {talle_buscado}.",
                "fuente": {}
            })

    # --------------------------------------------------------
    # BÚSQUEDA UNIVERSAL
    # --------------------------------------------------------
    df_universal = buscar_universal(df, keywords, columnas)

    if not df_universal.empty:
        grupos = agrupar_por_modelo(
            df_universal,
            col_desc,
            columnas["talle"],
            columnas["stock"],
            columnas["marca"],
            columnas["rubro"],
            columnas["publico"],
            columnas["color"],
            columnas["codigo"]
        )
        return limpiar_nan_en_objeto({
            "tipo": "lista",
            "items": grupos,
            "voz": f"Encontré {len(grupos)} modelos relacionados.",
            "fuente": {}
        })

    # --------------------------------------------------------
    # SIN RESULTADOS
    # --------------------------------------------------------
    return limpiar_nan_en_objeto({
        "tipo": "lista",
        "items": [],
        "voz": "No encontré resultados para tu búsqueda.",
        "fuente": {}
    })

# ===== FIN INDEXER UNIVERSAL + AUTOCOMPLETADO =====
