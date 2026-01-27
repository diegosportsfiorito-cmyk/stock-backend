# ===== INDEXER UNIVERSAL v3.0 PRO =====
# Optimizado para búsquedas reales, coincidencias parciales,
# talles, plural/singular y consultas naturales.

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
# DETECTAR COLUMNA DE CÓDIGO
# ------------------------------------------------------------

def detectar_columna_codigo(df):
    for c in df.columns:
        nombre = c.lower()
        nombre = nombre.replace("í","i").replace("ó","o").replace("á","a").replace("é","e").replace("ú","u")
        if any(k in nombre for k in ["art", "codigo", "cod", "sku", "ean"]):
            return c
    return None

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
# DECODIFICACIÓN DE CÓDIGOS DE BARRA
# ------------------------------------------------------------

def decodificar_codigo_barras(cadena):
    s = cadena.replace("!!", "!").replace("¡", "!").replace("//", "/")
    partes = re.split(r"[!/]", s)
    partes = [p.strip() for p in partes if p.strip()]

    if not partes:
        return None, None, None

    articulo = partes[0]
    color = None
    talle = None

    if len(partes) == 1:
        return articulo, color, talle

    ultimo = partes[-1]

    if re.fullmatch(r"\d+(\.\d+)?", ultimo):
        talle = ultimo
        if len(partes) >= 3:
            color = " ".join(partes[1:-1])
    else:
        color = " ".join(partes[1:])

    return articulo, color, talle

# ------------------------------------------------------------
# DETECTAR INTENCIÓN
# ------------------------------------------------------------

def detectar_intencion(texto, columnas, df):
    tokens = texto.split()

    # Talle
    if any(re.fullmatch(r"\d{1,2}(\.\d+)?", t) for t in tokens):
        return {"tipo": "talle", "tokens": tokens}

    # Rango de precios
    if "entre" in texto and "y" in texto:
        return {"tipo": "rango_precio", "tokens": tokens}

    # Marca
    if columnas["marca"]:
        marcas = df[columnas["marca"]].astype(str).str.lower().unique()
        for t in tokens:
            if t in marcas:
                return {"tipo": "marca", "tokens": tokens}

    # Rubro
    if columnas["rubro"]:
        rubros = df[columnas["rubro"]].astype(str).str.lower().unique()
        for t in tokens:
            if t in rubros:
                return {"tipo": "rubro", "tokens": tokens}

    # Color
    if columnas["color"]:
        colores = df[columnas["color"]].astype(str).str.lower().unique()
        for t in tokens:
            if t in colores:
                return {"tipo": "color", "tokens": tokens}

    # Mixto
    if len(tokens) > 1:
        return {"tipo": "mixto", "tokens": tokens}

    return {"tipo": "texto", "tokens": tokens}

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
# AUTOCOMPLETADO
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
                    if len(t) >= 1:
                        palabras.add(t)

    return sorted(list(palabras))

def autocompletar(df, columnas, texto):
    texto = normalizar(texto)
    if len(texto) < 1:
        return []

    dicc = generar_diccionario_autocompletado(df, columnas)
    sugerencias = [p for p in dicc if p.startswith(texto)]
    return sugerencias[:12]
# ------------------------------------------------------------
# PROCESAR PREGUNTA PRINCIPAL (OPTIMIZADO)
# ------------------------------------------------------------

def procesar_pregunta(df, pregunta):
    pregunta_norm = normalizar(pregunta)

    columnas = {
        "rubro": next((c for c in df.columns if "rubro" in c.lower()), None),
        "marca": next((c for c in df.columns if "marca" in c.lower()), None),
        "codigo": detectar_columna_codigo(df),
        "color": next((c for c in df.columns if "color" in c.lower()), None),
        "talle": next((c for c in df.columns if "talle" in c.lower()), None),
        "stock": next((c for c in df.columns if "cant" in c.lower() or "stock" in c.lower()), None),
        "publico": next((c for c in df.columns if "lista" in c.lower() or "precio" in c.lower()), None),
        "valorizado": next((c for c in df.columns if "valoriz" in c.lower()), None),
    }

    col_desc = detectar_columna_descripcion(df)
    columnas["descripcion"] = col_desc

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
    # CÓDIGO DE BARRAS
    # --------------------------------------------------------
    if any(sep in pregunta_norm for sep in ["/", "!", "¡"]):
        articulo, color_extra, talle_extra = decodificar_codigo_barras(pregunta_norm)

        if articulo and columnas["codigo"]:
            df_art = df[df[columnas["codigo"]].astype(str).str.strip() == articulo]

            if df_art.empty:
                return {"tipo": "lista", "items": [], "voz": "No encontré ese artículo."}

            if talle_extra and columnas["talle"]:
                df_art = df_art[df_art[columnas["talle"]].astype(str).str.contains(str(talle_extra), na=False)]

            if color_extra and columnas["color"]:
                df_art = df_art[df_art[columnas["color"]].astype(str).str.lower().str.contains(color_extra.lower(), na=False)]

            grupos = agrupar_por_modelo(
                df_art, col_desc, columnas["talle"], columnas["stock"],
                columnas["marca"], columnas["rubro"], columnas["publico"],
                columnas["color"], columnas["codigo"]
            )

            return limpiar_nan_en_objeto({
                "tipo": "lista",
                "items": grupos,
                "voz": "Código de barras interpretado correctamente."
            })

    # --------------------------------------------------------
    # CÓDIGO EXACTO
    # --------------------------------------------------------
    if columnas["codigo"]:
        df[columnas["codigo"]] = df[columnas["codigo"]].astype(str).str.strip()
        if pregunta_norm.upper() in df[columnas["codigo"]].values:
            fila = df[df[columnas["codigo"]] == pregunta_norm.upper()].iloc[0]
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
                "voz": f"Encontré el artículo {fila[col_desc]}"
            })

    # --------------------------------------------------------
    # DETECTAR INTENCIÓN
    # --------------------------------------------------------
    intent = detectar_intencion(pregunta_norm, columnas, df)

    # --------------------------------------------------------
    # RANGO DE PRECIOS
    # --------------------------------------------------------
    if intent["tipo"] == "rango_precio" and columnas["publico"]:
        m = re.search(r"entre\s+(\d+)\s+y\s+(\d+)", pregunta_norm)
        if m:
            pmin, pmax = float(m.group(1)), float(m.group(2))
            precios = df[columnas["publico"]].apply(parse_numero)
            df_rango = df[(precios >= pmin) & (precios <= pmax)]

            if not df_rango.empty:
                resumen = generar_resumen(df_rango, columnas)
                grupos = agrupar_por_modelo(
                    df_rango, col_desc, columnas["talle"], columnas["stock"],
                    columnas["marca"], columnas["rubro"], columnas["publico"],
                    columnas["color"], columnas["codigo"]
                )
                return limpiar_nan_en_objeto({
                    "tipo": "resumen",
                    "criterio": "rango_precio",
                    "resumen": resumen,
                    "items": grupos,
                    "voz": f"Encontré {resumen['cant_articulos']} artículos entre {pmin} y {pmax} pesos."
                })

    # --------------------------------------------------------
    # TOKENS ÚTILES + USO DE __search
    # --------------------------------------------------------
    stopwords = {"que", "qué", "hay", "en", "de", "el", "la", "los", "las", "un", "una", "unos", "unas", "stock", "dime","decime","mostrame","mostra","busca","buscar"}
    
    tokens = [t for t in intent["tokens"] if t not in stopwords]

    if not tokens:
        tokens = [pregunta_norm]

    df_filtrado = df.copy()

    # Caso especial: búsqueda por talle
    if intent["tipo"] == "talle" and columnas["talle"]:
        numero_talle = next((t for t in tokens if re.fullmatch(r"\d{1,2}(\.\d+)?", t)), None)
        if numero_talle:
            df_filtrado = df_filtrado[
                df_filtrado[columnas["talle"]].astype(str).str.contains(numero_talle, na=False)
            ]
    else:
        # Búsqueda general usando __search
        if "__search" in df_filtrado.columns:
            for t in tokens:
                df_filtrado = df_filtrado[
                    df_filtrado["__search"].astype(str).str.contains(t, na=False)
                ]
        else:
            # Fallback
            for t in tokens:
                mask = False
                for col in columnas.values():
                    if col:
                        mask = mask | df_filtrado[col].astype(str).str.lower().str.contains(t)
                df_filtrado = df_filtrado[mask]

    if not df_filtrado.empty:
        resumen = generar_resumen(df_filtrado, columnas)
        grupos = agrupar_por_modelo(
            df_filtrado, col_desc, columnas["talle"], columnas["stock"],
            columnas["marca"], columnas["rubro"], columnas["publico"],
            columnas["color"], columnas["codigo"]
        )
        return limpiar_nan_en_objeto({
            "tipo": "resumen",
            "criterio": intent["tipo"],
            "resumen": resumen,
            "items": grupos,
            "voz": f"Resumen generado para tu consulta."
        })

    return {"tipo": "lista", "items": [], "voz": "No encontré resultados."}

