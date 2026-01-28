# ===== INDEXER UNIVERSAL v3.5 PRO =====
# Mejoras:
# - Normalización fuerte (unicode, espacios invisibles, dobles espacios)
# - Coincidencia por tokens flexibles
# - Coincidencia por raíz (stemming simple)
# - Coincidencia por prefijo + substring
# - Coincidencia por similitud (levenshtein light)
# - Mantiene: códigos, talles, rangos, resumen, agrupado, autocomplete

import pandas as pd
import re
import math

# ------------------------------------------------------------
# NORMALIZACIÓN FUERTE
# ------------------------------------------------------------

ESPACIOS_RAROS = [
    "\u00A0",  # NO-BREAK SPACE
    "\u2007",  # FIGURE SPACE
    "\u202F",  # NARROW NO-BREAK SPACE
    "\u200B",  # ZERO WIDTH SPACE
    "\u200C",  # ZERO WIDTH NON-JOINER
    "\u200D",  # ZERO WIDTH JOINER
    "\u2060",  # WORD JOINER
]

def _limpiar_espacios_raros(texto: str) -> str:
    for ch in ESPACIOS_RAROS:
        texto = texto.replace(ch, " ")
    return texto

def _normalizar_unicode_basico(texto: str) -> str:
    # reemplazo de acentos y caracteres comunes
    reemplazos = {
        "á": "a", "à": "a", "ä": "a", "â": "a",
        "é": "e", "è": "e", "ë": "e", "ê": "e",
        "í": "i", "ì": "i", "ï": "i", "î": "i",
        "ó": "o", "ò": "o", "ö": "o", "ô": "o",
        "ú": "u", "ù": "u", "ü": "u", "û": "u",
        "ñ": "n",
    }
    res = []
    for c in texto:
        res.append(reemplazos.get(c, c))
    return "".join(res)

def normalizar(texto):
    if not isinstance(texto, str):
        texto = str(texto)
    texto = texto.strip().lower()
    texto = _limpiar_espacios_raros(texto)
    texto = re.sub(r"\s+", " ", texto)  # dobles espacios
    texto = _normalizar_unicode_basico(texto)
    return texto

# ------------------------------------------------------------
# STEMMING SIMPLE (raíz)
# ------------------------------------------------------------

def raiz(palabra: str) -> str:
    palabra = normalizar(palabra)
    # recorte de sufijos típicos muy simples
    sufijos = [
        "es", "s", "itos", "itas", "ito", "ita",
        "ones", "onas", "on", "ona",
        "nes", "nas",
    ]
    for suf in sufijos:
        if palabra.endswith(suf) and len(palabra) > len(suf) + 2:
            palabra = palabra[: -len(suf)]
            break
    return palabra

# ------------------------------------------------------------
# PARSE NUMÉRICO
# ------------------------------------------------------------

def parse_numero(valor):
    if pd.isna(valor):
        return 0
    if isinstance(valor, (int, float)):
        return valor
    v = str(valor)
    v = v.replace(".", "").replace(",", ".")
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
        nombre = normalizar(str(c))
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
        "pantalon","short",
        "botin","botines","media","medias",
        "guante","guantes","bolsa","boxeo",
        "mochila","plush","avengers","marvel","slide","bermuda"
    ]

    columnas_prohibidas = ["rubro","marca","color","talle","cantidad","stock","precio","lista","valoriz"]

    cols_norm = {c: normalizar(c) for c in df.columns}
    puntajes = {}

    for col in df.columns:
        nombre = cols_norm[col]
        if any(p in nombre for p in columnas_prohibidas):
            continue

        serie = df[col].astype(str).map(normalizar)
        puntaje = 0
        for kw in keywords:
            puntaje += serie.str.contains(kw, na=False).sum()
        puntajes[col] = puntaje

    if puntajes and max(puntajes.values()) > 0:
        return max(puntajes, key=puntajes.get)

    # fallback: columna con texto más largo promedio
    return max(df.columns, key=lambda c: df[c].astype(str).str.len().mean())

# ------------------------------------------------------------
# DECODIFICACIÓN DE CÓDIGOS DE BARRA
# ------------------------------------------------------------

def decodificar_codigo_barras(cadena):
    s = cadena.replace("!!", "!").replace("¡", "!").replace("//", "/")
    s = _limpiar_espacios_raros(s)
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
    tokens = [normalizar(t) for t in texto.split() if t.strip()]

    if any(re.fullmatch(r"\d{1,2}(\.\d+)?", t) for t in tokens):
        return {"tipo": "talle", "tokens": tokens}

    if "entre" in texto and "y" in texto:
        return {"tipo": "rango_precio", "tokens": tokens}

    if columnas.get("marca"):
        marcas = df[columnas["marca"]].astype(str).map(normalizar).unique()
        for t in tokens:
            if t in marcas:
                return {"tipo": "marca", "tokens": tokens}

    if columnas.get("rubro"):
        rubros = df[columnas["rubro"]].astype(str).map(normalizar).unique()
        for t in tokens:
            if t in rubros:
                return {"tipo": "rubro", "tokens": tokens}

    if columnas.get("color"):
        colores = df[columnas["color"]].astype(str).map(normalizar).unique()
        for t in tokens:
            if t in colores:
                return {"tipo": "color", "tokens": tokens}

    if len(tokens) > 1:
        return {"tipo": "mixto", "tokens": tokens}

    return {"tipo": "texto", "tokens": tokens}

# ------------------------------------------------------------
# RESUMEN INTELIGENTE
# ------------------------------------------------------------

def generar_resumen(df, columnas):
    resumen = {}

    resumen["cant_articulos"] = df[columnas["codigo"]].nunique() if columnas.get("codigo") else len(df)
    resumen["unidades_totales"] = int(df[columnas["stock"]].apply(parse_numero).sum()) if columnas.get("stock") else None
    resumen["valorizado_total"] = float(df[columnas["valorizado"]].apply(parse_numero).sum()) if columnas.get("valorizado") else None

    if columnas.get("publico"):
        precios = df[columnas["publico"]].apply(parse_numero)
        precios = precios[precios > 0]
        if len(precios) > 0:
            resumen["precio_min"] = float(precios.min())
            resumen["precio_max"] = float(precios.max())
        else:
            resumen["precio_min"] = None
            resumen["precio_max"] = None

    if columnas.get("rubro"):
        resumen["rubros"] = sorted(set(df[columnas["rubro"]].astype(str)))

    if columnas.get("color"):
        resumen["colores"] = sorted(set(df[columnas["color"]].astype(str)))

    if columnas.get("talle"):
        resumen["talles"] = sorted(set(df[columnas["talle"]].astype(str)))

    if columnas.get("marca"):
        resumen["marcas"] = sorted(set(df[columnas["marca"]].astype(str)))

    return resumen

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
        columnas.get("descripcion"),
        columnas.get("marca"),
        columnas.get("rubro"),
        columnas.get("color"),
        columnas.get("codigo"),
        columnas.get("talle"),
    ]

    for col in columnas_relevantes:
        if col:
            for v in df[col].astype(str).tolist():
                v_norm = normalizar(v)
                tokens = re.split(r"\W+", v_norm)
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
# LEVENSHTEIN LIGHT (DISTANCIA)
# ------------------------------------------------------------

def distancia_light(a: str, b: str) -> int:
    a = normalizar(a)
    b = normalizar(b)
    if a == b:
        return 0
    if not a or not b:
        return max(len(a), len(b))
    # versión muy simple: conteo de diferencias de longitud + mismatches
    diff_len = abs(len(a) - len(b))
    mismatches = sum(1 for x, y in zip(a, b) if x != y)
    return diff_len + mismatches

def es_similar(a: str, b: str, max_dist: int = 2) -> bool:
    return distancia_light(a, b) <= max_dist

# ------------------------------------------------------------
# MATCH FLEXIBLE (RAÍZ + PREFIJO + SUBSTRING + SIMILITUD)
# ------------------------------------------------------------

def match_flexible(texto: str, token: str) -> bool:
    if not texto:
        return False
    t_norm = normalizar(texto)
    q_norm = normalizar(token)

    if not q_norm:
        return False

    # raíz
    t_r = raiz(t_norm)
    q_r = raiz(q_norm)

    # 1) raíz dentro de raíz
    if q_r and t_r and q_r in t_r:
        return True

    # 2) prefijo
    if t_norm.startswith(q_norm) or t_r.startswith(q_r):
        return True

    # 3) substring
    if q_norm in t_norm:
        return True

    # 4) similitud light
    if es_similar(t_r, q_r, max_dist=2):
        return True

    return False

# ------------------------------------------------------------
# PROCESAR PREGUNTA PRINCIPAL
# ------------------------------------------------------------

def procesar_pregunta(df, pregunta):
    pregunta_norm = normalizar(pregunta)

    columnas = {
        "rubro": next((c for c in df.columns if "rubro" in normalizar(c)), None),
        "marca": next((c for c in df.columns if "marca" in normalizar(c)), None),
        "codigo": detectar_columna_codigo(df),
        "color": next((c for c in df.columns if "color" in normalizar(c)), None),
        "talle": next((c for c in df.columns if "talle" in normalizar(c)), None),
        "stock": next((c for c in df.columns if "cant" in normalizar(c) or "stock" in normalizar(c)), None),
        "publico": next((c for c in df.columns if "lista" in normalizar(c) or "precio" in normalizar(c)), None),
        "valorizado": next((c for c in df.columns if "valoriz" in normalizar(c)), None),
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
    if any(sep in pregunta for sep in ["/", "!", "¡"]):
        articulo, color_extra, talle_extra = decodificar_codigo_barras(pregunta)

        if articulo and columnas.get("codigo"):
            df_art = df[df[columnas["codigo"]].astype(str).str.strip() == articulo]

            if df_art.empty:
                return {"tipo": "lista", "items": [], "voz": "No encontré ese artículo."}

            if talle_extra and columnas.get("talle"):
                df_art = df_art[
                    df_art[columnas["talle"]].astype(str).map(normalizar).str.contains(normalizar(str(talle_extra)), na=False)
                ]

            if color_extra and columnas.get("color"):
                df_art = df_art[
                    df_art[columnas["color"]].astype(str).map(normalizar).str.contains(normalizar(color_extra), na=False)
                ]

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
    if columnas.get("codigo"):
        df[columnas["codigo"]] = df[columnas["codigo"]].astype(str).str.strip()
        if pregunta_norm.upper() in df[columnas["codigo"]].values:
            fila = df[df[columnas["codigo"]] == pregunta_norm.upper()].iloc[0]
            return limpiar_nan_en_objeto({
                "tipo": "producto",
                "data": {
                    "descripcion": fila[col_desc],
                    "marca": fila[columnas["marca"]] if columnas.get("marca") else "",
                    "rubro": fila[columnas["rubro"]] if columnas.get("rubro") else "",
                    "precio": parse_numero(fila[columnas["publico"]]) if columnas.get("publico") else None,
                    "stock_total": parse_numero(fila[columnas["stock"]]) if columnas.get("stock") else None,
                    "talles": [],
                    "color": fila[columnas["color"]] if columnas.get("color") else "",
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
    if intent["tipo"] == "rango_precio" and columnas.get("publico"):
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
    # TOKENS ÚTILES + MATCH FLEXIBLE
    # --------------------------------------------------------
    stopwords = {
        "que","qué","hay","en","de","el","la","los","las",
        "un","una","unos","unas","stock",
        "dime","decime","mostrame","mostra","busca","buscar",
        "cuanto","cuánta","cuanta","cuantos","cuántos","cuantas","cuántas"
    }

    tokens = [t for t in intent["tokens"] if t not in stopwords]
    if not tokens:
        tokens = [pregunta_norm]

    df_filtrado = df.copy()

    # Caso especial: búsqueda por talle
    if intent["tipo"] == "talle" and columnas.get("talle"):
        numero_talle = next((t for t in tokens if re.fullmatch(r"\d{1,2}(\.\d+)?", t)), None)
        if numero_talle:
            df_filtrado = df_filtrado[
                df_filtrado[columnas["talle"]].astype(str).map(normalizar).str.contains(normalizar(numero_talle), na=False)
            ]
    else:
        # MATCH FLEXIBLE SOBRE COLUMNAS RELEVANTES
        columnas_relevantes = [
            columnas.get("descripcion"),
            columnas.get("marca"),
            columnas.get("rubro"),
            columnas.get("color"),
            columnas.get("codigo"),
            columnas.get("talle"),
        ]
        for t in tokens:
            mask_global = False
            for col in columnas_relevantes:
                if not col:
                    continue
                serie = df_filtrado[col].astype(str)
                mask_col = serie.map(lambda x: match_flexible(x, t))
                mask_global = mask_global | mask_col
            df_filtrado = df_filtrado[mask_global]

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
            "voz": "Resumen generado para tu consulta."
        })

    return {"tipo": "lista", "items": [], "voz": "No encontré resultados."}
