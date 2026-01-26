# ===== INDEXER UNIVERSAL COMPLETO =====

import pandas as pd
import re

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
# EXTRAER KEYWORDS
# ------------------------------------------------------------

def extraer_keywords(pregunta):
    p = normalizar(pregunta)
    tokens = re.split(r"\W+", p)
    stopwords = ["que","hay","tengo","de","el","la","los","las","un","una","stock","en","para","con"]
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
        "mochila","plush","avengers","marvel"
    ]

    columnas_prohibidas = ["rubro","marca","color","talle","cantidad","stock","precio"]

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

    # fallback: columna con más texto
    return max(df.columns, key=lambda c: df[c].astype(str).str.len().mean())

# ------------------------------------------------------------
# AGRUPACIÓN POR MODELO
# ------------------------------------------------------------

def _orden_talle(t):
    try:
        return int(re.sub(r"\D", "", str(t)))
    except:
        return 9999

def agrupar_por_modelo(df, col_desc, col_talle, col_stock, col_marca, col_rubro, col_publico, col_color, col_codigo):
    grupos = []

    for desc, g in df.groupby(col_desc):
        talles = []
        if col_talle and col_stock:
            for tl, gg in g.groupby(col_talle):
                stock_t = int(sum(parse_numero(v) for v in gg[col_stock]))
                talles.append({"talle": str(tl).strip(), "stock": stock_t})

        grupos.append({
            "descripcion": desc,
            "marca": g[col_marca].iloc[0] if col_marca else "",
            "rubro": g[col_rubro].iloc[0] if col_rubro else "",
            "precio": parse_numero(g[col_publico].iloc[0]) if col_publico else None,
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
# PROCESAR PREGUNTA
# ------------------------------------------------------------

def procesar_pregunta(df, pregunta):
    pregunta_norm = normalizar(pregunta)
    keywords = extraer_keywords(pregunta)

    columnas = {
        "rubro": next((c for c in df.columns if "rubro" in c.lower()), None),
        "marca": next((c for c in df.columns if "marca" in c.lower()), None),
        "codigo": next((c for c in df.columns if "art" in c.lower() or "codigo" in c.lower()), None),
        "color": next((c for c in df.columns if "color" in c.lower()), None),
        "talle": next((c for c in df.columns if "talle" in c.lower()), None),
        "stock": next((c for c in df.columns if "cant" in c.lower() or "stock" in c.lower()), None),
        "publico": next((c for c in df.columns if "lista" in c.lower() or "precio" in c.lower()), None),
        "valorizado": next((c for c in df.columns if "valoriz" in c.lower()), None),
    }

    col_desc = detectar_columna_descripcion(df)
    columnas["descripcion"] = col_desc

    # --------------------------------------------------------
    # BÚSQUEDA POR CÓDIGO EXACTO
    # --------------------------------------------------------
    if columnas["codigo"]:
        df[columnas["codigo"]] = df[columnas["codigo"]].astype(str).str.strip()
        if pregunta_norm.upper() in df[columnas["codigo"]].values:
            fila = df[df[columnas["codigo"]] == pregunta_norm.upper()].iloc[0]
            return {
                "tipo": "producto",
                "data": {
                    "descripcion": fila[col_desc],
                    "marca": fila[columnas["marca"]],
                    "rubro": fila[columnas["rubro"]],
                    "precio": parse_numero(fila[columnas["publico"]]),
                    "stock_total": parse_numero(fila[columnas["stock"]]),
                    "talles": [],
                    "color": fila[columnas["color"]],
                    "codigo": fila[columnas["codigo"]]
                },
                "voz": f"Encontré el artículo {fila[col_desc]}",
                "fuente": {}
            }

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
            return {
                "tipo": "lista",
                "items": grupos,
                "voz": f"Encontré {len(grupos)} modelos en talle {talle_buscado}.",
                "fuente": {}
            }

    # --------------------------------------------------------
    # BÚSQUEDA UNIVERSAL (marca, rubro, color, descripción, etc.)
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
        return {
            "tipo": "lista",
            "items": grupos,
            "voz": f"Encontré {len(grupos)} modelos relacionados.",
            "fuente": {}
        }

    # --------------------------------------------------------
    # SIN RESULTADOS
    # --------------------------------------------------------
    return {
        "tipo": "lista",
        "items": [],
        "voz": "No encontré resultados para tu búsqueda.",
        "fuente": {}
    }

# ===== FIN INDEXER UNIVERSAL =====
