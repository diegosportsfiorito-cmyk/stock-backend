# ===== INICIO INDEXER COMPLETO =====

import pandas as pd
import re

# ------------------------------------------------------------
# NORMALIZACIÓN Y UTILIDADES
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
# DETECCIÓN INTELIGENTE DE COLUMNA DESCRIPCIÓN
# ------------------------------------------------------------

def detectar_columna_descripcion(df):
    """
    Detecta la columna correcta de descripción aunque:
    - haya varias columnas llamadas 'Descripción'
    - cambie el orden
    - cambien los nombres
    - haya columnas vacías
    """

    keywords = [
        "pantufla", "pantuflas", "pantu",
        "zapatilla", "zapatillas", "zapa",
        "remera", "camiseta",
        "buzo", "campera",
        "pantalon", "pantalón",
        "short",
        "botin", "botines",
        "media", "medias",
        "guante", "guantes",
        "bolsa", "boxeo",
        "mochila",
        "plush",
        "avengers", "marvel"
    ]

    columnas_prohibidas = ["rubro", "marca", "color", "talle", "cantidad", "stock", "precio"]

    cols_norm = {c: c.lower().strip() for c in df.columns}

    puntajes = {}

    for col in df.columns:
        nombre = cols_norm[col]

        # descartar columnas prohibidas
        if any(p in nombre for p in columnas_prohibidas):
            continue

        serie = df[col].astype(str).str.lower()

        # descartar columnas vacías
        if serie.replace("", None).count() == 0:
            continue

        puntaje = 0

        for kw in keywords:
            puntaje += serie.str.contains(kw, na=False).sum()

        puntajes[col] = puntaje

    # Si alguna columna tiene coincidencias → esa es la descripción
    if puntajes and max(puntajes.values()) > 0:
        return max(puntajes, key=puntajes.get)

    # Fallback: elegir la columna con más texto útil
    mejor_col = None
    mejor_score = -1

    for col in df.columns:
        nombre = cols_norm[col]

        if any(p in nombre for p in columnas_prohibidas):
            continue

        serie = df[col].astype(str)
        score = serie.str.len().mean()

        if score > mejor_score:
            mejor_score = score
            mejor_col = col

    return mejor_col

# ------------------------------------------------------------
# AGRUPACIÓN Y BÚSQUEDAS
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

def buscar_por_descripcion(df, col_desc, texto):
    t = normalizar(texto)
    return df[df[col_desc].astype(str).str.lower().str.contains(t)]

# ------------------------------------------------------------
# PROCESAR PREGUNTA
# ------------------------------------------------------------

def procesar_pregunta(df, pregunta):
    pregunta_norm = normalizar(pregunta)

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

    # Detectar columna correcta de descripción
    col_desc = detectar_columna_descripcion(df)
    columnas["descripcion"] = col_desc

    # Búsqueda por código exacto
    col_codigo = columnas["codigo"]
    if col_codigo:
        df[col_codigo] = df[col_codigo].astype(str).str.strip()
        if pregunta_norm.upper() in df[col_codigo].values:
            fila = df[df[col_codigo] == pregunta_norm.upper()].iloc[0]
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
                    "codigo": fila[col_codigo]
                },
                "voz": f"Encontré el artículo {fila[col_desc]}",
                "fuente": {}
            }

    # Búsqueda por descripción
    encontrados = buscar_por_descripcion(df, col_desc, pregunta_norm)
    if not encontrados.empty:
        grupos = agrupar_por_modelo(
            encontrados,
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

    # Sin resultados
    return {
        "tipo": "lista",
        "items": [],
        "voz": "No encontré resultados para tu búsqueda.",
        "fuente": {}
    }

# ===== FIN INDEXER COMPLETO =====
