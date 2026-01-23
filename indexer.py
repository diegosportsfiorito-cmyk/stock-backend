import io
import re
from typing import List, Dict, Tuple, Optional
import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

# ============================================================
# ALIAS PARA COLUMNAS OPCIONALES
# ============================================================

ALIAS = {
    "marca": ["marca", "brand"],
    "rubro": ["rubro", "categoria", "cat"],
    "grupo": ["grupo", "grupo_articulo"],
    "proveedor": ["proveedor", "supplier"],
}

def obtener_columna(df, posibles):
    for col in df.columns:
        c = str(col).strip().lower().replace(" ", "").replace(".", "").replace("/", "").replace("_", "")
        for p in posibles:
            p_norm = p.strip().lower().replace(" ", "").replace(".", "").replace("/", "").replace("_", "")
            if c == p_norm:
                return col
    return None

# ============================================================
# DETECCIÓN AUTOMÁTICA DE COLUMNA CÓDIGO
# ============================================================

def detectar_columna_codigo(df: pd.DataFrame) -> Optional[str]:
    posibles = ["articulo", "artículo", "codigo", "cod", "id", "sku"]
    for col in df.columns:
        if str(col).strip().lower().replace(" ", "") in posibles:
            return col

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
# DETECCIÓN AUTOMÁTICA DE COLUMNAS DE PRECIO
# ============================================================

def detectar_columnas_precio(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    col_publico = None
    col_costo = None

    for col in df.columns:
        c = str(col).strip().lower().replace(" ", "").replace("_", "").replace(".", "")
        if c in ["lista1", "precio1", "publico", "ppublico"]:
            col_publico = col
        if c in ["lista0", "precio0", "costo", "pcosto"]:
            col_costo = col

    return col_publico, col_costo

def detectar_columna_stock(df: pd.DataFrame) -> Optional[str]:
    posibles = ["stock", "stockal", "stocka/l", "stocka_l", "stock_a/l", "stock_a_l"]
    for col in df.columns:
        c = str(col).strip().lower().replace(" ", "").replace("_", "").replace("/", "")
        if c in posibles:
            return col
    return None

# ============================================================
# NORMALIZACIÓN
# ============================================================

def normalizar_encabezados(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .map(lambda x: str(x))
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
        if len(partes) == 2 and partes[0].replace(".", "").isdigit() and partes[1].replace(".", "").isdigit():
            return f"{partes[0]}/{partes[1]}"
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
# DETECCIÓN DE CÓDIGO EN LA PREGUNTA
# ============================================================

def detectar_codigo_en_pregunta(pregunta: str, codigos_existentes: List[str]) -> str:
    tokens = re.findall(r"[A-Za-z0-9\-]{2,}", pregunta, flags=re.IGNORECASE)
    codigos_set = {str(c).strip().upper() for c in codigos_existentes}

    candidatos = []
    for t in tokens:
        if any(ch.isdigit() for ch in t):
            candidatos.append(t)

    for t in reversed(candidatos):
        if t.upper() in codigos_set:
            return t

    return candidatos[-1] if candidatos else ""

# ============================================================
# LECTURA DEL EXCEL
# ============================================================

def cargar_excel_completo(file_bytes) -> pd.DataFrame:
    excel_file = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(excel_file)

    df_total = []

    for sheet in xls.sheet_names:
        df = xls.parse(sheet, dtype=str).fillna("")
        df = normalizar_encabezados(df)

        for col in df.columns:
            df[col] = df[col].astype(str).str.strip()

        df_total.append(df)

    if not df_total:
        return pd.DataFrame()

    df = pd.concat(df_total, ignore_index=True)
    return df

# ============================================================
# BÚSQUEDA POR DESCRIPCIÓN + AGRUPACIÓN + CONTEXTO IA
# ============================================================

def tokenizar(texto: str) -> List[str]:
    texto = texto.lower().strip()
    tokens = re.split(r"\W+", texto)
    return [t for t in tokens if len(t) > 2]

def buscar_por_descripcion(df: pd.DataFrame, pregunta: str) -> pd.DataFrame:
    if "descripcion" not in df.columns:
        return pd.DataFrame()

    desc = df["descripcion"].astype(str).str.lower()
    tokens = tokenizar(pregunta)

    if not tokens:
        return pd.DataFrame()

    mask = True
    for t in tokens:
        mask = mask & desc.str.contains(t, na=False)

    resultados = df[mask].copy()

    if resultados.empty:
        return resultados

    resultados["score"] = resultados["descripcion"].str.count("|".join(tokens))
    resultados = resultados.sort_values("score", ascending=False)

    return resultados

def agrupar_por_modelo(df: pd.DataFrame) -> List[Dict]:
    grupos = []

    for desc, grupo in df.groupby("descripcion"):
        talles = sorted({str(t).strip() for t in grupo.get("talle", []) if str(t).strip()})
        stock_total = 0

        if "stock" in grupo.columns:
            stock_total = int(sum(parse_numero(v) for v in grupo["stock"]))

        grupos.append({
            "descripcion": desc,
            "talles": talles,
            "stock_total": stock_total,
            "items": grupo.to_dict(orient="records")
        })

    grupos = sorted(grupos, key=lambda x: x["stock_total"], reverse=True)
    return grupos

def generar_contexto_para_ia(grupos: List[Dict], top_n: int = 5) -> str:
    if not grupos:
        return "No se encontraron artículos relevantes."

    grupos = grupos[:top_n]

    lineas = []
    for g in grupos:
        linea = f"- {g['descripcion']} | Stock total: {g['stock_total']} | Talles: {', '.join(g['talles']) if g['talles'] else 'N/A'}"
        lineas.append(linea)

    return "\n".join(lineas)

# ============================================================
# BÚSQUEDA PRINCIPAL (CÓDIGO + DESCRIPCIÓN)
# ============================================================

def buscar_articulo_en_archivos(pregunta: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)

    def _key(a):
        return a.get("modifiedTime", "")

    archivos_ordenados = sorted(archivos, key=_key, reverse=True)

    for archivo in archivos_ordenados:
        nombre = archivo["name"]
        file_id = archivo["id"]
        mod_time = archivo.get("modifiedTime")

        if not nombre.lower().endswith(".xlsx"):
            continue

        try:
            file_bytes = descargar_archivo_por_id(file_id)
            df = cargar_excel_completo(file_bytes)

            if df.empty:
                continue

            col_codigo = detectar_columna_codigo(df)
            col_desc = obtener_columna(df, ["descripcion_color", "descripcion_original", "descripcion"])

            if col_desc:
                df["descripcion"] = df[col_desc].astype(str).apply(normalizar_descripcion)

            # ============================================================
            # 1) BÚSQUEDA POR CÓDIGO
            # ============================================================
            if col_codigo:
                codigos_existentes = df[col_codigo].astype(str).tolist()
                codigo = detectar_codigo_en_pregunta(pregunta, codigos_existentes)

                if codigo:
                    df_art = df[df[col_codigo].astype(str).str.strip().str.upper() == codigo.upper()]
                    if not df_art.empty:
                        fila = df_art.iloc[0]

                        col_publico, col_costo = detectar_columnas_precio(df)
                        col_stock = detectar_columna_stock(df)

                        precio_publico = parse_numero(fila.get(col_publico)) if col_publico else None
                        precio_costo = parse_numero(fila.get(col_costo)) if col_costo else None

                        stock_total = None
                        if col_stock:
                            stock_total = int(sum(parse_numero(v) for v in df_art[col_stock]))

                        talles = []
                        if "talle" in df_art.columns:
                            talles = sorted({normalizar_talle(t) for t in df_art["talle"]})

                        info_articulo = {
                            "codigo": codigo.upper(),
                            "descripcion": normalizar_descripcion(fila.get("descripcion", "")),
                            "precio_publico": precio_publico,
                            "precio_costo": precio_costo,
                            "stock_total": stock_total,
                            "talles": talles,
                            "contexto": f"{codigo.upper()} | {normalizar_descripcion(fila.get('descripcion',''))} | Stock: {stock_total} | Talles: {', '.join(talles)}"
                        }

                        fuente = {
                            "archivo": nombre,
                            "fecha": mod_time or "Fecha desconocida",
                        }

                        return info_articulo, fuente

            # ============================================================
            # 2) BÚSQUEDA POR DESCRIPCIÓN
            # ============================================================
            if col_desc:
                encontrados = buscar_por_descripcion(df, pregunta)

                if not encontrados.empty:
                    grupos = agrupar_por_modelo(encontrados)
                    contexto = generar_contexto_para_ia(grupos, top_n=5)

                    info_articulo = {
                        "lista_completa": grupos,
                        "contexto": contexto
                    }

                    fuente = {
                        "archivo": nombre,
                        "fecha": mod_time or "Fecha desconocida",
                    }

                    return info_articulo, fuente

        except Exception as e:
            print(f"ERROR procesando archivo {nombre}: {e}")
            continue

    return None, None

