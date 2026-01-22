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
            base = partes[0]
            siguiente = partes[1]
            return f"{base}/{siguiente}"
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
    """
    Busca en la pregunta tokens alfanuméricos que contengan dígitos
    y que además EXISTAN en la lista de códigos del Excel.
    """
    tokens = re.findall(r"[A-Za-z0-9\-]{2,}", pregunta, flags=re.IGNORECASE)
    codigos_set = {str(c).strip().upper() for c in codigos_existentes}

    candidatos = []
    for t in tokens:
        if any(ch.isdigit() for ch in t):
            candidatos.append(t)

    # Preferimos el ÚLTIMO token que exista en los códigos del Excel
    for t in reversed(candidatos):
        if t.upper() in codigos_set:
            return t

    # Si ninguno coincide, devolvemos el último candidato (por si acaso)
    return candidatos[-1] if candidatos else ""

# ============================================================
# LECTURA DEL EXCEL (GENÉRICA)
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
# BÚSQUEDA DE ARTÍCULO + PRECIOS EN TODOS LOS ARCHIVOS
# ============================================================

def buscar_articulo_en_archivos(pregunta: str) -> Tuple[Optional[Dict], Optional[Dict]]:
    """
    Recorre los archivos de la carpeta (ordenados por fecha DESC),
    busca el código mencionado en la pregunta y devuelve:
      - info_articulo: dict con código, descripción, precios, stock, talles
      - fuente: dict con archivo y fecha
    """
    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)

    # Ordenar por fecha de modificación (más nuevo primero)
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
            if not col_codigo:
                continue

            codigos_existentes = df[col_codigo].astype(str).tolist()
            codigo = detectar_codigo_en_pregunta(pregunta, codigos_existentes)

            if not codigo:
                continue

            df_art = df[df[col_codigo].astype(str).str.strip().str.upper() == codigo.upper()]
            if df_art.empty:
                continue

            col_publico, col_costo = detectar_columnas_precio(df)
            col_stock = detectar_columna_stock(df)

            fila = df_art.iloc[0]

            precio_publico = parse_numero(fila.get(col_publico)) if col_publico else None
            precio_costo = parse_numero(fila.get(col_costo)) if col_costo else None

            stock_total = None
            if col_stock:
                stock_total = int(
                    sum(parse_numero(v) for v in df_art[col_stock])
                )

            # Descripción: usamos la mejor columna disponible
            col_desc = obtener_columna(df, ["descripcion_color", "descripcion_original", "descripcion"])
            descripcion = normalizar_descripcion(fila.get(col_desc, "")) if col_desc else ""

            talles = []
            if "talle" in df_art.columns:
                talles = sorted({normalizar_talle(t) for t in df_art["talle"]})

            info_articulo = {
                "codigo": codigo.upper(),
                "descripcion": descripcion,
                "precio_publico": precio_publico,
                "precio_costo": precio_costo,
                "stock_total": stock_total,
                "talles": talles,
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

# ============================================================
# CONTEXTO PARA LA IA (SE MANTIENE POR SI LO USÁS PARA STOCK DETALLADO)
# ============================================================

def extraer_contenido_excel(file_bytes, nombre_archivo, pregunta):
    try:
        df = cargar_excel_completo(file_bytes)

        if df.empty:
            return "No se pudo leer contenido del Excel."

        col_codigo = detectar_columna_codigo(df)
        if not col_codigo:
            return "No se pudo detectar la columna de código."

        codigos_existentes = df[col_codigo].astype(str).tolist()
        codigo = detectar_codigo_en_pregunta(pregunta, codigos_existentes)

        if codigo:
            df_filtrado = df[df[col_codigo].astype(str).str.strip().str.upper() == codigo.upper()]
        else:
            # Si no hay código claro, filtramos por texto completo
            p = pregunta.lower()
            mask = df.apply(lambda row: p in str(row).lower(), axis=1)
            df_filtrado = df[mask]

        if df_filtrado.empty:
            return "No se encontraron artículos relevantes."

        col_stock = detectar_columna_stock(df)
        col_desc = obtener_columna(df, ["descripcion_color", "descripcion_original", "descripcion"])

        grupos = {}
        for _, row in df_filtrado.iterrows():
            cod = str(row.get(col_codigo, "")).strip()
            if not cod:
                continue

            if cod not in grupos:
                grupos[cod] = {
                    "descripcion": normalizar_descripcion(row.get(col_desc, "")) if col_desc else "",
                    "stock_total": 0,
                    "talles": {},
                }

            talle = normalizar_talle(row.get("talle", ""))
            stock_val = 0
            if col_stock:
                stock_val = int(parse_numero(row.get(col_stock, "0")))
            grupos[cod]["stock_total"] += stock_val
            grupos[cod]["talles"][talle] = grupos[cod]["talles"].get(talle, 0) + stock_val

        grupos = dict(list(grupos.items())[:10])

        partes = []
        for cod, info in grupos.items():
            talles = "\n".join([f"  - {t}: {s} unidades" for t, s in info["talles"].items()])
            partes.append(
                f"Artículo: {cod}\n"
                f"Descripción: {info['descripcion']}\n"
                f"Stock total: {info['stock_total']}\n"
                f"Talles:\n{talles}\n"
                "-------------------------\n"
            )

        return "\n".join(partes)

    except Exception as e:
        return f"ERROR LECTURA EXCEL: {e}"

def obtener_contexto_para_pregunta(pregunta: str) -> List[Dict]:
    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)

    def _key(a):
        return a.get("modifiedTime", "")

    archivos_ordenados = sorted(archivos, key=_key, reverse=True)

    contextos = []

    for archivo in archivos_ordenados:
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
