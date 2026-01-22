# indexer.py
import io
import re
from typing import List, Dict
from datetime import datetime

import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"


# ============================================================
# NORMALIZACIÓN
# ============================================================

MAPA_COLORES = {
    "NE": "NEGRO",
    "BLA": "BLANCO",
    "GRI": "GRIS",
    "AZ": "AZUL",
    "RO": "ROJO",
}


def normalizar_encabezados(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "", regex=False)
        .str.replace("á", "a")
        .str.replace("é", "e")
        .str.replace("í", "i")
        .str.replace("ó", "o")
        .str.replace("ú", "u")
    )
    return df


def parse_numero(valor) -> float:
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
        if len(partes) == 2 and partes[0].isdigit() and partes[1].isdigit():
            base = int(partes[0])
            siguiente = int(partes[1]) + 1
            return f"{base}-{siguiente}"
    return str(valor).strip()


def normalizar_color(valor: str) -> str:
    if not isinstance(valor, str):
        return ""
    partes = valor.replace("/", "-").split("-")
    partes_norm = []
    for p in partes:
        p = p.strip().upper()
        partes_norm.append(MAPA_COLORES.get(p, p))
    return "-".join(partes_norm)


def normalizar_descripcion(valor: str) -> str:
    if not isinstance(valor, str):
        return ""
    valor = valor.strip()
    valor = " ".join(valor.split())
    return valor.title()


# ============================================================
# AGRUPACIÓN
# ============================================================

def agrupar_por_articulo(df: pd.DataFrame) -> Dict[str, Dict]:
    grupos: Dict[str, Dict] = {}

    for _, row in df.iterrows():
        codigo = str(row.get("articulo", "")).strip()
        if not codigo or codigo == "0":
            continue

        descripcion = normalizar_descripcion(row.get("descripcion_original", ""))
        color = normalizar_color(row.get("color", ""))
        talle = normalizar_talle(row.get("talle", ""))
        stock = parse_numero(row.get("stock", "0"))
        lista1 = parse_numero(row.get("lista1", "0"))
        lista0 = parse_numero(row.get("lista0", "0"))
        ult_ingreso = str(row.get("ult_ingreso", "")).strip()

        if codigo not in grupos:
            grupos[codigo] = {
                "descripcion": descripcion,
                "color": color,
                "stock_total": 0,
                "talles": {},
                "precio_publico": lista1,
                "precio_costo": lista0,
                "ultimo_ingreso": ult_ingreso,
                "ventas": {}
            }

        grupos[codigo]["stock_total"] += int(stock)
        grupos[codigo]["talles"][talle] = grupos[codigo]["talles"].get(talle, 0) + int(stock)

        for col in df.columns:
            if col.isdigit():
                unidades = parse_numero(row.get(col, "0"))
                if unidades != 0:
                    grupos[codigo]["ventas"][col] = grupos[codigo]["ventas"].get(col, 0) + int(unidades)

    return grupos


# ============================================================
# FILTRADO
# ============================================================

def extraer_codigo_de_pregunta(pregunta: str) -> str:
    m = re.search(r"\b[\d\-]{4,}\b", pregunta.lower())
    return m.group(0) if m else ""


def filtrar_por_pregunta(df: pd.DataFrame, pregunta: str) -> pd.DataFrame:
    p = pregunta.lower()
    codigo = extraer_codigo_de_pregunta(p)

    if codigo and "articulo" in df.columns:
        mask = df["articulo"].astype(str).str.strip() == codigo
        filtrado = df[mask]
        if not filtrado.empty:
            return filtrado

    mask = df.apply(lambda row: p in str(row).lower(), axis=1)
    filtrado = df[mask]
    if not filtrado.empty:
        return filtrado

    return df


# ============================================================
# LECTURA DEL EXCEL
# ============================================================

def extraer_contenido_excel(file_bytes: bytes, nombre_archivo: str, pregunta: str) -> str:
    try:
        excel_file = io.BytesIO(file_bytes)
        xls = pd.ExcelFile(excel_file)

        df_total = []

        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name, dtype=str)
            df = df.fillna("")
            df = df.applymap(lambda x: str(x).strip())
            df = normalizar_encabezados(df)
            df_total.append(df)

        df = pd.concat(df_total, ignore_index=True)

        df = filtrar_por_pregunta(df, pregunta)

        grupos = agrupar_por_articulo(df)

        if not grupos:
            return "No se encontraron artículos relevantes."

        partes = []
        for codigo, info in grupos.items():
            talles = "\n".join([f"  - {t}: {s} unidades" for t, s in info["talles"].items()])
            ventas = "\n".join([f"  - {año}: {u} unidades" for año, u in info["ventas"].items()])

            partes.append(
                f"Artículo: {codigo}\n"
                f"Descripción: {info['descripcion']}\n"
                f"Color: {info['color']}\n"
                f"Precio público: {info['precio_publico']}\n"
                f"Precio costo: {info['precio_costo']}\n"
                f"Último ingreso: {info['ultimo_ingreso']}\n"
                f"Stock total: {info['stock_total']}\n"
                f"Talles:\n{talles}\n"
                f"Ventas:\n{ventas}\n"
                "-------------------------\n"
            )

        return "\n".join(partes)

    except Exception as e:
        return f"ERROR LECTURA EXCEL: {e}"


# ============================================================
# CONTEXTO PARA LA IA (CON DEBUG)
# ============================================================

def obtener_contexto_para_pregunta(pregunta: str) -> List[Dict]:
    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)
    contextos = []

    for archivo in archivos:
        nombre = archivo["name"]
        file_id = archivo["id"]
        mod_time = archivo.get("modifiedTime")

        if not nombre.lower().endswith(".xlsx"):
            continue

        file_bytes = descargar_archivo_por_id(file_id)
        contenido = extraer_contenido_excel(file_bytes, nombre, pregunta)

        print("\n\n================ DEBUG INDEXER ================")
        print("Archivo:", nombre)
        print("Pregunta:", pregunta)
        print("Contenido (primeros 800 chars):")
        print(contenido[:800])
        print("===============================================\n\n")

        fecha_str = mod_time or "Fecha desconocida"

        contextos.append({
            "archivo": nombre,
            "fecha": fecha_str,
            "contenido": contenido
        })

    return contextos
