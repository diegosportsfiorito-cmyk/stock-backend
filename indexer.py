# indexer.py
import io
import re
from typing import List, Dict
from datetime import datetime

import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

# ID de la carpeta de Drive
CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"


# ============================================================
# UTILIDADES DE NORMALIZACIÓN
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
    """
    Convierte strings tipo '30.000,00' -> 30000.0
    Si no se puede, devuelve 0.0
    """
    if valor is None:
        return 0.0
    s = str(valor).strip()
    if s == "":
        return 0.0
    # quitar puntos de miles y cambiar coma por punto
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0


def normalizar_talle(valor: str) -> str:
    """
    Convierte talles tipo '26/7' en '26-27', '27/8' -> '27-28', '29/0' -> '29-30', etc.
    Si no matchea el patrón, lo deja como está.
    """
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
# AGRUPACIÓN POR ARTÍCULO
# ============================================================

def agrupar_por_articulo(df: pd.DataFrame) -> Dict[str, Dict]:
    grupos: Dict[str, Dict] = {}

    for _, row in df.iterrows():
        codigo = str(row.get("articulo", "")).strip()
        if not codigo or codigo == "0":
            # Ignoramos filas sin código o código 0 (ARTICULO SIN CODIFICAR)
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

        if talle not in grupos[codigo]["talles"]:
            grupos[codigo]["talles"][talle] = 0
        grupos[codigo]["talles"][talle] += int(stock)

        # Ventas por año (columnas dinámicas: 2026, 2025, 2024, 2023, etc.)
        for col in df.columns:
            if col.isdigit():
                unidades = parse_numero(row.get(col, "0"))
                if unidades != 0:
                    if col not in grupos[codigo]["ventas"]:
                        grupos[codigo]["ventas"][col] = 0
                    grupos[codigo]["ventas"][col] += int(unidades)

    return grupos


# ============================================================
# FILTRADO SEGÚN LA PREGUNTA
# ============================================================

def extraer_codigo_de_pregunta(pregunta: str) -> str:
    """
    Intenta extraer un código de artículo de la pregunta.
    Ej: 'stock del artículo 01303-4' -> '01303-4'
    """
    p = pregunta.lower()
    # buscar patrones tipo 01303-4, 100000089, etc.
    m = re.search(r"\b[\d\-]{4,}\b", p)
    if m:
        return m.group(0)
    return ""


def filtrar_por_pregunta(df: pd.DataFrame, pregunta: str) -> pd.DataFrame:
    p = pregunta.lower()
    codigo_pregunta = extraer_codigo_de_pregunta(pregunta)

    # Si hay código y existe la columna articulo, filtramos por código
    if codigo_pregunta and "articulo" in df.columns:
        mask = df["articulo"].astype(str).str.strip() == codigo_pregunta
        filtrado = df[mask]
        if not filtrado.empty:
            return filtrado

    # Si no hay código o no matchea, buscamos por texto (ej: "pantuflas")
    texto = p.strip()
    if texto:
        mask = df.apply(lambda row: texto in str(row).lower(), axis=1)
        filtrado = df[mask]
        if not filtrado.empty:
            return filtrado

    # Fallback: devolvemos todo
    return df


# ============================================================
# LECTURA DEL EXCEL Y CONSTRUCCIÓN DE CONTEXTO
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

        if not df_total:
            return f"No se encontró contenido en el Excel {nombre_archivo}."

        df = pd.concat(df_total, ignore_index=True)

        # Filtrado según la pregunta
        df = filtrar_por_pregunta(df, pregunta)

        # Agrupación por artículo
        grupos = agrupar_por_articulo(df)

        if not grupos:
            return "No se encontraron artículos relevantes para la pregunta en los datos de stock."

        partes = []
        for codigo, info in grupos.items():
            lineas_talles = [
                f"  - Talle {t}: {s} unidades"
                for t, s in sorted(info["talles"].items(), key=lambda x: str(x[0]))
            ]

            if info["ventas"]:
                lineas_ventas = [
                    f"  - Año {anio}: {unidades} unidades vendidas"
                    for anio, unidades in sorted(info["ventas"].items(), key=lambda x: x[0], reverse=True)
                ]
                bloque_ventas = "Ventas por año:\n" + "\n".join(lineas_ventas) + "\n"
            else:
                bloque_ventas = "Ventas por año: sin datos registrados.\n"

            partes.append(
                f"Artículo: {codigo}\n"
                f"Descripción: {info['descripcion']}\n"
                f"Color: {info['color']}\n"
                f"Precio público (LISTA1): {int(info['precio_publico']) if info['precio_publico'] else 0}\n"
                f"Precio costo (LISTA0): {int(info['precio_costo']) if info['precio_costo'] else 0}\n"
                f"Último ingreso: {info['ultimo_ingreso']}\n"
                f"Stock total: {info['stock_total']}\n"
                f"Detalle por talle:\n" + "\n".join(lineas_talles) + "\n"
                + bloque_ventas +
                "-------------------------\n"
            )

        return "\n".join(partes)

    except Exception as e:
        return f"No se pudo leer el Excel {nombre_archivo}: {e}"


# ============================================================
# OBTENER CONTEXTO DESDE DRIVE
# ============================================================

def obtener_contexto_para_pregunta(pregunta: str) -> List[Dict]:
    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)

    contextos: List[Dict] = []

    for archivo in archivos:
        nombre = archivo["name"]
        file_id = archivo["id"]
        mod_time = archivo.get("modifiedTime")

        if not (nombre.lower().endswith(".xlsx") or nombre.lower().endswith(".xls")):
            continue

        file_bytes = descargar_archivo_por_id(file_id)

        contenido = extraer_contenido_excel(file_bytes, nombre, pregunta)

        fecha_str = None
        if mod_time:
            try:
                dt = datetime.fromisoformat(mod_time.replace("Z", "+00:00"))
                fecha_str = dt.strftime("%Y-%m-%d")
            except Exception:
                fecha_str = mod_time

        contextos.append(
            {
                "archivo": nombre,
                "fecha": fecha_str or "Fecha desconocida",
                "contenido": contenido,
            }
        )

    return contextos
