# indexer.py
import io
from typing import List, Dict
from datetime import datetime

import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

# ID de la carpeta de Drive
CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"


# ============================================================
# NORMALIZACIÓN DE TALLES, COLORES Y DESCRIPCIONES
# ============================================================

MAPA_COLORES = {
    "NE": "NEGRO",
    "BLA": "BLANCO",
    "GRI": "GRIS",
    "AZ": "AZUL",
    "RO": "ROJO",
}


def normalizar_talle(valor: str) -> str:
    """
    Convierte talles tipo '26/7' en '26-27'.
    Maneja casos como '27/8', '30/1', '29/0', etc.
    """
    if isinstance(valor, str) and "/" in valor:
        partes = valor.split("/")
        if len(partes) == 2 and partes[0].isdigit() and partes[1].isdigit():
            base = int(partes[0])
            siguiente = int(partes[1]) + 1
            return f"{base}-{siguiente}"
    return valor


def normalizar_color(valor: str) -> str:
    """
    Normaliza colores, expandiendo abreviaturas y unificando formato.
    Ej: 'NE/BLA' -> 'NEGRO-BLANCO'
    """
    if not isinstance(valor, str):
        return valor

    partes = valor.replace("/", "-").split("-")
    partes_norm = []

    for p in partes:
        p = p.strip().upper()
        partes_norm.append(MAPA_COLORES.get(p, p))

    return "-".join(partes_norm)


def normalizar_descripcion(valor: str) -> str:
    """
    Limpia espacios y aplica formato tipo título.
    """
    if not isinstance(valor, str):
        return valor
    valor = valor.strip()
    valor = " ".join(valor.split())  # elimina espacios dobles
    return valor.title()


# ============================================================
# AGRUPACIÓN POR ARTÍCULO Y CÁLCULO DE STOCK
# ============================================================

def agrupar_por_articulo(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Devuelve un diccionario:
    {
        "01303-4": {
            "descripcion": "...",
            "color": "...",
            "stock_total": 6,
            "talles": {
                "27-28": 2,
                "28-29": 4
            },
            "precio_publico": 30000,
            "precio_costo": 13347,
            "ultimo_ingreso": "2025-08-19",
            "ventas": {2026: 1, 2025: 2, ...}
        }
    }
    """
    grupos: Dict[str, Dict] = {}

    for _, row in df.iterrows():
        codigo = str(row.get("articulo", "")).strip()
        if not codigo:
            continue

        descripcion = normalizar_descripcion(row.get("descripcion_original", ""))
        color = normalizar_color(row.get("color", ""))
        talle = normalizar_talle(row.get("talle", ""))
        stock = row.get("stock", "0")
        lista1 = row.get("lista1", "")
        lista0 = row.get("lista0", "")
        ult_ingreso = row.get("ult_ingreso", "")

        try:
            stock = int(float(stock))
        except:
            stock = 0

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

        # Stock total
        grupos[codigo]["stock_total"] += stock

        # Stock por talle
        grupos[codigo]["talles"][talle] = grupos[codigo]["talles"].get(talle, 0) + stock

        # Ventas por año (columnas dinámicas)
        for col in df.columns:
            if col.isdigit():  # columnas tipo 2026, 2025, etc.
                try:
                    unidades = int(float(row.get(col, 0)))
                except:
                    unidades = 0
                grupos[codigo]["ventas"][col] = grupos[codigo]["ventas"].get(col, 0) + unidades

    return grupos


# ============================================================
# FILTRADO INTELIGENTE SEGÚN LA PREGUNTA
# ============================================================

def filtrar_por_pregunta(df: pd.DataFrame, pregunta: str) -> pd.DataFrame:
    p = pregunta.lower()

    # Si el usuario menciona un código de artículo
    for codigo in df["articulo"].unique():
        if isinstance(codigo, str) and codigo.lower() in p:
            return df[df["articulo"] == codigo]

    # Búsqueda por texto en todas las columnas
    mask = df.apply(lambda row: p in str(row).lower(), axis=1)
    filtrado = df[mask]

    if not filtrado.empty:
        return filtrado

    return df  # fallback seguro


# ============================================================
# LECTURA ROBUSTA DEL EXCEL + NORMALIZACIÓN DE ENCABEZADOS
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

            # NORMALIZACIÓN DE ENCABEZADOS (CRÍTICO)
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

            df_total.append(df)

        if not df_total:
            return f"No se encontró contenido en el Excel {nombre_archivo}."

        df = pd.concat(df_total, ignore_index=True)

        # Filtrado inteligente
        df = filtrar_por_pregunta(df, pregunta)

        # Agrupación por artículo
        grupos = agrupar_por_articulo(df)

        # Construcción del contexto para la IA
        partes = []
        for codigo, info in grupos.items():
            lineas_talles = [
                f"  - {t}: {s} unidades"
                for t, s in info["talles"].items()
            ]

            lineas_ventas = [
                f"  - {anio}: {unidades} unidades"
                for anio, unidades in info["ventas"].items()
            ]

            partes.append(
                f"Artículo: {codigo}\n"
                f"Descripción: {info['descripcion']}\n"
                f"Color: {info['color']}\n"
                f"Precio público (LISTA1): {info['precio_publico']}\n"
                f"Precio costo (LISTA0): {info['precio_costo']}\n"
                f"Último ingreso: {info['ultimo_ingreso']}\n"
                f"Stock total: {info['stock_total']}\n"
                f"Detalle por talle:\n" + "\n".join(lineas_talles) + "\n"
                f"Ventas por año:\n" + "\n".join(lineas_ventas) + "\n"
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

        # Fecha formateada
        fecha_str = None
        if mod_time:
            try:
                dt = datetime.fromisoformat(mod_time.replace("Z", "+00:00"))
                fecha_str = dt.strftime("%Y-%m-%d")
            except:
                fecha_str = mod_time

        contextos.append(
            {
                "archivo": nombre,
                "fecha": fecha_str or "Fecha desconocida",
                "contenido": contenido,
            }
        )

    return contextos
