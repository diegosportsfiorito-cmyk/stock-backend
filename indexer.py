# indexer.py
import io
from typing import List, Dict
from datetime import datetime

import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

# ID de la carpeta de Drive (solo el ID, no la URL completa)
CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"


# ---------- Normalización de talles, colores y descripciones ----------

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
    Si no coincide con el patrón, devuelve el valor original.
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


# ---------- Agrupación por artículo y cálculo de stock ----------

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
            }
        },
        ...
    }
    """
    grupos: Dict[str, Dict] = {}

    for _, row in df.iterrows():
        codigo = str(row.get("Artículo", "")).strip()
        if not codigo:
            continue

        descripcion = normalizar_descripcion(row.get("Descripción original", ""))
        color = normalizar_color(row.get("Color", ""))
        talle = normalizar_talle(row.get("Talle", ""))
        stock = row.get("Stock", "0")

        try:
            stock = int(float(stock))
        except Exception:
            stock = 0

        if codigo not in grupos:
            grupos[codigo] = {
                "descripcion": descripcion,
                "color": color,
                "stock_total": 0,
                "talles": {}
            }

        grupos[codigo]["stock_total"] += stock
        grupos[codigo]["talles"][talle] = grupos[codigo]["talles"].get(talle, 0) + stock

    return grupos


# ---------- Filtrado inteligente según la pregunta ----------

def filtrar_por_pregunta(df: pd.DataFrame, pregunta: str) -> pd.DataFrame:
    """
    Filtra el DataFrame según la pregunta:
    - Si la pregunta contiene un código de artículo, filtra por ese código.
    - Si no, busca coincidencias de texto en las filas.
    - Si no encuentra nada, devuelve el df completo.
    """
    p = pregunta.lower()

    # Si el usuario menciona un código de artículo en la pregunta
    if "Artículo" in df.columns:
        for codigo in df["Artículo"].unique():
            if isinstance(codigo, str) and codigo.lower() in p:
                return df[df["Artículo"] == codigo]

    # Búsqueda por texto en todas las columnas
    mask = df.apply(lambda row: p in str(row).lower(), axis=1)
    filtrado = df[mask]

    if not filtrado.empty:
        return filtrado

    # Si no hay coincidencias, devolvemos todo (mejor que nada)
    return df


# ---------- Lectura robusta del Excel y construcción de contexto ----------

def extraer_contenido_excel(file_bytes: bytes, nombre_archivo: str, pregunta: str) -> str:
    """
    Lee un Excel de forma robusta, evitando que pandas interprete talles como fechas.
    Normaliza datos, agrupa por artículo y genera un contexto compacto para la IA.
    """
    try:
        excel_file = io.BytesIO(file_bytes)
        xls = pd.ExcelFile(excel_file)

        df_total = []

        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name, dtype=str)
            df = df.fillna("")
            df = df.applymap(lambda x: str(x).strip())
            df_total.append(df)

        if not df_total:
            return f"No se encontró contenido en el Excel {nombre_archivo}."

        df = pd.concat(df_total, ignore_index=True)

        # Filtrado inteligente según la pregunta
        df = filtrar_por_pregunta(df, pregunta)

        # Agrupación por artículo
        grupos = agrupar_por_articulo(df)

        # Construimos texto para la IA
        partes = []
        for codigo, info in grupos.items():
            lineas_talles = [
                f"  - Talle {t}: {s} unidades"
                for t, s in info["talles"].items()
            ]

            partes.append(
                f"Artículo: {codigo}\n"
                f"Descripción: {info['descripcion']}\n"
                f"Color: {info['color']}\n"
                f"Stock total: {info['stock_total']}\n"
                f"Detalle por talle:\n" +
                "\n".join(lineas_talles) +
                "\n-------------------------\n"
            )

        if not partes:
            return f"No se encontró información relevante en {nombre_archivo}."

        return "\n".join(partes)

    except Exception as e:
        return f"No se pudo leer el Excel {nombre_archivo}: {e}"


def obtener_contexto_para_pregunta(pregunta: str) -> List[Dict]:
    """
    1) Lista archivos de la carpeta de Drive.
    2) Filtra por extensión Excel.
    3) Descarga y extrae contenido (ya normalizado y agrupado).
    4) Devuelve lista de dicts con archivo, fecha, contenido.
    """
    archivos = listar_archivos_en_carpeta(CARPETA_STOCK_ID)

    contextos: List[Dict] = []

    for archivo in archivos:
        nombre = archivo["name"]
        file_id = archivo["id"]
        mime_type = archivo.get("mimeType", "")
        mod_time = archivo.get("modifiedTime")

        if not (nombre.lower().endswith(".xlsx") or nombre.lower().endswith(".xls")):
            continue

        file_bytes = descargar_archivo_por_id(file_id)

        contenido = extraer_contenido_excel(file_bytes, nombre, pregunta)

        # Formateamos fecha
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
