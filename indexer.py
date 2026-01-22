# indexer.py
import io
from typing import List, Dict
from datetime import datetime

import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

# ID de la carpeta de Drive
CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"


def normalizar_encabezados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza encabezados para que sean predecibles:
    - minúsculas
    - sin tildes
    - espacios -> _
    - sin puntos
    """
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


def extraer_contenido_excel(file_bytes: bytes, nombre_archivo: str, pregunta: str) -> str:
    """
    MODO DIAGNÓSTICO:
    - Lee el Excel (xls/xlsx)
    - Normaliza encabezados
    - Devuelve las primeras filas como texto plano
    para que la IA vea exactamente qué hay.
    """
    try:
        excel_file = io.BytesIO(file_bytes)
        xls = pd.ExcelFile(excel_file)

        partes = []

        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name, dtype=str)
            df = df.fillna("")
            df = df.applymap(lambda x: str(x).strip())
            df = normalizar_encabezados(df)

            # Limitamos filas para no explotar contexto
            df_preview = df.head(50)

            partes.append(
                f"Hoja: {sheet_name}\n"
                f"Columnas: {list(df_preview.columns)}\n"
                f"Contenido:\n{df_preview.to_string(index=False)}\n"
                "-------------------------\n"
            )

        if not partes:
            return f"No se encontró contenido en el Excel {nombre_archivo}."

        return "\n".join(partes)

    except Exception as e:
        # IMPORTANTE: mostramos el error real de pandas
        return f"No se pudo leer el Excel {nombre_archivo}: {e}"


def obtener_contexto_para_pregunta(pregunta: str) -> List[Dict]:
    """
    1) Lista archivos de la carpeta de Drive.
    2) Filtra por Excel.
    3) Descarga y extrae contenido (modo diagnóstico).
    4) Devuelve lista de dicts con archivo, fecha, contenido.
    """
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
