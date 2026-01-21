# indexer.py
import io
from typing import List, Dict
from datetime import datetime

import pandas as pd

from drive import listar_archivos_en_carpeta, descargar_archivo_por_id

# ID de la carpeta de Drive (solo el ID, no la URL completa)
CARPETA_STOCK_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"


def extraer_contenido_excel(file_bytes: bytes, nombre_archivo: str) -> str:
    """
    Lee un Excel y devuelve un resumen de contenido en texto.
    Por ahora: concatenamos filas relevantes.
    Más adelante podemos hacer algo más sofisticado.
    """
    try:
        excel_file = io.BytesIO(file_bytes)
        xls = pd.ExcelFile(excel_file)

        partes = []
        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            # Limitamos filas para no explotar el contexto
            df = df.head(200)
            partes.append(f"Hoja: {sheet_name}\n{df.to_string(index=False)}\n")

        return "\n".join(partes)
    except Exception as e:
        return f"No se pudo leer el Excel {nombre_archivo}: {e}"


def obtener_contexto_para_pregunta(pregunta: str) -> List[Dict]:
    """
    1) Lista archivos de la carpeta de Drive.
    2) Filtra por extensión Excel.
    3) Descarga y extrae contenido.
    4) Devuelve lista de dicts con archivo, fecha, contenido.
    Más adelante podemos hacer selección inteligente según la pregunta.
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

        contenido = extraer_contenido_excel(file_bytes, nombre)

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

    # Por ahora devolvemos todos. Más adelante podemos filtrar por relevancia.
    return contextos
