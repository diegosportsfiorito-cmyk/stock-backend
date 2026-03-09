import json
import os
from typing import List, Dict
import io

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ============================================================
# CONFIGURACIÓN
# ============================================================

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# Ahora la ruta viene desde Azure (variable de entorno)
SERVICE_ACCOUNT_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


# ============================================================
# VALIDACIÓN DE CREDENCIALES
# ============================================================

def _get_drive_service():
    """
    Inicializa el cliente de Google Drive usando la ruta definida
    en GOOGLE_APPLICATION_CREDENTIALS.
    """

    if not SERVICE_ACCOUNT_PATH:
        raise RuntimeError(
            "La variable GOOGLE_APPLICATION_CREDENTIALS no está configurada."
        )

    if not os.path.exists(SERVICE_ACCOUNT_PATH):
        raise RuntimeError(
            f"Archivo de credenciales no encontrado: {SERVICE_ACCOUNT_PATH}. "
            "Asegurate de subir el archivo service_account.json al App Service."
        )

    try:
        with open(SERVICE_ACCOUNT_PATH, "r") as f:
            info = json.load(f)

        creds = service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )

        service = build("drive", "v3", credentials=creds)
        return service

    except Exception as e:
        raise RuntimeError(f"Error inicializando Google Drive API: {repr(e)}")


# ============================================================
# LISTAR ARCHIVOS EN CARPETA
# ============================================================

def listar_archivos_en_carpeta(folder_id: str) -> List[Dict]:
    """
    Lista archivos dentro de una carpeta de Drive.
    Devuelve lista vacía si algo falla.
    """
    try:
        service = _get_drive_service()

        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType, modifiedTime)",
        ).execute()

        return results.get("files", [])

    except Exception as e:
        print(">>> ERROR en listar_archivos_en_carpeta:", repr(e))
        return []


# ============================================================
# DESCARGAR ARCHIVO POR ID
# ============================================================

def descargar_archivo_por_id(file_id: str) -> bytes:
    """
    Descarga un archivo de Drive por ID.
    """
    try:
        service = _get_drive_service()
        request = service.files().get_media(fileId=file_id)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)
        return fh.read()

    except Exception as e:
        raise RuntimeError(f"Error descargando archivo {file_id}: {repr(e)}")
