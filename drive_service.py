import os
import json
from typing import List, Dict, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ============================================================
# CREACIÓN DEL ARCHIVO DE CREDENCIALES EN AZURE
# ============================================================

def _ensure_service_account_file() -> str:
    """
    En Azure App Service NO existen los Secret Files.
    Por eso:
    - GOOGLE_SERVICE_ACCOUNT_JSON contiene el JSON completo como string.
    - GOOGLE_APPLICATION_CREDENTIALS apunta a la ruta donde debe existir el archivo.
    Esta función crea el archivo físico si no existe.
    """
    json_env = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not json_env:
        raise RuntimeError(
            "La variable GOOGLE_SERVICE_ACCOUNT_JSON no está configurada o está vacía."
        )

    if not credentials_path:
        credentials_path = "/home/secrets/service_account.json"

    # Crear carpeta si no existe
    base_dir = os.path.dirname(credentials_path)
    if base_dir and not os.path.exists(base_dir):
        os.makedirs(base_dir, exist_ok=True)

    # Crear archivo si no existe
    if not os.path.exists(credentials_path):
        try:
            parsed = json.loads(json_env)
            with open(credentials_path, "w") as f:
                json.dump(parsed, f)
            print(">>> Archivo de credenciales creado en:", credentials_path)
        except Exception as e:
            print(">>> ERROR al crear archivo de credenciales:", repr(e))
            raise RuntimeError("No se pudo crear el archivo de credenciales desde GOOGLE_SERVICE_ACCOUNT_JSON")

    return credentials_path


# ============================================================
# CLIENTE DE GOOGLE DRIVE
# ============================================================

def _get_drive_service():
    """
    Inicializa el cliente de Google Drive usando el service account.
    """
    try:
        credentials_path = _ensure_service_account_file()

        scopes = ["https://www.googleapis.com/auth/drive.readonly"]
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=scopes,
        )

        service = build("drive", "v3", credentials=credentials)
        return service

    except Exception as e:
        print(">>> ERROR al inicializar Google Drive service:", repr(e))
        raise RuntimeError("No se pudo inicializar el cliente de Google Drive")


# ============================================================
# LISTAR ARCHIVOS EN UNA CARPETA
# ============================================================

def listar_archivos_en_carpeta(folder_id: str) -> List[Dict[str, Any]]:
    """
    Lista archivos dentro de una carpeta de Google Drive por folder_id.
    """
    try:
        service = _get_drive_service()

        query = f"'{folder_id}' in parents and trashed = false"
        fields = "files(id, name, mimeType, modifiedTime)"

        results = service.files().list(
            q=query,
            fields=fields,
            pageSize=1000,
        ).execute()

        files = results.get("files", [])
        print(f">>> listar_archivos_en_carpeta: encontrados {len(files)} archivos en {folder_id}")
        return files

    except HttpError as e:
        print(">>> ERROR en listar_archivos_en_carpeta (HttpError):", repr(e))
        raise RuntimeError(f"Error al listar archivos en carpeta de Drive: {e}")
    except Exception as e:
        print(">>> ERROR en listar_archivos_en_carpeta:", repr(e))
        raise RuntimeError("Error inesperado al listar archivos en carpeta de Drive")


# ============================================================
# DESCARGAR ARCHIVO POR ID
# ============================================================

def descargar_archivo_por_id(file_id: str) -> bytes:
    """
    Descarga un archivo de Google Drive por su ID.
    """
    from googleapiclient.http import MediaIoBaseDownload
    import io

    try:
        service = _get_drive_service()

        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f">>> Descargando {file_id}: {int(status.progress() * 100)}%")

        fh.seek(0)
        print(f">>> Archivo descargado correctamente desde Drive: {file_id}")
        return fh.read()

    except HttpError as e:
        print(">>> ERROR en descargar_archivo_por_id (HttpError):", repr(e))
        raise RuntimeError(f"Error al descargar archivo de Drive: {e}")
    except Exception as e:
        print(">>> ERROR en descargar_archivo_por_id:", repr(e))
        raise RuntimeError("Error inesperado al descargar archivo de Drive")
