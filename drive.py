import json
import os
from typing import List, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

def _get_drive_service():
    # Leer el archivo secreto montado por Render
    path = "/etc/secrets/service_account.json"
    with open(path, "r") as f:
        info = json.load(f)

    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    service = build("drive", "v3", credentials=creds)
    return service

def listar_archivos_en_carpeta(folder_id: str) -> List[Dict]:
    service = _get_drive_service()
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType, modifiedTime)",
    ).execute()
    return results.get("files", [])

def descargar_archivo_por_id(file_id: str) -> bytes:
    service = _get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()
