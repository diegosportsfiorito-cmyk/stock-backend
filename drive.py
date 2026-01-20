# drive.py
import io
import os
import json
from typing import List, Dict, Any

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from PyPDF2 import PdfReader
from PIL import Image
import pytesseract

DRIVE_ROOT_FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

def get_google_creds():
    service_account_info = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return creds

def get_drive_service():
    creds = get_google_creds()
    return build("drive", "v3", credentials=creds)

def get_sheets_service():
    creds = get_google_creds()
    return build("sheets", "v4", credentials=creds)

def list_files_recursive(folder_id: str) -> List[Dict[str, Any]]:
    drive = get_drive_service()
    all_files = []

    def _list_folder(fid: str):
        page_token = None
        while True:
            resp = drive.files().list(
                q=f"'{fid}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
            ).execute()
            for f in resp.get("files", []):
                all_files.append(f)
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    _list_folder(f["id"])
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    _list_folder(folder_id)
    return all_files

def download_file_content(file_id: str, mime_type: str = None) -> bytes:
    drive = get_drive_service()
    if mime_type:
        request = drive.files().export_media(fileId=file_id, mimeType=mime_type)
    else:
        request = drive.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    reader = PdfReader(io.BytesIO(pdf_bytes))
    texts = []
    for page in reader.pages:
        texts.append(page.extract_text() or "")
    return "\n".join(texts)

def extract_text_from_image_bytes(img_bytes: bytes) -> str:
    image = Image.open(io.BytesIO(img_bytes))
    text = pytesseract.image_to_string(image, lang="spa+eng")
    return text

def read_sheet_as_rows(spreadsheet_id: str) -> Dict[str, List[List[str]]]:
    sheets_service = get_sheets_service()
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    result = {}
    for sheet in meta.get("sheets", []):
        title = sheet["properties"]["title"]
        resp = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{title}!A:Z"
        ).execute()
        rows = resp.get("values", [])
        result[title] = rows
    return result

def collect_drive_corpus() -> Dict[str, Any]:
    """
    Devuelve un índice unificado:
    {
      "sheets": [{ "name": ..., "id": ..., "sheets": {title: rows} }],
      "pdfs": [{ "name": ..., "id": ..., "text": ... }],
      "images": [{ "name": ..., "id": ..., "text": ... }],
      "others": [...]
    }
    """
    files = list_files_recursive(DRIVE_ROOT_FOLDER_ID)
    corpus = {
        "sheets": [],
        "pdfs": [],
        "images": [],
        "others": [],
    }

    for f in files:
        fid = f["id"]
        name = f["name"]
        mime = f["mimeType"]

        # Google Sheets
        if mime == "application/vnd.google-apps.spreadsheet":
            try:
                sheets_data = read_sheet_as_rows(fid)
                corpus["sheets"].append({
                    "id": fid,
                    "name": name,
                    "sheets": sheets_data,
                })
            except Exception as e:
                corpus["others"].append({
                    "id": fid,
                    "name": name,
                    "mimeType": mime,
                    "error": str(e),
                })

        # PDF (Google Drive file or uploaded PDF)
        elif mime == "application/pdf":
            try:
                pdf_bytes = download_file_content(fid)
                text = extract_text_from_pdf_bytes(pdf_bytes)
                corpus["pdfs"].append({
                    "id": fid,
                    "name": name,
                    "text": text,
                })
            except Exception as e:
                corpus["others"].append({
                    "id": fid,
                    "name": name,
                    "mimeType": mime,
                    "error": str(e),
                })

        # Google Docs exportable as PDF or text (opcional)
        elif mime == "application/vnd.google-apps.document":
            try:
                pdf_bytes = download_file_content(fid, mime_type="application/pdf")
                text = extract_text_from_pdf_bytes(pdf_bytes)
                corpus["pdfs"].append({
                    "id": fid,
                    "name": name,
                    "text": text,
                })
            except Exception as e:
                corpus["others"].append({
                    "id": fid,
                    "name": name,
                    "mimeType": mime,
                    "error": str(e),
                })

        # Imágenes (OCR)
        elif mime.startswith("image/"):
            try:
                img_bytes = download_file_content(fid)
                text = extract_text_from_image_bytes(img_bytes)
                corpus["images"].append({
                    "id": fid,
                    "name": name,
                    "text": text,
                })
            except Exception as e:
                corpus["others"].append({
                    "id": fid,
                    "name": name,
                    "mimeType": mime,
                    "error": str(e),
                })

        else:
            corpus["others"].append({
                "id": fid,
                "name": name,
                "mimeType": mime,
            })

    return corpus