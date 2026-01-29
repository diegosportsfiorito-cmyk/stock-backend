import os
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from indexer import Indexer
from style_manager import load_style, save_style
from apply_style import apply_style

from googleapiclient.discovery import build
from google.oauth2 import service_account

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    question: str
    solo_stock: bool = False

class StyleRequest(BaseModel):
    style: str
    admin_key: str

def load_excel_from_drive():
    SERVICE_ACCOUNT_FILE = "/etc/secrets/service_account.json"
    SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)

    FOLDER_ID = "1F0FUEMJmeHgb3ZY7XBBdacCGB3SZK4O-"

    results = service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
        fields="files(id, name, modifiedTime)",
        orderBy="modifiedTime desc"
    ).execute()

    files = results.get("files", [])
    newest = files[0]
    FILE_ID = newest["id"]

    request = service.files().get_media(fileId=FILE_ID)
    file = request.execute()

    with open("stock.xlsx", "wb") as f:
        f.write(file)

    df = pd.read_excel("stock.xlsx")
    return df, newest

df, metadata = load_excel_from_drive()
indexer = Indexer(df)

@app.get("/style")
async def get_style():
    return {"style": load_style()}

@app.post("/style")
async def set_style(req: StyleRequest):
    if req.admin_key != os.getenv("ADMIN_KEY"):
        return {"error": "Unauthorized"}

    save_style(req.style)
    return {"status": "ok", "style": req.style}

@app.post("/query")
async def query_stock(req: QueryRequest):
    question = req.question.strip()
    solo_stock = req.solo_stock

    result = indexer.query(question, solo_stock)

    style = load_style()
    result = apply_style(style, result, question)

    result["fuente"] = metadata
    result["style"] = style

    return result

@app.get("/")
async def root():
    return {
        "status": "OK",
        "message": "Backend Stock IA PRO v5.0 listo."
    }
