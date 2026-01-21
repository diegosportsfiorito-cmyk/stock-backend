from fastapi import FastAPI
from pydantic import BaseModel

from indexer import build_unified_context
from ai_openrouter import ask_openrouter

app = FastAPI(title="Notebook-Like Backend")

class QueryRequest(BaseModel):
    question: str

class QueryResponse(BaseModel):
    answer: str

SYSTEM_PROMPT = (
    "Eres un asistente experto que responde SOLO en base a la información "
    "proporcionada en el contexto. El contexto proviene de Google Drive "
    "(Sheets, PDFs, imágenes con OCR, etc.). Si no encuentras la respuesta "
    "en el contexto, dilo explícitamente."
)

@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    # 1) Construir contexto unificado desde Drive
    context = build_unified_context()

    # 2) Armar prompt para la IA
    user_prompt = (
        f"Contexto:\n{context}\n\n"
        f"Pregunta del usuario:\n{req.question}\n\n"
        "Responde de forma clara, concisa y basada SOLO en el contexto."
    )

    # 3) Preguntar a OpenRouter
    answer = ask_openrouter(SYSTEM_PROMPT, user_prompt)

    return QueryResponse(answer=answer)
