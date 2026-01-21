# main.py
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from ai_engine import responder_pregunta

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # podés restringirlo a tu dominio
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/query")
async def query(request: Request):
    body = await request.json()
    pregunta = body.get("pregunta") or body.get("question") or body.get("query")

    if not pregunta:
        return {"error": "Falta el campo 'pregunta'."}

    resultado = responder_pregunta(pregunta)

    return {
        "answer": resultado["respuesta"],
        "fuente": resultado["fuente"],
    }


@app.get("/")
async def root():
    return {"status": "ok", "message": "Stock backend activo"}
