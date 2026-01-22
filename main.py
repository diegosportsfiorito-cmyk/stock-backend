# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from indexer import obtener_contexto_para_pregunta
from ai_engine import consultar_ia  # tu función que llama al modelo

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Pregunta(BaseModel):
    question: str


@app.post("/query")
async def query(p: Pregunta):
    pregunta = p.question.strip()

    print("\n\n=========== DEBUG /query ===========")
    print("Pregunta recibida:", pregunta)

    contextos = obtener_contexto_para_pregunta(pregunta)

    print("Cantidad de contextos:", len(contextos))
    if contextos:
        print("Primer contexto archivo:", contextos[0]["archivo"])
        print("Primer contexto contenido (primeros 400 chars):")
        print(contextos[0]["contenido"][:400])
    print("====================================\n\n")

    respuesta = consultar_ia(pregunta, contextos)

    return {
        "respuesta": respuesta,
        "fuente": contextos
    }
