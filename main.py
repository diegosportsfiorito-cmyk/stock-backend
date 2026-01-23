from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from indexer import buscar_articulo_en_archivos

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

    info, fuente = buscar_articulo_en_archivos(pregunta)

    if not info:
        respuesta = "No encontré artículos relacionados con tu consulta."
        return {"respuesta": respuesta, "voz": respuesta, "fuente": None}

    # Caso 1: artículo por código
    if "codigo" in info:
        desc = info.get("descripcion", "el artículo")
        stock = info.get("stock_total")
        talles_lista = info.get("talles") or []
        talles = ", ".join(talles_lista) if talles_lista else "sin talles registrados"
        precio = info.get("precio_publico")

        partes = []

        partes.append(f"Encontré {desc}.")
        if stock is not None:
            partes.append(f"Hay {stock} unidades en stock.")
        if talles_lista:
            partes.append(f"Los talles disponibles son: {talles}.")
        if precio is not None:
            partes.append(f"El precio al público es {precio} pesos.")

        respuesta = " ".join(partes)

        return {"respuesta": respuesta, "voz": respuesta, "fuente": fuente}

    # Caso 2: lista completa por descripción
    if "lista_completa" in info:
        grupos = info["lista_completa"]

        if not grupos:
            respuesta = "No encontré artículos relacionados con tu consulta."
            return {"respuesta": respuesta, "voz": respuesta, "fuente": fuente}

        lineas = []
        for g in grupos[:5]:
            desc = g["descripcion"]
            stock = g["stock_total"]
            talles = ", ".join(g["talles"]) if g["talles"] else "sin talles"
            lineas.append(f"{desc} — Stock: {stock} — Talles: {talles}")

        respuesta = "Esto es lo que encontré:\n" + "\n".join(lineas)

        return {"respuesta": respuesta, "voz": respuesta, "fuente": fuente}

    # Fallback
    respuesta = "Tengo información, pero no pude interpretarla correctamente."
    return {"respuesta": respuesta, "voz": respuesta, "fuente": fuente}
