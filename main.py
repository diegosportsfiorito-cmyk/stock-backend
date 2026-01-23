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

    # ============================================================
    # RESPUESTA HUMANA
    # ============================================================

    # Caso 1: artículo por código
    if "codigo" in info:
        desc = info["descripcion"]
        stock = info["stock_total"]
        talles = ", ".join(info["talles"]) if info["talles"] else "sin talles registrados"
        precio = info["precio_publico"]

        respuesta = (
            f"Encontré el artículo {desc}. "
            f"Hay {stock} unidades en stock y los talles disponibles son: {talles}. "
            f"El precio al público es {precio} pesos."
        )

        return {"respuesta": respuesta, "voz": respuesta, "fuente": fuente}

    # Caso 2: lista completa por descripción
    if "lista_completa" in info:
        grupos = info["lista_completa"]

        lineas = []
        for g in grupos[:5]:
            talles = ", ".join(g["talles"]) if g["talles"] else "sin talles"
            lineas.append(f"{g['descripcion']} — Stock: {g['stock_total']} — Talles: {talles}")

        respuesta = (
            "Esto es lo que encontré:\n" +
            "\n".join(lineas)
        )

        return {"respuesta": respuesta, "voz": respuesta, "fuente": fuente}

    # fallback
    respuesta = "Tengo información, pero no pude interpretarla correctamente."
    return {"respuesta": respuesta, "voz": respuesta, "fuente": fuente}
