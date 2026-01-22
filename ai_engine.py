from fastapi import FastAPI
from pydantic import BaseModel

from indexer import buscar_articulo_en_archivos

app = FastAPI()

class Query(BaseModel):
    question: str

@app.post("/query")
def responder_pregunta(q: Query):
    pregunta = q.question.strip()

    info, fuente = buscar_articulo_en_archivos(pregunta)

    if not info:
        return {
            "respuesta": "Por ahora solo estoy preparado para responder consultas de stock y precios basados en el Excel.",
            "fuente": None
        }

    codigo = info.get("codigo", "")
    descripcion = info.get("descripcion", "")
    precio_publico = info.get("precio_publico", None)
    precio_costo = info.get("precio_costo", None)
    stock_total = info.get("stock_total", None)
    talles = info.get("talles", [])

    partes = []

    # LISTA1 = PRECIO PÚBLICO
    if precio_publico is not None and precio_publico > 0:
        partes.append(
            f"El PRECIO PÚBLICO (LISTA1) del artículo {codigo} ({descripcion}) es ${precio_publico:,.2f}."
        )

    # LISTA0 = PRECIO DE COSTO
    if precio_costo is not None and precio_costo > 0:
        partes.append(
            f"El PRECIO DE COSTO (LISTA0) es ${precio_costo:,.2f}."
        )

    if stock_total is not None:
        partes.append(
            f"El stock total actual es de {stock_total} unidades."
        )

    if talles:
        partes.append(
            "Talles disponibles: " + ", ".join(talles) + "."
        )

    if not partes:
        partes.append(
            f"Encontré el artículo {codigo} ({descripcion}), pero no pude leer correctamente los precios."
        )

    respuesta = " ".join(partes)

    return {
        "respuesta": respuesta,
        "fuente": fuente
    }
