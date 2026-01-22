# intent_engine.py

def clasificar_intencion(pregunta: str) -> str:
    p = pregunta.lower()

    if any(x in p for x in ["stock", "hay de", "talle", "talles", "disponible", "disponibilidad"]):
        return "consulta_stock"

    if any(x in p for x in ["precio", "lista", "cuanto sale", "vale"]):
        return "consulta_precio"

    return "general"
