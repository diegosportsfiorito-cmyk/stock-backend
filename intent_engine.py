import re

def detect_intent(query):
    q = query.lower()

    # Detectar código de artículo
    codigo = None
    m = re.search(r"\d{5,}", q)
    if m:
        codigo = m.group()

    # Detectar talle
    talle = None
    m2 = re.search(r"talle\s+(\d+\.?\d*)", q)
    if m2:
        talle = m2.group(1)

    # Stock por código
    if "stock" in q and codigo:
        return {"intent": "stock_por_codigo", "codigo": codigo, "talle": talle}

    # Precio por código
    if "precio" in q and codigo:
        return {"intent": "precio_por_codigo", "codigo": codigo}

    # Análisis global
    if "análisis" in q or "analisis" in q or "global" in q:
        return {"intent": "analisis_global"}

    # Stock negativo
    if "negativo" in q:
        return {"intent": "stock_negativo"}

    # Sin stock
    if "sin stock" in q:
        return {"intent": "sin_stock"}

    # Consultas complejas → IA pura
    return {"intent": "consulta_compleja", "query": query}