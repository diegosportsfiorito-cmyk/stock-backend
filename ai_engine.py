from indexer import buscar_articulo_en_archivos

# ============================================================
# CONVERSIÓN DE NÚMEROS A TEXTO (PARA VOZ)
# ============================================================

def numero_a_texto(n):
    try:
        n = str(n).replace(".", "").replace(",", ".")
        valor = float(n)
    except:
        return str(n)

    entero = int(valor)
    return f"{entero:,}".replace(",", " ")

# ============================================================
# PREPARAR TEXTO PARA VOZ (SIN DÓLARES)
# ============================================================

def preparar_texto_para_voz(texto):
    # 1) Eliminar la sección "Fuente"
    texto = texto.split("Fuente:")[0]

    # 2) Reemplazar $ por "pesos"
    texto = texto.replace("$", " pesos ")

    # 3) Convertir números a texto hablado
    texto = texto.replace(",", ".")
    texto = texto.replace("ARS", "")

    import re
    texto = re.sub(r"([0-9][0-9\.]*)", lambda m: numero_a_texto(m.group(1)) + " pesos", texto)

    # Evitar "pesos pesos"
    texto = texto.replace("pesos pesos", "pesos")

    return texto.strip()

# ============================================================
# RESPONDER PREGUNTA
# ============================================================

def responder_pregunta(q):
    # Acepta string o Query
    if isinstance(q, str):
        pregunta = q.strip()
    else:
        pregunta = q.question.strip()

    info, fuente = buscar_articulo_en_archivos(pregunta)

    if not info:
        return {
            "respuesta": "Por ahora solo estoy preparado para responder consultas de stock y precios basados en el Excel.",
            "voz": "Por ahora solo estoy preparado para responder consultas de stock y precios basados en el Excel.",
            "fuente": None
        }

    codigo = info.get("codigo", "")
    descripcion = info.get("descripcion", "")
    precio_publico = info.get("precio_publico", None)
    precio_costo = info.get("precio_costo", None)
    stock_total = info.get("stock_total", None)
    talles = info.get("talles", [])

    partes = []

    # PRECIO PÚBLICO
    if precio_publico:
        partes.append(
            f"El PRECIO PÚBLICO (LISTA1) del artículo {codigo} ({descripcion}) es ${precio_publico:,.2f}."
        )

    # PRECIO COSTO
    if precio_costo:
        partes.append(
            f"El PRECIO DE COSTO (LISTA0) es ${precio_costo:,.2f}."
        )

    # STOCK
    if stock_total is not None:
        partes.append(
            f"El stock total actual es de {stock_total} unidades."
        )

    # TALLES
    if talles:
        partes.append(
            "Talles disponibles: " + ", ".join(talles) + "."
        )

    respuesta = " ".join(partes)

    # Preparar texto para voz (sin dólares)
    texto_voz = preparar_texto_para_voz(respuesta)

    return {
        "respuesta": respuesta,
        "voz": texto_voz,
        "fuente": fuente
    }
