# ai_engine.py
import indexer
from ai_openrouter import ask_openrouter
from intent_engine import clasificar_intencion

SYSTEM_PROMPT = """
Sos un asistente de stock para una zapatería deportiva.
Respondé siempre en español.
Usá solo la información del contexto.
No inventes datos.
"""

def construir_prompt_con_contexto(pregunta: str):
    contextos = indexer.obtener_contexto_para_pregunta(pregunta)

    if not contextos:
        return {
            "prompt_final": "No se encontró información relevante.",
            "fuente": None,
        }

    partes = [ctx["contenido"] for ctx in contextos]
    prompt_contexto = "\n".join(partes)

    MAX_CHARS = 20000
    if len(prompt_contexto) > MAX_CHARS:
        prompt_contexto = prompt_contexto[:MAX_CHARS] + "\n[CONTEXTO RECORTADO]"

    prompt_final = f"""
Contexto:
{prompt_contexto}

Pregunta:
{pregunta}

Instrucciones:
- Respondé de forma clara y concreta.
- No inventes datos.
- Usá solo el contexto provisto.
"""

    return {
        "prompt_final": prompt_final,
        "fuente": contextos[0],
    }

# ============================================================
# RESPUESTA DE PRECIOS (usa el contexto generado por indexer)
# ============================================================

def generar_respuesta_precio(pregunta: str, contexto: str) -> str:
    lineas = contexto.split("\n")

    codigo = None
    precio_publico = None
    precio_costo = None

    for linea in lineas:
        if linea.startswith("Artículo:"):
            codigo = linea.replace("Artículo:", "").strip()
        if "Precio público (LISTA1):" in linea:
            precio_publico = linea.split(":", 1)[1].strip()
        if "Precio de costo (LISTA0):" in linea:
            precio_costo = linea.split(":", 1)[1].strip()

    if not codigo:
        return "No pude identificar el artículo para obtener su precio."

    partes = []
    if precio_publico:
        partes.append(f"El precio público del artículo {codigo} es {precio_publico}.")
    if precio_costo:
        partes.append(f"El precio de costo del artículo {codigo} es {precio_costo}.")

    if partes:
        return " ".join(partes)

    return f"No encontré precios para el artículo {codigo}."

# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def responder_pregunta(pregunta: str):
    intencion = clasificar_intencion(pregunta)

    armado = construir_prompt_con_contexto(pregunta)

    if not armado["fuente"]:
        return {
            "respuesta": "No se encontró información relevante en los archivos de stock.",
            "fuente": None,
        }

    if intencion == "consulta_precio":
        respuesta = generar_respuesta_precio(pregunta, armado["fuente"]["contenido"])
        return {
            "respuesta": respuesta,
            "fuente": armado["fuente"],
        }

    if intencion == "consulta_stock":
        try:
            respuesta = ask_openrouter(SYSTEM_PROMPT, armado["prompt_final"])
        except Exception as e:
            respuesta = f"Error al consultar el motor de IA: {e}"

        return {
            "respuesta": respuesta,
            "fuente": armado["fuente"],
        }

    return {
        "respuesta": "Por ahora solo estoy preparado para responder consultas de stock y precios basados en el Excel.",
        "fuente": None,
    }
