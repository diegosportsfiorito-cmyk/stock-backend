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

def responder_pregunta(pregunta: str):
    intencion = clasificar_intencion(pregunta)

    if intencion == "consulta_stock":
        armado = construir_prompt_con_contexto(pregunta)
        try:
            respuesta = ask_openrouter(SYSTEM_PROMPT, armado["prompt_final"])
        except Exception as e:
            respuesta = f"Error al consultar el motor de IA: {e}"

        return {
            "respuesta": respuesta,
            "fuente": armado["fuente"],
        }

    # Para otras intenciones, por ahora respondemos simple
    return {
        "respuesta": "Por ahora solo estoy preparado para responder consultas de stock basadas en el Excel.",
        "fuente": None,
    }
