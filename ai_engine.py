# ai_engine.py
from ai_openrouter import ask_openrouter
import indexer   # ✔ Importación segura del módulo completo

SYSTEM_PROMPT = """
Sos un asistente de stock para una zapatería deportiva.
Respondé siempre en español.
Usá solo la información del contexto.
No inventes datos.
"""

# ============================================================
# CONSTRUCCIÓN DEL PROMPT
# ============================================================

def construir_prompt_con_contexto(pregunta: str):
    # ✔ Llamamos a la función desde el módulo
    contextos = indexer.obtener_contexto_para_pregunta(pregunta)

    if not contextos:
        return {
            "prompt_final": "No se encontró información relevante.",
            "fuente": None
        }

    partes = []
    for ctx in contextos:
        partes.append(ctx["contenido"])

    prompt_contexto = "\n".join(partes)

    # ✔ LIMITADOR DE CONTEXTO (evita errores 400 de OpenRouter)
    MAX_CHARS = 20000
    if len(prompt_contexto) > MAX_CHARS:
        prompt_contexto = prompt_contexto[:MAX_CHARS] + "\n[CONTEXTO RECORTADO]"

    prompt_final = f"""
Contexto:
{prompt_contexto}

Pregunta:
{pregunta}

Instrucciones:
- Respondé de forma clara.
- No inventes datos.
- Usá solo el contexto.
"""

    return {
        "prompt_final": prompt_final,
        "fuente": contextos[0]  # ✔ Devolvemos el archivo usado
    }

# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def responder_pregunta(pregunta: str):
    armado = construir_prompt_con_contexto(pregunta)

    try:
        respuesta = ask_openrouter(SYSTEM_PROMPT, armado["prompt_final"])
    except Exception as e:
        respuesta = f"Error al consultar el motor de IA: {e}"

    return {
        "respuesta": respuesta,
        "fuente": armado["fuente"]
    }
