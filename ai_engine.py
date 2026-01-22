# ai_engine.py
from typing import List, Dict
from ai_openrouter import ask_openrouter
from indexer import obtener_contexto_para_pregunta

SYSTEM_PROMPT = """
Sos un asistente de stock para una zapatería deportiva.
Respondé siempre en español.
Usá solo la información del contexto.
No inventes datos.
"""

def construir_prompt_con_contexto(pregunta: str):
    contextos = obtener_contexto_para_pregunta(pregunta)

    if not contextos:
        return {
            "prompt_final": "No se encontró información relevante.",
            "fuente": None
        }

    partes = []
    for ctx in contextos:
        partes.append(ctx["contenido"])

    prompt_contexto = "\n".join(partes)

    # 🔥 LIMITADOR DE CONTEXTO
    if len(prompt_contexto) > 20000:
        prompt_contexto = prompt_contexto[:20000] + "\n[CONTEXTO RECORTADO]"

    prompt_final = f"""
Contexto:
{prompt_contexto}

Pregunta:
{pregunta}

Instrucciones:
- Respondé de forma clara.
- No inventes datos.
"""

    return {
        "prompt_final": prompt_final,
        "fuente": contextos[0]
    }

def responder_pregunta(pregunta: str):
    armado = construir_prompt_con_contexto(pregunta)
    respuesta = ask_openrouter(SYSTEM_PROMPT, armado["prompt_final"])
    return {
        "respuesta": respuesta,
        "fuente": armado["fuente"]
    }
