# ai_engine.py
from typing import List, Dict
from ai_openrouter import ask_openrouter
from indexer import obtener_contexto_para_pregunta


SYSTEM_PROMPT = """
Sos un asistente de stock para una zapatería deportiva.
Tenés acceso a información de inventario proveniente de archivos de Excel.

Reglas:
- Respondé siempre en español, de forma clara y profesional.
- Si el usuario pregunta por un código de artículo (ej: 100000089), devolvé:
  - Modelo / nombre del artículo.
  - Breve descripción.
  - Precio.
  - Stock total.
  - Detalle de stock por talle (si está disponible), en viñetas.
- Si no encontrás información suficiente en los datos, decilo explícitamente.
- No inventes datos que no estén en el contexto.
- No inventes archivos ni fechas.
- No incluyas la fuente en el texto de la respuesta: la fuente la maneja el sistema.
"""

def construir_prompt_con_contexto(pregunta: str) -> Dict:
    """
    Usa el indexador para obtener contexto relevante desde Drive
    y arma el prompt final para el modelo.
    Debe devolver:
    - prompt_final (str)
    - fuente (dict con archivo, fecha) o None
    """
    # Esta función la asumimos existente en indexer.py:
    # devuelve una lista de dicts con: archivo, fecha, contenido
    contextos: List[Dict] = obtener_contexto_para_pregunta(pregunta)

    if not contextos:
        prompt_contexto = "No se encontró información relevante en los archivos de stock."
        fuente = None
    else:
        partes = []
        for ctx in contextos:
            partes.append(
                f"Archivo: {ctx['archivo']}\n"
                f"Fecha: {ctx['fecha']}\n"
                f"Contenido relevante:\n{ctx['contenido']}\n"
                "-------------------------\n"
            )
        prompt_contexto = "\n".join(partes)

        # Por ahora tomamos la primera fuente como principal
        fuente = {
            "archivo": contextos[0]["archivo"],
            "fecha": contextos[0]["fecha"],
        }

    prompt_final = f"""
Contexto de inventario (extraído de archivos de stock):

{prompt_contexto}

Pregunta del usuario:
{pregunta}

Instrucciones:
- Respondé de forma clara y ordenada.
- Si la pregunta es sobre un código de artículo, devolvé:
  - Modelo / nombre del artículo.
  - Breve descripción.
  - Precio.
  - Stock total.
  - Detalle de stock por talle (si está disponible), en viñetas.
- Si no hay datos suficientes, decilo.
- No menciones los nombres de los archivos ni las fechas en el texto de la respuesta.
"""

    return {
        "prompt_final": prompt_final,
        "fuente": fuente,
    }


def responder_pregunta(pregunta: str) -> Dict:
    armado = construir_prompt_con_contexto(pregunta)
    prompt_final = armado["prompt_final"]
    fuente = armado["fuente"]

    respuesta_texto = ask_openrouter(SYSTEM_PROMPT, prompt_final)

    return {
        "respuesta": respuesta_texto,
        "fuente": fuente,
    }
