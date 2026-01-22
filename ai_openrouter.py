# ai_openrouter.py
import os
import requests

API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "openai/gpt-4o-mini"

def ask_openrouter(system_prompt: str, user_prompt: str) -> str:
    if not API_KEY:
        return "No hay API Key configurada para OpenRouter."

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        resp = requests.post(OPENROUTER_URL, json=payload, headers=headers, timeout=60)
    except Exception as e:
        return f"Error de conexión con OpenRouter: {e}"

    if resp.status_code != 200:
        print("OpenRouter error:", resp.status_code, resp.text)
        return "La consulta es demasiado grande o inválida, o hubo un problema con el modelo."

    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"]
    except Exception:
        return "No se pudo interpretar la respuesta del modelo."
