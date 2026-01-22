# ai_openrouter.py
import requests
import os

API_KEY = os.getenv("OPENROUTER_API_KEY")

def ask_openrouter(system_prompt, user_prompt):
    url = "https://openrouter.ai/api/v1/chat/completions"

    payload = {
        "model": "openai/gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
    }

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    resp = requests.post(url, json=payload, headers=headers)

    if resp.status_code != 200:
        print("OpenRouter error:", resp.text)
        return "La consulta es demasiado grande o inválida. Probá ser más específico."

    data = resp.json()
    return data["choices"][0]["message"]["content"]
