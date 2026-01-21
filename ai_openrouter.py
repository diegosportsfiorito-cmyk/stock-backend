import os
import requests

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

URL = "https://openrouter.ai/api/v1/chat/completions"

# Modelo gratuito y disponible
MODEL = "meta-llama/llama-3.1-8b-instruct"


def ask_openrouter(system_prompt: str, user_prompt: str) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "Referer": "https://stock-backend-1-twzg.onrender.com",
        "X-Title": "Stock Backend",
    }

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }

    resp = requests.post(URL, json=payload, headers=headers, timeout=60)

    if resp.status_code != 200:
        print("OpenRouter error:", resp.status_code, resp.text)

    resp.raise_for_status()
    data = resp.json()

    return data["choices"][0]["message"]["content"]
