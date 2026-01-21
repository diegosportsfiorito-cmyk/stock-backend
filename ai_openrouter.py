import os
import requests

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "openai/gpt-4.1-mini"  # ajustable

def ask_openrouter(system_prompt: str, user_prompt: str) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }

    resp = requests.post(OPENROUTER_BASE_URL, json=payload, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    return data["choices"][0]["message"]["content"]
