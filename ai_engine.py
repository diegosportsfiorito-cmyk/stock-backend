import os
from dotenv import load_dotenv
from openai import OpenAI

# Cargar variables de entorno (.env en local, Environment en Render)
load_dotenv()

DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

if not DEEPSEEK_API_KEY:
    raise RuntimeError("Falta la variable DEEPSEEK_API_KEY en el entorno")

# Cliente apuntando a DeepSeek (usa SDK de OpenAI)
client = OpenAI(
    api_key=DEEPSEEK_API_KEY,
    base_url="https://api.deepseek.com"
)

def ask_ai(prompt: str) -> str:
    """
    Llama al modelo DeepSeek para responder sobre el stock.
    """
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Sos un asistente experto en stock de productos. "
                        "Respondé siempre en español, de forma clara y precisa."
                    )
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2,
            max_tokens=800
        )

        # Formato de respuesta del SDK de OpenAI / DeepSeek
        return response.choices[0].message.content

    except Exception as e:
        print("🔥 ERROR EN ask_ai:", e)
        return "Hubo un error al consultar la IA. Intentalo nuevamente."
