from groq import Groq
import os

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def ask_ai(prompt):
    response = client.chat.completions.create(
        modelo="llama3-8b-8192",
        messages=[
            {
                "role": "system",
                "content": (
                    "Sos un analista experto en stock. "
                    "Respondé siempre usando únicamente los datos proporcionados. "
                    "No inventes datos que no estén en el Excel. "
                    "Explicá de forma clara, profesional y útil."
                )
            },
            {"role": "user", "content": prompt}
        ],
        temperature=0.2,
        max_tokens=800
    )


    return response.choices[0].message["content"]

