import re
import unicodedata
from difflib import get_close_matches

class Indexer:
    def __init__(self, df):
        self.df = df
        self.df["texto"] = self.df.apply(self._build_text, axis=1)

        self.synonyms = {
            "pelota": "balon",
            "pelotas": "balones",
            "balon": "balones",
            "balones": "balones",
            "remera": "camiseta",
            "remeras": "camisetas",
            "zapatilla": "calzado",
            "zapatillas": "calzado",
            "gorra": "cap",
            "buzo": "hoodie"
        }

        self.stop_phrases = [
            "dime", "decime", "mostrame", "mostrar", "que hay", "qué hay",
            "que tenes", "qué tenés", "quiero ver", "hay", "tengo", "busco",
            "necesito", "decime que hay", "mostrame que hay"
        ]

        self.brand_map = {
            "ch1 sports": "ch1",
            "ch1 sport": "ch1",
            "ch1": "ch1",
            "adidas": "adidas",
            "nike": "nike",
            "topper": "topper"
        }

    def _build_text(self, row):
        parts = [
            str(row.get("Descripción", "")),
            str(row.get("Artículo", "")),
            str(row.get("Descripción.2", "")),
            str(row.get("Descripción.3", "")),
            str(row.get("Talle", "")),
            str(row.get("Marca", "")),
            str(row.get("Rubro", ""))
        ]
        text = " ".join(parts)
        return self._normalize(text)

    def _normalize(self, text):
        text = text.lower()
        text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
        text = re.sub(r"[^a-z0-9 ]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _clean_query(self, q):
        q = self._normalize(q)

        for phrase in self.stop_phrases:
            q = q.replace(phrase, "")

        words = q.split()
        cleaned = []

        for w in words:
            if w in self.synonyms:
                cleaned.append(self.synonyms[w])
            else:
                cleaned.append(w)

        return " ".join(cleaned).strip()

    def _normalize_brand(self, word):
        matches = get_close_matches(word, self.brand_map.keys(), n=1, cutoff=0.6)
        if matches:
            return self.brand_map[matches[0]]
        return word

    def query(self, question, solo_stock=False):
        q = self._clean_query(question)

        if not q:
            return {
                "tipo": "lista",
                "items": [],
                "voz": "Decime qué producto querés buscar."
            }

        words = q.split()
        words = [self._normalize_brand(w) for w in words]

        results = []

        for _, row in self.df.iterrows():
            text = row["texto"]

            if all(w in text for w in words):
                results.append(row)

        if not results:
            return {
                "tipo": "lista",
                "items": [],
                "voz": "No encontré resultados exactos, pero tengo alternativas si querés."
            }

        items = []
        for r in results:
            items.append({
                "codigo": r.get("Artículo", ""),
                "descripcion": r.get("Descripción", ""),
                "marca": r.get("Marca", ""),
                "rubro": r.get("Rubro", ""),
                "talles": [{"talle": r.get("Talle", ""), "stock": r.get("Cantidad", -1)}],
                "precio": r.get("LISTA1", 0),
                "color": r.get("Descripción.3", "")
            })

        return {
            "tipo": "lista",
            "items": items,
            "voz": f"Encontré {len(items)} resultados para {q}."
        }
