import re
import unicodedata
from difflib import get_close_matches
from collections import defaultdict

class Indexer:
    def __init__(self, df):
        self.df = df

        # Mapeo REAL de columnas según tu Excel
        # Col 0: Descripción (MARCA)
        # Col 1: Descripción (RUBRO)
        # Col 2: Artículo (CÓDIGO)
        # Col 3: Descripción (NOMBRE)
        # Col 4: Descripción (COLOR)
        # Col 5: Talle
        # Col 6: Cantidad (STOCK)
        # Col 7: LISTA1 (PRECIO)
        # Col 8: Valorizado LISTA1 (STOCK * PRECIO)
        self.df["marca"] = self.df.iloc[:, 0].astype(str)
        self.df["rubro"] = self.df.iloc[:, 1].astype(str)
        self.df["codigo"] = self.df.iloc[:, 2].astype(str)
        self.df["nombre"] = self.df.iloc[:, 3].astype(str)
        self.df["color"] = self.df.iloc[:, 4].astype(str)
        self.df["talle"] = self.df.iloc[:, 5].astype(str)
        self.df["stock"] = self.df.iloc[:, 6]
        self.df["precio"] = self.df.iloc[:, 7]
        self.df["valorizado"] = self.df.iloc[:, 8]

        # Texto indexado para búsqueda
        self.df["texto"] = self.df.apply(self._build_text, axis=1)

        # Sinónimos
        self.synonyms = {
            "pelota": "balon",
            "pelotas": "balon",
            "balon": "balon",
            "balones": "balon",
            "remera": "camiseta",
            "remeras": "camiseta",
            "zapatilla": "calzado",
            "zapatillas": "calzado",
            "gorra": "gorra",
            "buzo": "hoodie",
            "ojota": "ojotas",
            "ojotas": "ojotas",
            "sandalia": "sandalia",
            "sandalias": "sandalia"
        }

        # Frases que no aportan significado
        self.stop_phrases = [
            "dime", "decime", "mostrame", "mostrar", "que hay", "qué hay",
            "que tenes", "qué tenés", "quiero ver", "hay", "tengo", "busco",
            "necesito", "decime que hay", "mostrame que hay", "cuantos", "cuánto"
        ]

    def _normalize(self, text):
        text = str(text).lower()
        text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
        text = re.sub(r"[^a-z0-9 ]", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _build_text(self, row):
        parts = [
            row["marca"],
            row["rubro"],
            row["codigo"],
            row["nombre"],
            row["color"],
            row["talle"]
        ]
        return self._normalize(" ".join(parts))

    def _clean_query(self, q):
        q = self._normalize(q)

        for phrase in self.stop_phrases:
            q = q.replace(phrase, "")

        words = q.split()
        cleaned = []

        for w in words:
            cleaned.append(self.synonyms.get(w, w))

        return " ".join(cleaned).strip()

    def query(self, question, solo_stock=False):
        q = self._clean_query(question)

        if not q:
            return {
                "tipo": "lista",
                "items": [],
                "voz": "Decime qué producto querés buscar."
            }

        words = q.split()

        results = []
        for _, row in self.df.iterrows():
            text = row["texto"]
            if all(w in text for w in words):
                if solo_stock:
                    try:
                        if float(str(row["stock"]).replace(",", ".")) <= 0:
                            continue
                    except Exception:
                        pass
                results.append(row)

        if not results:
            return {
                "tipo": "lista",
                "items": [],
                "voz": f"No encontré resultados para '{question}', pero puedo buscar algo parecido."
            }

        grouped = defaultdict(lambda: {
            "codigo": "",
            "descripcion": "",
            "marca": "",
            "rubro": "",
            "talles": [],
            "precio": 0,
            "valorizado": 0,
            "color": ""
        })

        for r in results:
            code = r["codigo"]

            grouped[code]["codigo"] = r["codigo"]
            grouped[code]["descripcion"] = r["nombre"]
            grouped[code]["marca"] = r["marca"]
            grouped[code]["rubro"] = r["rubro"]
            grouped[code]["precio"] = r["precio"]
            grouped[code]["color"] = r["color"]

            grouped[code]["talles"].append({
                "talle": r["talle"],
                "stock": r["stock"]
            })

            try:
                grouped[code]["valorizado"] += float(str(r["valorizado"]).replace(".", "").replace(",", "."))
            except Exception:
                pass

        items = list(grouped.values())

        return {
            "tipo": "lista",
            "items": items,
            "voz": f"Encontré {len(items)} resultados para '{question}'."
        }
