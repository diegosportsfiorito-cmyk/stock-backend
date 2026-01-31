import re
import unicodedata
from collections import defaultdict

class Indexer:
    def __init__(self, df):
        self.df = df

        # Mapeo REAL de columnas según tu Excel
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

    # ---------------------------------------------------------
    # NORMALIZACIÓN
    # ---------------------------------------------------------
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

    # ---------------------------------------------------------
    # ARMADO DE RESPUESTA
    # ---------------------------------------------------------
    def _build_response(self, df_subset, question):
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

        for _, r in df_subset.iterrows():
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
            except:
                pass

        items = list(grouped.values())

        return {
            "tipo": "lista",
            "items": items,
            "voz": f"Encontré {len(items)} resultados para '{question}'."
        }

    # ---------------------------------------------------------
    # QUERY PRINCIPAL (CON COINCIDENCIA EXACTA + PREFIJO)
    # ---------------------------------------------------------
    def query(self, question, solo_stock=False):
        q = self._clean_query(question)

        if not q:
            return {
                "tipo": "lista",
                "items": [],
                "voz": "Decime qué producto querés buscar."
            }

        q_lower = q.lower()

        # -----------------------------------------------------
        # 1) MATCH EXACTO POR PALABRA COMPLETA EN NOMBRE
        # -----------------------------------------------------
        mask_exact = self.df["nombre"].str.lower() == q_lower
        df_exact = self.df[mask_exact]

        if not df_exact.empty:
            if solo_stock:
                df_exact = df_exact[df_exact["stock"] > 0]
            return self._build_response(df_exact, question)

        # -----------------------------------------------------
        # 2) MATCH POR PREFIJO (VENDA → VENDA ELÁSTICA)
        # -----------------------------------------------------
        mask_prefix = self.df["nombre"].str.lower().str.startswith(q_lower)
        df_prefix = self.df[mask_prefix]

        if not df_prefix.empty:
            if solo_stock:
                df_prefix = df_prefix[df_prefix["stock"] > 0]
            return self._build_response(df_prefix, question)

        # -----------------------------------------------------
        # 3) BÚSQUEDA NORMAL (tu método original)
        # -----------------------------------------------------
        words = q_lower.split()
        results = []

        for _, row in self.df.iterrows():
            text = row["texto"]
            if all(w in text for w in words):
                if solo_stock:
                    try:
                        if float(str(row["stock"]).replace(",", ".")) <= 0:
                            continue
                    except:
                        pass
                results.append(row)

        if not results:
            return {
                "tipo": "lista",
                "items": [],
                "voz": f"No encontré resultados para '{question}', pero puedo buscar algo parecido."
            }

        return self._build_response(results, question)
