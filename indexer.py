import pandas as pd
import unicodedata
from difflib import get_close_matches

# ============================================================
# INDEXER v5.0 — ADAPTADO A TU EXCEL
# ============================================================

class Indexer:
    def __init__(self, df):
        self.df_original = df
        self.df = self._normalize_dataframe(df)

    # ============================================================
    # NORMALIZACIÓN COMPLETA DEL EXCEL
    # ============================================================
    def _normalize_dataframe(self, df):
        df = df.copy()

        # Normalizar nombres de columnas
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        cols = list(df.columns)

        # Mapeo explícito según tu estructura
        # 0: marca, 1: rubro, 2: codigo, 3: desc1, 4: desc2, 5: talle, 6: cantidad, 7: lista1, 8: valorizado
        marca_col     = cols[0] if len(cols) > 0 else None
        rubro_col     = cols[1] if len(cols) > 1 else None
        codigo_col    = cols[2] if len(cols) > 2 else None
        desc_cols     = [c for c in cols[3:5] if c]  # 3 y 4
        talle_col     = cols[5] if len(cols) > 5 else None
        cantidad_col  = cols[6] if len(cols) > 6 else None
        precio_col    = cols[7] if len(cols) > 7 else None
        valorizado_col= cols[8] if len(cols) > 8 else None

        # Crear columnas estándar
        if marca_col:
            df["marca"] = df[marca_col].astype(str)
        else:
            df["marca"] = ""

        if rubro_col:
            df["rubro"] = df[rubro_col].astype(str)
        else:
            df["rubro"] = ""

        if codigo_col:
            df["codigo"] = df[codigo_col].astype(str)
        else:
            df["codigo"] = ""

        if desc_cols:
            df["descripcion"] = df[desc_cols].astype(str).apply(lambda x: " ".join(x), axis=1)
        else:
            df["descripcion"] = ""

        if talle_col:
            df["talle"] = df[talle_col].astype(str)
        else:
            df["talle"] = ""

        if cantidad_col:
            df["stock"] = pd.to_numeric(df[cantidad_col], errors="coerce").fillna(0)
        else:
            df["stock"] = 0

        if precio_col:
            df["precio"] = pd.to_numeric(df[precio_col], errors="coerce").fillna(0)
        else:
            df["precio"] = None

        if valorizado_col:
            df["valorizado"] = pd.to_numeric(df[valorizado_col], errors="coerce").fillna(0)
        else:
            df["valorizado"] = 0

        # Texto unificado para búsqueda: marca + rubro + descripción + código
        df["texto"] = (
            df["marca"].fillna("") + " " +
            df["rubro"].fillna("") + " " +
            df["descripcion"].fillna("") + " " +
            df["codigo"].fillna("")
        )

        # Normalizar texto (minúsculas + sin acentos + plurales)
        df["texto"] = df["texto"].str.lower().apply(self._strip_accents)
        df["texto"] = df["texto"].apply(self._normalize_plural)

        return df

    # ============================================================
    # UTILIDADES DE NORMALIZACIÓN
    # ============================================================
    def _strip_accents(self, text):
        if not isinstance(text, str):
            text = str(text)
        return "".join(
            c for c in unicodedata.normalize("NFKD", text)
            if not unicodedata.combining(c)
        )

    def _normalize_plural(self, text):
        # balones → balon, pelotas → pelota, zapatillas → zapatilla
        text = text.strip()
        if len(text) > 3 and text.endswith("es"):
            return text[:-2]
        if len(text) > 2 and text.endswith("s"):
            return text[:-1]
        return text

    # ============================================================
    # PROCESAR CONSULTA
    # ============================================================
    def query(self, question, solo_stock=False):
        if not question:
            return self._respuesta_vacia("No entendí la consulta.")

        q = question.lower().strip()
        q = self._strip_accents(q)
        q = self._normalize_plural(q)

        # 1) Búsqueda directa en texto
        directos = self.df[self.df["texto"].str.contains(q, regex=False, na=False)]

        if solo_stock:
            directos = directos[directos["stock"] > 0]

        if len(directos) > 0:
            return self._formatear_respuesta(directos, question)

        # 2) Búsqueda por palabras individuales
        palabras = [p for p in q.split() if p]
        if len(palabras) > 1:
            filtro = self.df.copy()
            for p in palabras:
                filtro = filtro[filtro["texto"].str.contains(p, regex=False, na=False)]

            if solo_stock:
                filtro = filtro[filtro["stock"] > 0]

            if len(filtro) > 0:
                return self._formatear_respuesta(filtro, question)

        # 3) Búsqueda por similitud
        todos_los_textos = self.df["texto"].tolist()
        candidatos = get_close_matches(q, todos_los_textos, n=30, cutoff=0.6)

        if candidatos:
            simil = self.df[self.df["texto"].isin(candidatos)]
            if solo_stock:
                simil = simil[simil["stock"] > 0]

            if len(simil) > 0:
                return self._formatear_respuesta(simil, question)

        # 4) Sin resultados
        return self._respuesta_vacia("No encontré resultados.")

    # ============================================================
    # FORMATEAR RESPUESTA
    # ============================================================
    def _formatear_respuesta(self, df, question):
        items = []

        for _, row in df.iterrows():
            items.append({
                "codigo": row.get("codigo", ""),
                "descripcion": row.get("descripcion", ""),
                "marca": row.get("marca", ""),
                "rubro": row.get("rubro", ""),
                "talles": [{
                    "talle": row.get("talle", ""),
                    "stock": row.get("stock", 0)
                }],
                "precio": row.get("precio", None),
                "color": ""
            })

        return {
            "tipo": "lista",
            "items": items,
            "voz": f"Encontré {len(items)} resultados para {question}.",
            "fuente": {}
        }

    # ============================================================
    # RESPUESTA VACÍA
    # ============================================================
    def _respuesta_vacia(self, msg):
        return {
            "tipo": "lista",
            "items": [],
            "voz": msg,
            "fuente": {}
        }
