import pandas as pd
import numpy as np
import re
from difflib import get_close_matches

# ============================================================
# INDEXER v4.0 — CORREGIDO Y MEJORADO
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

        # Detectar columnas relevantes
        columnas_texto = []
        for col in df.columns:
            if any(key in col for key in ["desc", "rubro", "categoria", "articulo", "artículo"]):
                columnas_texto.append(col)

        # Si no detecta nada, fallback
        if not columnas_texto:
            columnas_texto = [df.columns[0]]

        # Crear columna "texto" unificada
        df["texto"] = df[columnas_texto].astype(str).apply(lambda x: " ".join(x), axis=1)

        # Normalizar texto
        df["texto"] = df["texto"].str.lower()
        df["texto"] = df["texto"].str.normalize("NFKD").str.encode("ascii", "ignore").str.decode("ascii")

        # Normalizar plurales
        df["texto"] = df["texto"].apply(self._normalize_plural)

        # Normalizar stock
        if "stock" in df.columns:
            df["stock"] = pd.to_numeric(df["stock"], errors="coerce").fillna(0)
        else:
            df["stock"] = 0

        return df

    # ============================================================
    # NORMALIZACIÓN DE PLURALES
    # ============================================================
    def _normalize_plural(self, text):
        # balones → balon, pelotas → pelota, zapatillas → zapatilla
        if text.endswith("es"):
            return text[:-2]
        if text.endswith("s"):
            return text[:-1]
        return text

    # ============================================================
    # PROCESAR CONSULTA
    # ============================================================
    def query(self, question, solo_stock=False):
        if not question:
            return self._respuesta_vacia("No entendí la consulta.")

        q = question.lower().strip()
        q = self._normalize_plural(q)

        # Normalizar acentos
        q = (
            q.normalize("NFKD")
            .encode("ascii", "ignore")
            .decode("ascii")
            if hasattr(q, "normalize")
            else q
        )

        # ============================================================
        # 1) BÚSQUEDA DIRECTA
        # ============================================================
        directos = self.df[self.df["texto"].str.contains(q, regex=False, na=False)]

        if solo_stock:
            directos = directos[directos["stock"] > 0]

        if len(directos) > 0:
            return self._formatear_respuesta(directos, question)

        # ============================================================
        # 2) BÚSQUEDA POR PALABRAS INDIVIDUALES
        # ============================================================
        palabras = q.split()
        if len(palabras) > 1:
            filtro = self.df.copy()
            for p in palabras:
                filtro = filtro[filtro["texto"].str.contains(p, regex=False, na=False)]

            if solo_stock:
                filtro = filtro[filtro["stock"] > 0]

            if len(filtro) > 0:
                return self._formatear_respuesta(filtro, question)

        # ============================================================
        # 3) BÚSQUEDA POR SIMILITUD (difflib)
        # ============================================================
        todos_los_textos = self.df["texto"].tolist()
        candidatos = get_close_matches(q, todos_los_textos, n=20, cutoff=0.6)

        if candidatos:
            simil = self.df[self.df["texto"].isin(candidatos)]
            if solo_stock:
                simil = simil[simil["stock"] > 0]

            if len(simil) > 0:
                return self._formatear_respuesta(simil, question)

        # ============================================================
        # 4) BÚSQUEDA POR RUBRO (columna 2 del Excel)
        # ============================================================
        if "descripcion_1" in self.df.columns:
            rubro = self.df[self.df["descripcion_1"].str.contains(q, regex=False, na=False)]
            if solo_stock:
                rubro = rubro[rubro["stock"] > 0]

            if len(rubro) > 0:
                return self._formatear_respuesta(rubro, question)

        # ============================================================
        # 5) SIN RESULTADOS
        # ============================================================
        return self._respuesta_vacia("No encontré resultados.")

    # ============================================================
    # FORMATEAR RESPUESTA
    # ============================================================
    def _formatear_respuesta(self, df, question):
        items = []

        for _, row in df.iterrows():
            items.append({
                "codigo": row.get("codigo", ""),
                "descripcion": row.get("texto", ""),
                "talles": [{"talle": row.get("talle", ""), "stock": row.get("stock", 0)}],
                "precio": row.get("precio", None)
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
