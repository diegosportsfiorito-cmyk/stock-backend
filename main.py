from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from indexer import buscar_articulo_en_archivos

app = FastAPI()

# ============================================================
# CORS (permitir frontend)
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Podés restringir a tu dominio si querés
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# MODELO DE ENTRADA
# ============================================================

class QueryRequest(BaseModel):
    question: str   # <-- ahora coincide con el frontend

# ============================================================
# ENDPOINT PRINCIPAL
# ============================================================

@app.post("/query")
async def query(req: QueryRequest):
    pregunta = req.question.strip()

    try:
        info, fuente = buscar_articulo_en_archivos(pregunta)

        # ============================================================
        # SIN RESULTADOS
        # ============================================================
        if info is None:
            return {
                "tipo": "mensaje",
                "mensaje": "No encontré artículos relacionados con tu consulta.",
                "voz": "No encontré artículos relacionados con tu consulta.",
                "fuente": fuente,
            }

        # ============================================================
        # PRODUCTO INDIVIDUAL
        # ============================================================
        if info.get("tipo") == "producto":
            descripcion = info.get("descripcion", "")
            stock_total = info.get("stock_total", 0)
            precio = info.get("precio", None)

            voz = f"{descripcion}. Stock total {stock_total} unidades."
            if precio:
                voz += f" Precio {precio} pesos."

            return {
                "tipo": "producto",
                "data": info,
                "voz": voz,
                "fuente": fuente,
            }

        # ============================================================
        # LISTA DE PRODUCTOS
        # ============================================================
        if info.get("tipo") == "lista":
            lista = info.get("lista_completa", [])
            cantidad = len(lista)

            voz = f"Encontré {cantidad} modelos relacionados."

            return {
                "tipo": "lista",
                "cantidad": cantidad,
                "items": lista,   # <-- corregido para coincidir con el frontend
                "voz": voz,
                "fuente": fuente,
            }

        # ============================================================
        # RESUMEN POR MARCA
        # ============================================================
        if info.get("tipo") == "marca_resumen":
            marca = info.get("marca", "")
            stock_total = info.get("stock_total", 0)
            valorizado_total = info.get("valorizado_total", 0)

            voz = f"La marca {marca} tiene {stock_total} unidades en total."
            if valorizado_total:
                voz += f" El valorizado total es de {valorizado_total} pesos."

            return {
                "tipo": "marca_resumen",
                "data": info,
                "voz": voz,
                "fuente": fuente,
            }

        # ============================================================
        # RESUMEN POR RUBRO
        # ============================================================
        if info.get("tipo") == "rubro_resumen":
            rubro = info.get("rubro", "")
            stock_total = info.get("stock_total", 0)
            valorizado_total = info.get("valorizado_total", 0)

            voz = f"El rubro {rubro} tiene {stock_total} unidades en total."
            if valorizado_total:
                voz += f" El valorizado total es de {valorizado_total} pesos."

            return {
                "tipo": "rubro_resumen",
                "data": info,
                "voz": voz,
                "fuente": fuente,
            }

        # ============================================================
        # FALLBACK
        # ============================================================
        return {
            "tipo": "mensaje",
            "mensaje": "No pude interpretar la consulta.",
            "voz": "No pude interpretar la consulta.",
            "fuente": fuente,
        }

    except Exception as e:
        print("ERROR EN /query:", e)
        return {
            "tipo": "mensaje",
            "mensaje": "Ocurrió un error procesando la consulta.",
            "voz": "Ocurrió un error procesando la consulta.",
            "error": str(e),
        }

# ============================================================
# ENDPOINT DE PRUEBA
# ============================================================

@app.get("/")
async def root():
    return {"status": "OK", "message": "Stock IA Backend funcionando."}
