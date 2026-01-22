# ai_engine.py
import indexer
from ai_openrouter import ask_openrouter
from intent_engine import clasificar_intencion

SYSTEM_PROMPT = """
Sos un asistente de stock para una zapatería deportiva.
Respondé siempre en español.
Usá solo la información del contexto.
No inventes datos.
"""

def construir_prompt_con_contexto(pregunta: str):
    contextos = indexer.obtener_contexto_para_pregunta(pregunta)

    if not contextos:
        return {
            "prompt_final": "No se encontró información relevante.",
            "fuente": None,
        }

    partes = [ctx["contenido"] for ctx in contextos]
    prompt_contexto = "\n".join(partes)

    MAX_CHARS = 20000
    if len(prompt_contexto) > MAX_CHARS:
        prompt_contexto = prompt_contexto[:MAX_CHARS] + "\n[CONTEXTO RECORTADO]"

    prompt_final = f"""
Contexto:
{prompt_contexto}

Pregunta:
{pregunta}

Instrucciones:
- Respondé de forma clara y concreta.
- No inventes datos.
- Usá solo el contexto provisto.
"""

    return {
        "prompt_final": prompt_final,
        "fuente": contextos[0],
    }

# ============================================================
# RESPUESTA DE PRECIOS
# ============================================================

def generar_respuesta_precio(pregunta, contexto):
    lineas = contexto.split("\n")

    codigo = None
    precio_publico = None
    precio_costo = None

    for linea in lineas:
        if linea.startswith("Artículo:"):
            codigo = linea.replace("Artículo:", "").strip()

    if not codigo:
        return "No pude identificar el artículo para obtener su precio."

    # Volvemos a cargar el Excel para extraer precios reales
    archivos = indexer.listar_archivos_en_carpeta(indexer.CARPETA_STOCK_ID)
    for archivo in archivos:
        if archivo["name"].lower().endswith(".xlsx"):
            file_bytes = indexer.descargar_archivo_por_id(archivo["id"])
            excel_file = indexer.io.BytesIO(file_bytes)
            xls = pd.ExcelFile(excel_file)
            df = pd.concat([xls.parse(sheet, dtype=str).fillna("") for sheet in xls.sheet_names])
            df = indexer.normalizar_encabezados(df)

            col_codigo = indexer.detectar_columna_codigo(df)
            precios = indexer.obtener_precios(df, col_codigo, codigo)

            if precios:
                precio_publico = precios["publico"]
                precio_costo = precios["costo"]
                break

    partes = []
    if precio_publico:
        partes.append(f"El precio público del artículo {codigo} es ${precio_publico}.")
    if precio_costo:
        partes.append(f"El precio de costo del artículo {codigo} es ${precio_costo}.")

    if partes:
        return " ".join(partes)

    return f"No encontré precios para el artículo {codigo}."

# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def responder_pregunta(pregunta: str):
    intencion = clasificar_intencion(pregunta)

    armado = construir_prompt_con_contexto(pregunta)

    if intencion == "consulta_precio":
        return {
            "respuesta": generar_respuesta_precio(pregunta, armado["fuente"]["contenido"]),
            "fuente": armado["fuente"],
        }

    if intencion == "consulta_stock":
        try:
            respuesta = ask_openrouter(SYSTEM_PROMPT, armado["prompt_final"])
        except Exception as e:
            respuesta = f"Error al consultar el motor de IA: {e}"

        return {
            "respuesta": respuesta,
            "fuente": armado["fuente"],
        }

    return {
        "respuesta": "Por ahora solo estoy preparado para responder consultas de stock y precios basados en el Excel.",
        "fuente": None,
    }
