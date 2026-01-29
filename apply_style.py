def apply_style(style, result, query):
    items = result.get("items", [])
    count = len(items)

    # -------------------------
    # ESTILO: VENDEDOR EXPERTO
    # -------------------------
    if style == "vendedor_experto":
        if count == 0:
            result["voz"] = (
                "No tengo ese exacto, pero tengo alternativas que te pueden servir. "
                "Decime si querés que te muestre opciones parecidas."
            )
        else:
            result["voz"] = (
                f"¡Genial elección! Encontré {count} opciones. "
                "Si querés, te muestro solo los que tienen stock o los más vendidos."
            )
        return result

    # -------------------------
    # ESTILO: AMIGABLE
    # -------------------------
    if style == "amigable":
        if count == 0:
            result["voz"] = "No encontré eso exacto, pero tengo alternativas que te pueden servir."
        else:
            result["voz"] = f"Tengo {count} opciones para vos."
        return result

    # -------------------------
    # ESTILO: PROFESIONAL
    # -------------------------
    if style == "profesional":
        result["voz"] = f"{count} resultados encontrados."
        return result

    # -------------------------
    # ESTILO: MINIMALISTA
    # -------------------------
    if style == "minimalista":
        result["voz"] = f"{count} resultados."
        return result

    # -------------------------
    # ESTILO: TECNICO
    # -------------------------
    if style == "tecnico":
        if count == 0:
            result["voz"] = "No hay coincidencias exactas. Podés intentar con otros términos."
        else:
            result["voz"] = f"{count} coincidencias exactas encontradas."
        return result

    return result
