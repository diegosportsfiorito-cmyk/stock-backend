# indexer.py
from typing import Dict, Any, List
from drive import collect_drive_corpus

def build_unified_context() -> str:
    """
    Convierte todo el corpus en un gran contexto textual
    para pasárselo a la IA.
    """
    corpus = collect_drive_corpus()
    chunks: List[str] = []

    # Sheets
    for sheet_file in corpus["sheets"]:
        chunks.append(f"=== GOOGLE SHEET: {sheet_file['name']} ===")
        for sheet_name, rows in sheet_file["sheets"].items():
            chunks.append(f"-- Hoja: {sheet_name} --")
            for row in rows:
                line = " | ".join(row)
                chunks.append(line)

    # PDFs
    for pdf in corpus["pdfs"]:
        chunks.append(f"=== PDF: {pdf['name']} ===")
        chunks.append(pdf["text"])

    # Imágenes (OCR)
    for img in corpus["images"]:
        chunks.append(f"=== IMAGEN (OCR): {img['name']} ===")
        chunks.append(img["text"])

    # Otros (solo metadata)
    for other in corpus["others"]:
        chunks.append(f"=== OTRO ARCHIVO: {other['name']} ({other.get('mimeType', 'desconocido')}) ===")

    return "\n".join(chunks)