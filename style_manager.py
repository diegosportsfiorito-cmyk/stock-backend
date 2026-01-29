import json
import os

STYLE_FILE = "style_config.json"

def load_style():
    try:
        with open(STYLE_FILE, "r") as f:
            data = json.load(f)
            return data.get("style", "vendedor_experto")
    except:
        return "vendedor_experto"

def save_style(new_style):
    with open(STYLE_FILE, "w") as f:
        json.dump({"style": new_style}, f)
