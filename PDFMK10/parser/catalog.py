# parser/catalog.py

CATALOG = {
    "206000": {"name": "ARROZ EL CACIQUE *250G (20)"},
    "208288": {"name": "ARROZ EL CACIQUE *1KG (10)"},
    "206001": {"name": "ARROZ EL CACIQUE *500G (10)"},
    "208320": {"name": "AZUCAR EL CACIQUE *1KG (10)"},
    "205998": {"name": "AZUCAR EL CACIQUE *250G (20)"},
    "205999": {"name": "AZUCAR EL CACIQUE *500G (10)"},
    "228894": {"name": "GALL. MOLIDA EL CACIQUE *400GR (10)"},
    "228895": {"name": "GALL. MOLIDA EL CACIQUE *800GR (10)"},
    "213598": {"name": "LOCRILLO EL CACIQUE *200GR (20)"},
    "213599": {"name": "LOCRILLO EL CACIQUE *400GR (10)"},
    "213597": {"name": "LOCRO EL CACIQUE *400GR (10)"},   # código lógico para asegurar captura
    "213595": {"name": "LOCRO EL CACIQUE *200GR (20)"},
    "205996": {"name": "PORORO EL CACIQUE *200 (20)"},
    "205997": {"name": "PORORO EL CACIQUE *400 (10)"},       # corregido: NO es locro
    "213601": {"name": "PORORO EL CACIQUE *800GR (10)"},
    "206002": {"name": "POROTO ROJO EL CACIQUE*200G (20)"},
    "206003": {"name": "POROTO ROJO EL CACIQUE*400 (10)"},
}

ALIASES = {
    # ARROZ
    r"\barroz\s+el\s+cacique.*250\s*g\b": "206000",
    r"\barroz\s+el\s+cacique.*1\s*kg\b":  "208288",
    r"\barroz\s+el\s+cacique.*500\s*g\b": "206001",

    # AZÚCAR
    r"\bazucar\s+el\s+cacique.*1\s*kg\b":  "208320",
    r"\bazucar\s+el\s+cacique.*250\s*g\b": "205998",
    r"\bazucar\s+el\s+cacique.*500\s*g\b": "205999",

    # GALL. MOLIDA
    r"\bgall\.\s*molida\s+el\s+cacique.*400\s*gr\b": "228894",
    r"\bgall\.\s*molida\s+el\s+cacique.*800\s*gr\b": "228895",

    # LOCRILLO
    r"\blocri?llo\s+el\s+cacique.*400\s*gr\b": "213599",
    r"\blocri?llo\s+el\s+cacique.*200\s*gr\b": "213598",


    # LOCRO (400GR y 200GR con sus códigos reales)
    r"\blocro\s+el\s+cacique.*400\s*gr\b": "213597",
    r"\blocro\s+el\s+cacique.*200\s*gr\b": "213595",

    # PORORO
    r"\bpororo\s+el\s+cacique.*200\b":    "205996",
    r"\bpororo\s+el\s+cacique.*400\b":    "205997",
    r"\bpororo\s+el\s+cacique.*800\s*gr\b": "213601",

    # POROTO ROJO
    r"\bporoto\s+rojo\s+el\s+cacique.*200\s*g\b": "206002",
    r"\bporoto\s+rojo\s+el\s+cacique.*400\b":     "206003",
}

