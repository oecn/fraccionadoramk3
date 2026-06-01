from pydantic import BaseModel


class ProduccionProductoRow(BaseModel):
    producto: str
    maquina: str
    unidades: int
    paquetes: int
    kg_consumidos: float
    fraccionamientos: int


class ProduccionMesRow(BaseModel):
    ym: str
    total_unidades: int
    maquina1_unidades: int
    maquina2_unidades: int
    promedio_3m: float


class ProduccionOptions(BaseModel):
    years: list[int]
    current_year: int
    current_month: int


class ProduccionResumen(BaseModel):
    year: int
    month: int
    range_months: int
    total_unidades: int
    maquina1_unidades: int
    maquina2_unidades: int
    total_paquetes: int
    total_kg: float
    total_fraccionamientos: int
    rows: list[ProduccionProductoRow]
    trend: list[ProduccionMesRow]
