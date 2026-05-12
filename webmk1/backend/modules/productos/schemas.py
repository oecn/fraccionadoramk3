from pydantic import BaseModel


class PrecioRow(BaseModel):
    product_id: int
    producto: str
    gramaje: int
    price_gs: float
    iva: int
    paquetes_stock: int


class PrecioUpdate(BaseModel):
    product_id: int
    gramaje: int
    price_gs: float
    iva: int


class ProductoPrecioSummary(BaseModel):
    rows: list[PrecioRow]


class PrecioHistoryRow(BaseModel):
    fecha: str
    price_gs: float
    iva: int


class PrecioHistoryResponse(BaseModel):
    product_id: int
    producto: str
    gramaje: int
    rows: list[PrecioHistoryRow]
