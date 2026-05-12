from pydantic import BaseModel, Field


class BankCreate(BaseModel):
    banco_nombre: str = Field(min_length=1)
    nro_cuenta: str = Field(min_length=1)
    resumen: str = ""


class BankRow(BaseModel):
    bank_id: str
    banco_nombre: str
    nro_cuenta: str
    resumen: str
    ts_registro: str


class CheckbookCreate(BaseModel):
    bank_id: str = Field(min_length=1)
    formato_chequera: str = "Formulario"
    tipo_cheque: str = "Vista"
    serie: str = ""
    fecha_recibimiento: str = ""
    nro_inicio: int = Field(ge=0)
    nro_fin: int = Field(ge=0)
    recibido_por: str = Field(min_length=1)
    resumen: str = ""


class CheckbookRow(BaseModel):
    chequera_id: str
    bank_id: str
    banco_nombre: str
    nro_cuenta: str
    formato_chequera: str
    tipo_cheque: str
    serie: str
    fecha_recibimiento: str
    nro_inicio: int
    nro_fin: int
    recibido_por: str
    resumen: str
    ts_registro: str


class UsedCheckRow(BaseModel):
    id: int
    chequera_id: str
    cheque_no: str
    serie: str
    referencia: str
    payment_group_id: str
    used_ts: str
    banco_nombre: str
    nro_cuenta: str
    formato_chequera: str
    tipo_cheque: str


class ChequerasSummary(BaseModel):
    banks: list[BankRow]
    checkbooks: list[CheckbookRow]
    used_checks: list[UsedCheckRow]
