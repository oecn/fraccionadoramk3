export type BankRow = {
  bank_id: string;
  banco_nombre: string;
  nro_cuenta: string;
  resumen: string;
  ts_registro: string;
};

export type BankCreate = {
  banco_nombre: string;
  nro_cuenta: string;
  resumen: string;
};

export type CheckbookRow = {
  chequera_id: string;
  bank_id: string;
  banco_nombre: string;
  nro_cuenta: string;
  formato_chequera: string;
  tipo_cheque: string;
  serie: string;
  fecha_recibimiento: string;
  nro_inicio: number;
  nro_fin: number;
  recibido_por: string;
  resumen: string;
  ts_registro: string;
};

export type CheckbookCreate = {
  bank_id: string;
  formato_chequera: string;
  tipo_cheque: string;
  serie: string;
  fecha_recibimiento: string;
  nro_inicio: number;
  nro_fin: number;
  recibido_por: string;
  resumen: string;
};

export type UsedCheckRow = {
  id: number;
  chequera_id: string;
  cheque_no: string;
  serie: string;
  referencia: string;
  payment_group_id: string;
  used_ts: string;
  banco_nombre: string;
  nro_cuenta: string;
  formato_chequera: string;
  tipo_cheque: string;
};

export type ChequerasSummary = {
  banks: BankRow[];
  checkbooks: CheckbookRow[];
  used_checks: UsedCheckRow[];
};
