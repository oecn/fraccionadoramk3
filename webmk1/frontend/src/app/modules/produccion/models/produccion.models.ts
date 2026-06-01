export type ProduccionProductoRow = {
  producto: string;
  maquina: string;
  unidades: number;
  paquetes: number;
  kg_consumidos: number;
  fraccionamientos: number;
};

export type ProduccionMesRow = {
  ym: string;
  total_unidades: number;
  maquina1_unidades: number;
  maquina2_unidades: number;
  promedio_3m: number;
};

export type ProduccionOptions = {
  years: number[];
  current_year: number;
  current_month: number;
};

export type ProduccionResumen = {
  year: number;
  month: number;
  range_months: number;
  total_unidades: number;
  maquina1_unidades: number;
  maquina2_unidades: number;
  total_paquetes: number;
  total_kg: number;
  total_fraccionamientos: number;
  rows: ProduccionProductoRow[];
  trend: ProduccionMesRow[];
};
