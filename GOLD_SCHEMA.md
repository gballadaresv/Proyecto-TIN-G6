# Esquema de la Capa Gold — CuprumAndes SpA

Este documento describe el esquema, las fórmulas y las decisiones de diseño
del archivo Parquet que produce la etapa de métricas (`code/metrics.py`)
en la carpeta `gold/`.

---

## Principios de la capa gold

- **Solo métricas derivadas:** gold no contiene datos en crudo ni datos
  limpios sin transformar. Toda columna es un resultado calculado a partir
  de silver, salvo `mes` que es la clave de agregación.
- **Grano mensual a nivel empresa:** una fila por mes de 2025. No hay
  desglose por planta ni por rajo (decisión de diseño documentada en
  ARCHITECTURE.md, decisiones 11 y 19).
- **Sin dimensiones:** no existe `dim_planta` ni `dim_tiempo`; el mes se
  almacena directamente como columna de la tabla de hechos.
- **NaN explícito ante datos incompletos:** si falta cualquier input de un
  mes (un componente de costo, un precio o un tipo de cambio), las métricas
  dependientes quedan como NaN. No se imputan valores. Se emite una alerta
  en `validation_log.parquet`.
- **Unidades:** libras (lb) para el cobre, USD para los montos monetarios.
  El tipo de cambio CLP/USD es interno al cálculo y no aparece como columna.

---

## Archivo

| Parámetro  | Valor                         |
|------------|-------------------------------|
| Ruta       | `gold/gold_metricas.parquet`  |
| Formato    | Parquet (pandas/Power BI)     |
| Filas      | 12 (una por mes de 2025)      |
| Columnas   | 7 (ver tabla abajo)           |

---

## Esquema de columnas

| # | Columna               | Tipo    | Unidad    | Descripción                                                  |
|---|-----------------------|---------|-----------|--------------------------------------------------------------|
| 1 | `mes`                 | string  | —         | Mes del año en formato `YYYY-MM` (ej. `"2025-06"`).         |
| 2 | `cobre_fino_lb`       | float64 | lb        | Libras de cobre fino producidas en el mes (ver Fórmula A).  |
| 3 | `precio_cobre_usd_lb` | float64 | USD/lb    | Precio del cobre del último día hábil del mes (Fórmula B).  |
| 4 | `ingresos_usd`        | float64 | USD       | Ingresos por venta de cobre en el mes (Fórmula C).          |
| 5 | `costos_totales_usd`  | float64 | USD       | Costos operacionales totales en USD (Fórmula D). NaN si falta algún componente de costo. |
| 6 | `cash_cost_usd_lb`    | float64 | USD/lb    | Costo operacional por libra de cobre fino (Fórmula E). NaN si `costos_totales_usd` es NaN. |
| 7 | `utilidades_usd`      | float64 | USD       | Utilidad operacional del mes (Fórmula F). NaN si `costos_totales_usd` es NaN. |

---

## Fórmulas

### Fórmula A — Cobre fino mensual (libras)

Se calcula **primero a nivel de lote individual** y luego se agrega:

```
cobre_fino_lote_t  = toneladas_tratadas × (ley_pct / 100) × (recuperacion_pct / 100)
cobre_fino_mes_t   = SUM(cobre_fino_lote_t)   [todos los lotes del mes, todas las plantas]
cobre_fino_lb      = cobre_fino_mes_t × MT_TO_LB
```

donde `MT_TO_LB = 2 204.62262` (toneladas métricas a libras).

La ponderación por tonelaje queda implícita: los lotes más grandes
contribuyen más a la suma sin necesidad de calcular una ley ponderada
separada. Esta es la REGLA 2 de `business_rules.py`.

**Fuente:** `silver/silver_lotes.parquet` (columnas `mes`,
`toneladas_tratadas`, `ley_pct`, `recuperacion_pct`).

---

### Fórmula B — Precio del cobre (USD/lb)

```
precio_cobre_usd_lb = cotización del ÚLTIMO día hábil del mes en silver_precio_cobre
```

Se elige la fila con la fecha más reciente dentro de cada mes.
Si no existe cotización para un mes, el valor queda NaN y se emite una
alerta (REGLA 4 de `business_rules.py`).

**Fuente:** `silver/silver_precio_cobre.parquet` (columnas `fecha`, `valor`).

El **tipo de cambio USD-CLP** (dólar observado) se obtiene con el mismo
criterio — último día hábil del mes — desde `silver/silver_tipo_cambio.parquet`.
Se usa internamente para convertir costos de CLP a USD; no aparece como
columna en gold (ARCHITECTURE.md, decisión 19).

---

### Fórmula C — Ingresos (USD)

```
ingresos_usd = cobre_fino_lb × precio_cobre_usd_lb
```

**Fuentes:** Fórmulas A y B.

---

### Fórmula D — Costos totales (USD)

```
costos_totales_clp = SUM(energia_clp + insumos_clp + mano_obra_clp)
                     [todas las filas del mes en silver_costos]
costos_totales_usd = costos_totales_clp / tipo_cambio_clp_usd
```

Si cualquier fila del mes tiene un componente en NaN (ej. `insumos_clp`
de una planta), el total del mes completo queda NaN para no subvalorar
los costos. Se emite una alerta identificando la fila y el campo faltantes
(REGLA 3 de `business_rules.py`, `IMPUTE_MISSING_COST=False`).

**Fuente:** `silver/silver_costos.parquet` (columnas `mes`, `energia_clp`,
`insumos_clp`, `mano_obra_clp`).

---

### Fórmula E — Cash cost (USD/lb)

```
cash_cost_usd_lb = costos_totales_usd / cobre_fino_lb
```

Es la métrica central de la pregunta de negocio: ¿cuánto costó producir
cada libra de cobre? Un valor bajo indica producción más eficiente; un
valor que cae en paralelo a ingresos crecientes indica que el buen resultado
se debe al precio del cobre, no a una reducción de costos.

Queda NaN si `costos_totales_usd` es NaN.

---

### Fórmula F — Utilidades (USD)

```
utilidades_usd = ingresos_usd - costos_totales_usd
```

Utilidad operacional antes de impuestos e intereses. Queda NaN si
`costos_totales_usd` es NaN.

---

## Comportamiento ante datos incompletos

| Dato faltante                        | Columnas afectadas en gold                                      | Alerta emitida |
|--------------------------------------|-----------------------------------------------------------------|----------------|
| Componente de costo NaN (una planta) | `costos_totales_usd`, `cash_cost_usd_lb`, `utilidades_usd`      | Sí (REGLA 3)   |
| Sin cotización de precio del cobre   | `precio_cobre_usd_lb`, `ingresos_usd`, `cash_cost_usd_lb`, `utilidades_usd` | Sí (REGLA 4) |
| Sin cotización del dólar             | `costos_totales_usd`, `cash_cost_usd_lb`, `utilidades_usd`      | Sí (REGLA 4)   |
| Mes sin lotes de producción          | Fila ausente de gold                                            | Sí (REGLA 6)   |

---

## Verificaciones de sanidad (REGLA 7)

Al finalizar el cálculo, se comprueban automáticamente:

- `cobre_fino_lb >= 0`
- `precio_cobre_usd_lb` dentro del rango histórico plausible (1–20 USD/lb)
- `ingresos_usd >= 0`
- `costos_totales_usd >= 0`
- `cash_cost_usd_lb >= 0`

`utilidades_usd` puede ser negativa (pérdida operacional) y no se valida
por signo. Las verificaciones registran alertas pero nunca modifican los
valores calculados ni detienen la ejecución.

---

## Estado conocido de los datos (2025)

- **2025-06:** `costos_totales_usd`, `cash_cost_usd_lb` y `utilidades_usd`
  son NaN. La planta Cerro Norte no reportó `insumos_clp` para ese mes
  (valor `N/A` en el CSV original). Ver alerta en `validation_log.parquet`
  con `rule = REGLA 3 / IMPUTE_MISSING_COST=False`.
- Los 11 meses restantes tienen las 7 columnas completas.

---

## Reglas de negocio de referencia

Todas las reglas aplicadas en esta etapa están documentadas como constantes
comentadas en `code/business_rules.py`:

| Regla    | Descripción resumida                                        |
|----------|-------------------------------------------------------------|
| REGLA 1  | Precio y tipo de cambio: último día hábil del mes          |
| REGLA 2  | Cobre fino: cálculo por lote, suma mensual, conversión a lb|
| REGLA 3  | Costo faltante: NaN en el mes, sin imputación              |
| REGLA 4  | Sin cotización en el mes: NaN en métricas dependientes     |
| REGLA 6  | Alertar si falta algún mes de los 12 esperados             |
| REGLA 7  | Sanidad: no-negatividad y rangos plausibles                |
