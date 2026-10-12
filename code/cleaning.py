"""
cleaning.py
===========
Etapa 2 del pipeline: Limpieza y validacion.

Lee exclusivamente desde bronze/ (Parquet, todos los valores como texto) y escribe
datos limpios y tipados a silver/ (Parquet).

Responsabilidades:
  1. Leer bronze_costos, bronze_lotes, bronze_precio_cobre y bronze_tipo_cambio.
     (bronze_leyes_laboratorio permanece en bronze; no va a silver por decision de diseno.)
  2. Eliminar filas de cabecera repetida y la fila TOTAL de costos; registrar cada eliminacion.
  3. Estandarizar formatos:
       - mes (lotes)     : entero de mes -> string canonico "YYYY-MM" (anio = ANALYSIS_YEAR)
       - mes (costos)    : "YYYY-MM" ya canonico -> validar y dejar como string
       - fecha (series)  : ISO 8601 UTC -> "YYYY-MM-DD"
       - numericos costos: eliminar "$", puntos de miles, comas decimales, ",00" -> float
       - numericos lotes : manejar punto-como-miles y coma-como-decimal -> float
  4. Validar CADA campo despues de la conversion usando VALID_RANGES de business_rules.
     Una falla de validacion se registra en el log y NUNCA detiene la ejecucion.
  5. Detectar duplicados y registrar alertas (no se eliminan).
  6. Verificar que las series de precios tengan cotizacion en el ultimo dia de cada mes.
  7. Escribir silver_costos, silver_lotes, silver_precio_cobre y silver_tipo_cambio.

Por que esta etapa existe separada de las metricas:
  Separa "limpiar" de "calcular". Silver contiene solo datos tipados y verificados;
  ninguna metrica derivada, ninguna agregacion y ninguna conversion de moneda.
"""

import re
import pandas as pd

from config import BRONZE_DIR, SILVER_DIR, ANALYSIS_YEAR
import business_rules as br
import validation_log as vlog

# ---------------------------------------------------------------------------
# Constantes internas de este modulo
# ---------------------------------------------------------------------------

# Nombre de etapa que aparece en cada registro del log
_STAGE = "cleaning"

# Patron canonico de mes: "YYYY-MM"
_CANONICAL_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

# Patron canonico de fecha: "YYYY-MM-DD"
_CANONICAL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ===========================================================================
# FUNCIONES DE CONVERSION NUMERICA
# ===========================================================================

def _parse_clp_amount(raw: str):
    """
    Convierte un valor de costo en CLP desde texto a float.

    Maneja todos los formatos observados en bronze_costos:
      - Prefijo "$"                         -> se elimina
      - Puntos como separador de miles      -> se eliminan
      - Coma decimal (incluso trailing ",00") -> reemplazada por punto
      - Numero limpio sin formato especial  -> se convierte directamente

    Retorna None si la conversion falla; el llamador registrara la alerta.
    """
    s = raw.strip()
    if not s:
        return None
    # Eliminar simbolo de moneda
    s = s.replace("$", "").strip()
    # Punto Y coma presentes: puntos son miles, coma es decimal
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        # Solo coma: decimal (incluye trailing ",00")
        s = s.replace(",", ".")
    else:
        # Solo puntos o ninguno
        # Si hay exactamente un punto y exactamente 3 digitos tras el -> miles
        parts = s.split(".")
        if len(parts) > 1 and all(len(p) == 3 and p.isdigit() for p in parts[1:]):
            s = s.replace(".", "")
        # De lo contrario el punto es decimal (caso raro en CLP pero posible)
    try:
        return float(s)
    except ValueError:
        return None


def _parse_numeric_lotes(raw: str, field: str):
    """
    Convierte un valor numerico de lotes desde texto a float.

    En bronze_lotes coexisten dos convenciones de formato:
      - Punto como separador de miles (ej. "2.834", "3.191" en toneladas)
      - Coma como separador decimal   (ej. "0,85", "90,5")
      - Numeros enteros limpios       (ej. "2962", "85")

    La heuristica para el punto es DEPENDIENTE DEL CAMPO porque el patron
    "X.XXX" (un digito entero, punto, tres digitos) aparece tanto en toneladas
    como separador de miles (ej. "2.834" = 2834 t) como en ley como decimal
    (ej. "1.005" = 1.005 %Cu). No es posible distinguirlos solo por formato.

    Reglas por campo:
      - toneladas_tratadas: X.XXX con parte entera >= 1 -> miles (2.834 = 2834).
        Si parte entera == "0" -> decimal (no hay 0.XXX en toneladas en este dataset).
      - ley_pct: NUNCA se trata el punto como miles. Los valores de ley en este
        dataset son todos < 2 %Cu; un valor de 1000+ seria imposible.
        Excepcion: si hay coma en el mismo string, la coma es decimal y el punto
        seria miles, pero eso no ocurre en ley_pct en este dataset.
      - recuperacion_pct: igual que ley_pct; los valores son 85-115, nunca miles.
      - Cualquier otro campo: heuristica generica (parte entera != "0" y 3 digitos
        exactos tras el punto -> miles).

    Retorna None si la conversion falla.
    """
    s = raw.strip()
    if not s:
        return None

    if "," in s:
        # Coma decimal en todos los campos; puntos previos (si los hay) son miles
        s = s.replace(".", "").replace(",", ".")
    elif "." in s:
        parts = s.split(".")
        three_digit_decimal = (
            len(parts) == 2
            and len(parts[1]) == 3
            and parts[1].isdigit()
            and parts[0].isdigit()
        )
        if three_digit_decimal:
            if field == "toneladas_tratadas" and parts[0] != "0":
                # Toneladas: parte entera >= 1 y 3 digitos -> miles (2.834 = 2834)
                s = s.replace(".", "")
            elif field in ("ley_pct", "recuperacion_pct"):
                # Leyes y recuperacion: NUNCA son miles; el punto es siempre decimal
                pass  # dejar s sin cambio -> float(s) lo interpreta como decimal
            else:
                # Campo generico: parte entera != "0" -> miles
                if parts[0] != "0":
                    s = s.replace(".", "")
        # Si no cumple three_digit_decimal el punto ya es decimal; no se modifica s
    try:
        return float(s)
    except ValueError:
        return None


# ===========================================================================
# FUNCIONES DE ESTANDARIZACION DE FECHAS / MESES
# ===========================================================================

def _standardize_month_from_int(raw: str):
    """
    Convierte un numero de mes entero (ej. "3", "12") al formato canonico "YYYY-MM".

    El anio se toma de ANALYSIS_YEAR definido en config.py, porque bronze_lotes
    solo contiene el numero de mes sin anio (artefacto de la fuente original SQLite).
    Retorna None si el valor no puede interpretarse como mes valido (1-12).
    """
    s = raw.strip()
    try:
        m = int(s)
    except ValueError:
        return None
    if not (1 <= m <= 12):
        return None
    return f"{ANALYSIS_YEAR}-{m:02d}"


def _standardize_month_costos(raw: str):
    """
    Valida y normaliza el campo 'mes' de costos, que ya deberia venir en "YYYY-MM".

    Si el valor coincide con el patron canonico lo retorna sin modificaciones.
    Si no coincide retorna None para que el llamador registre la alerta.
    """
    s = raw.strip()
    if _CANONICAL_MONTH_RE.match(s):
        return s
    return None


def _standardize_date_iso(raw: str):
    """
    Convierte una fecha ISO 8601 UTC (ej. "2025-12-30T03:00:00.000Z") a "YYYY-MM-DD".

    La API de mindicador.cl entrega las fechas siempre en ese formato ISO.
    Se usa pandas.to_datetime con utc=True para manejar cualquier variante del formato.
    Retorna None si la conversion falla.
    """
    s = raw.strip()
    if not s:
        return None
    try:
        dt = pd.to_datetime(s, utc=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


# ===========================================================================
# FUNCIONES DE VALIDACION DE CAMPO
# ===========================================================================

def _validate_month(value, dataset: str, source_ref: str, field: str, original: str) -> None:
    """
    Valida que el mes canonico tenga formato "YYYY-MM" y pertenezca a ANALYSIS_YEAR.

    Registra una alerta si:
      - value es None (fallo la estandarizacion)
      - el formato no coincide con el patron canonico
      - el anio no es ANALYSIS_YEAR
    """
    if value is None:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"mes no pudo estandarizarse a YYYY-MM (valor original: '{original}')",
        )
        return
    if not _CANONICAL_MONTH_RE.match(value):
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"mes estandarizado '{value}' no cumple formato YYYY-MM",
        )
        return
    year = int(value[:4])
    if year != ANALYSIS_YEAR:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"mes '{value}' esta fuera del anio de analisis {ANALYSIS_YEAR}",
        )


def _validate_date(value, dataset: str, source_ref: str, field: str, original: str) -> None:
    """
    Valida que la fecha canonica tenga formato "YYYY-MM-DD" y pertenezca a ANALYSIS_YEAR.

    Registra una alerta si la conversion fallo, el formato es incorrecto o el anio
    no es ANALYSIS_YEAR.
    """
    if value is None:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"fecha no pudo estandarizarse a YYYY-MM-DD (valor original: '{original}')",
        )
        return
    if not _CANONICAL_DATE_RE.match(value):
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"fecha estandarizada '{value}' no cumple formato YYYY-MM-DD",
        )
        return
    year = int(value[:4])
    if year != ANALYSIS_YEAR:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"fecha '{value}' esta fuera del anio de analisis {ANALYSIS_YEAR}",
        )


def _validate_numeric(value, field: str, dataset: str, source_ref: str, original: str) -> None:
    """
    Valida que un campo numerico sea no-nulo, no-negativo y dentro del rango plausible
    definido en business_rules.VALID_RANGES.

    Tres reglas se verifican en secuencia:
      1. No nulo: el valor debe haber podido convertirse desde texto.
      2. No negativo: costos, toneladas y porcentajes no pueden ser negativos.
      3. Rango plausible: detecta transformaciones erroneas (ej. coma leida como miles).

    Se registra una alerta por cada regla que falle; multiples alertas en un mismo
    campo son posibles y deseadas para que el autor entienda exactamente que ocurrio.
    """
    if value is None:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=original,
            rule=f"campo '{field}' no pudo convertirse a numerico (valor original: '{original}')",
        )
        return
    if value < 0:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=str(value),
            rule=f"campo '{field}' es negativo ({value}); se espera >= 0",
        )
    rng = br.VALID_RANGES.get(field)
    if rng is not None:
        lo, hi = rng
        if not (lo <= value <= hi):
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=source_ref,
                field=field, value=str(value),
                rule=(
                    f"campo '{field}' = {value} fuera del rango plausible "
                    f"[{lo}, {hi}]; posible transformacion incorrecta"
                ),
            )


def _validate_percentage(value, field: str, dataset: str, source_ref: str, original: str) -> None:
    """
    Valida que un porcentaje este en unidades de porcentaje (0-100), no fracciones (0-1).

    Esta validacion es adicional a _validate_numeric y se aplica a los campos
    'ley_pct' y 'recuperacion_pct'. Alerta si el valor esta entre 0 y 1 (probable
    lectura como fraccion en lugar de porcentaje) o si supera 100.
    """
    _validate_numeric(value, field, dataset, source_ref, original)
    if value is None:
        return
    if 0 < value <= 1.0:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=str(value),
            rule=(
                f"campo '{field}' = {value} esta en rango (0,1]; "
                "se esperan unidades de porcentaje (0-100), no fraccion"
            ),
        )
    if value > 100:
        vlog.register_failure(
            stage=_STAGE, dataset=dataset, source_ref=source_ref,
            field=field, value=str(value),
            rule=f"campo '{field}' = {value} supera 100%; imposible para un porcentaje",
        )


# ===========================================================================
# LIMPIEZA POR DATASET
# ===========================================================================

def _clean_costos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y valida bronze_costos -> produce silver_costos.

    Pasos:
      1. Detectar y descartar la fila de cabecera repetida (donde 'mes' == 'mes').
         Se registra en el log; no pasa a silver.
      2. Detectar y descartar la fila TOTAL/vacia (mes vacio o "TOTAL").
         Se registra en el log; no pasa a silver.
      3. Estandarizar 'mes' al formato canonico "YYYY-MM".
      4. Convertir energia_clp, insumos_clp, mano_obra_clp a float.
         Valores 'N/A' y vacios quedan como NaN; se registra una alerta por cada uno.
         El comportamiento esta controlado por business_rules.IMPUTE_MISSING_COST.
      5. Validar todos los campos con rango y tipo.
      6. Detectar filas duplicadas (misma combinacion mes+energia+insumos+mano_obra).
         Las filas duplicadas se mantienen en silver; solo se alerta.

    Las plantas no se identifican ni se estandarizan (fuera de alcance por diseno).
    """
    dataset = "bronze_costos"
    numeric_fields = ["energia_clp", "insumos_clp", "mano_obra_clp"]
    out_rows = []

    for _, row in df.iterrows():
        ref = row["source_row_index"]
        mes_raw = str(row["mes"]).strip()

        # Paso 1: detectar y descartar cabecera repetida
        if mes_raw.lower() == "mes":
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="mes", value=mes_raw,
                rule="fila de cabecera repetida dentro de los datos; se elimina de silver",
            )
            continue

        # Paso 2: detectar y descartar fila TOTAL / vacia
        if mes_raw == "" or mes_raw.upper() == "TOTAL":
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=ref,
                field="mes", value=mes_raw,
                rule="fila de resumen/TOTAL sin periodo valido; se elimina de silver",
            )
            continue

        # Paso 3: estandarizar mes
        mes_clean = _standardize_month_costos(mes_raw)
        _validate_month(mes_clean, dataset, ref, "mes", mes_raw)

        # Paso 4: convertir numericos
        numeric_values = {}
        for field in numeric_fields:
            raw_val = str(row[field]).strip()
            if raw_val.upper() in ("N/A", "NA", ""):
                # Componente faltante: comportamiento controlado por regla de negocio
                if not br.IMPUTE_MISSING_COST:
                    vlog.register_failure(
                        stage=_STAGE, dataset=dataset, source_ref=ref,
                        field=field, value=raw_val,
                        rule=(
                            f"componente de costo '{field}' faltante "
                            f"(valor original: '{raw_val}'); "
                            "IMPUTE_MISSING_COST=False -> se deja como NaN sin imputar"
                        ),
                    )
                numeric_values[field] = None
            else:
                parsed = _parse_clp_amount(raw_val)
                _validate_numeric(parsed, field, dataset, ref, raw_val)
                numeric_values[field] = parsed

        out_rows.append({
            "mes": mes_clean,
            "energia_clp": numeric_values["energia_clp"],
            "insumos_clp": numeric_values["insumos_clp"],
            "mano_obra_clp": numeric_values["mano_obra_clp"],
            "source_file": row["source_file"],
            "source_row_index": ref,
        })

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    result["mes"] = result["mes"].astype("string")
    for f in numeric_fields:
        result[f] = pd.to_numeric(result[f], errors="coerce")
    result["source_file"] = result["source_file"].astype("string")
    result["source_row_index"] = result["source_row_index"].astype("string")

    # Validar no-nulos residuales en mes
    for _, row in result.iterrows():
        if pd.isna(row["mes"]):
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=row["source_row_index"],
                field="mes", value="",
                rule="mes quedo nulo tras la estandarizacion; requiere revision manual",
            )

    # Detectar duplicados (se mantienen en silver; solo se alerta)
    dup_cols = ["mes", "energia_clp", "insumos_clp", "mano_obra_clp"]
    dup_mask = result.duplicated(subset=dup_cols, keep=False)
    if dup_mask.any():
        for _, row in result[dup_mask].iterrows():
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=row["source_row_index"],
                field="(mes,energia_clp,insumos_clp,mano_obra_clp)",
                value=str(row[dup_cols].to_dict()),
                rule="fila duplicada detectada; se mantiene en silver para revision del autor",
            )

    return result


def _clean_lotes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y valida bronze_lotes -> produce silver_lotes.

    Pasos:
      1. Estandarizar 'mes' de numero entero ("1"-"12") a "YYYY-MM" canonico.
      2. Convertir toneladas_tratadas a float (punto-miles / coma-decimal).
      3. Convertir ley_pct y recuperacion_pct a float; validar como porcentaje (0-100).
      4. Validar rangos plausibles de todos los campos numericos.
      5. Detectar filas duplicadas (se mantienen en silver; solo se alerta).

    No hay cabeceras repetidas en bronze_lotes (la fuente es SQLite).
    """
    dataset = "bronze_lotes"
    out_rows = []

    for _, row in df.iterrows():
        ref = row["source_row_index"]

        # Paso 1: estandarizar mes
        mes_raw = str(row["mes"]).strip()
        mes_clean = _standardize_month_from_int(mes_raw)
        _validate_month(mes_clean, dataset, ref, "mes", mes_raw)

        # Paso 2: convertir toneladas
        ton_raw = str(row["toneladas_tratadas"]).strip()
        ton = _parse_numeric_lotes(ton_raw, "toneladas_tratadas")
        _validate_numeric(ton, "toneladas_tratadas", dataset, ref, ton_raw)

        # Paso 3: convertir y validar porcentajes
        ley_raw = str(row["ley_pct"]).strip()
        ley = _parse_numeric_lotes(ley_raw, "ley_pct")
        _validate_percentage(ley, "ley_pct", dataset, ref, ley_raw)

        rec_raw = str(row["recuperacion_pct"]).strip()
        rec = _parse_numeric_lotes(rec_raw, "recuperacion_pct")
        _validate_percentage(rec, "recuperacion_pct", dataset, ref, rec_raw)

        out_rows.append({
            "mes": mes_clean,
            "toneladas_tratadas": ton,
            "ley_pct": ley,
            "recuperacion_pct": rec,
            "source_file": row["source_file"],
            "source_row_index": ref,
        })

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    result["mes"] = result["mes"].astype("string")
    result["toneladas_tratadas"] = pd.to_numeric(result["toneladas_tratadas"], errors="coerce")
    result["ley_pct"] = pd.to_numeric(result["ley_pct"], errors="coerce")
    result["recuperacion_pct"] = pd.to_numeric(result["recuperacion_pct"], errors="coerce")
    result["source_file"] = result["source_file"].astype("string")
    result["source_row_index"] = result["source_row_index"].astype("string")

    # Detectar duplicados
    dup_cols = ["mes", "toneladas_tratadas", "ley_pct", "recuperacion_pct"]
    dup_mask = result.duplicated(subset=dup_cols, keep=False)
    if dup_mask.any():
        for _, row in result[dup_mask].iterrows():
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=row["source_row_index"],
                field="(mes,toneladas_tratadas,ley_pct,recuperacion_pct)",
                value=str(row[dup_cols].to_dict()),
                rule="fila duplicada detectada; se mantiene en silver para revision del autor",
            )

    return result


def _check_last_day_of_month(df: pd.DataFrame, dataset: str, value_field: str) -> None:
    """
    Verifica que la serie de precios tenga cotizacion en el ultimo dia calendario
    de cada mes de ANALYSIS_YEAR.

    Si un mes no tiene cotizacion en su ultimo dia, se registra una alerta.
    No se imputa ningun valor (REGLA 4 en business_rules.py).

    Esta comprobacion es necesaria porque la etapa de metricas usara el precio
    del ultimo dia del mes; la ausencia del dato dejara ese mes sin metricas.
    """
    fechas_dt = pd.to_datetime(df["fecha"].dropna().values, format="%Y-%m-%d", errors="coerce")
    fechas_set = set(pd.Series(fechas_dt).dropna().dt.strftime("%Y-%m-%d"))

    for month in range(1, 13):
        last_day = (
            pd.Timestamp(year=ANALYSIS_YEAR, month=month, day=1) + pd.offsets.MonthEnd(0)
        )
        last_day_str = last_day.strftime("%Y-%m-%d")
        mes_str = f"{ANALYSIS_YEAR}-{month:02d}"
        if last_day_str not in fechas_set:
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref="serie_completa",
                field="fecha", value=last_day_str,
                rule=(
                    f"no existe cotizacion de '{value_field}' para el ultimo dia "
                    f"del mes {mes_str} ({last_day_str}); "
                    "la etapa de metricas no podra calcular ese mes (REGLA 4)"
                ),
            )


def _clean_price_series(df: pd.DataFrame, dataset: str, value_field: str) -> pd.DataFrame:
    """
    Limpia y valida una serie de precios (bronze_precio_cobre o bronze_tipo_cambio).

    Parametros
    ----------
    df          : DataFrame leido de bronze (todas las columnas como texto).
    dataset     : nombre del dataset para el log.
    value_field : nombre semantico del campo de valor para buscar en VALID_RANGES
                  ("precio_cobre_usd_lb" o "tipo_cambio_usd_clp").

    Pasos:
      1. Convertir 'fecha' de ISO 8601 UTC a "YYYY-MM-DD".
      2. Convertir 'valor' a float.
      3. Validar fecha (formato, anio de analisis).
      4. Validar valor (rango plausible definido en business_rules.VALID_RANGES).
      5. Detectar duplicados por fecha.
      6. Verificar cotizacion en el ultimo dia de cada mes del anio de analisis.
    """
    out_rows = []

    for _, row in df.iterrows():
        ref = row["source_row_index"]

        fecha_raw = str(row["fecha"]).strip()
        fecha_clean = _standardize_date_iso(fecha_raw)
        _validate_date(fecha_clean, dataset, ref, "fecha", fecha_raw)

        valor_raw = str(row["valor"]).strip()
        try:
            valor = float(valor_raw)
        except ValueError:
            valor = None
        _validate_numeric(valor, value_field, dataset, ref, valor_raw)

        out_rows.append({
            "fecha": fecha_clean,
            "valor": valor,
            "source_file": row["source_file"],
            "source_row_index": ref,
        })

    result = pd.DataFrame(out_rows)
    if result.empty:
        return result

    result["fecha"] = result["fecha"].astype("string")
    result["valor"] = pd.to_numeric(result["valor"], errors="coerce")
    result["source_file"] = result["source_file"].astype("string")
    result["source_row_index"] = result["source_row_index"].astype("string")

    # Detectar duplicados por fecha
    dup_mask = result.duplicated(subset=["fecha"], keep=False)
    if dup_mask.any():
        for _, row in result[dup_mask].iterrows():
            vlog.register_failure(
                stage=_STAGE, dataset=dataset, source_ref=row["source_row_index"],
                field="fecha", value=str(row["fecha"]),
                rule="fecha duplicada en la serie; se mantiene en silver para revision",
            )

    # Verificar cotizacion en el ultimo dia de cada mes
    _check_last_day_of_month(result, dataset, value_field)

    return result


# ===========================================================================
# PUNTO DE ENTRADA PUBLICO
# ===========================================================================

def run_cleaning() -> None:
    """
    Ejecuta la etapa completa de limpieza y validacion.

    Orquesta la lectura desde bronze/, la limpieza de cada dataset y la escritura
    a silver/. Imprime resumenes de filas por dataset para auditoria rapida.

    Es el unico punto de entrada publico del modulo; main.py lo llama directamente.
    """
    print("[cleaning] Etapa de limpieza y validacion iniciada.")
    SILVER_DIR.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. COSTOS
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_costos...")
    df_costos_bronze = pd.read_parquet(BRONZE_DIR / "bronze_costos.parquet")
    df_costos_silver = _clean_costos(df_costos_bronze)
    df_costos_silver.to_parquet(SILVER_DIR / "silver_costos.parquet", index=False)
    print(
        f"[cleaning] silver_costos: {len(df_costos_bronze)} filas bronze -> "
        f"{len(df_costos_silver)} filas silver "
        f"({len(df_costos_bronze) - len(df_costos_silver)} descartadas)"
    )

    # ------------------------------------------------------------------
    # 2. LOTES
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_lotes...")
    df_lotes_bronze = pd.read_parquet(BRONZE_DIR / "bronze_lotes.parquet")
    df_lotes_silver = _clean_lotes(df_lotes_bronze)
    df_lotes_silver.to_parquet(SILVER_DIR / "silver_lotes.parquet", index=False)
    print(
        f"[cleaning] silver_lotes: {len(df_lotes_bronze)} filas bronze -> "
        f"{len(df_lotes_silver)} filas silver "
        f"({len(df_lotes_bronze) - len(df_lotes_silver)} descartadas)"
    )

    # ------------------------------------------------------------------
    # 3. PRECIO COBRE
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_precio_cobre...")
    df_pc_bronze = pd.read_parquet(BRONZE_DIR / "bronze_precio_cobre.parquet")
    df_pc_silver = _clean_price_series(df_pc_bronze, "bronze_precio_cobre", "precio_cobre_usd_lb")
    df_pc_silver.to_parquet(SILVER_DIR / "silver_precio_cobre.parquet", index=False)
    print(
        f"[cleaning] silver_precio_cobre: {len(df_pc_bronze)} filas bronze -> "
        f"{len(df_pc_silver)} filas silver"
    )

    # ------------------------------------------------------------------
    # 4. TIPO DE CAMBIO
    # ------------------------------------------------------------------
    print("[cleaning] Procesando bronze_tipo_cambio...")
    df_tc_bronze = pd.read_parquet(BRONZE_DIR / "bronze_tipo_cambio.parquet")
    df_tc_silver = _clean_price_series(df_tc_bronze, "bronze_tipo_cambio", "tipo_cambio_usd_clp")
    df_tc_silver.to_parquet(SILVER_DIR / "silver_tipo_cambio.parquet", index=False)
    print(
        f"[cleaning] silver_tipo_cambio: {len(df_tc_bronze)} filas bronze -> "
        f"{len(df_tc_silver)} filas silver"
    )

    print("[cleaning] Etapa de limpieza y validacion completada.")
