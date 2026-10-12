"""
config.py
=========
Módulo de configuración central del pipeline.

Define rutas a las cuatro capas de datos, la ruta del log de validaciones,
y las constantes analíticas compartidas por todos los demás módulos.

Al centralizar estas constantes aquí, cualquier cambio de ruta o valor
numérico se hace en un solo lugar y se propaga a todo el proyecto.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Raíz del proyecto
# Se calcula de forma relativa al archivo actual para que el proyecto sea
# portable: funciona en cualquier máquina sin importar dónde esté el repo.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Rutas de las cuatro capas de datos (medallion architecture)
# ---------------------------------------------------------------------------

# raw: archivos originales tal como fueron entregados + respuesta de la API.
# NUNCA se modifica su contenido desde el código.
RAW_DIR = PROJECT_ROOT / "raw"

# bronze: datos extraídos de raw, sin limpiar, en formato Parquet.
# Todos los valores se almacenan como texto (dtype object) para no perder
# información antes de la limpieza.
BRONZE_DIR = PROJECT_ROOT / "bronze"

# silver: datos limpios, con tipos correctos (fecha, numérico, texto).
# Es la única entrada de la etapa de métricas.
SILVER_DIR = PROJECT_ROOT / "silver"

# gold: métricas mensuales a nivel empresa, listas para dashboards e informe.
GOLD_DIR = PROJECT_ROOT / "gold"

# ---------------------------------------------------------------------------
# Ruta del log de validaciones
# Se escribe al final de cada ejecución como un archivo tabular Parquet
# legible con pandas.
# ---------------------------------------------------------------------------
VALIDATION_LOG_PATH = PROJECT_ROOT / "validation_log.parquet"

# ---------------------------------------------------------------------------
# Constantes analíticas
# ---------------------------------------------------------------------------

# Año de análisis. Solo se procesan datos de este año.
ANALYSIS_YEAR = 2025

# Factor de conversión de toneladas métricas a libras.
# Fuente: definición exacta del sistema métrico / sistema imperial.
MT_TO_LB = 2204.62262

# ---------------------------------------------------------------------------
# Direcciones de la API de precios (mindicador.cl)
# Llamadas solo durante la etapa de extracción; las etapas posteriores
# leen exclusivamente desde raw/.
# ---------------------------------------------------------------------------

# Precio del cobre en libras (USD/lb) para el año de análisis.
API_COPPER_PRICE_URL = "https://mindicador.cl/api/libra_cobre/2025"

# Tipo de cambio USD-CLP para el año de análisis.
API_USD_CLP_URL = "https://mindicador.cl/api/dolar/2025"
