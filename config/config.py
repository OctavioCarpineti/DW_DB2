"""
Archivo de configuración para conexión a MySQL
"""

# Configuración de MySQL
MYSQL_CONFIG = {
    'host': 'localhost',        # Cambiar si tu MySQL está en otro servidor
    'user': 'root',             # Tu usuario de MySQL
    'password': 'Dacota12',  # ⚠️ CAMBIA ESTO por tu password de MySQL
    'database': 'dw_salud_mental',
    'raise_on_warnings': True
}

# Rutas de archivos
CSV_RAW_PATH = 'data/raw/mental_health.csv'
CSV_CLEAN_PATH = 'data/processed/mental_health_clean.csv'

# Configuración de logging
LOG_FILE = 'logs/etl_log.txt'