"""
Script 1: Limpieza y validación del dataset
Entrada: data/raw/mental_health.csv
Salida: data/processed/mental_health_clean.csv
"""

import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# Agregar el directorio raíz al path para importar config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import CSV_RAW_PATH, CSV_CLEAN_PATH, LOG_FILE


def log_message(message):
    """Registrar mensajes en el log y en consola"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)

    # Crear directorio logs si no existe
    os.makedirs('logs', exist_ok=True)

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')


def cargar_csv():
    """Cargar el CSV original"""
    log_message("=" * 50)
    log_message("INICIANDO LIMPIEZA DE DATOS")
    log_message("=" * 50)

    try:
        log_message(f"Cargando CSV desde: {CSV_RAW_PATH}")
        df = pd.read_csv(CSV_RAW_PATH)
        log_message(f"✅ CSV cargado exitosamente: {len(df)} registros, {len(df.columns)} columnas")
        return df
    except FileNotFoundError:
        log_message(f"❌ ERROR: No se encontró el archivo {CSV_RAW_PATH}")
        log_message("Verifica que el CSV esté en la carpeta data/raw/")
        sys.exit(1)
    except Exception as e:
        log_message(f"❌ ERROR al cargar CSV: {str(e)}")
        sys.exit(1)


def analizar_calidad_datos(df):
    """Analizar calidad de los datos antes de limpiar"""
    log_message("\n--- ANÁLISIS DE CALIDAD DE DATOS ---")

    # Valores nulos por columna
    log_message("\nValores nulos por columna:")
    nulls = df.isnull().sum()
    for col, count in nulls.items():
        if count > 0:
            porcentaje = (count / len(df)) * 100
            log_message(f"  {col}: {count} ({porcentaje:.2f}%)")

    # Duplicados
    duplicados = df.duplicated().sum()
    log_message(f"\nRegistros duplicados: {duplicados}")

    # Tipos de datos
    log_message("\nTipos de datos:")
    for col, dtype in df.dtypes.items():
        log_message(f"  {col}: {dtype}")


def limpiar_datos(df):
    """Limpiar y transformar los datos"""
    log_message("\n--- INICIANDO LIMPIEZA ---")

    df_clean = df.copy()
    registros_iniciales = len(df_clean)

    # 1. Eliminar duplicados exactos
    duplicados_antes = df_clean.duplicated().sum()
    if duplicados_antes > 0:
        df_clean = df_clean.drop_duplicates()
        log_message(f"✅ Eliminados {duplicados_antes} registros duplicados")

    # 2. Limpiar espacios en blanco de todas las columnas de texto
    log_message("Limpiando espacios en blanco...")
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].str.strip()

    # 3. Normalizar valores categóricos
    log_message("Normalizando valores categóricos...")

    # Gender: capitalizar
    if 'Gender' in df_clean.columns:
        df_clean['Gender'] = df_clean['Gender'].str.title()

    # 4. Eliminar registros con valores NULL en campos críticos
    campos_criticos = [
        'Timestamp', 'Gender', 'Country', 'Occupation',
        'family_history', 'treatment', 'Days_Indoors',
        'Growing_Stress', 'Mood_Swings', 'Coping_Struggles',
        'Social_Weakness', 'care_options', 'mental_health_interview'
    ]

    log_message("Eliminando registros con valores NULL en campos críticos...")
    for campo in campos_criticos:
        if campo in df_clean.columns:
            antes = len(df_clean)
            df_clean = df_clean[df_clean[campo].notna()]
            eliminados = antes - len(df_clean)
            if eliminados > 0:
                log_message(f"  {campo}: eliminados {eliminados} registros con NULL")

    # 5. Validar valores de campos categóricos
    log_message("Validando valores categóricos...")

    # Gender: solo Male, Female
    if 'Gender' in df_clean.columns:
        valores_validos_gender = ['Male', 'Female']
        antes = len(df_clean)
        df_clean = df_clean[df_clean['Gender'].isin(valores_validos_gender)]
        eliminados = antes - len(df_clean)
        if eliminados > 0:
            log_message(f"  Gender: eliminados {eliminados} registros con valores inválidos")

    # Days_Indoors: validar valores esperados
    if 'Days_Indoors' in df_clean.columns:
        valores_validos_days = [
            'Go out Every day', '1-14 days', '15-30 days',
            '31-60 days', 'More than 2 months'
        ]
        antes = len(df_clean)
        df_clean = df_clean[df_clean['Days_Indoors'].isin(valores_validos_days)]
        eliminados = antes - len(df_clean)
        if eliminados > 0:
            log_message(f"  Days_Indoors: eliminados {eliminados} registros con valores inválidos")

    # 6. Resumen de limpieza
    registros_finales = len(df_clean)
    registros_eliminados = registros_iniciales - registros_finales
    porcentaje_eliminado = (registros_eliminados / registros_iniciales) * 100

    log_message(f"\n✅ LIMPIEZA COMPLETADA:")
    log_message(f"  Registros iniciales: {registros_iniciales}")
    log_message(f"  Registros eliminados: {registros_eliminados} ({porcentaje_eliminado:.2f}%)")
    log_message(f"  Registros finales: {registros_finales}")

    return df_clean


def guardar_csv_limpio(df):
    """Guardar el CSV limpio"""
    try:
        # Crear directorio si no existe
        os.makedirs('data/processed', exist_ok=True)

        df.to_csv(CSV_CLEAN_PATH, index=False, encoding='utf-8')
        log_message(f"\n✅ CSV limpio guardado en: {CSV_CLEAN_PATH}")
        log_message(f"  {len(df)} registros, {len(df.columns)} columnas")
    except Exception as e:
        log_message(f"❌ ERROR al guardar CSV limpio: {str(e)}")
        sys.exit(1)


def main():
    """Función principal"""
    # 1. Cargar CSV
    df = cargar_csv()

    # 2. Analizar calidad
    analizar_calidad_datos(df)

    # 3. Limpiar datos
    df_clean = limpiar_datos(df)

    # 4. Guardar CSV limpio
    guardar_csv_limpio(df_clean)

    log_message("\n" + "=" * 50)
    log_message("LIMPIEZA COMPLETADA EXITOSAMENTE")
    log_message("=" * 50)


if __name__ == "__main__":
    main()
