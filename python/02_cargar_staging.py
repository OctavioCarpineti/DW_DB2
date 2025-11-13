"""
Script 2: Cargar datos del CSV limpio a la tabla staging de MySQL
Entrada: data/processed/mental_health_clean.csv
Salida: Tabla mental_health_staging poblada en MySQL
"""

import pandas as pd
import mysql.connector
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import MYSQL_CONFIG, CSV_CLEAN_PATH, LOG_FILE


def log_message(message):
    """Registrar mensajes en log y consola"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')


def conectar_mysql():
    """Conectar a MySQL"""
    try:
        log_message("Conectando a MySQL...")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        log_message("✅ Conexión exitosa a MySQL")
        return conn
    except mysql.connector.Error as err:
        log_message(f"❌ ERROR de conexión: {err}")
        log_message("Verifica que:")
        log_message("  1. MySQL esté ejecutándose")
        log_message("  2. Las credenciales en config.py sean correctas")
        log_message("  3. La base de datos 'dw_salud_mental' exista")
        sys.exit(1)


def verificar_tabla_staging(conn):
    """Verificar que la tabla staging existe"""
    log_message("\nVerificando tabla mental_health_staging...")

    cursor = conn.cursor()

    query = """
    SELECT COUNT(*) 
    FROM information_schema.TABLES 
    WHERE TABLE_SCHEMA = 'dw_salud_mental' 
      AND TABLE_NAME = 'mental_health_staging'
    """

    cursor.execute(query)
    exists = cursor.fetchone()[0]

    if exists == 0:
        log_message("❌ ERROR: La tabla mental_health_staging no existe")
        log_message("Ejecuta primero el script 02_crear_tablas.sql")
        cursor.close()
        sys.exit(1)

    log_message("✅ Tabla mental_health_staging existe")
    cursor.close()


def limpiar_tabla_staging(conn):
    """Limpiar tabla staging antes de cargar"""
    log_message("\nLimpiando tabla staging...")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE mental_health_staging")
    conn.commit()

    log_message("✅ Tabla staging limpiada")
    cursor.close()


def cargar_csv():
    """Cargar CSV limpio"""
    log_message(f"\nCargando CSV desde: {CSV_CLEAN_PATH}")

    try:
        df = pd.read_csv(CSV_CLEAN_PATH)
        log_message(f"✅ CSV cargado: {len(df)} registros, {len(df.columns)} columnas")
        return df
    except FileNotFoundError:
        log_message(f"❌ ERROR: No se encontró {CSV_CLEAN_PATH}")
        log_message("Ejecuta primero el script 01_limpiar_datos.py")
        sys.exit(1)
    except Exception as e:
        log_message(f"❌ ERROR al cargar CSV: {str(e)}")
        sys.exit(1)


def insertar_datos_batch(conn, df, batch_size=1000):
    """
    Insertar datos en lotes para optimizar rendimiento
    """
    log_message(f"\nInsertando datos en lotes de {batch_size} registros...")

    cursor = conn.cursor()

    # Query de inserción
    query = """
    INSERT INTO mental_health_staging (
        Timestamp, Gender, Country, Occupation, self_employed,
        family_history, treatment, Days_Indoors, Growing_Stress,
        Changes_Habits, Mental_Health_History, Mood_Swings,
        Coping_Struggles, Work_Interest, Social_Weakness,
        mental_health_interview, care_options
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """

    total_registros = len(df)
    registros_insertados = 0
    errores = 0

    # Procesar en lotes
    for start_idx in range(0, total_registros, batch_size):
        end_idx = min(start_idx + batch_size, total_registros)
        batch = df.iloc[start_idx:end_idx]

        # Preparar datos del lote
        batch_data = []
        for _, row in batch.iterrows():
            # Convertir NaN a None para MySQL
            row_data = tuple(None if pd.isna(val) else val for val in row)
            batch_data.append(row_data)

        try:
            # Insertar lote
            cursor.executemany(query, batch_data)
            conn.commit()

            registros_insertados += len(batch_data)
            porcentaje = (registros_insertados / total_registros) * 100

            log_message(f"  Procesado: {registros_insertados}/{total_registros} ({porcentaje:.1f}%)")

        except mysql.connector.Error as err:
            errores += 1
            log_message(f"⚠️ Error en lote {start_idx}-{end_idx}: {err}")
            conn.rollback()

    cursor.close()

    log_message(f"\n✅ Inserción completada:")
    log_message(f"  Registros insertados: {registros_insertados}")
    log_message(f"  Errores: {errores}")

    return registros_insertados, errores


def validar_carga(conn, registros_esperados):
    """Validar que la carga fue exitosa"""
    log_message("\n--- VALIDANDO CARGA ---")

    cursor = conn.cursor()

    # 1. Contar registros
    cursor.execute("SELECT COUNT(*) FROM mental_health_staging")
    count = cursor.fetchone()[0]

    log_message(f"Registros en staging: {count}")
    log_message(f"Registros esperados: {registros_esperados}")

    if count == registros_esperados:
        log_message("✅ Cantidad de registros correcta")
    else:
        log_message(f"⚠️ ADVERTENCIA: Diferencia de {abs(count - registros_esperados)} registros")

    # 2. Verificar valores NULL en campos críticos
    log_message("\nVerificando campos críticos (no deben tener NULL)...")

    campos_criticos = [
        'Timestamp', 'Gender', 'Country', 'Occupation',
        'family_history', 'treatment', 'Days_Indoors',
        'Growing_Stress', 'Mood_Swings', 'Coping_Struggles',
        'Social_Weakness', 'care_options', 'mental_health_interview'
    ]

    for campo in campos_criticos:
        query = f"SELECT COUNT(*) FROM mental_health_staging WHERE {campo} IS NULL"
        cursor.execute(query)
        nulls = cursor.fetchone()[0]

        if nulls > 0:
            log_message(f"  ⚠️ {campo}: {nulls} valores NULL")
        else:
            log_message(f"  ✅ {campo}: sin valores NULL")

    # 3. Distribución por género
    log_message("\nDistribución por género:")
    cursor.execute("""
        SELECT Gender, COUNT(*) as cantidad
        FROM mental_health_staging
        GROUP BY Gender
    """)
    for row in cursor.fetchall():
        log_message(f"  {row[0]}: {row[1]} registros")

    # 4. Rango de fechas
    log_message("\nRango de fechas:")
    cursor.execute("""
        SELECT 
            MIN(STR_TO_DATE(Timestamp, '%m/%d/%Y %H:%i')) as fecha_min,
            MAX(STR_TO_DATE(Timestamp, '%m/%d/%Y %H:%i')) as fecha_max
        FROM mental_health_staging
    """)
    fecha_min, fecha_max = cursor.fetchone()
    log_message(f"  Desde: {fecha_min}")
    log_message(f"  Hasta: {fecha_max}")

    # 5. Top 5 países por cantidad de registros
    log_message("\nTop 5 países:")
    cursor.execute("""
        SELECT Country, COUNT(*) as cantidad
        FROM mental_health_staging
        GROUP BY Country
        ORDER BY cantidad DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        log_message(f"  {row[0]}: {row[1]} registros")

    cursor.close()


def main():
    """Función principal"""
    log_message("=" * 50)
    log_message("INICIANDO CARGA DE STAGING")
    log_message("=" * 50)

    # 1. Conectar a MySQL
    conn = conectar_mysql()

    try:
        # 2. Verificar que la tabla existe
        verificar_tabla_staging(conn)

        # 3. Limpiar tabla
        limpiar_tabla_staging(conn)

        # 4. Cargar CSV
        df = cargar_csv()

        # 5. Insertar datos
        registros_insertados, errores = insertar_datos_batch(conn, df, batch_size=1000)

        # 6. Validar carga
        validar_carga(conn, len(df))

        log_message("\n" + "=" * 50)
        log_message("CARGA DE STAGING COMPLETADA EXITOSAMENTE")
        log_message("=" * 50)

    except Exception as e:
        log_message(f"\n❌ ERROR: {str(e)}")
        conn.rollback()
        raise

    finally:
        conn.close()
        log_message("Conexión cerrada")


if __name__ == "__main__":
    main()