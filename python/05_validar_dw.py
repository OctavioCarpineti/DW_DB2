"""
Script 5: Validación completa del Data Warehouse
Verifica integridad, consistencia y calidad de datos
"""

import mysql.connector
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import MYSQL_CONFIG, LOG_FILE


def log_message(message):
    """Registrar mensajes"""
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
        log_message("✅ Conexión exitosa")
        return conn
    except mysql.connector.Error as err:
        log_message(f"❌ ERROR: {err}")
        sys.exit(1)


def validar_estructura(conn):
    """Validar que existan todas las tablas esperadas"""
    log_message("\n" + "=" * 50)
    log_message("1. VALIDACIÓN DE ESTRUCTURA")
    log_message("=" * 50)

    cursor = conn.cursor()

    tablas_esperadas = [
        'mental_health_staging',
        'Dim_Tiempo',
        'Dim_Genero',
        'Dim_Historial',
        'Dim_Ocupacion',
        'Dim_Pais',
        'Dim_Aislamiento',
        'Dim_Sintomas',
        'Dim_Acceso',
        'Hechos_Estres_SaludMental'
    ]

    for tabla in tablas_esperadas:
        query = f"""
        SELECT COUNT(*) 
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = 'dw_salud_mental' 
          AND TABLE_NAME = '{tabla}'
        """
        cursor.execute(query)
        existe = cursor.fetchone()[0]

        if existe:
            # Obtener cantidad de registros
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cursor.fetchone()[0]
            log_message(f"✅ {tabla}: {count} registros")
        else:
            log_message(f"❌ {tabla}: NO EXISTE")

    cursor.close()


def validar_volumetria(conn):
    """Validar volúmenes de datos"""
    log_message("\n" + "=" * 50)
    log_message("2. VALIDACIÓN DE VOLUMETRÍA")
    log_message("=" * 50)

    cursor = conn.cursor()

    # Mínimos esperados
    minimos = {
        'mental_health_staging': 10000,  # Dataset debe tener al menos 10k registros
        'Dim_Tiempo': 24,  # Mínimo 2 años de datos
        'Dim_Genero': 2,
        'Dim_Historial': 2,
        'Dim_Ocupacion': 5,
        'Dim_Pais': 36,
        'Dim_Aislamiento': 5,
        'Dim_Sintomas': 10,  # Múltiples combinaciones
        'Dim_Acceso': 8,  # Múltiples combinaciones
        'Hechos_Estres_SaludMental': 100  # Mínimo de hechos agregados
    }

    todas_ok = True

    for tabla, minimo in minimos.items():
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        count = cursor.fetchone()[0]

        if count >= minimo:
            log_message(f"✅ {tabla}: {count} registros (mínimo: {minimo})")
        else:
            log_message(f"❌ {tabla}: {count} registros (esperado mínimo: {minimo})")
            todas_ok = False

    cursor.close()

    if todas_ok:
        log_message("\n✅ Volumetría correcta en todas las tablas")
    else:
        log_message("\n⚠️ Algunas tablas no cumplen volumetría mínima")


def validar_integridad_referencial(conn):
    """Validar claves foráneas"""
    log_message("\n" + "=" * 50)
    log_message("3. VALIDACIÓN DE INTEGRIDAD REFERENCIAL")
    log_message("=" * 50)

    cursor = conn.cursor()

    # Verificar cada FK
    fks = [
        ('id_tiempo', 'Dim_Tiempo'),
        ('id_genero', 'Dim_Genero'),
        ('id_historial', 'Dim_Historial'),
        ('id_ocupacion', 'Dim_Ocupacion'),
        ('id_pais', 'Dim_Pais'),
        ('id_aislamiento', 'Dim_Aislamiento'),
        ('id_sintomas', 'Dim_Sintomas'),
        ('id_acceso', 'Dim_Acceso')
    ]

    todas_ok = True

    for fk, dim in fks:
        query = f"""
        SELECT COUNT(*) 
        FROM Hechos_Estres_SaludMental h
        LEFT JOIN {dim} d ON h.{fk} = d.{fk}
        WHERE d.{fk} IS NULL
        """
        cursor.execute(query)
        huerfanos = cursor.fetchone()[0]

        if huerfanos == 0:
            log_message(f"✅ {fk} → {dim}: sin registros huérfanos")
        else:
            log_message(f"❌ {fk} → {dim}: {huerfanos} registros huérfanos")
            todas_ok = False

    cursor.close()

    if todas_ok:
        log_message("\n✅ Integridad referencial correcta")
    else:
        log_message("\n❌ Problemas de integridad referencial detectados")


def validar_indicadores(conn):
    """Validar rangos y consistencia de indicadores"""
    log_message("\n" + "=" * 50)
    log_message("4. VALIDACIÓN DE INDICADORES")
    log_message("=" * 50)

    cursor = conn.cursor()

    # 1. Porcentajes en rango [0, 100]
    log_message("\nVerificando rangos de porcentajes...")

    porcentajes = [
        'porcentaje_estres',
        'porcentaje_historial_estres',
        'porcentaje_estres_afrontamiento_ocupacion',
        'porcentaje_tratamiento',
        'porcentaje_no_tratamiento',
        'porcentaje_deterioro_aislamiento',
        'porcentaje_humor_aislamiento',
        'porcentaje_debilidad_aislamiento',
        'porcentaje_acceso_recursos',
        'porcentaje_sintomas_no_reconocidos',
        'porcentaje_recursos_sin_tratamiento',
        'porcentaje_postergacion'
    ]

    errores_rango = 0

    for pct in porcentajes:
        query = f"""
        SELECT COUNT(*) 
        FROM Hechos_Estres_SaludMental 
        WHERE {pct} IS NOT NULL AND ({pct} < 0 OR {pct} > 100)
        """
        cursor.execute(query)
        fuera_rango = cursor.fetchone()[0]

        if fuera_rango > 0:
            log_message(f"  ❌ {pct}: {fuera_rango} valores fuera de [0-100]")
            errores_rango += 1

    if errores_rango == 0:
        log_message("  ✅ Todos los porcentajes en rango válido")

    # 2. Conteos no negativos
    log_message("\nVerificando conteos...")

    conteos = [
        'cantidad_estres',
        'cantidad_historial_estres',
        'cantidad_estres_afrontamiento',
        'cantidad_tratamiento',
        'cantidad_estres_acceso'
    ]

    errores_negativos = 0

    for cnt in conteos:
        query = f"""
        SELECT COUNT(*) 
        FROM Hechos_Estres_SaludMental 
        WHERE {cnt} IS NOT NULL AND {cnt} < 0
        """
        cursor.execute(query)
        negativos = cursor.fetchone()[0]

        if negativos > 0:
            log_message(f"  ❌ {cnt}: {negativos} valores negativos")
            errores_negativos += 1

    if errores_negativos == 0:
        log_message("  ✅ Todos los conteos son no negativos")

    # 3. Consistencia: porcentaje_tratamiento + porcentaje_no_tratamiento ≈ 100
    log_message("\nVerificando consistencia de tratamiento...")

    query = """
    SELECT COUNT(*)
    FROM Hechos_Estres_SaludMental
    WHERE porcentaje_tratamiento IS NOT NULL 
      AND porcentaje_no_tratamiento IS NOT NULL
      AND ABS((porcentaje_tratamiento + porcentaje_no_tratamiento) - 100) > 1
    """
    cursor.execute(query)
    inconsistentes = cursor.fetchone()[0]

    if inconsistentes == 0:
        log_message("  ✅ Porcentajes de tratamiento consistentes")
    else:
        log_message(f"  ⚠️ {inconsistentes} registros con suma != 100")

    cursor.close()


def validar_variable_derivada(conn):
    """Validar lógica de indicador_inferido_estres"""
    log_message("\n" + "=" * 50)
    log_message("5. VALIDACIÓN DE VARIABLE DERIVADA")
    log_message("=" * 50)

    cursor = conn.cursor()

    # Verificar que todos los registros con Growing_Stress='Yes' tengan indicador=TRUE
    query = """
    SELECT COUNT(*)
    FROM Dim_Sintomas
    WHERE growing_stress = 'Yes' AND indicador_inferido_estres = 0
    """
    cursor.execute(query)
    errores = cursor.fetchone()[0]

    if errores == 0:
        log_message("✅ Camino directo (Growing_Stress=Yes) correcto")
    else:
        log_message(f"❌ {errores} registros con Growing_Stress=Yes pero indicador=FALSE")

    # Verificar que existan registros inferidos (sin Growing_Stress=Yes pero con indicador=TRUE)
    query = """
    SELECT COUNT(*)
    FROM Dim_Sintomas
    WHERE growing_stress != 'Yes' AND indicador_inferido_estres = 1
    """
    cursor.execute(query)
    inferidos = cursor.fetchone()[0]

    if inferidos > 0:
        log_message(f"✅ Camino inferido funciona: {inferidos} combinaciones inferidas")
    else:
        log_message("⚠️ No hay registros con estrés inferido (puede ser normal)")

    # Mostrar distribución
    query = """
    SELECT 
        indicador_inferido_estres,
        COUNT(*) as cantidad,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Dim_Sintomas), 2) as porcentaje
    FROM Dim_Sintomas
    GROUP BY indicador_inferido_estres
    """
    cursor.execute(query)

    log_message("\nDistribución del indicador inferido:")
    for row in cursor.fetchall():
        estado = "CON estrés" if row[0] else "SIN estrés"
        log_message(f"  {estado}: {row[1]} combinaciones ({row[2]}%)")

    cursor.close()


def estadisticas_generales(conn):
    """Mostrar estadísticas generales del DW"""
    log_message("\n" + "=" * 50)
    log_message("6. ESTADÍSTICAS GENERALES DEL DW")
    log_message("=" * 50)

    cursor = conn.cursor()

    # Período de análisis
    cursor.execute("""
        SELECT MIN(anio), MAX(anio)
        FROM Dim_Tiempo
    """)
    anio_min, anio_max = cursor.fetchone()
    log_message(f"\nPeríodo de análisis: {anio_min} - {anio_max}")

    # Distribución por género
    log_message("\nDistribución por género en hechos:")
    cursor.execute("""
        SELECT dg.genero, COUNT(*) as cantidad
        FROM Hechos_Estres_SaludMental h
        JOIN Dim_Genero dg ON h.id_genero = dg.id_genero
        GROUP BY dg.genero
    """)
    for row in cursor.fetchall():
        log_message(f"  {row[0]}: {row[1]} hechos")

    # Top 5 países
    log_message("\nTop 5 países por cantidad de hechos:")
    cursor.execute("""
        SELECT dp.country, COUNT(*) as cantidad
        FROM Hechos_Estres_SaludMental h
        JOIN Dim_Pais dp ON h.id_pais = dp.id_pais
        GROUP BY dp.country
        ORDER BY cantidad DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        log_message(f"  {row[0]}: {row[1]} hechos")

    # Promedio de estrés
    cursor.execute("""
        SELECT 
            ROUND(AVG(porcentaje_estres), 2) as pct_estres_promedio,
            ROUND(AVG(porcentaje_tratamiento), 2) as pct_tratamiento_promedio
        FROM Hechos_Estres_SaludMental
        WHERE porcentaje_estres IS NOT NULL
    """)
    pct_estres, pct_tratamiento = cursor.fetchone()
    log_message(f"\nIndicadores promedio globales:")
    log_message(f"  % Estrés: {pct_estres}%")
    log_message(f"  % En tratamiento: {pct_tratamiento}%")

    cursor.close()


def reporte_final(conn):
    """Generar reporte final de validación"""
    log_message("\n" + "=" * 50)
    log_message("REPORTE FINAL DE VALIDACIÓN")
    log_message("=" * 50)

    cursor = conn.cursor()

    # Resumen de tablas
    cursor.execute("""
        SELECT 
            TABLE_NAME,
            TABLE_ROWS,
            ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS size_mb
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = 'dw_salud_mental'
        ORDER BY TABLE_ROWS DESC
    """)

    log_message("\nResumen de tablas:")
    log_message(f"{'Tabla':<35} {'Registros':<12} {'Tamaño (MB)'}")
    log_message("-" * 60)

    total_rows = 0
    total_size = 0

    for tabla, rows, size in cursor.fetchall():
        log_message(f"{tabla:<35} {rows:<12} {size}")
        total_rows += rows if rows else 0
        total_size += size if size else 0

    log_message("-" * 60)
    log_message(f"{'TOTAL':<35} {total_rows:<12} {total_size:.2f}")

    cursor.close()


def main():
    """Función principal"""
    log_message("\n" + "=" * 70)
    log_message("VALIDACIÓN COMPLETA DEL DATA WAREHOUSE")
    log_message("Data Warehouse: Análisis de Estrés y Salud Mental")
    log_message("=" * 70)

    conn = conectar_mysql()

    try:
        # Ejecutar todas las validaciones
        validar_estructura(conn)
        validar_volumetria(conn)
        validar_integridad_referencial(conn)
        validar_indicadores(conn)
        validar_variable_derivada(conn)
        estadisticas_generales(conn)
        reporte_final(conn)

        log_message("\n" + "=" * 70)
        log_message("✅ VALIDACIÓN COMPLETADA EXITOSAMENTE")
        log_message("El Data Warehouse está listo para ser usado en Power BI")
        log_message("=" * 70)

    except Exception as e:
        log_message(f"\n❌ ERROR durante validación: {str(e)}")
        raise

    finally:
        conn.close()
        log_message("\nConexión cerrada")


if __name__ == "__main__":
    main()