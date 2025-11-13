"""
Script 4: Cargar tabla de hechos con los 16 indicadores
Proceso: Agregación desde staging + JOINs con dimensiones
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


def verificar_dimensiones(conn):
    """Verificar que todas las dimensiones estén cargadas"""
    log_message("\n--- VERIFICANDO DIMENSIONES ---")

    cursor = conn.cursor()

    dimensiones = {
        'Dim_Tiempo': 12,  # Mínimo 1 año
        'Dim_Genero': 2,
        'Dim_Historial': 2,
        'Dim_Ocupacion': 5,
        'Dim_Pais': 35,
        'Dim_Aislamiento': 5,
        'Dim_Sintomas': 1,  # Mínimo 1 combinación
        'Dim_Acceso': 1  # Mínimo 1 combinación
    }

    todas_ok = True

    for tabla, minimo in dimensiones.items():
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        count = cursor.fetchone()[0]

        if count >= minimo:
            log_message(f"✅ {tabla}: {count} registros")
        else:
            log_message(f"❌ {tabla}: {count} registros (mínimo esperado: {minimo})")
            todas_ok = False

    cursor.close()

    if not todas_ok:
        log_message("\n❌ ERROR: Faltan dimensiones. Ejecuta 03_cargar_dimensiones.py primero")
        sys.exit(1)

    log_message("✅ Todas las dimensiones verificadas")


def limpiar_tabla_hechos(conn):
    """Limpiar tabla de hechos"""
    log_message("\nLimpiando tabla de hechos...")

    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("TRUNCATE TABLE Hechos_Estres_SaludMental")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()

    log_message("✅ Tabla de hechos limpiada")
    cursor.close()


def cargar_hechos(conn):
    """
    Cargar tabla de hechos con todos los indicadores

    Estrategia:
    1. Agrupar staging por las 8 dimensiones
    2. Hacer JOINs para obtener IDs de dimensiones
    3. Calcular los 16 indicadores para cada grupo
    4. Insertar en tabla de hechos
    """
    log_message("\n--- CARGANDO TABLA DE HECHOS ---")
    log_message("Esto puede tomar varios minutos...")

    cursor = conn.cursor()

    # Query complejo que:
    # - Agrupa staging por las 8 dimensiones
    # - Hace JOIN con cada dimensión para obtener IDs
    # - Calcula los 16 indicadores

    query = """
    INSERT INTO Hechos_Estres_SaludMental (
        id_tiempo, id_genero, id_historial, id_ocupacion, id_pais, 
        id_aislamiento, id_sintomas, id_acceso,
        cantidad_estres, porcentaje_estres,
        cantidad_historial_estres, porcentaje_historial_estres,
        cantidad_estres_afrontamiento, porcentaje_estres_afrontamiento_ocupacion,
        porcentaje_tratamiento, porcentaje_no_tratamiento, cantidad_tratamiento,
        porcentaje_deterioro_aislamiento, porcentaje_humor_aislamiento,
        porcentaje_debilidad_aislamiento, porcentaje_acceso_recursos,
        cantidad_estres_acceso, porcentaje_sintomas_no_reconocidos,
        porcentaje_recursos_sin_tratamiento, porcentaje_postergacion
    )
    SELECT 
        -- Claves foráneas
        dt.id_tiempo,
        dg.id_genero,
        dh.id_historial,
        do.id_ocupacion,
        dp.id_pais,
        da.id_aislamiento,
        ds.id_sintomas,
        dac.id_acceso,

        -- INDICADOR 1: Cantidad con estrés creciente
        SUM(CASE WHEN s.Growing_Stress = 'Yes' THEN 1 ELSE 0 END) as cantidad_estres,

        -- INDICADOR 2: Porcentaje con estrés creciente
        ROUND(
            (SUM(CASE WHEN s.Growing_Stress = 'Yes' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
            2
        ) as porcentaje_estres,

        -- INDICADOR 3: Cantidad con historial familiar y estrés
        SUM(
            CASE WHEN s.family_history = 'Yes' AND s.Growing_Stress = 'Yes' 
            THEN 1 ELSE 0 END
        ) as cantidad_historial_estres,

        -- INDICADOR 4: Proporción con historial familiar que desarrollan estrés
        CASE 
            WHEN SUM(CASE WHEN s.family_history = 'Yes' THEN 1 ELSE 0 END) > 0 
            THEN ROUND(
                (SUM(CASE WHEN s.family_history = 'Yes' AND s.Growing_Stress = 'Yes' THEN 1 ELSE 0 END) / 
                 SUM(CASE WHEN s.family_history = 'Yes' THEN 1 ELSE 0 END)) * 100, 
                2
            )
            ELSE NULL
        END as porcentaje_historial_estres,

        -- INDICADOR 5: Cantidad con estrés y dificultades de afrontamiento
        SUM(
            CASE WHEN s.Growing_Stress = 'Yes' AND s.Coping_Struggles = 'Yes' 
            THEN 1 ELSE 0 END
        ) as cantidad_estres_afrontamiento,

        -- INDICADOR 6: Proporción con estrés y dificultades por ocupación/país
        ROUND(
            (SUM(CASE WHEN s.Growing_Stress = 'Yes' AND s.Coping_Struggles = 'Yes' THEN 1 ELSE 0 END) / 
             COUNT(*)) * 100, 
            2
        ) as porcentaje_estres_afrontamiento_ocupacion,

        -- INDICADOR 7a: Porcentaje en tratamiento
        ROUND(
            (SUM(CASE WHEN s.treatment = 'Yes' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
            2
        ) as porcentaje_tratamiento,

        -- INDICADOR 7b: Porcentaje sin tratamiento
        ROUND(
            (SUM(CASE WHEN s.treatment = 'No' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
            2
        ) as porcentaje_no_tratamiento,

        -- INDICADOR 8: Cantidad en tratamiento
        SUM(CASE WHEN s.treatment = 'Yes' THEN 1 ELSE 0 END) as cantidad_tratamiento,

        -- INDICADOR 9: Proporción con deterioro emocional por aislamiento
        ROUND(
            (SUM(CASE WHEN s.Growing_Stress = 'Yes' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
            2
        ) as porcentaje_deterioro_aislamiento,

        -- INDICADOR 10: Proporción con cambios de humor por aislamiento
        ROUND(
            (SUM(CASE WHEN s.Mood_Swings IN ('Medium', 'High') THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
            2
        ) as porcentaje_humor_aislamiento,

        -- INDICADOR 11: Proporción con debilidad social por aislamiento
        ROUND(
            (SUM(CASE WHEN s.Social_Weakness = 'Yes' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 
            2
        ) as porcentaje_debilidad_aislamiento,

        -- INDICADOR 12: Proporción con estrés que tienen acceso a recursos
        CASE 
            WHEN SUM(CASE WHEN s.Growing_Stress = 'Yes' THEN 1 ELSE 0 END) > 0 
            THEN ROUND(
                (SUM(CASE WHEN s.Growing_Stress = 'Yes' AND s.care_options IN ('Yes', 'Not sure') THEN 1 ELSE 0 END) / 
                 SUM(CASE WHEN s.Growing_Stress = 'Yes' THEN 1 ELSE 0 END)) * 100, 
                2
            )
            ELSE NULL
        END as porcentaje_acceso_recursos,

        -- INDICADOR 13: Cantidad con estrés y acceso a recursos
        SUM(
            CASE WHEN s.Growing_Stress = 'Yes' AND s.care_options IN ('Yes', 'Not sure') 
            THEN 1 ELSE 0 END
        ) as cantidad_estres_acceso,

        -- INDICADOR 14: Proporción con síntomas no reconocidos que buscan tratamiento
        CASE 
            WHEN SUM(
                CASE WHEN s.Growing_Stress IN ('No', 'Maybe') 
                     AND s.Mood_Swings IN ('Medium', 'High')
                     AND s.Coping_Struggles = 'Yes'
                     AND s.Days_Indoors IN ('15-30 days', '31-60 days', 'More than 2 months')
                THEN 1 ELSE 0 END
            ) > 0
            THEN ROUND(
                (SUM(
                    CASE WHEN s.Growing_Stress IN ('No', 'Maybe')
                         AND s.Mood_Swings IN ('Medium', 'High')
                         AND s.Coping_Struggles = 'Yes'
                         AND s.Days_Indoors IN ('15-30 days', '31-60 days', 'More than 2 months')
                         AND s.treatment = 'Yes'
                    THEN 1 ELSE 0 END
                ) / 
                SUM(
                    CASE WHEN s.Growing_Stress IN ('No', 'Maybe')
                         AND s.Mood_Swings IN ('Medium', 'High')
                         AND s.Coping_Struggles = 'Yes'
                         AND s.Days_Indoors IN ('15-30 days', '31-60 days', 'More than 2 months')
                    THEN 1 ELSE 0 END
                )) * 100,
                2
            )
            ELSE NULL
        END as porcentaje_sintomas_no_reconocidos,

        -- INDICADOR 15: Proporción con recursos disponibles que no buscan tratamiento
        CASE 
            WHEN SUM(
                CASE WHEN ds.indicador_inferido_estres = 1 
                     AND s.care_options IN ('Yes', 'Not sure')
                THEN 1 ELSE 0 END
            ) > 0
            THEN ROUND(
                (SUM(
                    CASE WHEN ds.indicador_inferido_estres = 1
                         AND s.care_options IN ('Yes', 'Not sure')
                         AND s.treatment = 'No'
                    THEN 1 ELSE 0 END
                ) / 
                SUM(
                    CASE WHEN ds.indicador_inferido_estres = 1
                         AND s.care_options IN ('Yes', 'Not sure')
                    THEN 1 ELSE 0 END
                )) * 100,
                2
            )
            ELSE NULL
        END as porcentaje_recursos_sin_tratamiento,

        -- INDICADOR 16: Proporción que posterga tratamiento con recursos disponibles
        CASE 
            WHEN SUM(
                CASE WHEN ds.indicador_inferido_estres = 1
                     AND s.mental_health_interview IN ('Yes', 'Maybe')
                     AND s.care_options IN ('Yes', 'Not sure')
                THEN 1 ELSE 0 END
            ) > 0
            THEN ROUND(
                (SUM(
                    CASE WHEN ds.indicador_inferido_estres = 1
                         AND s.mental_health_interview IN ('Yes', 'Maybe')
                         AND s.care_options IN ('Yes', 'Not sure')
                         AND s.treatment = 'No'
                    THEN 1 ELSE 0 END
                ) / 
                SUM(
                    CASE WHEN ds.indicador_inferido_estres = 1
                         AND s.mental_health_interview IN ('Yes', 'Maybe')
                         AND s.care_options IN ('Yes', 'Not sure')
                    THEN 1 ELSE 0 END
                )) * 100,
                2
            )
            ELSE NULL
        END as porcentaje_postergacion

    FROM mental_health_staging s

    -- JOINs con dimensiones para obtener IDs
    INNER JOIN Dim_Tiempo dt ON 
        dt.anio = CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(s.Timestamp, ' ', 1), '/', -1) AS UNSIGNED)
        AND dt.mes = CAST(SUBSTRING_INDEX(s.Timestamp, '/', 1) AS UNSIGNED)

    INNER JOIN Dim_Genero dg ON 
        dg.genero = s.Gender

    INNER JOIN Dim_Historial dh ON 
        dh.family_history = s.family_history

    INNER JOIN Dim_Ocupacion do ON 
        do.occupation = s.Occupation

    INNER JOIN Dim_Pais dp ON 
        dp.country = s.Country

    INNER JOIN Dim_Aislamiento da ON 
        da.days_indoors = s.Days_Indoors

    INNER JOIN Dim_Sintomas ds ON 
        ds.growing_stress = s.Growing_Stress
        AND ds.mood_swings = s.Mood_Swings
        AND ds.coping_struggles = s.Coping_Struggles
        AND ds.social_weakness = s.Social_Weakness

    INNER JOIN Dim_Acceso dac ON 
        dac.care_options = s.care_options
        AND dac.mental_health_interview = s.mental_health_interview

    -- Agrupar por todas las dimensiones
    GROUP BY 
        dt.id_tiempo, dg.id_genero, dh.id_historial, do.id_ocupacion,
        dp.id_pais, da.id_aislamiento, ds.id_sintomas, dac.id_acceso
    """

    try:
        log_message("Ejecutando query de agregación...")
        cursor.execute(query)
        conn.commit()

        # Obtener cantidad de registros insertados
        cursor.execute("SELECT COUNT(*) FROM Hechos_Estres_SaludMental")
        count = cursor.fetchone()[0]

        log_message(f"✅ Tabla de hechos cargada: {count} registros agregados")

    except mysql.connector.Error as err:
        log_message(f"❌ ERROR al cargar hechos: {err}")
        conn.rollback()
        raise

    cursor.close()


def validar_hechos(conn):
    """Validar tabla de hechos"""
    log_message("\n--- VALIDANDO TABLA DE HECHOS ---")

    cursor = conn.cursor()

    # 1. Cantidad total de registros
    cursor.execute("SELECT COUNT(*) FROM Hechos_Estres_SaludMental")
    total = cursor.fetchone()[0]
    log_message(f"Total de hechos: {total}")

    # 2. Verificar que no hay registros huérfanos
    log_message("\nVerificando integridad referencial...")

    dimensiones = [
        ('id_tiempo', 'Dim_Tiempo'),
        ('id_genero', 'Dim_Genero'),
        ('id_historial', 'Dim_Historial'),
        ('id_ocupacion', 'Dim_Ocupacion'),
        ('id_pais', 'Dim_Pais'),
        ('id_aislamiento', 'Dim_Aislamiento'),
        ('id_sintomas', 'Dim_Sintomas'),
        ('id_acceso', 'Dim_Acceso')
    ]

    for fk, tabla_dim in dimensiones:
        query = f"""
        SELECT COUNT(*) 
        FROM Hechos_Estres_SaludMental h
        LEFT JOIN {tabla_dim} d ON h.{fk} = d.{fk}
        WHERE d.{fk} IS NULL
        """
        cursor.execute(query)
        huerfanos = cursor.fetchone()[0]

        if huerfanos == 0:
            log_message(f"  ✅ {fk}: sin huérfanos")
        else:
            log_message(f"  ❌ {fk}: {huerfanos} registros huérfanos")

    # 3. Verificar rangos de indicadores
    log_message("\nVerificando rangos de indicadores...")

    # Porcentajes deben estar entre 0 y 100
    porcentajes = [
        'porcentaje_estres', 'porcentaje_historial_estres',
        'porcentaje_estres_afrontamiento_ocupacion', 'porcentaje_tratamiento',
        'porcentaje_no_tratamiento', 'porcentaje_deterioro_aislamiento',
        'porcentaje_humor_aislamiento', 'porcentaje_debilidad_aislamiento',
        'porcentaje_acceso_recursos', 'porcentaje_sintomas_no_reconocidos',
        'porcentaje_recursos_sin_tratamiento', 'porcentaje_postergacion'
    ]

    for pct in porcentajes:
        query = f"""
        SELECT COUNT(*) 
        FROM Hechos_Estres_SaludMental 
        WHERE {pct} IS NOT NULL AND ({pct} < 0 OR {pct} > 100)
        """
        cursor.execute(query)
        fuera_rango = cursor.fetchone()[0]

        if fuera_rango > 0:
            log_message(f"  ⚠️ {pct}: {fuera_rango} valores fuera de rango [0-100]")

    # 4. Muestra de datos
    log_message("\nMuestra de hechos (primeros 3 registros):")
    cursor.execute("""
        SELECT 
            id_hecho, id_tiempo, id_genero, id_pais,
            cantidad_estres, porcentaje_estres, cantidad_tratamiento
        FROM Hechos_Estres_SaludMental
        LIMIT 3
    """)

    for row in cursor.fetchall():
        log_message(f"  Hecho {row[0]}: tiempo={row[1]}, genero={row[2]}, pais={row[3]}, "
                    f"cant_estres={row[4]}, pct_estres={row[5]}, cant_trat={row[6]}")

    # 5. Distribución por género
    log_message("\nDistribución de hechos por género:")
    cursor.execute("""
        SELECT dg.genero, COUNT(*) as cantidad
        FROM Hechos_Estres_SaludMental h
        JOIN Dim_Genero dg ON h.id_genero = dg.id_genero
        GROUP BY dg.genero
    """)
    for row in cursor.fetchall():
        log_message(f"  {row[0]}: {row[1]} hechos")

    cursor.close()


def main():
    """Función principal"""
    log_message("=" * 50)
    log_message("INICIANDO CARGA DE TABLA DE HECHOS")
    log_message("=" * 50)

    conn = conectar_mysql()

    try:
        # 1. Verificar dimensiones
        verificar_dimensiones(conn)

        # 2. Limpiar hechos
        limpiar_tabla_hechos(conn)

        # 3. Cargar hechos
        cargar_hechos(conn)

        # 4. Validar
        validar_hechos(conn)

        log_message("\n" + "=" * 50)
        log_message("TABLA DE HECHOS CARGADA EXITOSAMENTE")
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