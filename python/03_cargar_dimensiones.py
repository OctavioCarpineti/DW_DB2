"""
Script 3: Cargar todas las dimensiones desde staging
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


def cargar_dim_tiempo(conn):
    """Cargar Dim_Tiempo"""
    log_message("\n--- CARGANDO DIM_TIEMPO ---")

    cursor = conn.cursor()

    # Limpiar dimensión
    cursor.execute("TRUNCATE TABLE Dim_Tiempo")

    # Extraer años y meses parseando manualmente el formato M/D/YYYY
    query_fechas = """
    SELECT DISTINCT
        CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(Timestamp, ' ', 1), '/', -1) AS UNSIGNED) AS anio,
        CAST(SUBSTRING_INDEX(Timestamp, '/', 1) AS UNSIGNED) AS mes
    FROM mental_health_staging
    WHERE Timestamp IS NOT NULL
        AND Timestamp != ''
    ORDER BY anio, mes
    """

    cursor.execute(query_fechas)
    fechas = cursor.fetchall()

    log_message(f"Encontrados {len(fechas)} períodos únicos")

    # Insertar cada período
    for anio, mes in fechas:
        nombres_meses = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }

        nombre_mes = nombres_meses.get(mes, 'Desconocido')
        periodo = f"{anio}-{mes:02d}"
        trimestre = (mes - 1) // 3 + 1
        semestre = 1 if mes <= 6 else 2

        query_insert = """
        INSERT INTO Dim_Tiempo (anio, mes, nombre_mes, periodo, trimestre, semestre)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        cursor.execute(query_insert, (anio, mes, nombre_mes, periodo, trimestre, semestre))

    conn.commit()

    # Verificar
    cursor.execute("SELECT COUNT(*) FROM Dim_Tiempo")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Tiempo cargada: {count} registros")

    cursor.close()


def cargar_dim_genero(conn):
    """Cargar Dim_Genero"""
    log_message("\n--- CARGANDO DIM_GENERO ---")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Dim_Genero")

    query = """
    INSERT INTO Dim_Genero (genero, descripcion)
    VALUES 
        ('Male', 'Masculino'),
        ('Female', 'Femenino')
    """

    cursor.execute(query)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM Dim_Genero")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Genero cargada: {count} registros")

    cursor.close()


def cargar_dim_historial(conn):
    """Cargar Dim_Historial"""
    log_message("\n--- CARGANDO DIM_HISTORIAL ---")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Dim_Historial")

    query = """
    INSERT INTO Dim_Historial (family_history, descripcion)
    VALUES 
        ('Yes', 'Con antecedentes familiares de problemas de salud mental'),
        ('No', 'Sin antecedentes familiares de problemas de salud mental')
    """

    cursor.execute(query)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM Dim_Historial")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Historial cargada: {count} registros")

    cursor.close()


def cargar_dim_ocupacion(conn):
    """Cargar Dim_Ocupacion"""
    log_message("\n--- CARGANDO DIM_OCUPACION ---")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Dim_Ocupacion")

    query = """
    INSERT INTO Dim_Ocupacion (occupation, descripcion)
    VALUES 
        ('Corporate', 'Empleado en sector corporativo'),
        ('Student', 'Estudiante'),
        ('Business', 'Empresario o negocio propio'),
        ('Housewife', 'Ama de casa'),
        ('Others', 'Otras ocupaciones')
    """

    cursor.execute(query)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM Dim_Ocupacion")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Ocupacion cargada: {count} registros")

    cursor.close()


def cargar_dim_pais(conn):
    """Cargar Dim_Pais"""
    log_message("\n--- CARGANDO DIM_PAIS ---")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Dim_Pais")

    query = """
    INSERT INTO Dim_Pais (country, region, codigo_iso)
    VALUES 
        -- América del Norte
        ('United States', 'América del Norte', 'USA'),
        ('Canada', 'América del Norte', 'CAN'),
        ('Mexico', 'América del Norte', 'MEX'),
        
        -- Europa
        ('United Kingdom', 'Europa', 'GBR'),
        ('Germany', 'Europa', 'DEU'),
        ('France', 'Europa', 'FRA'),
        ('Netherlands', 'Europa', 'NLD'),
        ('Sweden', 'Europa', 'SWE'),
        ('Denmark', 'Europa', 'DNK'),
        ('Finland', 'Europa', 'FIN'),
        ('Switzerland', 'Europa', 'CHE'),
        ('Belgium', 'Europa', 'BEL'),
        ('Ireland', 'Europa', 'IRL'),
        ('Poland', 'Europa', 'POL'),
        ('Portugal', 'Europa', 'PRT'),
        ('Greece', 'Europa', 'GRC'),
        ('Italy', 'Europa', 'ITA'),
        ('Czech Republic', 'Europa', 'CZE'),
        ('Croatia', 'Europa', 'HRV'),
        ('Bosnia and Herzegovina', 'Europa', 'BIH'),
        ('Russia', 'Europa', 'RUS'),
        ('Moldova', 'Europa', 'MDA'),
        ('Georgia', 'Europa', 'GEO'),
        
        -- Asia
        ('India', 'Asia', 'IND'),
        ('Philippines', 'Asia', 'PHL'),
        ('Thailand', 'Asia', 'THA'),
        ('Singapore', 'Asia', 'SGP'),
        ('Israel', 'Asia', 'ISR'),
        
        -- Oceanía
        ('Australia', 'Oceanía', 'AUS'),
        ('New Zealand', 'Oceanía', 'NZL'),
        
        -- África
        ('Nigeria', 'África', 'NGA'),
        ('South Africa', 'África', 'ZAF'),
        
        -- América Latina
        ('Brazil', 'América Latina', 'BRA'),
        ('Colombia', 'América Latina', 'COL'),
        ('Costa Rica', 'América Latina', 'CRI')
    """

    cursor.execute(query)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM Dim_Pais")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Pais cargada: {count} registros")

    cursor.close()


def cargar_dim_aislamiento(conn):
    """Cargar Dim_Aislamiento"""
    log_message("\n--- CARGANDO DIM_AISLAMIENTO ---")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Dim_Aislamiento")

    query = """
    INSERT INTO Dim_Aislamiento (days_indoors, orden, categoria)
    VALUES 
        ('Go out Every day', 1, 'Bajo'),
        ('1-14 days', 2, 'Bajo'),
        ('15-30 days', 3, 'Medio'),
        ('31-60 days', 4, 'Alto'),
        ('More than 2 months', 5, 'Alto')
    """

    cursor.execute(query)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM Dim_Aislamiento")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Aislamiento cargada: {count} registros")

    cursor.close()


def cargar_dim_sintomas(conn):
    """Cargar Dim_Sintomas con variable derivada"""
    log_message("\n--- CARGANDO DIM_SINTOMAS ---")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Dim_Sintomas")

    # Obtener combinaciones únicas
    query_combinaciones = """
    SELECT DISTINCT
        Growing_Stress,
        Mood_Swings,
        Coping_Struggles,
        Social_Weakness,
        Days_Indoors
    FROM mental_health_staging
    WHERE Growing_Stress IS NOT NULL
      AND Mood_Swings IS NOT NULL
      AND Coping_Struggles IS NOT NULL
      AND Social_Weakness IS NOT NULL
      AND Days_Indoors IS NOT NULL
    """

    cursor.execute(query_combinaciones)
    combinaciones = cursor.fetchall()

    log_message(f"Encontradas {len(combinaciones)} combinaciones únicas de síntomas")

    # Insertar cada combinación con el indicador calculado
    for growing, mood, coping, social, days in combinaciones:
        # Calcular indicador_inferido_estres
        if growing == 'Yes':
            indicador = True
        elif (mood in ('Medium', 'High') and
              coping == 'Yes' and
              days in ('15-30 days', '31-60 days', 'More than 2 months')):
            indicador = True
        else:
            indicador = False

        query_insert = """
        INSERT INTO Dim_Sintomas 
            (growing_stress, mood_swings, coping_struggles, social_weakness, indicador_inferido_estres)
        VALUES (%s, %s, %s, %s, %s)
        """

        cursor.execute(query_insert, (growing, mood, coping, social, indicador))

    conn.commit()

    # Verificar
    cursor.execute("SELECT COUNT(*) FROM Dim_Sintomas")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Sintomas cargada: {count} registros")

    # Mostrar distribución del indicador inferido
    cursor.execute("""
        SELECT 
            indicador_inferido_estres,
            COUNT(*) as cantidad
        FROM Dim_Sintomas
        GROUP BY indicador_inferido_estres
    """)

    log_message("  Distribución del indicador_inferido_estres:")
    for row in cursor.fetchall():
        log_message(f"    {row[0]}: {row[1]} combinaciones")

    cursor.close()


def cargar_dim_acceso(conn):
    """Cargar Dim_Acceso - CORREGIDO: sin treatment"""
    log_message("\n--- CARGANDO DIM_ACCESO ---")

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE Dim_Acceso")

    # Obtener combinaciones únicas (SIN treatment)
    query_combinaciones = """
    SELECT DISTINCT
        care_options,
        mental_health_interview
    FROM mental_health_staging
    WHERE care_options IS NOT NULL
      AND mental_health_interview IS NOT NULL
    """

    cursor.execute(query_combinaciones)
    combinaciones = cursor.fetchall()

    log_message(f"Encontradas {len(combinaciones)} combinaciones únicas de acceso")

    # Insertar cada combinación
    for care, interview in combinaciones:
        query_insert = """
        INSERT INTO Dim_Acceso (care_options, mental_health_interview)
        VALUES (%s, %s)
        """

        cursor.execute(query_insert, (care, interview))

    conn.commit()

    # Verificar
    cursor.execute("SELECT COUNT(*) FROM Dim_Acceso")
    count = cursor.fetchone()[0]
    log_message(f"✅ Dim_Acceso cargada: {count} registros")

    cursor.close()


def validar_dimensiones(conn):
    """Validar que todas las dimensiones se cargaron correctamente"""
    log_message("\n" + "=" * 50)
    log_message("VALIDACIÓN DE DIMENSIONES")
    log_message("=" * 50)

    cursor = conn.cursor()

    dimensiones = [
        'Dim_Tiempo',
        'Dim_Genero',
        'Dim_Historial',
        'Dim_Ocupacion',
        'Dim_Pais',
        'Dim_Aislamiento',
        'Dim_Sintomas',
        'Dim_Acceso'
    ]

    for dim in dimensiones:
        cursor.execute(f"SELECT COUNT(*) FROM {dim}")
        count = cursor.fetchone()[0]

        if count > 0:
            log_message(f"✅ {dim}: {count} registros")
        else:
            log_message(f"❌ {dim}: 0 registros (ERROR)")

    cursor.close()

"""
def main():
    Función principal
    log_message("=" * 50)
    log_message("INICIANDO CARGA DE DIMENSIONES")
    log_message("=" * 50)

    # Conectar
    conn = conectar_mysql()

    try:
        # Cargar cada dimensión
        cargar_dim_tiempo(conn)
        cargar_dim_genero(conn)
        cargar_dim_historial(conn)
        cargar_dim_ocupacion(conn)
        cargar_dim_pais(conn)
        cargar_dim_aislamiento(conn)
        cargar_dim_sintomas(conn)
        cargar_dim_acceso(conn)

        # Validar
        validar_dimensiones(conn)

        log_message("\n" + "=" * 50)
        log_message("DIMENSIONES CARGADAS EXITOSAMENTE")
        log_message("=" * 50)

    except Exception as e:
        log_message(f"\n❌ ERROR: {str(e)}")
        conn.rollback()
    finally:
        conn.close()
        log_message("Conexión cerrada")


if __name__ == "__main__":
    main()
"""


def main():
    log_message("=" * 50)
    log_message("INICIANDO CARGA DE DIMENSIONES")
    log_message("=" * 50)

    # Conectar
    conn = conectar_mysql()

    try:
        # DESACTIVAR verificación de claves foráneas
        cursor = conn.cursor()
        log_message("Desactivando verificación de claves foráneas...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        cursor.close()
        log_message("✅ FK checks desactivados")

        # Cargar cada dimensión
        cargar_dim_tiempo(conn)
        cargar_dim_genero(conn)
        cargar_dim_historial(conn)
        cargar_dim_ocupacion(conn)
        cargar_dim_pais(conn)
        cargar_dim_aislamiento(conn)
        cargar_dim_sintomas(conn)
        cargar_dim_acceso(conn)

        # REACTIVAR verificación de claves foráneas
        cursor = conn.cursor()
        log_message("\nReactivando verificación de claves foráneas...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        cursor.close()
        log_message("✅ FK checks reactivados")

        # Validar
        validar_dimensiones(conn)

        log_message("\n" + "=" * 50)
        log_message("DIMENSIONES CARGADAS EXITOSAMENTE")
        log_message("=" * 50)

    except Exception as e:
        log_message(f"\n❌ ERROR: {str(e)}")

        # Asegurarse de reactivar FK checks aunque haya error
        try:
            cursor = conn.cursor()
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            cursor.close()
            log_message("FK checks reactivados después del error")
        except:
            pass

        conn.rollback()
    finally:
        conn.close()
        log_message("Conexión cerrada")


if __name__ == "__main__":
    main()