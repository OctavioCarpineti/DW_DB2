-- ============================================
-- SCRIPT 2: CREAR TODAS LAS TABLAS DEL DW
-- Data Warehouse: Análisis de Estrés y Salud Mental
-- Metodología: Hefesto - Esquema Estrella
-- ============================================

USE dw_salud_mental;

-- ============================================
-- TABLA STAGING (temporal para ETL)
-- ============================================

DROP TABLE IF EXISTS mental_health_staging;

CREATE TABLE mental_health_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Timestamp VARCHAR(50) NOT NULL,
    Gender VARCHAR(10) NOT NULL,
    Country VARCHAR(50) NOT NULL,
    Occupation VARCHAR(20) NOT NULL,
    self_employed VARCHAR(10),
    family_history VARCHAR(5) NOT NULL,
    treatment VARCHAR(5) NOT NULL,
    Days_Indoors VARCHAR(30) NOT NULL,
    Growing_Stress VARCHAR(10) NOT NULL,
    Changes_Habits VARCHAR(10),
    Mental_Health_History VARCHAR(10),
    Mood_Swings VARCHAR(10) NOT NULL,
    Coping_Struggles VARCHAR(5) NOT NULL,
    Work_Interest VARCHAR(10),
    Social_Weakness VARCHAR(10) NOT NULL,
    mental_health_interview VARCHAR(10) NOT NULL,
    care_options VARCHAR(15) NOT NULL,

    -- Índices para optimizar ETL
    INDEX idx_timestamp (Timestamp),
    INDEX idx_gender (Gender),
    INDEX idx_country (Country),
    INDEX idx_occupation (Occupation),
    INDEX idx_days_indoors (Days_Indoors),
    INDEX idx_growing_stress (Growing_Stress)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- DIMENSIONES DEL DATA WAREHOUSE
-- ============================================

-- -------------------------------------------
-- Dimensión Tiempo
-- Granularidad: Mensual (2014-2016)
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Tiempo;

CREATE TABLE Dim_Tiempo (
    id_tiempo INT AUTO_INCREMENT PRIMARY KEY,
    anio INT NOT NULL,
    mes INT NOT NULL,
    nombre_mes VARCHAR(15) NOT NULL,
    periodo VARCHAR(7) NOT NULL UNIQUE,  -- Formato "YYYY-MM"
    trimestre INT NOT NULL,
    semestre INT NOT NULL,

    INDEX idx_periodo (periodo),
    INDEX idx_anio_mes (anio, mes)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -------------------------------------------
-- Dimensión Género
-- Valores: Male, Female
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Genero;

CREATE TABLE Dim_Genero (
    id_genero INT AUTO_INCREMENT PRIMARY KEY,
    genero VARCHAR(10) NOT NULL UNIQUE,
    descripcion VARCHAR(50) NULL,

    INDEX idx_genero (genero)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -------------------------------------------
-- Dimensión Historial Familiar
-- Valores: Yes, No
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Historial;

CREATE TABLE Dim_Historial (
    id_historial INT AUTO_INCREMENT PRIMARY KEY,
    family_history VARCHAR(3) NOT NULL UNIQUE,
    descripcion VARCHAR(100) NULL,

    INDEX idx_family_history (family_history)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -------------------------------------------
-- Dimensión Ocupación
-- Valores: Corporate, Student, Business, Housewife, Others
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Ocupacion;

CREATE TABLE Dim_Ocupacion (
    id_ocupacion INT AUTO_INCREMENT PRIMARY KEY,
    occupation VARCHAR(20) NOT NULL UNIQUE,
    descripcion VARCHAR(100) NULL,

    INDEX idx_occupation (occupation)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -------------------------------------------
-- Dimensión País
-- 36 países con región geográfica derivada
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Pais;

CREATE TABLE Dim_Pais (
    id_pais INT AUTO_INCREMENT PRIMARY KEY,
    country VARCHAR(50) NOT NULL UNIQUE,
    region VARCHAR(30) NOT NULL,
    codigo_iso VARCHAR(3) NULL,

    INDEX idx_country (country),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -------------------------------------------
-- Dimensión Aislamiento
-- Niveles de días en interior con orden y categoría
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Aislamiento;

CREATE TABLE Dim_Aislamiento (
    id_aislamiento INT AUTO_INCREMENT PRIMARY KEY,
    days_indoors VARCHAR(25) NOT NULL UNIQUE,
    orden INT NOT NULL,
    categoria VARCHAR(20) NOT NULL,

    INDEX idx_days_indoors (days_indoors),
    INDEX idx_orden (orden)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -------------------------------------------
-- Dimensión Síntomas
-- Incluye variable derivada: indicador_inferido_estres
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Sintomas;

CREATE TABLE Dim_Sintomas (
    id_sintomas INT AUTO_INCREMENT PRIMARY KEY,
    growing_stress VARCHAR(5) NOT NULL,
    mood_swings VARCHAR(10) NOT NULL,
    coping_struggles VARCHAR(3) NOT NULL,
    social_weakness VARCHAR(5) NOT NULL,
    indicador_inferido_estres BOOLEAN NOT NULL,

    INDEX idx_growing_stress (growing_stress),
    INDEX idx_inferido (indicador_inferido_estres)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -------------------------------------------
-- Dimensión Acceso a Recursos
-- CORREGIDO: Solo incluye care_options y mental_health_interview
-- treatment NO es una dimensión, es una MÉTRICA
-- -------------------------------------------
DROP TABLE IF EXISTS Dim_Acceso;

CREATE TABLE Dim_Acceso (
    id_acceso INT AUTO_INCREMENT PRIMARY KEY,
    care_options VARCHAR(10) NOT NULL,
    mental_health_interview VARCHAR(5) NOT NULL,

    INDEX idx_care (care_options),
    INDEX idx_interview (mental_health_interview)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- TABLA DE HECHOS CENTRAL
-- Esquema Estrella - Núcleo del Data Warehouse
-- ============================================

DROP TABLE IF EXISTS Hechos_Estres_SaludMental;

CREATE TABLE Hechos_Estres_SaludMental (
    id_hecho INT AUTO_INCREMENT PRIMARY KEY,

    -- ========================================
    -- CLAVES FORÁNEAS HACIA DIMENSIONES (8)
    -- ========================================
    id_tiempo INT NOT NULL,
    id_genero INT NOT NULL,
    id_historial INT NOT NULL,
    id_ocupacion INT NOT NULL,
    id_pais INT NOT NULL,
    id_aislamiento INT NOT NULL,
    id_sintomas INT NOT NULL,
    id_acceso INT NOT NULL,

    -- ========================================
    -- INDICADORES (MÉTRICAS) - Total: 16
    -- ========================================

    -- Indicador 1: Cantidad con estrés creciente
    cantidad_estres INT NULL,

    -- Indicador 2: Proporción con estrés creciente
    porcentaje_estres DECIMAL(5,2) NULL,

    -- Indicador 3: Cantidad con historial familiar y estrés
    cantidad_historial_estres INT NULL,

    -- Indicador 4: Proporción con historial familiar que desarrollan estrés
    porcentaje_historial_estres DECIMAL(5,2) NULL,

    -- Indicador 5: Cantidad con estrés y dificultades de afrontamiento
    cantidad_estres_afrontamiento INT NULL,

    -- Indicador 6: Proporción con estrés y dificultades por ocupación/país
    porcentaje_estres_afrontamiento_ocupacion DECIMAL(5,2) NULL,

    -- Indicador 7a: Proporción en tratamiento
    porcentaje_tratamiento DECIMAL(5,2) NULL,

    -- Indicador 7b: Proporción sin tratamiento
    porcentaje_no_tratamiento DECIMAL(5,2) NULL,

    -- Indicador 8: Cantidad en tratamiento
    cantidad_tratamiento INT NULL,

    -- Indicador 9: Proporción con deterioro emocional por aislamiento
    porcentaje_deterioro_aislamiento DECIMAL(5,2) NULL,

    -- Indicador 10: Proporción con cambios de humor por aislamiento
    porcentaje_humor_aislamiento DECIMAL(5,2) NULL,

    -- Indicador 11: Proporción con debilidad social por aislamiento
    porcentaje_debilidad_aislamiento DECIMAL(5,2) NULL,

    -- Indicador 12: Proporción con estrés que tienen acceso a recursos
    porcentaje_acceso_recursos DECIMAL(5,2) NULL,

    -- Indicador 13: Cantidad con estrés y acceso a recursos
    cantidad_estres_acceso INT NULL,

    -- Indicador 14: Proporción con síntomas no reconocidos que buscan tratamiento
    porcentaje_sintomas_no_reconocidos DECIMAL(5,2) NULL,

    -- Indicador 15: Proporción con recursos disponibles que no buscan tratamiento
    porcentaje_recursos_sin_tratamiento DECIMAL(5,2) NULL,

    -- Indicador 16: Proporción que posterga tratamiento con recursos disponibles
    porcentaje_postergacion DECIMAL(5,2) NULL,

    -- ========================================
    -- ÍNDICES PARA OPTIMIZAR CONSULTAS
    -- ========================================
    INDEX idx_tiempo (id_tiempo),
    INDEX idx_genero (id_genero),
    INDEX idx_pais (id_pais),
    INDEX idx_sintomas (id_sintomas),
    INDEX idx_acceso (id_acceso),

    -- Índice compuesto para consultas frecuentes
    INDEX idx_tiempo_genero (id_tiempo, id_genero),
    INDEX idx_pais_ocupacion (id_pais, id_ocupacion),

    -- ========================================
    -- RESTRICCIONES DE INTEGRIDAD REFERENCIAL
    -- ========================================
    FOREIGN KEY (id_tiempo) REFERENCES Dim_Tiempo(id_tiempo)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    FOREIGN KEY (id_genero) REFERENCES Dim_Genero(id_genero)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    FOREIGN KEY (id_historial) REFERENCES Dim_Historial(id_historial)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    FOREIGN KEY (id_ocupacion) REFERENCES Dim_Ocupacion(id_ocupacion)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    FOREIGN KEY (id_pais) REFERENCES Dim_Pais(id_pais)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    FOREIGN KEY (id_aislamiento) REFERENCES Dim_Aislamiento(id_aislamiento)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    FOREIGN KEY (id_sintomas) REFERENCES Dim_Sintomas(id_sintomas)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    FOREIGN KEY (id_acceso) REFERENCES Dim_Acceso(id_acceso)
        ON DELETE RESTRICT ON UPDATE CASCADE

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- VERIFICACIÓN Y REPORTE
-- ============================================

SELECT '✅ Todas las tablas creadas exitosamente' AS mensaje;


-- Mostrar estructura del Data Warehouse
 SELECT
    '📊 ESTRUCTURA DEL DATA WAREHOUSE' AS '';

 SELECT
    TABLE_NAME AS 'Tabla',
    TABLE_TYPE AS 'Tipo',
    ENGINE AS 'Motor',
    TABLE_ROWS AS 'Filas',
    ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS 'Tamaño (MB)'
FROM
    information_schema.TABLES
WHERE
    TABLE_SCHEMA = 'dw_salud_mental'
ORDER BY
    CASE
        WHEN TABLE_NAME LIKE 'Dim_%' THEN 2
        WHEN TABLE_NAME = 'Hechos_Estres_SaludMental' THEN 3
        ELSE 1
    END,
    TABLE_NAME;

SELECT '✅ Script 02_crear_tablas.sql ejecutado correctamente' AS resultado;