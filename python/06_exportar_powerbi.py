import pandas as pd

def exportar_para_powerbi(conn, CSV_EXPORT=None):
    """Exportar DW completo en formato plano para Power BI"""

    # Query que hace JOIN de hechos con todas las dimensiones
    query_export = """
    SELECT 
        -- Dimensión Tiempo
        dt.anio,
        dt.mes,
        dt.nombre_mes,
        dt.periodo,
        dt.trimestre,
        dt.semestre,

        -- Dimensión Género
        dg.genero,

        -- Dimensión Historial
        dh.family_history,

        -- Dimensión Ocupación
        do.occupation,

        -- Dimensión País
        dp.country,
        dp.region,

        -- Dimensión Aislamiento
        da.days_indoors,
        da.categoria as aislamiento_categoria,
        da.orden as aislamiento_orden,

        -- Dimensión Síntomas
        ds.growing_stress,
        ds.mood_swings,
        ds.coping_struggles,
        ds.social_weakness,
        ds.indicador_inferido_estres,

        -- Dimensión Acceso
        dac.care_options,
        dac.mental_health_interview,

        -- INDICADORES (16 métricas)
        h.cantidad_estres,
        h.porcentaje_estres,
        h.cantidad_historial_estres,
        h.porcentaje_historial_estres,
        h.cantidad_estres_afrontamiento,
        h.porcentaje_estres_afrontamiento_ocupacion,
        h.porcentaje_tratamiento,
        h.porcentaje_no_tratamiento,
        h.cantidad_tratamiento,
        h.porcentaje_deterioro_aislamiento,
        h.porcentaje_humor_aislamiento,
        h.porcentaje_debilidad_aislamiento,
        h.porcentaje_acceso_recursos,
        h.cantidad_estres_acceso,
        h.porcentaje_sintomas_no_reconocidos,
        h.porcentaje_recursos_sin_tratamiento,
        h.porcentaje_postergacion

    FROM Hechos_Estres_SaludMental h
    INNER JOIN Dim_Tiempo dt ON h.id_tiempo = dt.id_tiempo
    INNER JOIN Dim_Genero dg ON h.id_genero = dg.id_genero
    INNER JOIN Dim_Historial dh ON h.id_historial = dh.id_historial
    INNER JOIN Dim_Ocupacion do ON h.id_ocupacion = do.id_ocupacion
    INNER JOIN Dim_Pais dp ON h.id_pais = dp.id_pais
    INNER JOIN Dim_Aislamiento da ON h.id_aislamiento = da.id_aislamiento
    INNER JOIN Dim_Sintomas ds ON h.id_sintomas = ds.id_sintomas
    INNER JOIN Dim_Acceso dac ON h.id_acceso = dac.id_acceso
    ORDER BY dt.anio, dt.mes, dg.genero, dp.country
    """

    # Ejecutar query y cargar en DataFrame
    df = pd.read_sql(query_export, conn)

    # Exportar a CSV
    df.to_csv(CSV_EXPORT, index=False, encoding='utf-8')

    print(f"✅ Archivo exportado: {CSV_EXPORT}")
    print(f"   Registros: {len(df)}")
    print(f"   Columnas: {len(df.columns)}")
