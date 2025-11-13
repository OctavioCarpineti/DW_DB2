-- ============================================
-- SCRIPT 1: CREAR BASE DE DATOS
-- ============================================

-- Eliminar base de datos si existe (solo para desarrollo)
DROP DATABASE IF EXISTS dw_salud_mental;

-- Crear base de datos
CREATE DATABASE dw_salud_mental
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- Usar la base de datos
USE dw_salud_mental;

-- Mensaje de confirmación
SELECT 'Base de datos dw_salud_mental creada exitosamente' AS mensaje;
```

**6.2. Ejecutar este script en MySQL Workbench:**

1. Abre MySQL Workbench
2. Conecta a tu servidor MySQL local
3. Abre el archivo SQL (File → Open SQL Script) o copia y pega el contenido
4. Click en el rayo ⚡ "Execute" o presiona Ctrl+Shift+Enter

**Resultado esperado:**
```
1 row(s) returned
mensaje: Base de datos dw_salud_mental creada exitosamente