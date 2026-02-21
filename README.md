# Medallion_Liquor_Sales_Pipeline  
## End-to-End Data Engineering Pipeline

## 📌 Visión General del Proyecto
Este proyecto implementa un Data Warehouse transaccional robusto para el análisis de ventas minoristas, distribución y cadena de suministro de bebidas alcohólicas en el estado de Iowa, USA. 

El objetivo principal es procesar un volumen masivo de registros transaccionales (nivel línea de factura) para habilitar respuestas analíticas sobre tendencias de ventas, rentabilidad (Gross Margin), comportamiento geográfico de las tiendas y rendimiento del portafolio de productos.

## 🛠️ Stack Tecnológico
* **Procesamiento Distribuido:** Apache Spark (PySpark)
* **Almacenamiento y Formato:** Delta Lake
* **Plataforma:** Databricks / Unity Catalog
* **Lenguajes:** Python, Databricks SQL

## 🏗️ Arquitectura de Datos (Medallion Architecture)
El pipeline de datos sigue estrictamente la Arquitectura Medallion para garantizar la calidad, auditabilidad y rendimiento de los datos desde su estado crudo hasta el modelo analítico final.

### 🥉 Capa Bronze (Raw Data)
* **Ingesta:** Lectura de múltiples archivos CSV crudos (`AAAA_MM_iowa_sales.csv`).
* **Protección contra "Column Shift":** Implementación de lecturas permisivas con manejo avanzado de comillas y saltos de línea (`multiLine=true`).
* **Detección de Corrupción:** Reglas programáticas en PySpark usando `try_cast` para identificar filas con tipos de datos inválidos o columnas desplazadas sin interrumpir el pipeline.

### 🥈 Capa Silver (Cleansed & Conformed Data)
* **Estandarización:** Limpieza de strings (mayúsculas, trim), parseo robusto de fechas mixtas (`COALESCE` para formatos `MM/dd/yyyy` y `yyyy-MM-dd`), y transformación de strings financieros (ej. `$4,30` -> `4.30`).
* **Cuarentena y Deduplicación:** Filtrado de registros corruptos (aislados en cuarentena) y eliminación de transacciones duplicadas por llave primaria (`invoice_line_no`).
* **Integridad Referencial:** Manejo estratégico de valores nulos (Null Handling). Asignación del valor `-1` (Unknown Member) a atributos faltantes (ej. `county_number`) para evitar la pérdida de métricas financieras vitales durante los cruces (JOINs).

### 🥇 Capa Gold (Dimensional Modeling - Star Schema)
El modelo final está diseñado mediante un **Esquema Estrella (Star Schema)** optimizado para consultas analíticas de alto rendimiento.

**Tabla de Hechos (Fact Table):**
* `Fact_Sales`: Almacena métricas transaccionales (volumen de botellas, litros, dólares vendidos, costo y margen).

**Tablas de Dimensiones (Dimension Tables):**
* `Dim_Date`: Dimensión de tiempo generada dinámicamente.
* `Dim_Product`: Dimensión de productos y categorías (SCD Tipo 1).
* `Dim_Vendor`: Dimensión de proveedores (SCD Tipo 1).
* **`Dim_Store` (SCD Tipo 2):** Implementación de Slowly Changing Dimension Type 2 para mantener la fidelidad histórica de los cambios geográficos (mudanzas, cierres o cambios de condado) de las tiendas utilizando `valid_from`, `valid_to` e `is_current`.

## 🧠 Desafíos Técnicos Resueltos
1. **Manejo Dinámico de Esquemas:** Implementación de configuraciones programáticas en Python (mediante diccionarios e iteradores) para la inicialización parametrizada, escalable y eficiente de catálogos y esquemas.
2. **Formateo de Datos Internacionales:** Resolución de conflictos entre separadores de miles y decimales europeos/americanos directamente en la lectura de PySpark.
3. **Carga Histórica SCD2:** Diseño de un `MERGE` de dos fases (Update + Insert mediante `UNION ALL`) en Delta Lake para historizar dimensiones sin duplicar ventas en la tabla de hechos.

## 🚀 Estructura del Repositorio
* `00_Setup.ipynb`: Inicialización de la infraestructura en Unity Catalog y generación de la Dimensión de Tiempo.
* `01_Loading_Bronze.ipynb`: Ingesta segura de archivos crudos y auditoría de calidad de datos.
* `02_Loading_Silver.ipynb`: Estandarización, parseo de tipos de datos y manejo de nulos (Unknown Members).
* `03_Loading_Dimensions.ipynb`: Carga y actualización de dimensiones utilizando SCD Tipo 1 y Tipo 2.
* `04_Loading_Fact.ipynb`: Cruce eficiente con llaves subrogadas y carga de la tabla de hechos.
* `05_Consultas.ipynb`: Script SQL con las consultas de valor de negocio (YoY Growth, Market Share, Price Elasticity).
