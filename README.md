# Medallion_Liquor_Sales_Pipeline  
## End-to-End Data Engineering Pipeline

## 📌 Visión General del Proyecto
Este proyecto implementa un Data Warehouse transaccional robusto para el análisis de ventas minoristas, distribución y cadena de suministro de bebidas alcohólicas en el estado de Iowa, USA. 

El objetivo principal es procesar un volumen masivo de registros transaccionales (nivel línea de factura) para habilitar respuestas analíticas sobre tendencias de ventas, rentabilidad (Gross Margin), comportamiento geográfico de las tiendas y rendimiento del portafolio de productos.

## 🛠️ Stack Tecnológico
* **Procesamiento Distribuido:** Apache Spark (PySpark)
* **Almacenamiento y Formato:** Delta Lake, Parquet
* **Plataforma:** Databricks / Unity Catalog
* **Lenguajes:** Python, Databricks SQL

## 🏗️ Arquitectura de Datos (Medallion Architecture)
El pipeline de datos sigue estrictamente la Arquitectura Medallion para garantizar la calidad, auditabilidad y rendimiento de los datos desde su estado crudo hasta el modelo analítico final.

### 🥉 Capa Bronze (Raw Data - As-Is)
* **Ingesta Dinámica de Parquet:** Lectura de múltiples archivos `.parquet` históricos. 
* **Manejo de Inconsistencias de Tipos (Type Mismatch):** Implementación de lectura aislada de archivos y casteo temporal de todas las columnas a `String` en memoria para evitar fallos por discrepancias de tipos de datos entre archivos de diferentes años (ej. `DOUBLE` vs `STRING`).
* **Unión Tolerante a Fallos:** Uso de `unionByName` con `allowMissingColumns=True` para unificar dinámicamente esquemas mutables antes de persistir en Delta Lake.

### 🥈 Capa Silver (Cleansed, Conformed & Incremental)
* **Auditoría y Cuarentena (DLQ):** Implementación del patrón *Dead Letter Queue*. Las reglas programáticas en PySpark auditan los datos crudos mediante `try_cast`. Los registros corruptos o incompletos se rutean automáticamente a una tabla de auditoría (`sales_quarantine`) sin detener el pipeline.
* **Carga Incremental (Upsert):** Uso de la API `DeltaTable` para ejecutar operaciones `MERGE` eficientes, actualizando facturas existentes o insertando nuevas sin reprocesar toda la historia.
* **Estandarización y Tipado:** Limpieza de strings financieros (símbolos de moneda, separadores de miles), parseo robusto de fechas mixtas (`COALESCE`) y asignación definitiva de tipos de datos.
* **Integridad Referencial:** Asignación del valor `-1` (Unknown Member) a atributos descriptivos faltantes para evitar la pérdida de métricas financieras vitales durante los cruces dimensionales.

### 🥇 Capa Gold (Dimensional Modeling - Star Schema)
El modelo final está diseñado mediante un **Esquema Estrella (Star Schema)** optimizado para consultas analíticas de alto rendimiento.

**Tabla de Hechos (Fact Table):**
* `Fact_Sales`: Almacena métricas transaccionales (volumen de botellas, litros, dólares vendidos, costo y margen).

**Tablas de Dimensiones (Dimension Tables):**
* `Dim_Date`: Dimensión de tiempo generada dinámicamente.
* `Dim_Product`: Dimensión de productos y categorías (SCD Tipo 1).
* `Dim_Vendor`: Dimensión de proveedores (SCD Tipo 1).
* **`Dim_Store` (SCD Tipo 2):** Implementación de Slowly Changing Dimension Tipo 2 mediante `UNION ALL` y `MERGE` para mantener la fidelidad histórica de los cambios geográficos (mudanzas, cierres) de las tiendas utilizando `valid_from`, `valid_to` e `is_current`.

## 🧠 Desafíos Técnicos Resueltos
1. **Manejo Dinámico de Esquemas:** Resolución de conflictos críticos de tipos de datos en archivos Parquet de origen incierto, aislando lecturas y unificando metadatos programáticamente en Python.
2. **Implementación de Cuarentena (DLQ):** Diseño de un enrutamiento de datos bidireccional en la capa Silver para aislar registros anómalos, garantizando un 100% de éxito en la ingesta Bronze y facilitando la auditoría de calidad de datos.
3. **Cargas Incrementales y SCD2:** Transición de cargas *Full Refresh* a patrones `MERGE` (Upsert) tanto para la tabla de hechos en Silver como para el versionado histórico complejo de dimensiones en Gold, optimizando los costos de cómputo.

## 🚀 Estructura del Repositorio
* `00_Setup.ipynb`: Inicialización de la infraestructura en Unity Catalog y generación de la Dimensión de Tiempo.
* `01_Loading_Bronze.ipynb`: Ingesta cruda y manejo de Schema Drift en archivos Parquet.
* `02_Loading_Silver.ipynb`: Auditoría de calidad, patrón DLQ, estandarización y Upsert incremental.
* `03_Loading_Dimensions.ipynb`: Carga y actualización de dimensiones utilizando SCD Tipo 1 y Tipo 2.
* `04_Loading_Fact.ipynb`: Cruce eficiente con llaves subrogadas y carga de la tabla de hechos.
* `05_Consultas.ipynb`: Script SQL con las consultas de valor de negocio (YoY Growth, Market Share, Price Elasticity).