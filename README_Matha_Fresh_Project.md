# Matha Fresh Snowflake Data Warehouse Project

## Overview

This project demonstrates the complete design, development, and implementation of a modern, end‑to‑end **Snowflake Data Warehouse** built from raw CSV exports extracted from the Matha Fresh operational system. The objective of this work was to rebuild the company’s data foundation from the ground up by introducing a medallion‑style architecture (Bronze → Silver → Gold), applying structured transformation logic, and producing an analytics‑ready dimensional model suitable for BI, reporting, and data‑driven decision making.

Unlike a simple ingestion project, this warehouse showcases full lifecycle data engineering: ingestion, validation, transformation, modelling, orchestration, and quality enforcement. Every step is designed to reflect real‑world enterprise practices, while keeping the flow understandable, traceable, and reproducible.

---

## Architecture Overview

Below is a high‑level description of the layers implemented in Snowflake.

### Bronze Layer – Raw Staging  
The Bronze layer mirrors the original CSV files exactly as they were extracted from Matha Fresh’s operational system. No cleaning or corrections are applied at this stage. The purpose of Bronze is to preserve source‑of‑truth data for auditing and reproducibility.

**Placeholder for Image (Bronze Architecture Screenshot)**  
`![Bronze Layer Screenshot](PATH_TO_IMAGE_1_HERE)`

---

## Silver Layer – Standardisation, Cleaning, Integration

The Silver layer focuses on reshaping the raw data into clean, structured, and consistently formatted tables that can support downstream business modelling. Every table in Silver is a refined version of its Bronze counterpart.

**Placeholder for Image (Silver Transformation Flow Screenshot)**  
`![Silver Layer Screenshot](PATH_TO_IMAGE_2_HERE)`

This layer includes key transformation logic such as:

– Fixing corrupted date values and converting strings to proper DATE types  
– Standardising numeric fields and correcting invalid price/weight fields  
– Cleaning text fields, trimming unwanted characters, normalising case  
– Joining metadata and lookup fields  
– Ensuring all primary and foreign keys follow consistent formats  
– Rebuilding clean product, customer, order, and location structures

By the end of Silver, the dataset becomes fully reliable for modelling and business consumption.

---

## Gold Layer – Dimensional Business Model

The Gold layer represents the final analytical model of the warehouse. It is built using Kimball dimensional modelling principles, resulting in a structured star schema that supports reporting, profitability insights, and operational decision‑making.

**Placeholder for Image (Gold Schema Diagram Screenshot)**  
`![Gold Layer Screenshot](PATH_TO_IMAGE_3_HERE)`

Gold consists of:

### DIM_DATE  
A complete calendar dimension generated programmatically inside Snowflake to ensure consistency in reporting.

### DIM_CUSTOMER  
A consolidated customer table combining personal attributes, contact details, location, and metadata relevant to the Matha Fresh business processes.

### DIM_PRODUCT  
A structured product catalogue derived from the cleaned Silver product dataset. Includes SEO details, descriptions, unit measurements, and product classifications.

### DIM_LOCATION  
Built using Silver address, district, state, and country tables to provide a unified geographic mapping structure.

### DIM_DELIVERY_SLOT  
Cleaned and structured representation of delivery slots used by Matha Fresh operational workflows.

### FACT_ORDERS  
Order‑level fact table containing order amounts, discounts, delivery details, and payment metadata.

### FACT_ORDERLINES  
Line‑level dataset capturing the granularity of each item sold, linked to products, customers, dates, and locations.

---

## Data Flow Description

The following narrative describes the full lifecycle of data from ingestion to modelling.

1. CSV files were uploaded to a Snowflake internal stage and validated using `VALIDATION_MODE`, ensuring all ingestion errors were visible.  
2. Bronze tables were populated using Snowflake COPY commands with controlled error‑handling rules.  
3. Silver transformations were built using SELECT‑INSERT logic and business rules to clean, shape, and standardise columns.  
4. The Gold layer was generated using stored procedures that populate all dimensional and fact tables in proper order, ensuring referential integrity.  
5. Quality checks were performed after loading—including join‑coverage tests, row‑count validation, and null‑analysis for all crucial keys.  
6. Additional dependent views were created for exploring sales patterns, customer activity, delivery‑slot behaviour, and top‑product performance.

---

## End‑to‑End ETL Workflow

The end‑to‑end process is orchestrated using Snowflake Stored Procedures:

– `SP_CREATE_GOLD_TABLES`  
– `SP_BUILD_GOLD_LAYER`  

These procedures ensure deterministic re‑builds of the Gold Layer and maintain consistency across all dimensional and fact structures.

---

## Table Lineage (Silver → Gold)

Here is a reference of how the Silver tables feed into the Gold dimensional model:

– STG_USERS + STG_ADDRESS → DIM_CUSTOMER  
– STG_PRODUCT → DIM_PRODUCT  
– STG_ADDRESS + STG_DISTRICTS + STG_STATES + STG_COUNTRY → DIM_LOCATION  
– STG_DELIVERY_SLOT → DIM_DELIVERY_SLOT  
– STG_ORDERS + DIM_LOCATION + DIM_CUSTOMER → FACT_ORDERS  
– STG_ORDERLINES + DIM_PRODUCT + DIM_CUSTOMER + DIM_DATE → FACT_ORDERLINES  

This mapping reflects the actual logic applied in the Snowflake project.

---

## What This Project Demonstrates

This work is a complete demonstration of real data engineering capability:  
• raw ingestion from operational exports  
• robust cleaning and standardisation pipelines  
• integration across multiple domain entities  
• construction of an enterprise‑ready star schema  
• end‑to‑end orchestration using SQL Stored Procedures  
• reliable, testable, auditable transformation flows  
• analytics‑ready modelling suitable for Power BI and dashboarding  

Every decision in the project is grounded in real warehouse design standards rather than theoretical academic examples.

---

## Placeholders for Screenshots (Snowflake UI, SQL Results, ERDs)

Insert the following screenshots when you prepare the final GitHub upload:

`![Bronze Layer Table Preview](PATH_TO_IMAGE_1_HERE)`  
`![Silver Transformation Outputs](PATH_TO_IMAGE_2_HERE)`  
`![Gold Schema Diagram](PATH_TO_IMAGE_3_HERE)`  

Optionally, you can also include:  
`![Snowflake Query History Screenshot](PATH)`  
`![Stored Procedure Execution Screenshot](PATH)`  
`![Fact Table Row Counts](PATH)`  

---

## Final Summary

This project provides a complete Snowflake Data Warehouse built using real operational exports from Matha Fresh. It reconstructs the data into a clean, organised, multidimensional model designed for analytics and business insights. The structure is scalable, transparent, and maintainable, giving a solid foundation for any future BI expansion.

