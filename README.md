
# MF Snowflake Data Warehouse Project  
A Complete Medallion-Architecture Data Warehouse Built End-to-End

---

## 1. Introduction

This project presents a fully developed Snowflake data warehouse built entirely from raw CSV exports taken from Matha Fresh’s operational system. The work reflects the complete lifecycle of a data engineering solution, starting from raw ingestion and progressing all the way to curated, analytics-ready star‑schema models.  

The entire warehouse design, transformation logic, cleansing rules, stored procedures, data validation, and final modelling layers were implemented manually using Snowflake SQL with no external tools. Each step was constructed carefully so that the evolution of the data is transparent, traceable, and easy to maintain.

---

## 2. High-Level Architecture

The warehouse follows a medallion‑style architecture that organizes data into three layers: Bronze, Silver, and Gold. Each layer represents a clear stage of refinement, ensuring that raw data becomes progressively more structured and more useful as it moves through the pipeline.

### Architecture Diagram 

<img width="9040" height="4032" alt="image" src="https://github.com/user-attachments/assets/a4fa5bfe-0776-4700-87ab-a414405396e9" />



This design ensures that issues can be isolated at the correct layer, and transformations never overwrite the raw truth. It also supports future scalability and allows new datasets to plug into the pipeline easily.

---

## 3. Data Flow Diagram

The data flow represents the exact journey the Matha Fresh datasets take through the system, from raw files to dimensional tables.  

<img width="4984" height="3840" alt="image" src="https://github.com/user-attachments/assets/a569a67c-abf9-46d6-a16d-e1fb17aa9dca" />



Every stage of the pipeline is visible in this flow, making it easy to understand the dependencies and purpose of each transformation.

---

## 4. Star Schema Diagram

The Gold layer results in a clean dimensional model that supports business‑level reporting, analytics, dashboarding, and ad‑hoc exploratory work.
 

<img width="4648" height="6688" alt="image" src="https://github.com/user-attachments/assets/692259f8-3a27-4037-9d44-a42e265a2c60" />

The schema is intentionally simple and intuitive, using well‑structured fact tables linked to descriptive dimensions.

---

## 5. Bronze Layer — Raw Data Ingestion

The Bronze layer serves as an unmodified copy of the raw CSV source files. No cleaning, no type casting, and no transformations occur at this stage. Instead, Bronze preserves the original operational exports exactly as they were received.

In this layer, I created Snowflake tables that matched the structure of each raw CSV file and loaded the data directly from internal stages. The goal was to ensure traceability and to maintain a permanent historical baseline independent of any cleaning rules.

### Bronze Layer Screenshot  

<img width="1571" height="839" alt="Screenshot 2025-12-03 120915" src="https://github.com/user-attachments/assets/799d9a7f-5df6-4390-bfa8-eb8cad25c30d" />


This stage is essential because it gives later layers a reliable foundation and ensures that data inconsistencies can always be traced back to their original source.

---

## 6. Silver Layer — Cleaning, Standardisation, and Integration

The Silver layer performs the critical transformations that convert inconsistent and messy Bronze data into properly structured, usable tables. This stage is where most of the data engineering effort occurs.

In Silver, I converted raw string timestamps into proper Snowflake timestamp formats, standardised numerical values, removed HTML fragments from product descriptions, normalised casing, fixed inconsistent IDs, integrated address and geographic data, and ensured all foreign keys were valid.  

This layer also merged related datasets, such as users and addresses into unified staging structures that could be modelled reliably.

<img width="1586" height="831" alt="image" src="https://github.com/user-attachments/assets/0627d98e-2044-4f62-ad9b-7e0d08823875" />



By the time the data reaches the end of the Silver process, it is fully cleaned, structured, and ready to be shaped into the final business‑oriented star schema.

---

## 7. Gold Layer — Dimensional Modelling

The Gold layer represents the analytical core of the warehouse. Here, I designed a set of dimensional tables and fact tables based on Snowflake best practices and Kimball modelling principles.

### What I Built in the Gold Layer

DIM_DATE  
A complete date dimension generated using SQL, including year, month, day, weekday, and flags for weekend/weekday.

DIM_CUSTOMER  
A fully enriched customer profile created by joining Silver‑cleaned user data with location attributes.

DIM_PRODUCT  
A structured definition of each product with properly cleaned metadata.

DIM_LOCATION  
A consolidated geographic dimension built from address, district, state, and country datasets.

DIM_DELIVERY_SLOT  
A description of the delivery time‑slot windows used by Matha Fresh.

FACT_ORDERS  
A header level fact table that stores amounts, dates, order references, and delivery attributes.

FACT_ORDERLINES  
A line level fact table that stores quantities, unit prices, net amounts, and product identifiers.
  

<img width="1586" height="836" alt="Screenshot 2025-12-03 121723" src="https://github.com/user-attachments/assets/d397bdc5-cf25-48c5-aecf-df9cdddef3d6" />


This layer transforms the operational Matha Fresh records into a structured, business-ready analytical model suitable for BI dashboards, KPI tracking, forecasting, and deeper business insights.


---

## 9. Analytical Views

Once the Gold layer was completed, I designed several analytical views that join facts with dimensions to provide business insights. These views enable analysis such as customer buying behaviour and product performance efficiency.

<img width="1594" height="630" alt="image" src="https://github.com/user-attachments/assets/7de30dc6-73ca-455a-8d7e-67e7206d9e0d" />



---

## 10. Final Summary

This project showcases a complete end‑to‑end Snowflake data engineering workflow. Starting from raw CSV files, it builds a clean medallion‑style warehouse and then models it into a well structured dimensional schema.  

The project demonstrates practical engineering skills in ingestion, error handling, data quality management, SQL transformation logic, dimensional modelling, and the creation of reusable analytical views. Each layer was constructed carefully to ensure clarity, scalability, and long‑term maintainability.

---
