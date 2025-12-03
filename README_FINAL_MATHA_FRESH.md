
# Matha Fresh End-to-End Snowflake Data Engineering Project  
A Complete Medallion-Architecture Data Warehouse Built End-to-End

---

## 1. Introduction

This project is a complete Snowflake-based data warehouse built entirely from raw CSV files exported from the Matha Fresh operational system. The goal of the project was to design and implement a full medallion-style architecture that transforms fragmented, unclean operational data into a structured, analytics‑ready dimensional model.

Everything in this warehouse—from ingestion, staging, and cleansing, to modelling, fact aggregation, and analytical views—was implemented manually using Snowflake SQL, stored procedures, internal stages, and controlled transformation logic. This README describes what was built, why it was built in this manner, and how each layer contributes to the final analytical model.

---

## 2. High-Level Architecture

The architecture follows the medallion concept with three layers: Bronze, Silver, and Gold. Each layer serves a specific purpose and progressively improves the quality and usability of the data.

### Architecture Diagram  
(Insert your architecture image here)  
`![Architecture Diagram](INSERT_ARCHITECTURE_IMAGE_HERE)`

The flow begins with raw CSV ingestion into Bronze, continues with structured cleaning in Silver, and ends with star-schema modelling in Gold. Each step is isolated to ensure transparency, reproducibility, and maintainability.

---

## 3. Data Flow Diagram

The data flow diagram illustrates how data progresses from raw source files to the curated analytics layer.

### Data Flow Diagram  
(Insert your data flow image here)  
`![Data Flow Diagram](INSERT_DATA_FLOW_IMAGE_HERE)`

This section should show the exact flow: Internal Stage → Bronze → Silver → Gold → Views.

---

## 4. Star Schema Diagram

The final model is a classic dimensional star schema optimized for querying, reporting, dashboards, and analytical workloads.

### Star Schema Diagram  
(Insert your star schema image here)  
`![Star Schema](INSERT_STAR_SCHEMA_IMAGE_HERE)`

This diagram represents how the fact tables connect with their supporting dimensions.

---

## 5. Data Sources

The warehouse is built entirely from Matha Fresh operational CSV exports. These include:

Orders  
Orderlines  
Products  
Product Options  
Product Stocks  
Users  
Addresses  
Districts  
States  
Countries  
Delivery Locations  
Delivery Slots  

These files contained many inconsistencies such as invalid dates, HTML fragments in product descriptions, missing values, optional fields, and mismatched keys. Instead of cleaning them immediately, the project first loads everything into Bronze to preserve the original form for traceability.

---

## 6. Bronze Layer — Raw Data Ingestion

The Bronze layer holds exact copies of the source CSV files. No transformations or clean-up occur at this stage. The purpose is to create a stable, auditable foundation from which all later cleaning can be performed reliably.

In this stage, I:

Loaded all operational CSV files into Snowflake internal stages  
Created a consistent CSV file format  
Used COPY INTO with ON_ERROR logic to inspect loading issues  
Maintained raw data values including inconsistent or malformed fields  
Verified row counts against source data  

The Bronze layer serves as the “single source of raw truth.”

---

## 7. Silver Layer — Cleaning, Standardisation, and Integration

The Silver layer performs the heavy data transformation. All raw fields are converted, validated, cleaned, and aligned into refined staging tables that support the Gold modelling layer.

In this stage, I:

Cleaned timestamps and converted them into proper Snowflake TIMESTAMP_NTZ  
Standardised numeric fields and removed unwanted characters  
Cleaned free‑text product descriptions containing HTML or malformed text  
Normalised casing and spacing  
Handled NULL, empty, and inconsistent values  
Integrated location attributes by joining addresses, districts, states, and countries  
Ensured referential integrity by validating foreign keys  
Standardised product, customer, and delivery slot mappings  

Each Silver table is a cleaned, standardised, and business‑consistent version of the corresponding Bronze table.

---

## 8. Gold Layer — Dimensional Modelling

The Gold layer represents the analytical business model. It follows dimensional modelling best practices with a clear separation between fact and dimension tables.

### Dimensions Built  
DIM_DATE – a full calendar dimension generated within Snowflake  
DIM_CUSTOMER – enriched customer profiles with location metadata  
DIM_PRODUCT – structured product details with cleaned metadata  
DIM_LOCATION – unified geographic information built from Silver address/district/state/country tables  
DIM_DELIVERY_SLOT – delivery slot attributes  

### Fact Tables  
FACT_ORDERS – order‑level metrics including dates, monetary amounts, delivery attributes, and customer identifiers  
FACT_ORDERLINES – product‑level metrics for each order item, supporting analysis of quantities, revenue, product mix, and customer purchasing behaviour  

The Gold layer establishes the single analytical source of truth for Matha Fresh.

---

## 9. Lineage and Table Dependencies

The lineage of the project is fully structured:

STG_ORDERS → FACT_ORDERS  
STG_ORDERLINES + dimensions → FACT_ORDERLINES  
STG_USERS + STG_ADDRESS → DIM_CUSTOMER  
STG_PRODUCT → DIM_PRODUCT  
STG_ADDRESS + geography → DIM_LOCATION  
STG_DELIVERY_SLOT → DIM_DELIVERY_SLOT  

This ensures a transparent dependency chain for maintenance and quality checks.

---

## 10. Views and Analytical Outputs

The final outputs of the warehouse are analytical views designed for business insights. These include:

Customer purchase behaviour  
Order performance trends  
Product metrics  
Delivery slot efficiency  
Customer lifetime value (if included)  

These views aggregate facts with dimensions to simplify BI‑level consumption.

---

## 11. Screenshots and Visual Proof

Below are placeholders where you can add Snowflake screenshots of each layer:

### Bronze Layer Screenshot  
`![Bronze Screenshot](INSERT_BRONZE_SCREENSHOT_HERE)`

### Silver Layer Screenshot  
`![Silver Screenshot](INSERT_SILVER_SCREENSHOT_HERE)`

### Gold Layer Screenshot  
`![Gold Screenshot](INSERT_GOLD_SCREENSHOT_HERE)`

---

## 12. Final Summary

This project demonstrates the full lifecycle of data engineering work performed in Snowflake. Starting with raw CSV extracts, the pipeline organises data into a refined medallion architecture, performs structured cleaning and validation, and ends with a dimensional model designed for real analytics.

The design reflects real-world engineering practices:

Incremental refinement  
Robust SQL transformations  
Separation of ingestion, cleaning, and modelling  
Dimensional modelling based on business logic  
Traceability and auditability at every layer  

The end result is a scalable, reliable, and production‑ready data warehouse foundation for Matha Fresh.

---

### Image Placeholder Summary

INSERT_ARCHITECTURE_IMAGE_HERE  
INSERT_DATA_FLOW_IMAGE_HERE  
INSERT_STAR_SCHEMA_IMAGE_HERE  
INSERT_BRONZE_SCREENSHOT_HERE  
INSERT_SILVER_SCREENSHOT_HERE  
INSERT_GOLD_SCREENSHOT_HERE  

---

End of README  
