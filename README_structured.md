
# Matha Fresh Snowflake Data Warehouse Project

## Overview
This project delivers a complete end-to-end data warehouse implementation for Matha Fresh using Snowflake. The goal is to take raw operational data exported from the MySQL system and transform it into a clean, analytics-ready dimensional model. The work includes ingestion, staging, transformation, modeling, and final consumption layers, all implemented using SQL within Snowflake.

The architecture follows the medallion pattern: Bronze, Silver, and Gold layers. Each layer has a defined purpose and contributes to the reliability, traceability, and analytical usability of the final dataset.

---

## 1. Architecture Diagram
Insert Architecture Diagram Here  
`![Architecture Diagram](path_to_image)`

The architecture consists of:
- MySQL operational exports  
- Snowflake internal stage (MF_STAGE)  
- Bronze Layer for raw ingestion  
- Silver Layer for cleaned, typed, and standardized data  
- Gold Layer containing facts and dimensions  
- Analytical views for consumption  

---

## 2. Source System and Ingestion Workflow

### MySQL Source Exports
The project begins with raw CSV exports representing the operational data from Matha Fresh:
- Users  
- Addresses  
- Products  
- Product Options  
- Product Stocks  
- Orders  
- Orderlines  
- Delivery Locations  
- Delivery Slots  
- Region master data (Country, States, Districts)

These files are uploaded into a Snowflake internal stage (MF_STAGE).  
Insert Screenshot of MF_STAGE file listing here  
`![MF_STAGE Files](path_to_image)`

---

## 3. Bronze Layer (Raw Ingestion)

The Bronze layer is designed to store the data exactly as it arrives from the source. No transformations occur at this stage. A custom stored procedure performs:
- Truncation of Bronze tables  
- COPY INTO operations from MF_STAGE  
- Error handling rules for malformed rows  

This ensures reproducible and transparent ingestion.

Insert Screenshot of Bronze Layer Table Listing  
`![Bronze Layer Tables](path_to_image)`

---

## 4. Silver Layer (Cleaned and Standardized)

The Silver layer converts the raw Bronze data into structured, usable staging tables. This includes:
- Standardizing column naming  
- Converting field types such as dates, numbers, and booleans  
- Handling nulls and invalid values  
- Ensuring referential consistency across staging entities  

A stored procedure rebuilds all Silver tables consistently so the logic remains centralized and repeatable.

Insert Screenshot of Silver Layer Tables  
`![Silver Layer Tables](path_to_image)`

---

## 5. Gold Layer (Dimensional Model)

The Gold layer contains the finalized analytical model implemented as a star schema. This layer is composed of fact and dimension tables assembled through joins and transformations across Silver staging tables.

### Dimensions
- DIM_CUSTOMER  
- DIM_PRODUCT  
- DIM_LOCATION  
- DIM_DELIVERY_SLOT  
- DIM_DATE  

### Facts
- FACT_ORDERS  
- FACT_ORDERLINES  

Insert Gold Schema Screenshot  
`![Gold Schema](path_to_image)`

---

## 6. Star Schema Representation
A simplified star schema diagram illustrates the relationship between facts and dimensions.

Insert Star Schema Diagram Here  
`![Star Schema Diagram](path_to_image)`

---

## 7. Analytical Views

A set of business-friendly analytical views are created on top of the Gold layer. These include:
- Customer Lifetime Value  
- Product Performance  
- Delivery Slot Performance  
- Customer Purchase Patterns  

These views simplify analytical consumption while hiding the underlying schema complexity.

Insert Screenshot of Analytical View Results  
`![Analytical Views](path_to_image)`

---

## 8. Stored Procedures and Orchestration

The project uses SQL stored procedures to automate:
- Bronze ingestion  
- Silver preparation  
- Gold model build  

These procedures ensure the entire warehouse can be rebuilt automatically without manual intervention.

Insert Screenshot of Procedure Execution / Query History  
`![Procedure Execution](path_to_image)`

---

## 9. Summary

This project demonstrates the complete development lifecycle of a modern data warehouse in Snowflake, including ingestion, staging, transformation, modeling, and analytics. It represents a realistic, production-like implementation aligned with standard data engineering patterns.

