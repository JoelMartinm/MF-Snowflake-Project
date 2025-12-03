
# Matha Fresh Snowflake Data Warehouse Project

This project presents the complete design and implementation of a Snowflake‑based Data Warehouse built from raw CSV exports originating from the operational Matha Fresh system. The purpose of the project is not only to ingest and transform data, but to demonstrate how a disciplined warehouse architecture, supported by structured modelling, cleaning logic, and multi‑layer refinement, can convert operational data into an integrated analytical environment.

The warehouse follows a medallion‑style design consisting of Bronze, Silver, and Gold layers. Each layer represents a progressive increase in structure, quality, and business value. This layered approach ensures transparency, reproducibility, and maintainability, while allowing data to evolve in controlled steps rather than through a single, opaque transformation.

---

## Data Architecture

![Architecture Placeholder](path_to_architecture_image)

The architecture defines the journey of data from raw CSV files originating from MySQL exports, through the Snowflake internal stage, into the Bronze layer, then refined within Silver, and finally modelled into an analytical Gold star schema. Each stage has a clear purpose and increases the reliability and usability of the data. This incremental refinement provides the foundation for analysis, ensures the separation of concerns, and makes the transformation logic easier to trace and debug.

---

## Data Flow Overview

![Data Flow Placeholder](path_to_dataflow_image)

The flow describes how source files are uploaded into the internal stage, loaded into Bronze, cleaned in Silver, and reshaped into Gold. The design ensures that at every step, the data becomes more structured and meaningful. Raw operational inconsistencies are handled gradually so that by the time the data reaches the Gold layer, it is reliable, well‑typed, and aligned for reporting.

---

## Bronze Layer — Raw Ingestion

In the Bronze layer, all CSV files are imported exactly as they exist in the source system. The purpose of this layer is to preserve the original structure and content without modification. This provides full traceability, allowing issues found later in the pipeline to be traced back to the exact raw inputs.

In this stage, I created raw Bronze tables matching the structure of the source files and implemented a stored procedure to load them from Snowflake’s internal stage. The data types were kept generic to ensure no values were lost during ingestion, and the tables retained all imperfections such as missing values, inconsistent casing, or malformed dates. This ensured that any downstream transformation was based on a complete, untouched representation of the original data.

---

## Silver Layer — Cleaning, Standardisation, and Alignment

![Silver Layer Placeholder](path_to_silver_image)

The Silver layer contains the most substantial transformation work and represents the first stage where the operational data becomes structurally reliable. The goal of this layer is not to create analytical structures, but to normalise and prepare the data so that it is consistent, typed correctly, and aligned across tables.

In this stage, I converted textual numeric fields into actual numeric types, standardised timestamps, removed invalid or empty values, normalised text formatting, and resolved common issues such as inconsistent product names, missing customer identifiers, or unstructured address data. I also created unified staging tables for customers, products, orders, and locations by joining related Bronze tables. Through this layer, all inconsistencies from the source files were systematically addressed, creating a trusted foundation for modelling.

---

## Gold Layer — Business Data Model

![Gold Layer Placeholder](path_to_gold_image)

The Gold layer contains the final dimensional model of the warehouse. This model follows a star schema and has been designed to support analytical workloads and reporting tools. Unlike Bronze and Silver, which are operationally focused, the Gold layer is aligned with business meaning.

In this stage, I created fact and dimension tables including dim_customer, dim_product, dim_location, dim_delivery_slot, dim_date, fact_orders, and fact_orderlines. These tables were populated using transformations built on the Silver layer. Business rules were applied to create surrogate keys, derive date attributes, unify delivery information, and ensure consistent mapping between customers, orders, and products. The fact tables store the measurable transactions, while the dimensions enrich these events with descriptive context. This schema makes analyses such as sales trends, customer behaviour, location performance, and product activity intuitive and efficient.

---

## Star Schema Representation

![Star Schema Placeholder](path_to_star_schema_image)

The star schema brings together all refined structures into a single analytical model. Fact tables connect to their respective dimensions through surrogate keys, ensuring fast joins and clear analytical pathways. This structure supports time‑based analysis, customer segmentation, product performance measurement, delivery optimisation, and other business insights relevant to Matha Fresh operations.

---

## Final Summary

This project demonstrates the complete data engineering lifecycle implemented in Snowflake. Starting from raw CSV exports, I built a multi‑layered warehouse that improves data quality step by step, integrates multiple domains, enforces referential consistency, and results in a polished dimensional model ready for analytics. The design reflects not only SQL proficiency but also architectural thinking, modelling discipline, and practical data stewardship. Each layer serves a deliberate purpose, and together they produce a transparent, scalable, and business‑oriented data warehouse.

