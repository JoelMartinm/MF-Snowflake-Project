
# Matha Fresh Snowflake Data Warehouse Project

This project implements a complete end-to-end data warehouse for Matha Fresh using Snowflake as the analytical data platform. The work covers the full lifecycle of data engineering beginning from raw operational data extraction, movement into Snowflake, structured staging, transformation, dimensional modeling, and consumption through analytical views. The repository demonstrates how the entire pipeline is designed, built, and orchestrated with SQL-based procedures inside Snowflake.

The source data originates from the Matha Fresh operational MySQL system. Data is exported as CSV files representing core business domains such as customers, products, orders, delivery information, and regional master data. These CSV files are uploaded to a Snowflake internal stage named MF_STAGE, which acts as the single landing zone for ingestion. The staged data is then moved into the Bronze layer.

The Bronze layer stores the raw ingested data exactly as delivered from the source system. It does not apply any transformations. This layer exists to preserve lineage, debugging capability, and to ensure that any future transformations can be rebuilt from source without modifying the original input. Loading is handled through a stored procedure that truncates tables and reloads them using COPY INTO operations from MF_STAGE. Error-handling options were added to ensure ingestion continues even when specific rows contain malformed values.

The Silver layer is the cleaned and standardized version of the data. It corrects types, removes invalid entries, trims unwanted characters, resolves date fields into proper formats, and applies consistent naming conventions. Every Bronze table has a corresponding Silver table. A dedicated stored procedure rebuilds all Silver tables so that the transformations are always reproducible. The Silver layer functions as the foundation upon which the analytical model is built.

The Gold layer contains the final dimensional model. This includes dimensions such as customers, products, dates, delivery slots, and locations, along with fact tables for orders and order lines. These tables follow a star schema design where facts reference dimensions through surrogate keys. Transformations in this stage unify data from multiple Silver tables, support historical tracking, and prepare data for direct analytical use. The model was constructed to align with real business metrics such as order amounts, delivery performance, customer activity, and product sales.

To support user-friendly consumption, several views were created on top of the Gold layer. These views are designed to expose meaningful analytical metrics without requiring analysts to understand the underlying schema. Examples include product performance, customer lifetime metrics, and delivery slot summaries. These views draw directly from the dimensional model but present the data in simplified terms.

This repository also includes multiple diagrams that illustrate the system. A full architecture diagram shows how data flows from the MySQL system into the Snowflake environment through the Bronze, Silver, and Gold layers. A star schema diagram presents the relationships among the fact and dimension tables. Additional diagrams explain how source domains such as users, delivery, orders, and products map into the transformed warehouse layers.

Screenshots can be added in the spaces below to visually document the system.

Insert Architecture Diagram Here

Insert Screenshot of MF_STAGE File Listing Here

Insert Screenshot of Bronze Layer Tables in Snowflake Here

Insert Screenshot of Silver Layer Tables in Snowflake Here

Insert Screenshot of Gold Layer Schema Here

Insert Star Schema Diagram Here

Insert Screenshot of Fact and Dimension Table Data Here

Insert Example of Final Analytical View Output Here

Insert Procedure Execution or Query History Here

