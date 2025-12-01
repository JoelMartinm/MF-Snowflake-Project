# Snowflake Medallion Data Warehouse – Retail Orders (MySQL ➜ Snowflake)

This project is an end-to-end **Snowflake data warehouse** built on top of a real MySQL OLTP database from a family-run retail business (online orders + inventory).

It follows a **Medallion architecture**:

- **Bronze** – raw tables loaded from MySQL (1:1 with source)
- **Silver** – cleaned, typed, and modeled staging tables
- **Core** – fact & dimension tables (star-schema style)
- **Gold** – business-friendly marts for reporting

Everything is done using **SQL in Snowflake** (no dbt required, but the design is dbt-ready).

---

## 1. Business Context

The source system is an e-commerce / retail platform:

- Customers place orders online
- Each order has one or more order lines (products, quantities, prices)
- Products have options/variants (packet sizes, etc.)
- Stock is tracked per product, store, and batch
- Delivery uses locations and time slots
- Users/customers are stored in a central `USERS` table

The goal of this warehouse is to:

- Consolidate MySQL transaction tables into a **clean analytical model**
- Support analysis of:
  - Sales by day / week / month
  - Sales by product / category / packet size
  - Sales by store / delivery location / slot
  - Customer behaviour (order counts, wallet balance, etc.)
- Provide a clear **portfolio-ready Snowflake project** showing data modeling skills.

---

## 2. Tech Stack

- **Source**: MySQL (OLTP)
- **Target DWH**: Snowflake
- **Language**: SQL (Snowflake)
- **Architecture**: Medallion (Bronze → Silver → Core → Gold)
- **Optional** (future):
  - Snowflake Tasks & Stored Procedures for automation
  - dbt for orchestrating models
  - Power BI / Tableau for dashboards

---

## 3. High-Level Architecture

> _See `docs/architecture-diagram.png` — suggested diagram:_
> - MySQL → CSV export → Snowflake Stage
> - Stage → BRONZE schema (raw)
> - BRONZE → SILVER (STG tables)
> - SILVER → Core Fact & Dimensions
> - Core → GOLD marts → BI tool

**Snowflake Objects:**

- **Warehouse**: `MF_WH`
- **Database**: `MF_DWH`
- **Schemas**:
  - `BRONZE` – raw landing tables
  - `SILVER` – staging / cleaned tables
  - (optional) `GOLD` – reporting / marts

---

## 4. Data Model Overview

### 4.1 Core Fact

**`FACT_ORDER_LINE`**

Grain: **one row per order line per product**  
Sources: `STG_ORDERLINES_CLEAN`, `STG_ORDERS_CLEAN`

Key fields (typical):

- `ORDER_LINE_ID`
- `ORDER_ID`
- `ORDER_DATE`
- `STORE_ID`
- `USER_ID`
- `PRODUCT_ID`
- `PRODUCT_OPTION_ID`
- `DELIVERY_SLOT_ID`
- `DELIVERY_LOCATION_ID`
- `QTY`, `UNIT_PRICE`, `LINE_NET_AMOUNT`, `LINE_TAX`, `LINE_DISCOUNT`

This is the main table for all sales analytics.

---

### 4.2 Dimensions (planned/implemented)

- **`DIM_PRODUCT`**  
  - From `STG_PRODUCT`, `STG_PRODUCT_OPTIONS`, `STG_PRODUCTSTOCKS`
  - Contains product name, SKU, origin, repack flags, UOM, etc.

- **`DIM_CUSTOMER`**  
  - From `STG_USERS_CLEAN` + `STG_ADDRESS`
  - Contains name, email, mobile, status, location, basic behavioural fields.

- **`DIM_LOCATION`**  
  - From `STG_ADDRESS`, `STG_DISTRICTS`, `STG_STATES`, `STG_COUNTRY`
  - Hierarchy: Address → District → State → Country

- **`DIM_DELIVERY_SLOT`**  
  - From `STG_DELIVERY_SLOT`
  - Contains slot description, start / end time, cutoff time.

- (optional) `DIM_DATE`, `DIM_STORE`, `DIM_SUPPLIER`

---

## 5. Project Structure

```text
matha-fresh-snowflake-dwh/
├─ README.md

├─ docs/
│  ├─ architecture-diagram.png        # Snowflake + Medallion + MySQL
│  ├─ erd-mysql.png                   # Original ERD of source system
│  ├─ tables-and-relationships.md     # How core tables link together
│  └─ data-dictionary.md              # Business glossary for key fields

├─ sql/
│  ├─ 00_environment_setup/
│  │  ├─ 00_create_warehouse_db_schema.sql
│  │  └─ 01_create_stage_and_file_format.sql
│  │
│  ├─ 01_bronze_load/
│  │  ├─ 01_create_bronze_tables.sql         # CREATE TABLE MF_DWH.BRONZE.*
│  │  ├─ 02_copy_into_bronze_orders.sql
│  │  ├─ 02_copy_into_bronze_orderlines.sql
│  │  ├─ 02_copy_into_bronze_product.sql
│  │  └─ 02_copy_into_bronze_other_entities.sql
│  │
│  ├─ 02_silver_staging/
│  │  ├─ stg_orders.sql
│  │  ├─ stg_orderlines.sql
│  │  ├─ stg_product.sql
│  │  ├─ stg_productstocks.sql
│  │  ├─ stg_product_options.sql
│  │  ├─ stg_users.sql
│  │  ├─ stg_address.sql
│  │  ├─ stg_country.sql
│  │  ├─ stg_states.sql
│  │  ├─ stg_districts.sql
│  │  ├─ stg_delivery_location.sql
│  │  └─ stg_delivery_slot.sql
│  │
│  ├─ 03_silver_clean/
│  │  ├─ stg_orders_clean.sql
│  │  ├─ stg_orderlines_clean.sql
│  │  ├─ stg_users_clean.sql
│  │  └─ (other cleaned staging models)
│  │
│  ├─ 04_core_models/
│  │  ├─ fact_order_line.sql
│  │  ├─ dim_product.sql
│  │  ├─ dim_customer.sql
│  │  ├─ dim_location.sql
│  │  └─ dim_delivery_slot.sql
│  │
│  ├─ 05_gold_marts/
│  │  ├─ sales_daily_product.sql
│  │  ├─ sales_by_store.sql
│  │  └─ sales_by_location.sql
│  │
│  └─ 99_utility_queries/
│     ├─ data_quality_checks.sql
│     ├─ profiling_queries.sql
│     └─ sample_analytics_queries.sql

