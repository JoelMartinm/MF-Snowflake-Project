/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	Run this script to redefine the DDL structure of 'bronze' Tables
Usage Example:
    
CALL SP_CREATE_SILVER_TABLES();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE SP_CREATE_SILVER_TABLES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    ----------------------------------------------------------------
    -- SILVER.STG_ORDERS (cleaned, unused columns removed)
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ORDERS (
        ORDER_ID              NUMBER(38,0),
        ORDER_REFERENCE       VARCHAR,
        ORDER_INVOICE         VARCHAR,
        ORDER_TYPE_ID         NUMBER(38,0),

        ORDER_DATE            TIMESTAMP_NTZ,
        ASSIGNED_DATE         TIMESTAMP_NTZ,
        OUT_FOR_DELIVERY_DATE TIMESTAMP_NTZ,
        DELIVERY_DATE         TIMESTAMP_NTZ,
        UPDATED_ON            TIMESTAMP_NTZ,

        USER_ID               NUMBER(38,0),
        ADDRESS_ID            NUMBER(38,0),
        ADDED_BY              NUMBER(38,0),
        UPDATED_BY            NUMBER(38,0),
        DELIVERY_LOCATION_ID  NUMBER(38,0),
        DELIVERY_SLOT_ID      NUMBER(38,0),

        ORDER_AMOUNT          NUMBER(12,2),
        ORDER_DISCOUNT        NUMBER(12,2),
        SHIPPING_CHARGE       NUMBER(12,2),
        NET_AMOUNT            NUMBER(12,2),

        STATUS_ID             NUMBER(38,0),
        DELIVERY_BY           VARCHAR,
        SUPPLIER_ID           NUMBER(38,0),
        MODE_OF_PAYMENT_ID    NUMBER(38,0),

        POINTS_GAINED         NUMBER(12,2),
        CLUB_ID               VARCHAR
    );

    ----------------------------------------------------------------
    -- SILVER.STG_ORDERLINES
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ORDERLINES (
        ORDER_LINE_ID      NUMBER(38,0),
        ORDER_ID           NUMBER(38,0),
        PRODUCT_ID         NUMBER(38,0),
        PRODUCT_OPTION_ID  NUMBER(38,0),

        UNIT_PRICE         NUMBER(12,2),
        QTY                NUMBER(12,3),
        LINE_AMOUNT        NUMBER(12,2),
        LINE_NET_AMOUNT    NUMBER(12,2),

        POINTS_GAINED      NUMBER(12,2),
        UPDATED_BY         NUMBER(38,0),
        UPDATED_ON         TIMESTAMP_NTZ
    );

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCT
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCT (
        PRODUCT_ID         NUMBER(38,0),
        PRODUCT_TYPE       NUMBER(38,0),
        PRODUCT_NAME       VARCHAR,
        CART_DESC          VARCHAR,
        SHORT_DESC         VARCHAR,
        LONG_DESC          VARCHAR,
        SEO_TITLE          VARCHAR,
        SEO_DESCRIPTION    VARCHAR,
        SEO_KEYWORDS       VARCHAR,
        PRIORITY_SORT      NUMBER(38,0),
        UOM                VARCHAR,
        PER_KG_COUNT       NUMBER(12,3),
        KG_PER_COUNT       NUMBER(12,3)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCTSTOCKS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCTSTOCKS (
        STOCK_ID       NUMBER(38,0),
        PRODUCT_ID     NUMBER(38,0),
        BATCH          VARCHAR,
        UNIT_PRICE     NUMBER(12,2),
        INVOICE_PRICE  NUMBER(12,2),
        QTY            NUMBER(12,3),
        LOCATION       VARCHAR,
        STATUS_ID      NUMBER(38,0),
        DEALER_PRICE   NUMBER(12,2),
        STORE_ID       NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCT_OPTIONS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCT_OPTIONS (
        PRODUCT_OPTION_ID   NUMBER(38,0),
        PACKAGE_ID          NUMBER(38,0),
        PRODUCT_ID          NUMBER(38,0),
        OPTION_GROUP_ID     NUMBER(38,0),

        ADDED_ON            TIMESTAMP_NTZ,
        REMOVED_ON          TIMESTAMP_NTZ,
        UPDATED_ON          TIMESTAMP_NTZ,

        WASTE_PERCENTAGE    NUMBER(12,2),
        PRICE               NUMBER(12,2),
        DISPLAY_PRICE       NUMBER(12,2),

        IMAGE_ID            NUMBER(38,0),
        SORT_ORDER          NUMBER(38,0),

        NO_OF_PIECES        NUMBER(38,0),
        SERVED_QUANTITY     NUMBER(38,0),

        STORE_ID            NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_USERS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_USERS (
        USER_ID                  NUMBER(38,0),
        USER_NAME                VARCHAR,
        EMAIL                    VARCHAR,
        MOBILE                   VARCHAR,

        STATUS_ID                NUMBER(38,0),
        IS_MOBILE_VERIFIED       BOOLEAN,

        ADDED_ON                 TIMESTAMP_NTZ,
        UPDATED_ON               TIMESTAMP_NTZ,
        REMOVED_ON               TIMESTAMP_NTZ,
        LAST_LOGIN               TIMESTAMP_NTZ,
        AUTH_EXPIRES_ON          TIMESTAMP_NTZ,
        REFRESH_TOKEN_EXPIRES_ON TIMESTAMP_NTZ,

        ADDRESS_ID               NUMBER(38,0),

        STORE_ID                 NUMBER(38,0),
        WALLET_BALANCE           NUMBER(12,2),
        USER_TYPE_ID             NUMBER(38,0),

        DELIVERY_STORE_ID        NUMBER(38,0),
        DELIVERY_LOCATION_ID     NUMBER(38,0),

        ORDER_COUNT              NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_ADDRESS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ADDRESS (
        ADDRESS_ID           NUMBER(38,0),
        USER_ID              NUMBER(38,0),
        FULL_ADDRESS         VARCHAR,
        PINCODE              VARCHAR,
        CITY                 VARCHAR,
        DISTRICT_ID          NUMBER(38,0),
        STATE_ID             NUMBER(38,0),
        COUNTRY_ID           NUMBER(38,0),
        ADDRESS_TYPE         NUMBER(38,0),
        CONTACT_PHONE        VARCHAR,
        LATITUDE             NUMBER(18,8),
        LONGITUDE            NUMBER(18,8),
        ADDED_ON             TIMESTAMP_NTZ,
        ADDED_BY             NUMBER(38,0),
        REMOVED_ON           TIMESTAMP_NTZ,
        REMOVED_BY           NUMBER(38,0),
        DELIVERY_LOCATION_ID NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_COUNTRY
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_COUNTRY (
        COUNTRY_ID      NUMBER(38,0),
        COUNTRY_NAME    VARCHAR,
        CURRENCY_NAME   VARCHAR,
        CURRENCY_SHORT  VARCHAR,
        CURRENCY_SYMBOL VARCHAR
    );

    ----------------------------------------------------------------
    -- SILVER.STG_STATES
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_STATES (
        STATE_ID    NUMBER(38,0),
        STATE_NAME  VARCHAR,
        COUNTRY_ID  NUMBER(38,0),
        STATE_CODE  VARCHAR,
        GSTIN       VARCHAR
    );

    ----------------------------------------------------------------
    -- SILVER.STG_DISTRICTS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_DISTRICTS (
        DISTRICT_ID   NUMBER(38,0),
        DISTRICT_NAME VARCHAR,
        STATE_ID      NUMBER(38,0),
        COUNTRY_ID    NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_DELIVERY_LOCATION
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_DELIVERY_LOCATION (
        DELIVERY_LOCATION_ID  NUMBER(38,0),
        SUPPLIER_ID           NUMBER(38,0),
        PINCODE               VARCHAR,
        DISTRICT_ID           NUMBER(38,0),
        STATE_ID              NUMBER(38,0),
        ADDED_ON              TIMESTAMP_NTZ,
        REMOVED_ON            TIMESTAMP_NTZ,
        ROUTE_ID              NUMBER(38,0),
        PLACE                 VARCHAR,
        STORE_ID              NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_DELIVERY_SLOT
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_DELIVERY_SLOT (
        DELIVERY_SLOT_ID  NUMBER(38,0),
        DESCRIPTION       VARCHAR,
        START_TIME        TIME,
        CUTOFF_TIME       TIME,
        END_TIME          TIME,
        ADDED_ON          TIMESTAMP_NTZ,
        REMOVED_ON        TIMESTAMP_NTZ,
        SUPPLIER_ID       NUMBER(38,0),
        STORE_ID          NUMBER(38,0),
        SLOT_TYPE_ID      NUMBER(38,0)
    );

    RETURN 'Silver STG tables created successfully';

END;
$$;
