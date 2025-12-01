/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

CREATE OR REPLACE PROCEDURE SP_CREATE_SILVER_TABLES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    ----------------------------------------------------------------
    -- SILVER.STG_ORDERS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ORDERS (
        ORDER_ID                  NUMBER(38,0),
        ORDER_REFERENCE           VARCHAR,
        ORDER_INVOICE             VARCHAR,
        ORDER_TYPE_ID             NUMBER(38,0),

        ORDER_DATE                TIMESTAMP_NTZ,
        ASSIGNED_DATE             TIMESTAMP_NTZ,
        OUT_FOR_DELIVERY_DATE     TIMESTAMP_NTZ,
        DELIVERY_DATE             TIMESTAMP_NTZ,
        UPDATED_ON                TIMESTAMP_NTZ,
        COURIER_CHARGE_HELD_UPTO  TIMESTAMP_NTZ,
        LATE_PAYMENT_DATE         TIMESTAMP_NTZ,

        USER_ID                   NUMBER(38,0),
        STORE_ID                  NUMBER(38,0),
        ADDRESS_ID                NUMBER(38,0),
        ADDED_BY                  NUMBER(38,0),
        UPDATED_BY                NUMBER(38,0),
        DELIVERY_LOCATION_ID      NUMBER(38,0),
        DELIVERY_SLOT_ID          NUMBER(38,0),

        ORDER_AMOUNT              NUMBER(12,2),
        ORDER_TAX                 NUMBER(12,2),
        ORDER_DISCOUNT            NUMBER(12,2),
        CARD_AMOUNT               NUMBER(12,2),
        CASH_AMOUNT               NUMBER(12,2),
        SHIPPING_CHARGE           NUMBER(12,2),
        NET_AMOUNT                NUMBER(12,2),
        HANDLING_CHARGE           NUMBER(12,2),

        STATUS_ID                 NUMBER(38,0),
        DELIVERY_BY               VARCHAR,
        SUPPLIER_ID               NUMBER(38,0),
        SHIPPING_ID               NUMBER(38,0),
        SHIPPING_IS_FREE_FLAG     NUMBER(1,0),
        LATE_PAYMENT_FLAG         NUMBER(1,0),
        CANCEL_ACTION             VARCHAR,

        POINTS_GAINED             NUMBER(12,2),
        POINTS_REDEEMED           NUMBER(12,2),
        CLUB_ID                   VARCHAR,

        STOCK_ID                  NUMBER(38,0),
        HANDLING_CHARGE_PAYOUT_ID NUMBER(38,0),
        PAYMENT_RECEIPT_ID        NUMBER(38,0),
        RETURN_ID                 NUMBER(38,0),
        MODE_OF_PAYMENT_ID        NUMBER(38,0),
        PAY_COUNT                 NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_ORDERLINES
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ORDERLINES (
        ORDER_LINE_ID      NUMBER(38,0),
        ORDER_ID           NUMBER(38,0),
        PRODUCT_ID         NUMBER(38,0),
        PRODUCT_STOCK_ID   NUMBER(38,0),
        PRODUCT_OPTION_ID  NUMBER(38,0),
        FLASH_SALE_ID      NUMBER(38,0),

        UNIT_PRICE         NUMBER(12,2),
        QTY                NUMBER(12,3),
        LINE_AMOUNT        NUMBER(12,2),
        LINE_TAX           NUMBER(12,2),
        LINE_DISCOUNT      NUMBER(12,2),
        LINE_NET_AMOUNT    NUMBER(12,2),

        LINE_STATUS        VARCHAR,
        POINTS_GAINED      NUMBER(12,2),
        UPDATED_BY         NUMBER(38,0),
        UPDATED_ON         TIMESTAMP_NTZ
    );

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCT
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCT (
        PRODUCT_ID                         NUMBER(38,0),
        PRODUCT_TYPE                       NUMBER(38,0),
        PRODUCT_NAME                       VARCHAR,
        PRODUCT_SKU                        VARCHAR,
        CART_DESC                          VARCHAR,
        SHORT_DESC                         VARCHAR,
        LONG_DESC                          VARCHAR,

        THUMB_ID                           NUMBER(38,0),
        IMAGE_ID                           NUMBER(38,0),

        ADDED_ON                           TIMESTAMP_NTZ,
        REMOVED_ON                         TIMESTAMP_NTZ,
        REJECTED_ON                        TIMESTAMP_NTZ,
        APPROVED_ON                        TIMESTAMP_NTZ,
        UPDATED_ON                         TIMESTAMP_NTZ,

        ADDED_BY                           NUMBER(38,0),
        REMOVED_BY                         NUMBER(38,0),
        REJECTED_BY                        NUMBER(38,0),
        APPROVED_BY                        NUMBER(38,0),
        UPDATED_BY                         NUMBER(38,0),

        DENOMINATION_ID                    NUMBER(38,0),
        IS_REPACKED                        BOOLEAN,
        REPACKED_FROM_PRODUCT_ID           NUMBER(38,0),
        SUPPLIER_ID                        NUMBER(38,0),

        SEO_TITLE                          VARCHAR,
        SEO_DESCRIPTION                    VARCHAR,
        SEO_KEYWORDS                       VARCHAR,

        PURCHASE_POINTS                    NUMBER(12,2),
        REDEEM_POINTS                      NUMBER(12,2),
        VIEW_COUNT                         NUMBER(38,0),

        TAX_ID                             NUMBER(38,0),
        MIN_QTY_PER_ORDER                  NUMBER(12,3),
        MAX_QTY_PER_ORDER                  NUMBER(12,3),

        BARCODE                            VARCHAR,
        HSN_ID                             NUMBER(38,0),
        HSN_CODE                           VARCHAR,
        ORIGIN                             VARCHAR,
        MSL_QTY                            NUMBER(12,3),

        VIDEO_DEMO_LINK                    VARCHAR,
        BROCHURE_MEDIA_ID                  NUMBER(38,0),
        LAST_PO_MEDIA_ID                   NUMBER(38,0),

        DELIVERY_RETURN_PERIOD_DAYS        NUMBER(38,0),
        NEAR_EXPIRY_RETURN_PERIOD_DAYS     NUMBER(38,0),
        IS_PRESCRIPTION_REQUIRED           BOOLEAN,
        RETURN_EXCHANGE_POLICY             VARCHAR,

        SPECIAL_CATEGORY_ID                NUMBER(38,0),
        SPECIALITY_ID                      NUMBER(38,0),
        USAGE_TYPE_ID                      NUMBER(38,0),
        IS_PREMIUM                         BOOLEAN,

        LENGTH_CM                          NUMBER(12,3),
        BREADTH_CM                         NUMBER(12,3),
        HEIGHT_CM                          NUMBER(12,3),
        WEIGHT_G                           NUMBER(12,3),

        CASHBACK_PERCENTAGE                NUMBER(10,2),
        IS_SOLD_OUT                        BOOLEAN,
        SHOW_STOCK                         BOOLEAN,
        CLASSIFICATION                     NUMBER(38,0),
        WEIGHT_PERCENTAGE                  NUMBER(10,2),
        PRIORITY_SORT                      NUMBER(38,0),

        UOM                                VARCHAR,
        IS_COMBO                           BOOLEAN,
        PER_KG_COUNT                       NUMBER(12,3),
        KG_PER_COUNT                       NUMBER(12,3)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCTSTOCKS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCTSTOCKS (
        STOCK_ID            NUMBER(38,0),
        PRODUCT_ID          NUMBER(38,0),
        BATCH               VARCHAR,
        DATE_OF_MANUFACTURE DATE,
        EXPIRY_DATE         DATE,

        UNIT_PRICE          NUMBER(12,2),
        INVOICE_PRICE       NUMBER(12,2),
        QTY                 NUMBER(12,3),

        LOCATION            VARCHAR,
        STATUS_ID           NUMBER(38,0),

        WHOLESALE_PRICE     NUMBER(12,2),
        DEALER_PRICE        NUMBER(12,2),

        PRODUCT_OPTION_ID   NUMBER(38,0),
        STORE_ID            NUMBER(38,0),
        PRICE_CURRENCY_ID   NUMBER(38,0)
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

        ADDED_BY            NUMBER(38,0),
        REMOVED_BY          NUMBER(38,0),
        UPDATED_BY          NUMBER(38,0),

        WASTE_PERCENTAGE    NUMBER(12,2),
        PRICE               NUMBER(12,2),
        DISPLAY_PRICE       NUMBER(12,2),

        IMAGE_ID            NUMBER(38,0),
        ICON_ID             NUMBER(38,0),
        SORT_ORDER          NUMBER(38,0),

        NO_OF_PIECES        NUMBER(38,0),
        SERVED_QUANTITY     NUMBER(38,0),

        STORE_ID            NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_USERS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_USERS (
        USER_ID                    NUMBER(38,0),
        FIRST_NAME                 VARCHAR,
        LAST_NAME                  VARCHAR,
        EMAIL                      VARCHAR,
        MOBILE                     VARCHAR,

        STATUS_ID                  NUMBER(38,0),
        IS_EMAIL_VERIFIED          BOOLEAN,
        IS_MOBILE_VERIFIED         BOOLEAN,

        ADDED_ON                   TIMESTAMP_NTZ,
        UPDATED_ON                 TIMESTAMP_NTZ,
        REMOVED_ON                 TIMESTAMP_NTZ,
        LAST_LOGIN                 TIMESTAMP_NTZ,
        AUTH_EXPIRES_ON            TIMESTAMP_NTZ,
        REFRESH_TOKEN_EXPIRES_ON   TIMESTAMP_NTZ,

        ADDRESS_ID                 NUMBER(38,0),
        LATITUDE                   NUMBER(18,8),
        LONGITUDE                  NUMBER(18,8),

        IS_ADMIN                   BOOLEAN,
        SUPPLIER_ID                NUMBER(38,0),
        STORE_ID                   NUMBER(38,0),
        WALLET_BALANCE             NUMBER(12,2),
        USER_TYPE_ID               NUMBER(38,0),

        DESIGNATION                VARCHAR,
        PAYMENT_MODES              VARCHAR,

        IS_NEWSLETTER_SUBSCRIBED   BOOLEAN,
        FIREBASE_ID                VARCHAR,
        HAS_FREE_DELIVERY          BOOLEAN,

        DELIVERY_STORE_ID          NUMBER(38,0),
        DELIVERY_LOCATION_ID       NUMBER(38,0),

        DATE_OF_BIRTH              DATE,
        ORDER_COUNT                NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_ADDRESS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ADDRESS (
        ADDRESS_ID          NUMBER(38,0),
        USER_ID             NUMBER(38,0),
        ADDRESS_LINE1       VARCHAR,
        ADDRESS_LINE2       VARCHAR,
        ADDRESS_LINE3       VARCHAR,
        PINCODE             VARCHAR,
        CITY                VARCHAR,
        DISTRICT_ID         NUMBER(38,0),
        STATE_ID            NUMBER(38,0),
        COUNTRY_ID          NUMBER(38,0),
        ADDRESS_TYPE        NUMBER(38,0),
        CONTACT_NAME        VARCHAR,
        CONTACT_PHONE       VARCHAR,
        LATITUDE            NUMBER(18,8),
        LONGITUDE           NUMBER(18,8),
        ADDED_ON            TIMESTAMP_NTZ,
        ADDED_BY            NUMBER(38,0),
        REMOVED_ON          TIMESTAMP_NTZ,
        REMOVED_BY          NUMBER(38,0),
        DELIVERY_LOCATION_ID NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- SILVER.STG_COUNTRY
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_COUNTRY (
        COUNTRY_ID     NUMBER(38,0),
        COUNTRY_NAME   VARCHAR,
        CURRENCY_NAME  VARCHAR,
        CURRENCY_SHORT VARCHAR,
        CURRENCY_SYMBOL VARCHAR,
        MOBILE_PREFIX  VARCHAR,
        FLAG_MEDIA_ID  VARCHAR
    );

    ----------------------------------------------------------------
    -- SILVER.STG_STATES
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_STATES (
        STATE_ID      NUMBER(38,0),
        STATE_NAME    VARCHAR,
        COUNTRY_ID    NUMBER(38,0),
        STATE_CODE    VARCHAR,
        GSTIN         VARCHAR
    );

    ----------------------------------------------------------------
    -- SILVER.STG_DISTRICTS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_DISTRICTS (
        DISTRICT_ID   NUMBER(38,0),
        DISTRICT_NAME VARCHAR,
        STATE_ID      NUMBER(38,0),
        COUNTRY_ID    NUMBER(38,0),
        PREFIX        VARCHAR
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
