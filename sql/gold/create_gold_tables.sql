CREATE OR REPLACE PROCEDURE SP_CREATE_GOLD_TABLES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    ----------------------------------------------------------------
    -- Ensure GOLD schema exists
    ----------------------------------------------------------------
    CREATE SCHEMA IF NOT EXISTS MF_DWH.GOLD;

    ----------------------------------------------------------------
    -- DIM_DATE
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.GOLD.DIM_DATE (
        DATE_KEY        NUMBER(8,0),   -- yyyymmdd
        DATE_VALUE      DATE,
        YEAR            NUMBER(4,0),
        QUARTER         NUMBER(1,0),
        MONTH           NUMBER(2,0),
        MONTH_NAME      VARCHAR,
        DAY             NUMBER(2,0),
        DAY_OF_WEEK     NUMBER(1,0),   -- 1 = Monday
        DAY_NAME        VARCHAR,
        WEEK_OF_YEAR    NUMBER(2,0),
        IS_WEEKEND      BOOLEAN
    );

    ----------------------------------------------------------------
    -- DIM_CUSTOMER (from STG_USERS + STG_ADDRESS)
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.GOLD.DIM_CUSTOMER (
        CUSTOMER_ID           NUMBER(38,0),   -- USER_ID
        CUSTOMER_NAME         VARCHAR,
        EMAIL                 VARCHAR,
        MOBILE                VARCHAR,
        STATUS_ID             NUMBER(38,0),
        IS_MOBILE_VERIFIED    BOOLEAN,
        PRIMARY_ADDRESS_ID    NUMBER(38,0),
        PRIMARY_CITY          VARCHAR,
        PRIMARY_PINCODE       VARCHAR,
        PRIMARY_DISTRICT_ID   NUMBER(38,0),
        PRIMARY_STATE_ID      NUMBER(38,0),
        PRIMARY_COUNTRY_ID    NUMBER(38,0),
        STORE_ID              NUMBER(38,0),
        WALLET_BALANCE        NUMBER(12,2),
        USER_TYPE_ID          NUMBER(38,0),
        DELIVERY_STORE_ID     NUMBER(38,0),
        DELIVERY_LOCATION_ID  NUMBER(38,0),
        ORDER_COUNT           NUMBER(38,0),
        FIRST_ADDED_ON        TIMESTAMP_NTZ,
        LAST_UPDATED_ON       TIMESTAMP_NTZ,
        LAST_LOGIN            TIMESTAMP_NTZ
    );

    ----------------------------------------------------------------
    -- DIM_PRODUCT
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.GOLD.DIM_PRODUCT (
        PRODUCT_ID      NUMBER(38,0),
        PRODUCT_NAME    VARCHAR,
        PRODUCT_TYPE    NUMBER(38,0),
        CART_DESC       VARCHAR,
        SHORT_DESC      VARCHAR,
        LONG_DESC       VARCHAR,       -- (you can drop later if not needed)
        SEO_TITLE       VARCHAR,
        SEO_DESCRIPTION VARCHAR,
        SEO_KEYWORDS    VARCHAR,
        PRIORITY_SORT   NUMBER(38,0),
        UOM             VARCHAR,
        PER_KG_COUNT    NUMBER(12,3),
        KG_PER_COUNT    NUMBER(12,3)
    );

    ----------------------------------------------------------------
    -- DIM_LOCATION (from ADDRESS + DISTRICT + STATE + COUNTRY)
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.GOLD.DIM_LOCATION (
        LOCATION_ID      NUMBER(38,0),   -- surrogate key
        CITY             VARCHAR,
        PINCODE          VARCHAR,
        DISTRICT_ID      NUMBER(38,0),
        DISTRICT_NAME    VARCHAR,
        STATE_ID         NUMBER(38,0),
        STATE_NAME       VARCHAR,
        COUNTRY_ID       NUMBER(38,0),
        COUNTRY_NAME     VARCHAR
    );

    ----------------------------------------------------------------
    -- DIM_DELIVERY_SLOT
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.GOLD.DIM_DELIVERY_SLOT (
        DELIVERY_SLOT_ID NUMBER(38,0),
        DESCRIPTION      VARCHAR,
        START_TIME       TIME,
        CUTOFF_TIME      TIME,
        END_TIME         TIME,
        SUPPLIER_ID      NUMBER(38,0),
        STORE_ID         NUMBER(38,0),
        SLOT_TYPE_ID     NUMBER(38,0)
    );

    ----------------------------------------------------------------
    -- FACT_ORDERS (header-level fact)
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.GOLD.FACT_ORDERS (
        ORDER_ID              NUMBER(38,0),
        ORDER_DATE_KEY        NUMBER(8,0),
        ASSIGNED_DATE_KEY     NUMBER(8,0),
        OUT_FOR_DELIV_DATE_KEY NUMBER(8,0),
        DELIVERY_DATE_KEY     NUMBER(8,0),

        CUSTOMER_ID           NUMBER(38,0),
        ADDRESS_ID            NUMBER(38,0),
        LOCATION_ID           NUMBER(38,0),   -- FK to DIM_LOCATION (via ADDRESS)
        DELIVERY_LOCATION_ID  NUMBER(38,0),
        DELIVERY_SLOT_ID      NUMBER(38,0),
        STORE_ID              NUMBER(38,0),
        SUPPLIER_ID           NUMBER(38,0),

        MODE_OF_PAYMENT_ID    NUMBER(38,0),
        STATUS_ID             NUMBER(38,0),

        ORDER_REFERENCE       VARCHAR,
        ORDER_INVOICE         VARCHAR,
        CLUB_ID               VARCHAR,
        DELIVERY_BY           VARCHAR,

        ORDER_AMOUNT          NUMBER(12,2),
        ORDER_DISCOUNT        NUMBER(12,2),
        SHIPPING_CHARGE       NUMBER(12,2),
        NET_AMOUNT            NUMBER(12,2),
        POINTS_GAINED         NUMBER(12,2)
    );

    ----------------------------------------------------------------
    -- FACT_ORDERLINES (line-level sales fact)
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.GOLD.FACT_ORDERLINES (
        ORDER_LINE_ID     NUMBER(38,0),
        ORDER_ID          NUMBER(38,0),
        ORDER_DATE_KEY    NUMBER(8,0),
        CUSTOMER_ID       NUMBER(38,0),
        PRODUCT_ID        NUMBER(38,0),
        PRODUCT_OPTION_ID NUMBER(38,0),
        STORE_ID          NUMBER(38,0),
        DELIVERY_SLOT_ID  NUMBER(38,0),

        UNIT_PRICE        NUMBER(12,2),
        QTY               NUMBER(12,3),
        LINE_AMOUNT       NUMBER(12,2),
        LINE_NET_AMOUNT   NUMBER(12,2),
        POINTS_GAINED     NUMBER(12,2)
    );

    RETURN 'Gold layer tables created successfully';
END;
$$;
