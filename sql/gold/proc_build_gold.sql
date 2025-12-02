CREATE OR REPLACE PROCEDURE SP_BUILD_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    ----------------------------------------------------------------
    -- 1. DIM_DATE (from STG_ORDERS date range)
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.DIM_DATE;

    INSERT INTO MF_DWH.GOLD.DIM_DATE (
        DATE_KEY,
        DATE_VALUE,
        YEAR,
        QUARTER,
        MONTH,
        MONTH_NAME,
        DAY,
        DAY_OF_WEEK,
        DAY_NAME,
        WEEK_OF_YEAR,
        IS_WEEKEND
    )
    WITH bounds AS (
        SELECT
            DATE_TRUNC('DAY', MIN(ORDER_DATE)) AS MIN_DATE,
            DATE_TRUNC('DAY', MAX(ORDER_DATE)) AS MAX_DATE
        FROM MF_DWH.SILVER.STG_ORDERS
        WHERE ORDER_DATE IS NOT NULL
    )
    SELECT
        TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))      AS DATE_KEY,
        d                                      AS DATE_VALUE,
        YEAR(d)                                AS YEAR,
        QUARTER(d)                             AS QUARTER,
        MONTH(d)                               AS MONTH,
        TO_CHAR(d, 'Mon')                      AS MONTH_NAME,
        DAY(d)                                 AS DAY,
        DAYOFWEEKISO(d)                        AS DAY_OF_WEEK,  -- 1 = Monday
        TO_CHAR(d, 'DY')                       AS DAY_NAME,
        WEEKOFYEAR(d)                          AS WEEK_OF_YEAR,
        CASE WHEN DAYOFWEEKISO(d) IN (6, 7)
             THEN TRUE ELSE FALSE END          AS IS_WEEKEND
    FROM bounds,
         LATERAL (
             -- constant generator, then filter by date range
             SELECT DATEADD('DAY', SEQ4(), MIN_DATE) AS d
             FROM TABLE(GENERATOR(ROWCOUNT => 3650))  -- ~10 years
         ) g
    WHERE d BETWEEN MIN_DATE AND MAX_DATE;

    ----------------------------------------------------------------
    -- 2. DIM_CUSTOMER
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.DIM_CUSTOMER;

    INSERT INTO MF_DWH.GOLD.DIM_CUSTOMER (
        CUSTOMER_ID,
        CUSTOMER_NAME,
        EMAIL,
        MOBILE,
        STATUS_ID,
        IS_MOBILE_VERIFIED,
        PRIMARY_ADDRESS_ID,
        PRIMARY_CITY,
        PRIMARY_PINCODE,
        PRIMARY_DISTRICT_ID,
        PRIMARY_STATE_ID,
        PRIMARY_COUNTRY_ID,
        STORE_ID,
        WALLET_BALANCE,
        USER_TYPE_ID,
        DELIVERY_STORE_ID,
        DELIVERY_LOCATION_ID,
        ORDER_COUNT,
        FIRST_ADDED_ON,
        LAST_UPDATED_ON,
        LAST_LOGIN
    )
    SELECT
        u.USER_ID                        AS CUSTOMER_ID,
        u.USER_NAME                      AS CUSTOMER_NAME,
        u.EMAIL,
        u.MOBILE,
        u.STATUS_ID,
        u.IS_MOBILE_VERIFIED,
        u.ADDRESS_ID                     AS PRIMARY_ADDRESS_ID,
        a.CITY                           AS PRIMARY_CITY,
        a.PINCODE                        AS PRIMARY_PINCODE,
        a.DISTRICT_ID                    AS PRIMARY_DISTRICT_ID,
        a.STATE_ID                       AS PRIMARY_STATE_ID,
        a.COUNTRY_ID                     AS PRIMARY_COUNTRY_ID,
        1                                AS STORE_ID,             -- <- force to 1
        u.WALLET_BALANCE,
        u.USER_TYPE_ID,
        u.DELIVERY_STORE_ID,
        u.DELIVERY_LOCATION_ID,
        u.ORDER_COUNT,
        u.ADDED_ON                       AS FIRST_ADDED_ON,
        u.UPDATED_ON                     AS LAST_UPDATED_ON,
        u.LAST_LOGIN
    FROM MF_DWH.SILVER.STG_USERS u
    LEFT JOIN MF_DWH.SILVER.STG_ADDRESS a
        ON u.ADDRESS_ID = a.ADDRESS_ID
    WHERE u.USER_ID IS NOT NULL;

    ----------------------------------------------------------------
    -- 3. DIM_PRODUCT
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.DIM_PRODUCT;

    INSERT INTO MF_DWH.GOLD.DIM_PRODUCT (
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_TYPE,
        CART_DESC,
        SHORT_DESC,
        LONG_DESC,
        SEO_TITLE,
        SEO_DESCRIPTION,
        SEO_KEYWORDS,
        PRIORITY_SORT,
        UOM,
        PER_KG_COUNT,
        KG_PER_COUNT
    )
    SELECT
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.PRODUCT_TYPE,
        p.CART_DESC,
        p.SHORT_DESC,
        p.LONG_DESC,
        p.SEO_TITLE,
        p.SEO_DESCRIPTION,
        p.SEO_KEYWORDS,
        p.PRIORITY_SORT,
        p.UOM,
        p.PER_KG_COUNT,
        p.KG_PER_COUNT
    FROM MF_DWH.SILVER.STG_PRODUCT p
    WHERE p.PRODUCT_ID IS NOT NULL;

    ----------------------------------------------------------------
    -- 4. DIM_LOCATION
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.DIM_LOCATION;

    INSERT INTO MF_DWH.GOLD.DIM_LOCATION (
        LOCATION_ID,
        CITY,
        PINCODE,
        DISTRICT_ID,
        DISTRICT_NAME,
        STATE_ID,
        STATE_NAME,
        COUNTRY_ID,
        COUNTRY_NAME
    )
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY
                COALESCE(a.CITY, ''),
                COALESCE(a.PINCODE, ''),
                COALESCE(a.DISTRICT_ID, 0),
                COALESCE(a.STATE_ID, 0),
                COALESCE(a.COUNTRY_ID, 0)
        )                         AS LOCATION_ID,
        a.CITY,
        a.PINCODE,
        a.DISTRICT_ID,
        d.DISTRICT_NAME,
        a.STATE_ID,
        s.STATE_NAME,
        a.COUNTRY_ID,
        c.COUNTRY_NAME
    FROM (
        SELECT DISTINCT
            CITY,
            PINCODE,
            DISTRICT_ID,
            STATE_ID,
            COUNTRY_ID
        FROM MF_DWH.SILVER.STG_ADDRESS
        WHERE CITY IS NOT NULL
           OR PINCODE IS NOT NULL
           OR DISTRICT_ID IS NOT NULL
           OR STATE_ID IS NOT NULL
           OR COUNTRY_ID IS NOT NULL
    ) a
    LEFT JOIN MF_DWH.SILVER.STG_DISTRICTS d
        ON a.DISTRICT_ID = d.DISTRICT_ID
    LEFT JOIN MF_DWH.SILVER.STG_STATES s
        ON a.STATE_ID = s.STATE_ID
    LEFT JOIN MF_DWH.SILVER.STG_COUNTRY c
        ON a.COUNTRY_ID = c.COUNTRY_ID;

    ----------------------------------------------------------------
    -- 5. DIM_DELIVERY_SLOT
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.DIM_DELIVERY_SLOT;

    INSERT INTO MF_DWH.GOLD.DIM_DELIVERY_SLOT (
        DELIVERY_SLOT_ID,
        DESCRIPTION,
        START_TIME,
        CUTOFF_TIME,
        END_TIME,
        SUPPLIER_ID,
        STORE_ID,
        SLOT_TYPE_ID
    )
    SELECT
        DELIVERY_SLOT_ID,
        DESCRIPTION,
        START_TIME,
        CUTOFF_TIME,
        END_TIME,
        SUPPLIER_ID,
        STORE_ID,
        SLOT_TYPE_ID
    FROM MF_DWH.SILVER.STG_DELIVERY_SLOT
    WHERE DELIVERY_SLOT_ID IS NOT NULL;

    ----------------------------------------------------------------
    -- 6. FACT_ORDERS (header-level fact)
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.FACT_ORDERS;

    INSERT INTO MF_DWH.GOLD.FACT_ORDERS (
        ORDER_ID,
        ORDER_DATE_KEY,
        ASSIGNED_DATE_KEY,
        OUT_FOR_DELIV_DATE_KEY,
        DELIVERY_DATE_KEY,
        CUSTOMER_ID,
        ADDRESS_ID,
        LOCATION_ID,
        DELIVERY_LOCATION_ID,
        DELIVERY_SLOT_ID,
        STORE_ID,
        SUPPLIER_ID,
        MODE_OF_PAYMENT_ID,
        STATUS_ID,
        ORDER_REFERENCE,
        ORDER_INVOICE,
        CLUB_ID,
        DELIVERY_BY,
        ORDER_AMOUNT,
        ORDER_DISCOUNT,
        SHIPPING_CHARGE,
        NET_AMOUNT,
        POINTS_GAINED
    )
    SELECT
        o.ORDER_ID                                     AS ORDER_ID,

        CASE WHEN o.ORDER_DATE IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(o.ORDER_DATE, 'YYYYMMDD'))
        END                                           AS ORDER_DATE_KEY,
        CASE WHEN o.ASSIGNED_DATE IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(o.ASSIGNED_DATE, 'YYYYMMDD'))
        END                                           AS ASSIGNED_DATE_KEY,
        CASE WHEN o.OUT_FOR_DELIVERY_DATE IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(o.OUT_FOR_DELIVERY_DATE, 'YYYYMMDD'))
        END                                           AS OUT_FOR_DELIV_DATE_KEY,
        CASE WHEN o.DELIVERY_DATE IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(o.DELIVERY_DATE, 'YYYYMMDD'))
        END                                           AS DELIVERY_DATE_KEY,

        o.USER_ID                                     AS CUSTOMER_ID,
        o.ADDRESS_ID,
        loc.LOCATION_ID                               AS LOCATION_ID,
        o.DELIVERY_LOCATION_ID,
        o.DELIVERY_SLOT_ID,
        1                                             AS STORE_ID,         -- <- force 1
        o.SUPPLIER_ID,
        o.MODE_OF_PAYMENT_ID,
        o.STATUS_ID,
        o.ORDER_REFERENCE,
        o.ORDER_INVOICE,
        o.CLUB_ID,
        o.DELIVERY_BY,
        o.ORDER_AMOUNT,
        o.ORDER_DISCOUNT,
        o.SHIPPING_CHARGE,
        o.NET_AMOUNT,
        o.POINTS_GAINED
    FROM MF_DWH.SILVER.STG_ORDERS o
    LEFT JOIN MF_DWH.SILVER.STG_ADDRESS a
        ON o.ADDRESS_ID = a.ADDRESS_ID
    LEFT JOIN MF_DWH.GOLD.DIM_LOCATION loc
        ON COALESCE(a.CITY,        '')  = COALESCE(loc.CITY,        '')
       AND COALESCE(a.PINCODE,     '')  = COALESCE(loc.PINCODE,     '')
       AND COALESCE(a.DISTRICT_ID, 0)   = COALESCE(loc.DISTRICT_ID, 0)
       AND COALESCE(a.STATE_ID,    0)   = COALESCE(loc.STATE_ID,    0)
       AND COALESCE(a.COUNTRY_ID,  0)   = COALESCE(loc.COUNTRY_ID,  0)
    WHERE o.ORDER_ID IS NOT NULL;  -- minimal filter

    ----------------------------------------------------------------
    -- 7. FACT_ORDERLINES (line-level fact)
    --    *** RELAXED FILTERS so it actually populates ***
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.FACT_ORDERLINES;

    INSERT INTO MF_DWH.GOLD.FACT_ORDERLINES (
        ORDER_LINE_ID,
        ORDER_ID,
        PRODUCT_ID,
        PRODUCT_OPTION_ID,
        STORE_ID,
        UNIT_PRICE,
        QTY,
        LINE_AMOUNT,
        LINE_NET_AMOUNT,
        POINTS_GAINED
    )
    SELECT
        l.ORDER_LINE_ID,
        l.ORDER_ID,

        o.USER_ID                                     AS CUSTOMER_ID,
        l.PRODUCT_ID,
        l.PRODUCT_OPTION_ID,
        1                                             AS STORE_ID,         -- <- force 1

        l.UNIT_PRICE,
        l.QTY,
        l.LINE_AMOUNT,
        l.LINE_NET_AMOUNT,
        l.POINTS_GAINED
    FROM MF_DWH.SILVER.STG_ORDERLINES l
    LEFT JOIN MF_DWH.SILVER.STG_ORDERS o
        ON o.ORDER_ID = l.ORDER_ID
    WHERE l.ORDER_LINE_ID IS NOT NULL;   -- ONLY essential filter

    RETURN 'Gold layer (DIMs + FACTs) built successfully';

END;
$$;
