CREATE OR REPLACE PROCEDURE SP_BUILD_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    ----------------------------------------------------------------
    -- DIM_DATE
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.DIM_DATE;

    INSERT INTO MF_DWH.GOLD.DIM_DATE
    WITH date_bounds AS (
        SELECT
            DATE_TRUNC('DAY', MIN(ORDER_DATE)) AS MIN_DATE,
            DATE_TRUNC('DAY', MAX(ORDER_DATE)) AS MAX_DATE
        FROM MF_DWH.SILVER.STG_ORDERS
    ),
    seq AS (
        -- constant rowcount (10 years of days) – OK for GENERATOR
        SELECT SEQ4() AS SEQ
        FROM TABLE(GENERATOR(ROWCOUNT => 3650))
    ),
    date_series AS (
        SELECT
            DATEADD('DAY', s.SEQ, b.MIN_DATE) AS D
        FROM seq s
        CROSS JOIN date_bounds b
        WHERE DATEADD('DAY', s.SEQ, b.MIN_DATE) <= b.MAX_DATE
    )
    SELECT
        TO_NUMBER(TO_CHAR(D, 'YYYYMMDD'))             AS DATE_KEY,
        D                                             AS DATE_VALUE,
        YEAR(D)                                       AS YEAR,
        QUARTER(D)                                    AS QUARTER,
        MONTH(D)                                      AS MONTH,
        TO_CHAR(D, 'Mon')                             AS MONTH_NAME,
        DAY(D)                                        AS DAY,
        DAYOFWEEKISO(D)                               AS DAY_OF_WEEK,  -- 1=Mon, 7=Sun
        TO_CHAR(D, 'DY')                              AS DAY_NAME,
        WEEKOFYEAR(D)                                 AS WEEK_OF_YEAR,
        CASE WHEN DAYOFWEEKISO(D) IN (6, 7) THEN TRUE ELSE FALSE END AS IS_WEEKEND
    FROM date_series;


    ----------------------------------------------------------------
    -- DIM_CUSTOMER (no STORE_ID column in insert)
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
        ON u.ADDRESS_ID = a.ADDRESS_ID;


    ----------------------------------------------------------------
    -- DIM_PRODUCT (dropping KG_PER_COUNT column)
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
        PER_KG_COUNT
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
        p.PER_KG_COUNT
    FROM MF_DWH.SILVER.STG_PRODUCT p;


    ----------------------------------------------------------------
    -- DIM_LOCATION
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
            ORDER BY COALESCE(a.CITY, ''), COALESCE(a.PINCODE, '')
        )                                   AS LOCATION_ID,
        a.CITY                              AS CITY,
        a.PINCODE                           AS PINCODE,
        d.DISTRICT_ID                       AS DISTRICT_ID,
        d.DISTRICT_NAME                     AS DISTRICT_NAME,
        s.STATE_ID                          AS STATE_ID,
        s.STATE_NAME                        AS STATE_NAME,
        c.COUNTRY_ID                        AS COUNTRY_ID,
        c.COUNTRY_NAME                      AS COUNTRY_NAME
    FROM (
        SELECT DISTINCT
            CITY,
            PINCODE,
            DISTRICT_ID,
            STATE_ID,
            COUNTRY_ID
        FROM MF_DWH.SILVER.STG_ADDRESS
    ) a
    LEFT JOIN MF_DWH.SILVER.STG_DISTRICTS d
        ON a.DISTRICT_ID = d.DISTRICT_ID
    LEFT JOIN MF_DWH.SILVER.STG_STATES s
        ON a.STATE_ID = s.STATE_ID
    LEFT JOIN MF_DWH.SILVER.STG_COUNTRY c
        ON a.COUNTRY_ID = c.COUNTRY_ID;


    ----------------------------------------------------------------
    -- DIM_DELIVERY_SLOT
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
    FROM MF_DWH.SILVER.STG_DELIVERY_SLOT;


    ----------------------------------------------------------------
    -- FACT_ORDERS (no STORE_ID; enforce non-null ORDER_DATE & USER_ID)
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
        o.ORDER_ID,

        TO_NUMBER(TO_CHAR(o.ORDER_DATE, 'YYYYMMDD'))            AS ORDER_DATE_KEY,

        CASE WHEN o.ASSIGNED_DATE IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(o.ASSIGNED_DATE, 'YYYYMMDD'))
        END                                                     AS ASSIGNED_DATE_KEY,

        CASE WHEN o.OUT_FOR_DELIVERY_DATE IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(o.OUT_FOR_DELIVERY_DATE, 'YYYYMMDD'))
        END                                                     AS OUT_FOR_DELIV_DATE_KEY,

        CASE WHEN o.DELIVERY_DATE IS NOT NULL
             THEN TO_NUMBER(TO_CHAR(o.DELIVERY_DATE, 'YYYYMMDD'))
        END                                                     AS DELIVERY_DATE_KEY,

        o.USER_ID         AS CUSTOMER_ID,
        o.ADDRESS_ID,
        dl.LOCATION_ID    AS LOCATION_ID,
        o.DELIVERY_LOCATION_ID,
        o.DELIVERY_SLOT_ID,
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
    LEFT JOIN MF_DWH.GOLD.DIM_LOCATION dl
        ON dl.CITY        = a.CITY
       AND dl.PINCODE     = a.PINCODE
       AND dl.DISTRICT_ID = a.DISTRICT_ID
       AND dl.STATE_ID    = a.STATE_ID
       AND dl.COUNTRY_ID  = a.COUNTRY_ID
    WHERE o.ORDER_DATE IS NOT NULL
      AND o.USER_ID IS NOT NULL;


    ----------------------------------------------------------------
    -- FACT_ORDERLINES (no STORE_ID, no DELIVERY_SLOT_ID; enforce non-null date & customer)
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.GOLD.FACT_ORDERLINES;

    INSERT INTO MF_DWH.GOLD.FACT_ORDERLINES (
        ORDER_LINE_ID,
        ORDER_ID,
        ORDER_DATE_KEY,
        CUSTOMER_ID,
        PRODUCT_ID,
        PRODUCT_OPTION_ID,
        UNIT_PRICE,
        QTY,
        LINE_AMOUNT,
        LINE_NET_AMOUNT,
        POINTS_GAINED
    )
    SELECT
        l.ORDER_LINE_ID,
        l.ORDER_ID,

        TO_NUMBER(TO_CHAR(o.ORDER_DATE, 'YYYYMMDD'))  AS ORDER_DATE_KEY,

        o.USER_ID                                     AS CUSTOMER_ID,
        l.PRODUCT_ID,
        l.PRODUCT_OPTION_ID,

        l.UNIT_PRICE,
        l.QTY,
        l.LINE_AMOUNT,
        l.LINE_NET_AMOUNT,
        l.POINTS_GAINED
    FROM MF_DWH.SILVER.STG_ORDERLINES l
LEFT JOIN MF_DWH.SILVER.STG_ORDERS o
    ON o.ORDER_ID = l.ORDER_ID
LEFT JOIN MF_DWH.SILVER.STG_USERS u
    ON u.USER_ID = o.USER_ID
WHERE l.ORDER_LINE_ID IS NOT NULL;
    RETURN 'Gold layer built successfully';

END;
$$;
