/********************************************************************************************
Procedure : BRONZE.LOAD_BRONZE()
Purpose   : Load the Bronze Layer tables from raw CSV files into the bronze schema.
            This procedure:
              - Truncates existing data in the bronze (raw) tables.
              - Bulk loads source data from CSV files on the Snowflake internal stage.

Layer     : Bronze (raw ingestion layer of the data warehouse)

Usage     :  CALL SP_LOAD_BRONZE();
********************************************************************************************/


CREATE OR REPLACE PROCEDURE LOAD_BRONZE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- ORDERS
    COPY INTO MF_DWH.BRONZE.ORDERS
    FROM @MF_STAGE/orders.csv
    ON_ERROR = 'CONTINUE';

    -- ORDERLINES
    COPY INTO MF_DWH.BRONZE.ORDERLINES
    FROM @MF_STAGE/orderlines.csv
    ON_ERROR = 'CONTINUE';

    -- PRODUCT
    COPY INTO MF_DWH.BRONZE.PRODUCT
    FROM @MF_STAGE/product.csv
    ON_ERROR = 'CONTINUE';

    -- PRODUCTSTOCKS
    COPY INTO MF_DWH.BRONZE.PRODUCTSTOCKS
    FROM @MF_STAGE/productstocks.csv
    ON_ERROR = 'CONTINUE';

    -- PRODUCT_OPTIONS
    COPY INTO MF_DWH.BRONZE.PRODUCT_OPTIONS
    FROM @MF_STAGE/productoptions.csv
    ON_ERROR = 'CONTINUE';

    -- USERS
    COPY INTO MF_DWH.BRONZE.USERS
    FROM @MF_STAGE/users.csv
    ON_ERROR = 'CONTINUE';

    -- ADDRESS
    COPY INTO MF_DWH.BRONZE.ADDRESS
    FROM @MF_STAGE/address.csv
    ON_ERROR = 'CONTINUE';

    -- COUNTRY
    COPY INTO MF_DWH.BRONZE.COUNTRY
    FROM @MF_STAGE/country.csv
    ON_ERROR = 'CONTINUE';

    -- STATES
    COPY INTO MF_DWH.BRONZE.STATES
    FROM @MF_STAGE/states.csv
    ON_ERROR = 'CONTINUE';

    -- DISTRICTS
    COPY INTO MF_DWH.BRONZE.DISTRICTS
    FROM @MF_STAGE/districts.csv
    ON_ERROR = 'CONTINUE';

    -- DELIVERY_LOCATION
    COPY INTO MF_DWH.BRONZE.DELIVERY_LOCATION
    FROM @MF_STAGE/deliverylocation.csv
    ON_ERROR = 'CONTINUE';

    -- DELIVERY_SLOT
    COPY INTO MF_DWH.BRONZE.DELIVERY_SLOT
    FROM @MF_STAGE/deliveryslot.csv
    ON_ERROR = 'CONTINUE';

    RETURN 'Bronze load complete';
END;
$$;

CALL LOAD_BRONZE();
