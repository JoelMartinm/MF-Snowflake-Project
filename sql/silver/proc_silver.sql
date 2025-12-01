/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates and Inserts Into Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters.

Usage Example:
    
CALL SP_BUILD_SILVER();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE SP_BUILD_SILVER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    ----------------------------------------------------------------
    -- STG_ORDERS
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_ORDERS;

    INSERT INTO MF_DWH.SILVER.STG_ORDERS
    SELECT
        TRY_TO_NUMBER(ORDERID)                        AS ORDER_ID,
        NULLIF(TRIM(ORDERREFERENCE), '')              AS ORDER_REFERENCE,
        NULLIF(TRIM(ORDERINVOICE), '')                AS ORDER_INVOICE,
        TRY_TO_NUMBER(ORDERTYPEID)                    AS ORDER_TYPE_ID,

        TRY_TO_TIMESTAMP_NTZ(ORDERDATE)               AS ORDER_DATE,
        TRY_TO_TIMESTAMP_NTZ(ORDERASSIGNEDDATE)       AS ASSIGNED_DATE,
        TRY_TO_TIMESTAMP_NTZ(ORDEROUTFORDELIVERYDATE) AS OUT_FOR_DELIVERY_DATE,
        TRY_TO_TIMESTAMP_NTZ(ORDERDELIVERYDATE)       AS DELIVERY_DATE,
        TRY_TO_TIMESTAMP_NTZ(ORDERUPDATEON)           AS UPDATED_ON,

        TRY_TO_NUMBER(ORDERUSERID)                    AS USER_ID,
        TRY_TO_NUMBER(ORDERADDRESSID)                 AS ADDRESS_ID,
        TRY_TO_NUMBER(ORDERADDEDBY)                   AS ADDED_BY,
        TRY_TO_NUMBER(ORDERUPDATEDBY)                 AS UPDATED_BY,
        TRY_TO_NUMBER(ORDERDELIVERYLOCATIONID)        AS DELIVERY_LOCATION_ID,
        TRY_TO_NUMBER(ORDERDELIVERYSLOTID)            AS DELIVERY_SLOT_ID,

        ABS(TRY_TO_NUMBER(ORDERAMOUNT))               AS ORDER_AMOUNT,
        TRY_TO_NUMBER(ORDERDISCOUNT)                  AS ORDER_DISCOUNT,
        TRY_TO_NUMBER(ORDERSHIPPINGCHARGE)            AS SHIPPING_CHARGE,
        ABS(TRY_TO_NUMBER(ORDERNETAMOUNT))            AS NET_AMOUNT,

        TRY_TO_NUMBER(ORDERSTATUSID)                  AS STATUS_ID,
        NULLIF(TRIM(ORDERDELIVERYBY), '')             AS DELIVERY_BY,
        TRY_TO_NUMBER(ORDERSUPPLIERID)                AS SUPPLIER_ID,
        TRY_TO_NUMBER(ORDERMODEOFPAYMENTID)           AS MODE_OF_PAYMENT_ID,
        TRY_TO_NUMBER(ORDERPOINTSGAINED)              AS POINTS_GAINED,
        NULLIF(TRIM(ORDERCLUBID), '')                 AS CLUB_ID
    FROM MF_DWH.BRONZE.ORDERS
    WHERE ORDERID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_ORDERLINES
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_ORDERLINES;

    INSERT INTO MF_DWH.SILVER.STG_ORDERLINES
    SELECT
        TRY_TO_NUMBER(ORDERLINEID)              AS ORDER_LINE_ID,
        TRY_TO_NUMBER(ORDERLINEORDERID)         AS ORDER_ID,
        TRY_TO_NUMBER(ORDERLINEPRODUCTID)       AS PRODUCT_ID,
        TRY_TO_NUMBER(ORDERLINEPRODUCTOPTIONID) AS PRODUCT_OPTION_ID,

        TRY_TO_NUMBER(ORDERLINEPRICE)           AS UNIT_PRICE,
        TRY_TO_NUMBER(ORDERLINEQTY)             AS QTY,
        TRY_TO_NUMBER(ORDERLINEAMOUNT)          AS LINE_AMOUNT,
        TRY_TO_NUMBER(ORDERLINENETAMOUNT)       AS LINE_NET_AMOUNT,

        TRY_TO_NUMBER(ORDERLINEPOINTSGAINED)    AS POINTS_GAINED,
        TRY_TO_NUMBER(ORDERLINEUPDATEDBY)       AS UPDATED_BY,
        TRY_TO_TIMESTAMP_NTZ(ORDERLINEUPDATEON) AS UPDATED_ON
    FROM MF_DWH.BRONZE.ORDERLINES
    WHERE ORDERLINEID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_PRODUCT
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_PRODUCT;

    INSERT INTO MF_DWH.SILVER.STG_PRODUCT
    SELECT
        PRODUCTID                                AS PRODUCT_ID,
        PRODUCTTYPE                              AS PRODUCT_TYPE,
        NULLIF(TRIM(PRODUCTNAME), '')            AS PRODUCT_NAME,
        NULLIF(TRIM(PRODUCTCARTDESC), '')        AS CART_DESC,
        NULLIF(TRIM(PRODUCTSHORTDESC), '')       AS SHORT_DESC,
        NULLIF(TRIM(PRODUCTLONGDESC), '')        AS LONG_DESC,
        NULLIF(TRIM(PRODUCTSEOTITLE), '')        AS SEO_TITLE,
        NULLIF(TRIM(PRODUCTSEODESCRIPTIONS), '') AS SEO_DESCRIPTION,
        NULLIF(TRIM(PRODUCTSEOKEYWORDS), '')     AS SEO_KEYWORDS,
        PRODUCTPRIORITYSORT                      AS PRIORITY_SORT,
        NULLIF(TRIM(PRODUCTUOM), '')             AS UOM,
        PRODUCTPERKGCOUNT                        AS PER_KG_COUNT,
        PRODUCTKGPERCOUNT                        AS KG_PER_COUNT
    FROM MF_DWH.BRONZE.PRODUCT
    WHERE PRODUCTID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_PRODUCTSTOCKS
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_PRODUCTSTOCKS;

    INSERT INTO MF_DWH.SILVER.STG_PRODUCTSTOCKS
    SELECT
        STOCKID                      AS STOCK_ID,
        STOCKPRODUCTID               AS PRODUCT_ID,
        NULLIF(TRIM(STOCKBATCH), '') AS BATCH,
        STOCKUNITPRICE               AS UNIT_PRICE,
        STOCKINVOICEPRICE            AS INVOICE_PRICE,
        STOCKQTY                     AS QTY,
        NULLIF(TRIM(STOCKLOCATION), '') AS LOCATION,
        STOCKSTATUS                  AS STATUS_ID,
        STOCKDEALERPRICE             AS DEALER_PRICE,
        STOCKSTOREID                 AS STORE_ID
    FROM MF_DWH.BRONZE.PRODUCTSTOCKS
    WHERE STOCKID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_PRODUCT_OPTIONS
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_PRODUCT_OPTIONS;

    INSERT INTO MF_DWH.SILVER.STG_PRODUCT_OPTIONS
    SELECT
        PRODUCTOPTIONID          AS PRODUCT_OPTION_ID,
        PRODUCTPACKAGEID         AS PACKAGE_ID,
        PRODUCTOPTIONPRODUCTID   AS PRODUCT_ID,
        PRODUCTOPTIONSID         AS OPTION_GROUP_ID,
        PRODUCTOPTIONADDEDON     AS ADDED_ON,
        PRODUCTOPTIONREMOVEDON   AS REMOVED_ON,
        PRODUCTOPTIONUPDATEDON   AS UPDATED_ON,
        PRODUCTOPTIONWASTEPERCENTAGE AS WASTE_PERCENTAGE,
        PRODUCTOPTIONPRICE       AS PRICE,
        PRODUCTOPTIONDISPLAYPRICE AS DISPLAY_PRICE,
        PRODUCTOPTIONIMAGEID     AS IMAGE_ID,
        PRODUCTOPTIONSORT        AS SORT_ORDER,
        PRODUCTOPTIONNOOFPIECE   AS NO_OF_PIECES,
        PRODUCTOPTIONSERVED      AS SERVED_QUANTITY,
        PRODUCTOPTIONSTOREID     AS STORE_ID
    FROM MF_DWH.BRONZE.PRODUCT_OPTIONS
    WHERE PRODUCTOPTIONID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_USERS
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_USERS;

    INSERT INTO MF_DWH.SILVER.STG_USERS
    SELECT
        USERSID                                      AS USER_ID,
        INITCAP(REGEXP_REPLACE(TRIM(USERSNAME), '\\s+', ' ')) AS USER_NAME,
        NULLIF(TRIM(USERSEMAIL), '')                AS EMAIL,
        NULLIF(TRIM(USERSMOBILE), '')               AS MOBILE,
        USERSSTATUS                                  AS STATUS_ID,
        USERSMOBILEVERIFIED                          AS IS_MOBILE_VERIFIED,
        USERSADDEDON                                 AS ADDED_ON,
        USERSUPDATEDON                               AS UPDATED_ON,
        USERSREMOVEDON                               AS REMOVED_ON,
        USERSLASTLOGIN                               AS LAST_LOGIN,
        USERSAUTHEXPIREON                            AS AUTH_EXPIRES_ON,
        USERSAUTHREFRESHTOKENEXPIREON                AS REFRESH_TOKEN_EXPIRES_ON,
        USERSADDRESSID                               AS ADDRESS_ID,
        USERSSTOREID                                 AS STORE_ID,
        USERSWALLETBALANCE                           AS WALLET_BALANCE,
        USERSTYPEID                                  AS USER_TYPE_ID,
        USERSDELIVERYSTOREID                         AS DELIVERY_STORE_ID,
        USERSDELIVERYLOCATIONID                      AS DELIVERY_LOCATION_ID,
        ORDERCOUNT                                   AS ORDER_COUNT
    FROM MF_DWH.BRONZE.USERS
    WHERE USERSID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_ADDRESS
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_ADDRESS;

    INSERT INTO MF_DWH.SILVER.STG_ADDRESS
    SELECT
        TRY_TO_NUMBER(ADDRESSID)                                         AS ADDRESS_ID,
        TRY_TO_NUMBER(ADDRESSUSERID)                                     AS USER_ID,
      REGEXP_REPLACE(
        INITCAP(
            TRIM(
                COALESCE(ADDRESSLINE, '') || ' ' ||
                COALESCE(ADDRESSLINETWO, '') || ' ' ||
                COALESCE(ADDRESSLINETHREE, '')
            )
        ),
        '\\s+',
        ' '
    ),

        NULLIF(TRIM(ADDRESSPIN), '')                                     AS PINCODE,
        NULLIF(TRIM(ADDRESSCITY), '')                                    AS CITY,
        TRY_TO_NUMBER(ADDRESSDISTRICTID)                                 AS DISTRICT_ID,
        TRY_TO_NUMBER(ADDRESSSTATEID)                                    AS STATE_ID,
        TRY_TO_NUMBER(ADDRESSCOUNTRYID)                                  AS COUNTRY_ID,
        TRY_TO_NUMBER(ADDRESSTYPE)                                       AS ADDRESS_TYPE,
        NULLIF(TRIM(ADDRESSPHONE), '')                                   AS CONTACT_PHONE,
        TRY_TO_NUMBER(ADDRESSLATITUDE)                                   AS LATITUDE,
        TRY_TO_NUMBER(ADDRESSLONGITUDE)                                  AS LONGITUDE,
        ADDRESSADDEDON                                                   AS ADDED_ON,
        TRY_TO_NUMBER(ADDRESSADDEDBY)                                    AS ADDED_BY,
        ADDRESSREMOVEDON                                                 AS REMOVED_ON,
        TRY_TO_NUMBER(ADDRESSREMOVEDBY)                                  AS REMOVED_BY,
        TRY_TO_NUMBER(ADDRESSDELIVERYLOCATIONID)                         AS DELIVERY_LOCATION_ID
    FROM MF_DWH.BRONZE.ADDRESS
    WHERE ADDRESSID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_COUNTRY
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_COUNTRY;

    INSERT INTO MF_DWH.SILVER.STG_COUNTRY
    SELECT
        TRY_TO_NUMBER(COUNTRYID)                 AS COUNTRY_ID,
        NULLIF(TRIM(COUNTRYNAME), '')            AS COUNTRY_NAME,
        NULLIF(TRIM(COUNTRYCURRENCY), '')        AS CURRENCY_NAME,
        NULLIF(TRIM(COUNTRYCURRENCYSHORT), '')   AS CURRENCY_SHORT,
        NULLIF(TRIM(COUNTRYCURRENCYSYMBOL), '')  AS CURRENCY_SYMBOL
    FROM MF_DWH.BRONZE.COUNTRY
    WHERE COUNTRYID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_STATES
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_STATES;

    INSERT INTO MF_DWH.SILVER.STG_STATES
    SELECT
        TRY_TO_NUMBER(STATESID)          AS STATE_ID,
        NULLIF(TRIM(STATESNAME), '')     AS STATE_NAME,
        TRY_TO_NUMBER(STATESCOUNTRYID)   AS COUNTRY_ID,
        NULLIF(TRIM(STATECODE), '')      AS STATE_CODE,
        NULLIF(TRIM(STATEGSTIN), '')     AS GSTIN
    FROM MF_DWH.BRONZE.STATES
    WHERE STATESID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_DISTRICTS
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_DISTRICTS;

    INSERT INTO MF_DWH.SILVER.STG_DISTRICTS
    SELECT
        TRY_TO_NUMBER(DISTRICTSID)        AS DISTRICT_ID,
        NULLIF(TRIM(DISTRICTSNAME), '')   AS DISTRICT_NAME,
        TRY_TO_NUMBER(DISTRICTSSTATEID)   AS STATE_ID,
        TRY_TO_NUMBER(DISTRICTSCOUNTRYID) AS COUNTRY_ID
    FROM MF_DWH.BRONZE.DISTRICTS
    WHERE DISTRICTSID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_DELIVERY_LOCATION
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_DELIVERY_LOCATION;

    INSERT INTO MF_DWH.SILVER.STG_DELIVERY_LOCATION
    SELECT
        TRY_TO_NUMBER(DELIVERYLOCATIONID)          AS DELIVERY_LOCATION_ID,
        TRY_TO_NUMBER(DELIVERYLOCATIONSUPPLIERID)  AS SUPPLIER_ID,
        NULLIF(TRIM(DELIVERYLOCATIONPINCODE), '')  AS PINCODE,
        TRY_TO_NUMBER(DELIVERYLOCATIONDISTRICTID)  AS DISTRICT_ID,
        TRY_TO_NUMBER(DELIVERYLOCATIONSTATEID)     AS STATE_ID,
        DELIVERYLOCATIONADDEDON                    AS ADDED_ON,
        DELIVERYLOCATIONREMOVEDON                  AS REMOVED_ON,
        TRY_TO_NUMBER(DELIVERYLOCATIONROUTEID)     AS ROUTE_ID,
        NULLIF(TRIM(DELIVERYLOCATIONPLACE), '')    AS PLACE,
        TRY_TO_NUMBER(DELIVERYLOCATIONSTOREID)     AS STORE_ID
    FROM MF_DWH.BRONZE.DELIVERY_LOCATION
    WHERE DELIVERYLOCATIONID IS NOT NULL;

    ----------------------------------------------------------------
    -- STG_DELIVERY_SLOT
    ----------------------------------------------------------------
    TRUNCATE TABLE MF_DWH.SILVER.STG_DELIVERY_SLOT;

    INSERT INTO MF_DWH.SILVER.STG_DELIVERY_SLOT
    SELECT
        TRY_TO_NUMBER(DELIVERYSLOTID)          AS DELIVERY_SLOT_ID,
        NULLIF(TRIM(DELIVERYSLOTDESCRIPTION), '') AS DESCRIPTION,
        DELIVERYSLOTSTARTTIME                  AS START_TIME,
        DELIVERYSLOTCUTOFFTIME                 AS CUTOFF_TIME,
        DELIVERYSLOTENDTIME                    AS END_TIME,
        DELIVERYSLOTADDEDON                    AS ADDED_ON,
        DELIVERYSLOTREMOVEDON                  AS REMOVED_ON,
        TRY_TO_NUMBER(DELIVERYSLOTSUPPLIERID)  AS SUPPLIER_ID,
        TRY_TO_NUMBER(DELIVERYSLOTSTOREID)     AS STORE_ID,
        TRY_TO_NUMBER(DELIVERYSLOTTYPEID)      AS SLOT_TYPE_ID
    FROM MF_DWH.BRONZE.DELIVERY_SLOT
    WHERE DELIVERYSLOTID IS NOT NULL;

    RETURN 'Silver STG tables truncated and reloaded successfully';

END;
$$;




