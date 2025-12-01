/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Create OR REPLACES Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters.

Usage Example:
    
CALL BUILD_SILVER();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE BUILD_SILVER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    ----------------------------------------------------------------
    -- SILVER.STG_ORDERS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ORDERS AS
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
        TRY_TO_TIMESTAMP_NTZ(ORDERCOURIERCHARGEHELDUPTO) AS COURIER_CHARGE_HELD_UPTO,
        TRY_TO_TIMESTAMP_NTZ(ORDERLATEPAYMENTDATE)    AS LATE_PAYMENT_DATE,

        TRY_TO_NUMBER(ORDERUSERID)                    AS USER_ID,
        TRY_TO_NUMBER(ORDERSTOREID)                   AS STORE_ID,
        TRY_TO_NUMBER(ORDERADDRESSID)                 AS ADDRESS_ID,
        TRY_TO_NUMBER(ORDERADDEDBY)                   AS ADDED_BY,
        TRY_TO_NUMBER(ORDERUPDATEDBY)                 AS UPDATED_BY,
        TRY_TO_NUMBER(ORDERDELIVERYLOCATIONID)        AS DELIVERY_LOCATION_ID,
        TRY_TO_NUMBER(ORDERDELIVERYSLOTID)            AS DELIVERY_SLOT_ID,

        TRY_TO_NUMBER(ORDERAMOUNT)                    AS ORDER_AMOUNT,
        TRY_TO_NUMBER(ORDERTAX)                       AS ORDER_TAX,
        TRY_TO_NUMBER(ORDERDISCOUNT)                  AS ORDER_DISCOUNT,
        TRY_TO_NUMBER(ORDERCARDAMOUNT)                AS CARD_AMOUNT,
        TRY_TO_NUMBER(ORDERCASHAMOUNT)                AS CASH_AMOUNT,
        TRY_TO_NUMBER(ORDERSHIPPINGCHARGE)            AS SHIPPING_CHARGE,
        TRY_TO_NUMBER(ORDERNETAMOUNT)                 AS NET_AMOUNT,
        TRY_TO_NUMBER(ORDERHANDLINGCHARGE)            AS HANDLING_CHARGE,

        TRY_TO_NUMBER(ORDERSTATUSID)                  AS STATUS_ID,
        NULLIF(TRIM(ORDERDELIVERYBY), '')             AS DELIVERY_BY,
        TRY_TO_NUMBER(ORDERSUPPLIERID)                AS SUPPLIER_ID,
        TRY_TO_NUMBER(ORDERSHIPPINGID)                AS SHIPPING_ID,
        TRY_TO_NUMBER(ORDERSHIPPINGISFREE)            AS SHIPPING_IS_FREE_FLAG,
        TRY_TO_NUMBER(ORDERLATEPAYMENT)               AS LATE_PAYMENT_FLAG,
        NULLIF(TRIM(ORDERCANCELACTION), '')           AS CANCEL_ACTION,

        TRY_TO_NUMBER(ORDERPOINTSGAINED)              AS POINTS_GAINED,
        TRY_TO_NUMBER(ORDERPOINTSREDEEMED)            AS POINTS_REDEEMED,
        NULLIF(TRIM(ORDERCLUBID), '')                 AS CLUB_ID,
        TRY_TO_NUMBER(ORDERSTOCKID)                   AS STOCK_ID,
        TRY_TO_NUMBER(ORDERHANDLINGCHARGEPAYOUTID)    AS HANDLING_CHARGE_PAYOUT_ID,
        TRY_TO_NUMBER(ORDERPAYMENTRECEIPTID)          AS PAYMENT_RECEIPT_ID,
        TRY_TO_NUMBER(ORDERRETURNID)                  AS RETURN_ID,
        TRY_TO_NUMBER(ORDERMODEOFPAYMENTID)           AS MODE_OF_PAYMENT_ID,
        TRY_TO_NUMBER(ORDERPAYCOUNT)                  AS PAY_COUNT
    FROM MF_DWH.BRONZE.ORDERS
    WHERE ORDERID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_ORDERLINES
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ORDERLINES AS
    SELECT
        TRY_TO_NUMBER(ORDERLINEID)               AS ORDER_LINE_ID,
        TRY_TO_NUMBER(ORDERLINEORDERID)          AS ORDER_ID,
        TRY_TO_NUMBER(ORDERLINEPRODUCTID)        AS PRODUCT_ID,
        TRY_TO_NUMBER(ORDERLINEPRODUCTSTOCKID)   AS PRODUCT_STOCK_ID,
        TRY_TO_NUMBER(ORDERLINEPRODUCTOPTIONID)  AS PRODUCT_OPTION_ID,
        TRY_TO_NUMBER(ORDERLINEFLASHSALEID)      AS FLASH_SALE_ID,

        TRY_TO_NUMBER(ORDERLINEPRICE)            AS UNIT_PRICE,
        TRY_TO_NUMBER(ORDERLINEQTY)              AS QTY,
        TRY_TO_NUMBER(ORDERLINEAMOUNT)           AS LINE_AMOUNT,
        TRY_TO_NUMBER(ORDERLINETAX)              AS LINE_TAX,
        TRY_TO_NUMBER(ORDERLINEDISCOUNT)         AS LINE_DISCOUNT,
        TRY_TO_NUMBER(ORDERLINENETAMOUNT)        AS LINE_NET_AMOUNT,

        NULLIF(TRIM(ORDERLINESTATUS), '')        AS LINE_STATUS,
        TRY_TO_NUMBER(ORDERLINEPOINTSGAINED)     AS POINTS_GAINED,
        TRY_TO_NUMBER(ORDERLINEUPDATEDBY)        AS UPDATED_BY,
        TRY_TO_TIMESTAMP_NTZ(ORDERLINEUPDATEON)  AS UPDATED_ON
    FROM MF_DWH.BRONZE.ORDERLINES
    WHERE ORDERLINEID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCT
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCT AS
    SELECT
        PRODUCTID                                  AS PRODUCT_ID,
        PRODUCTTYPE                                AS PRODUCT_TYPE,
        NULLIF(TRIM(PRODUCTNAME), '')              AS PRODUCT_NAME,
        NULLIF(TRIM(PRODUCTSKU), '')               AS PRODUCT_SKU,
        NULLIF(TRIM(PRODUCTCARTDESC), '')          AS CART_DESC,
        NULLIF(TRIM(PRODUCTSHORTDESC), '')         AS SHORT_DESC,
        NULLIF(TRIM(PRODUCTLONGDESC), '')          AS LONG_DESC,
        PRODUCTTHUMBID                             AS THUMB_ID,
        PRODUCTIMAGEID                             AS IMAGE_ID,
        PRODUCTADDEDON                             AS ADDED_ON,
        PRODUCTREMOVEDON                           AS REMOVED_ON,
        PRODUCTREJECTEDON                          AS REJECTED_ON,
        PRODUCTAPPROVEDON                          AS APPROVED_ON,
        PRODUCTUPDATEDON                           AS UPDATED_ON,
        PRODUCTADDEDBY                             AS ADDED_BY,
        PRODUCTREMOVEDBY                           AS REMOVED_BY,
        PRODUCTREJECTEDBY                          AS REJECTED_BY,
        PRODUCTAPPROVEDBY                          AS APPROVED_BY,
        PRODUCTUPDATEDBY                           AS UPDATED_BY,
        PRODUCTDENOMINATIONID                      AS DENOMINATION_ID,
        PRODUCTREPACKED                            AS IS_REPACKED,
        PRODUCTREPACKEDFROM                        AS REPACKED_FROM_PRODUCT_ID,
        PRODUCTSUPPLIERID                          AS SUPPLIER_ID,
        NULLIF(TRIM(PRODUCTSEOTITLE), '')          AS SEO_TITLE,
        NULLIF(TRIM(PRODUCTSEODESCRIPTIONS), '')   AS SEO_DESCRIPTION,
        NULLIF(TRIM(PRODUCTSEOKEYWORDS), '')       AS SEO_KEYWORDS,
        PRODUCTPURCHASEPOINTS                      AS PURCHASE_POINTS,
        PRODUCTREDEEMPOINTS                        AS REDEEM_POINTS,
        PRODUCTVIEWCOUNT                           AS VIEW_COUNT,
        PRODUCTTAXID                               AS TAX_ID,
        PRODUCTPERORDERMINQTY                      AS MIN_QTY_PER_ORDER,
        PRODUCTPERORDERMAXQTY                      AS MAX_QTY_PER_ORDER,
        NULLIF(TRIM(PRODUCTBARCODE), '')           AS BARCODE,
        PRODUCTHSNID                               AS HSN_ID,
        NULLIF(TRIM(PRODUCTHSNCODE), '')           AS HSN_CODE,
        NULLIF(TRIM(PRODUCTORIGIN), '')            AS ORIGIN,
        PRODUCTMSLQTY                              AS MSL_QTY,
        NULLIF(TRIM(PRODUCTVIDEODEMOLINK), '')     AS VIDEO_DEMO_LINK,
        PRODUCTBROCHUREMEDIAID                     AS BROCHURE_MEDIA_ID,
        PRODUCTLASTPOMEDIAID                       AS LAST_PO_MEDIA_ID,
        PRODUCTDELIVERYRETURNPERIOD                AS DELIVERY_RETURN_PERIOD_DAYS,
        PRODUCTNEAREXPIRYRETURNPERIOD              AS NEAR_EXPIRY_RETURN_PERIOD_DAYS,
        PRODUCTPRESCRIPTIONYN                      AS IS_PRESCRIPTION_REQUIRED,
        NULLIF(TRIM(PRODUCTDELIVERYRETURNEXCHANGE), '') AS RETURN_EXCHANGE_POLICY,
        PRODUCTSPECIALCATEGORYID                   AS SPECIAL_CATEGORY_ID,
        PRODUCTSPECIALITYID                        AS SPECIALITY_ID,
        PRODUCTUSAGETYPEID                         AS USAGE_TYPE_ID,
        PRODUCTPREMIUMYN                           AS IS_PREMIUM,
        PRODUCTLENGTH                              AS LENGTH_CM,
        PRODUCTBREADTH                             AS BREADTH_CM,
        PRODCUTHEIGHT                              AS HEIGHT_CM,
        PRODCUTWEIGHT                              AS WEIGHT_G,
        PRODUCTCASHBACKPERCENTAGE                  AS CASHBACK_PERCENTAGE,
        PRODUCTSOLDOUT                             AS IS_SOLD_OUT,
        PRODUCTSTOCKDISPLAY                        AS SHOW_STOCK,
        PRODUCTCLASSIFICATION                      AS CLASSIFICATION,
        PRODUCTWEIGHTPERCENTAGE                    AS WEIGHT_PERCENTAGE,
        PRODUCTPRIORITYSORT                        AS PRIORITY_SORT,
        NULLIF(TRIM(PRODUCTUOM), '')               AS UOM,
        PRODUCTCOMBO                               AS IS_COMBO,
        PRODUCTPERKGCOUNT                          AS PER_KG_COUNT,
        PRODUCTKGPERCOUNT                          AS KG_PER_COUNT
    FROM MF_DWH.BRONZE.PRODUCT
    WHERE PRODUCTID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCTSTOCKS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCTSTOCKS AS
    SELECT
        STOCKID                 AS STOCK_ID,
        STOCKPRODUCTID          AS PRODUCT_ID,
        NULLIF(TRIM(STOCKBATCH), '') AS BATCH,
        STOCKDOM                AS DATE_OF_MANUFACTURE,
        STOCKEXPIRY             AS EXPIRY_DATE,
        STOCKUNITPRICE          AS UNIT_PRICE,
        STOCKINVOICEPRICE       AS INVOICE_PRICE,
        STOCKQTY                AS QTY,
        NULLIF(TRIM(STOCKLOCATION), '') AS LOCATION,
        STOCKSTATUS             AS STATUS_ID,
        STOCKWHOLESALEPRICE     AS WHOLESALE_PRICE,
        STOCKDEALERPRICE        AS DEALER_PRICE,
        STOCKPRODUCTOPTIONID    AS PRODUCT_OPTION_ID,
        STOCKSTOREID            AS STORE_ID,
        STOCKPRICECURRENCYID    AS PRICE_CURRENCY_ID
    FROM MF_DWH.BRONZE.PRODUCTSTOCKS
    WHERE STOCKID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_PRODUCT_OPTIONS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_PRODUCT_OPTIONS AS
    SELECT
        PRODUCTOPTIONID                     AS PRODUCT_OPTION_ID,
        PRODUCTPACKAGEID                    AS PACKAGE_ID,
        PRODUCTOPTIONPRODUCTID              AS PRODUCT_ID,
        PRODUCTOPTIONSID                    AS OPTION_GROUP_ID,
        PRODUCTOPTIONADDEDON                AS ADDED_ON,
        PRODUCTOPTIONREMOVEDON              AS REMOVED_ON,
        PRODUCTOPTIONUPDATEDON              AS UPDATED_ON,
        PRODUCTOPTIONADDEDBY                AS ADDED_BY,
        PRODUCTOPTIONREMOVEDBY              AS REMOVED_BY,
        PRODUCTOPTIONUPDATEDBY              AS UPDATED_BY,
        PRODUCTOPTIONWASTEPERCENTAGE        AS WASTE_PERCENTAGE,
        PRODUCTOPTIONPRICE                  AS PRICE,
        PRODUCTOPTIONDISPLAYPRICE           AS DISPLAY_PRICE,
        PRODUCTOPTIONIMAGEID                AS IMAGE_ID,
        PRODUCTOPTIONICONID                 AS ICON_ID,
        PRODUCTOPTIONSORT                   AS SORT_ORDER,
        PRODUCTOPTIONNOOFPIECE              AS NO_OF_PIECES,
        PRODUCTOPTIONSERVED                 AS SERVED_QUANTITY,
        PRODUCTOPTIONSTOREID                AS STORE_ID
    FROM MF_DWH.BRONZE.PRODUCT_OPTIONS
    WHERE PRODUCTOPTIONID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_USERS (no passwords / tokens)
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_USERS AS
    SELECT
        USERSID                         AS USER_ID,
        NULLIF(TRIM(USERSNAME), '')     AS FIRST_NAME,
        NULLIF(TRIM(USERSLASTNAME), '') AS LAST_NAME,
        NULLIF(TRIM(USERSEMAIL), '')    AS EMAIL,
        NULLIF(TRIM(USERSMOBILE), '')   AS MOBILE,
        USERSSTATUS                     AS STATUS_ID,
        USERSEMAILVERIFIED              AS IS_EMAIL_VERIFIED,
        USERSMOBILEVERIFIED             AS IS_MOBILE_VERIFIED,
        USERSADDEDON                    AS ADDED_ON,
        USERSUPDATEDON                  AS UPDATED_ON,
        USERSREMOVEDON                  AS REMOVED_ON,
        USERSLASTLOGIN                  AS LAST_LOGIN,
        USERSAUTHEXPIREON               AS AUTH_EXPIRES_ON,
        USERSAUTHREFRESHTOKENEXPIREON   AS REFRESH_TOKEN_EXPIRES_ON,
        USERSADDRESSID                  AS ADDRESS_ID,
        TRY_TO_NUMBER(USERSLATITUDE)    AS LATITUDE,
        TRY_TO_NUMBER(USERSLONGITUDE)   AS LONGITUDE,
        USERSISADMIN                    AS IS_ADMIN,
        USERSSUPPLIERID                 AS SUPPLIER_ID,
        USERSSTOREID                    AS STORE_ID,
        USERSWALLETBALANCE              AS WALLET_BALANCE,
        USERSTYPEID                     AS USER_TYPE_ID,
        NULLIF(TRIM(USERSDESIGNATION), '') AS DESIGNATION,
        NULLIF(TRIM(USERPAYMENTMODES), '') AS PAYMENT_MODES,
        USERSNEWSLETTER                 AS IS_NEWSLETTER_SUBSCRIBED,
        NULLIF(TRIM(USERSFIREBASEID), '')  AS FIREBASE_ID,
        USERSFREEDELIVERY               AS HAS_FREE_DELIVERY,
        USERSDELIVERYSTOREID            AS DELIVERY_STORE_ID,
        USERSDELIVERYLOCATIONID         AS DELIVERY_LOCATION_ID,
        USERSDOB                        AS DATE_OF_BIRTH,
        ORDERCOUNT                      AS ORDER_COUNT
        -- PASSWORD / TOKENS intentionally excluded
    FROM MF_DWH.BRONZE.USERS
    WHERE USERSID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_ADDRESS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_ADDRESS AS
    SELECT
        TRY_TO_NUMBER(ADDRESSID)                     AS ADDRESS_ID,
        TRY_TO_NUMBER(ADDRESSUSERID)                 AS USER_ID,
        NULLIF(TRIM(ADDRESSLINE), '')                AS ADDRESS_LINE1,
        NULLIF(TRIM(ADDRESSLINETWO), '')             AS ADDRESS_LINE2,
        NULLIF(TRIM(ADDRESSLINETHREE), '')           AS ADDRESS_LINE3,
        NULLIF(TRIM(ADDRESSPIN), '')                 AS PINCODE,
        NULLIF(TRIM(ADDRESSCITY), '')                AS CITY,
        TRY_TO_NUMBER(ADDRESSDISTRICTID)             AS DISTRICT_ID,
        TRY_TO_NUMBER(ADDRESSSTATEID)                AS STATE_ID,
        TRY_TO_NUMBER(ADDRESSCOUNTRYID)              AS COUNTRY_ID,
        TRY_TO_NUMBER(ADDRESSTYPE)                   AS ADDRESS_TYPE,
        NULLIF(TRIM(ADDRESSUSERNAME), '')            AS CONTACT_NAME,
        NULLIF(TRIM(ADDRESSPHONE), '')               AS CONTACT_PHONE,
        TRY_TO_NUMBER(ADDRESSLATITUDE)               AS LATITUDE,
        TRY_TO_NUMBER(ADDRESSLONGITUDE)              AS LONGITUDE,
        ADDRESSADDEDON                               AS ADDED_ON,
        TRY_TO_NUMBER(ADDRESSADDEDBY)                AS ADDED_BY,
        ADDRESSREMOVEDON                             AS REMOVED_ON,
        TRY_TO_NUMBER(ADDRESSREMOVEDBY)              AS REMOVED_BY,
        TRY_TO_NUMBER(ADDRESSDELIVERYLOCATIONID)     AS DELIVERY_LOCATION_ID
    FROM MF_DWH.BRONZE.ADDRESS
    WHERE ADDRESSID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_COUNTRY
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_COUNTRY AS
    SELECT
        TRY_TO_NUMBER(COUNTRYID)              AS COUNTRY_ID,
        NULLIF(TRIM(COUNTRYNAME), '')         AS COUNTRY_NAME,
        NULLIF(TRIM(COUNTRYCURRENCY), '')     AS CURRENCY_NAME,
        NULLIF(TRIM(COUNTRYCURRENCYSHORT), '') AS CURRENCY_SHORT,
        NULLIF(TRIM(COUNTRYCURRENCYSYMBOL), '') AS CURRENCY_SYMBOL,
        NULLIF(TRIM(COUNTRYMOBILEPREFIX), '')  AS MOBILE_PREFIX,
        NULLIF(TRIM(COUNTRYFLAGMEDIAID), '')   AS FLAG_MEDIA_ID
    FROM MF_DWH.BRONZE.COUNTRY
    WHERE COUNTRYID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_STATES
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_STATES AS
    SELECT
        TRY_TO_NUMBER(STATESID)          AS STATE_ID,
        NULLIF(TRIM(STATESNAME), '')     AS STATE_NAME,
        TRY_TO_NUMBER(STATESCOUNTRYID)   AS COUNTRY_ID,
        NULLIF(TRIM(STATECODE), '')      AS STATE_CODE,
        NULLIF(TRIM(STATEGSTIN), '')     AS GSTIN
    FROM MF_DWH.BRONZE.STATES
    WHERE STATESID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_DISTRICTS
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_DISTRICTS AS
    SELECT
        TRY_TO_NUMBER(DISTRICTSID)             AS DISTRICT_ID,
        NULLIF(TRIM(DISTRICTSNAME), '')        AS DISTRICT_NAME,
        TRY_TO_NUMBER(DISTRICTSSTATEID)        AS STATE_ID,
        TRY_TO_NUMBER(DISTRICTSCOUNTRYID)      AS COUNTRY_ID,
        NULLIF(TRIM(DISTRICTSPREFIX), '')      AS PREFIX
    FROM MF_DWH.BRONZE.DISTRICTS
    WHERE DISTRICTSID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_DELIVERY_LOCATION
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_DELIVERY_LOCATION AS
    SELECT
        TRY_TO_NUMBER(DELIVERYLOCATIONID)               AS DELIVERY_LOCATION_ID,
        TRY_TO_NUMBER(DELIVERYLOCATIONSUPPLIERID)       AS SUPPLIER_ID,
        NULLIF(TRIM(DELIVERYLOCATIONPINCODE), '')       AS PINCODE,
        TRY_TO_NUMBER(DELIVERYLOCATIONDISTRICTID)       AS DISTRICT_ID,
        TRY_TO_NUMBER(DELIVERYLOCATIONSTATEID)          AS STATE_ID,
        DELIVERYLOCATIONADDEDON                         AS ADDED_ON,
        DELIVERYLOCATIONREMOVEDON                       AS REMOVED_ON,
        TRY_TO_NUMBER(DELIVERYLOCATIONROUTEID)          AS ROUTE_ID,
        NULLIF(TRIM(DELIVERYLOCATIONPLACE), '')         AS PLACE,
        TRY_TO_NUMBER(DELIVERYLOCATIONSTOREID)          AS STORE_ID
    FROM MF_DWH.BRONZE.DELIVERY_LOCATION
    WHERE DELIVERYLOCATIONID IS NOT NULL;

    ----------------------------------------------------------------
    -- SILVER.STG_DELIVERY_SLOT
    ----------------------------------------------------------------
    CREATE OR REPLACE TABLE MF_DWH.SILVER.STG_DELIVERY_SLOT AS
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

    RETURN 'Silver layer (STG_*) tables built and cleaned successfully';

END;
$$;
