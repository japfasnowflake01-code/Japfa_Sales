-------------------------Zone 1 Start-------------------------------------------
-- create database
CREATE DATABASE I_JAPFA_DEV;

--create schema 
CREATE SCHEMA IND_UTIL;

-- Create stage file formate for the CSV file
CREATE OR REPLACE STAGE I_JAPFA_DEV.IND_UTIL.BILLING_STAGE_FF
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

//CREATING STAGE TABLE    
CREATE OR REPLACE TABLE I_JAPFA_DEV.IND_UTIL.BILLING_TAB (
    BILLING_DOCUMENT NUMBER,
    ITEM NUMBER,
    BILLING_TYPE VARCHAR,
    BILLING_DATE DATE,
    BILLING_QTY_IN_SKU NUMBER,
    MATERIAL VARCHAR,
    ITEM_DESCRIPTION VARCHAR,
    MATERIAL_GROUP VARCHAR,
    ASSIGNMENT NUMBER

);

ALTER TABLE I_JAPFA_DEV.IND_UTIL.BILLING_TAB
ADD DATE_OF_INSERTION TIMESTAMP;




//copying stage data to landing table
COPY INTO I_JAPFA_DEV.IND_UTIL.BILLING_TAB
FROM @I_JAPFA_DEV.IND_UTIL.BILLING_STAGE_FF/Sample_Sales.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ) ON_ERROR=CONTINUE;

select * from I_JAPFA_DEV.IND_UTIL.BILLING_TAB;


//CREATING STREAM ON STAGE TABLE
CREATE OR REPLACE STREAM I_JAPFA_DEV.IND_UTIL.BILLING_TAB_STREAM ON TABLE I_JAPFA_DEV.IND_UTIL.BILLING_TAB;

select * from  I_JAPFA_DEV.IND_UTIL.BILLING_TAB_STREAM;


INSERT INTO I_JAPFA_DEV.IND_UTIL.BILLING_TAB
(
    BILLING_DOCUMENT,
    ITEM,
    BILLING_TYPE,
    BILLING_DATE,
    BILLING_QTY_IN_SKU,
    MATERIAL,
    ITEM_DESCRIPTION,
    MATERIAL_GROUP,
    ASSIGNMENT,
    DATE_OF_INSERTION
)VALUES(
    60000004,
    10,
    'ZF2',
    '2024-09-23',
    3250,
    '34000000',
    'COMFEED P Oroiler Starter CRB50',
    'FPBF01',
    60000004,
    '2025-10-13 11:51:26.135 -0700'
);


-- To validate the data present in table
select * from I_JAPFA_DEV.IND_UTIL.BILLING_TAB;

-- To validate the data present in stream
select * from I_JAPFA_DEV.IND_UTIL.BILLING_TAB_STREAM


-------------------------Zone 1 End --------------------------------------------------




------------------------Zone 2 Start ---Starting of Bronze layer----------------------------------------------
//new schema for Zone 2 i.e Bronze Layer
CREATE SCHEMA IND_SAPHANA_DEV_TN;

//CREATING STAGE TABLE
CREATE OR REPLACE TABLE I_JAPFA_DEV.IND_SAPHANA_DEV_TN.BILLING_TAB (
    Z2_BILLING_DOCUMENTMBER,
    Z2_ITEM NUMBER,
    Z2_BILLING_TYPE VARCHAR,
    Z2_BILLING_DATE DATE,
    Z2_BILLING_QTY_IN_SKU NUMBER,
    Z2_MATERIAL VARCHAR,
    Z2_ITEM_DESCRIPTION VARCHAR,
    Z2_MATERIAL_GROUP VARCHAR,
    Z2_ASSIGNMENT NUMBER,
   Z2_DATE_OF_INSERT NUION TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



//CREATING TASK IN STAGE TO PROPOGATE DATA INTO BRONZE LAYER
CREATE OR REPLACE TASK I_JAPFA_DEV.IND_SAPHANA_DEV_TN.TASK_TO_Z2_NEW_BRONZE_LAYER
WAREHOUSE = COMPUTE_WH
-- SCHEDULE = 'USING CRON 0/5 0-8 * * * Asia/Kolkata'
SCHEDULE = '15 SECONDS '
WHEN SYSTEM$STREAM_HAS_DATA('I_JAPFA_DEV.IND_UTIL.BILLING_TAB_STREAM')
AS
MERGE INTO I_JAPFA_DEV.IND_SAPHANA_DEV_TN.BILLING_TAB bt
USING(SELECT * FROM I_JAPFA_DEV.IND_UTIL.BILLING_TAB_STREAM)st
ON bt.Z2_BILLING_DOCUMENT = st.BILLING_DOCUMENT
AND bt.Z2_ITEM = st.ITEM
AND bt.Z2_MATERIAL = st.MATERIAL
AND bt.Z2_BILLING_TYPE = st.BILLING_TYPE

WHEN MATCHED THEN UPDATE
    SET     
        bt.Z2_BILLING_DOCUMENT = st.BILLING_DOCUMENT,
        bt.Z2_ITEM = st.ITEM,
        bt.Z2_BILLING_TYPE = st.BILLING_TYPE,
        bt.Z2_BILLING_DATE = st.BILLING_DATE,
        bt.Z2_BILLING_QTY_IN_SKU = st.BILLING_QTY_IN_SKU,
        bt.Z2_MATERIAL = st.MATERIAL,
        bt.Z2_ITEM_DESCRIPTION = st.ITEM_DESCRIPTION,
        bt.Z2_MATERIAL_GROUP = st.MATERIAL_GROUP,
        bt.Z2_ASSIGNMENT = st.ASSIGNMENT
WHEN NOT MATCHED THEN INSERT
    (Z2_BILLING_DOCUMENT,Z2_ITEM,Z2_BILLING_TYPE,Z2_BILLING_DATE,Z2_BILLING_QTY_IN_SKU,Z2_MATERIAL,Z2_ITEM_DESCRIPTION,Z2_MATERIAL_GROUP,Z2_ASSIGNMENT, Z2_DATE_OF_INSERTION)
    VALUES
    (st.BILLING_DOCUMENT,st.ITEM,st.BILLING_TYPE,st.BILLING_DATE,st.BILLING_QTY_IN_SKU,st.MATERIAL,st.ITEM_DESCRIPTION,st.MATERIAL_GROUP,st.ASSIGNMENT, CURRENT_TIMESTAMP());

-- Below cmd is for to pause and stop the task
ALTER TASK I_JAPFA_DEV.IND_SAPHANA_DEV_TN.TASK_TO_Z2_NEW_BRONZE_LAYER RESUME;
ALTER TASK I_JAPFA_DEV.IND_SAPHANA_DEV_TN.TASK_TO_Z2_NEW_BRONZE_LAYER SUSPEND;


-- to Directly run the task
EXECUTE TASK I_JAPFA_DEV.IND_SAPHANA_DEV_TN.TASK_TO_Z2_NEW_BRONZE_LAYER;

-- show task status
show tasks

--To validate the data capture by table from task
select * from I_JAPFA_DEV.IND_SAPHANA_DEV_TN.BILLING_TAB

truncate I_JAPFA_DEV.IND_SAPHANA_DEV_TN.BILLING_TAB



----------------------------------------------End of Zone 2 Bronze layer ---------------------------------------------------------




-------------------------------------------starting of zone 3 Silver layer --------------------------------------------------------

//create schema
CREATE SCHEMA IND

-- CREATE NEW TABLE FOR SILVER LAYER
CREATE OR REPLACE TABLE I_JAPFA_DEV.IND.BILLING_TAB_TRANSF AS
SELECT 
  Z2_BILLING_DOCUMENT  AS  Z3_BILLING_DOCUMENT,      
  Z2_ITEM    AS Z3_ITEM ,                  
  Z2_BILLING_TYPE AS Z3_BILLING_TYPE,     
  Z2_BILLING_DATE      AS Z3_BILLING_DATE, 
  Z2_BILLING_QTY_IN_SKU     AS Z3_BILLING_QTY_IN_SKU,    
  Z2_MATERIAL     AS Z3_MATERIAL,               
  Z2_ITEM_DESCRIPTION      AS Z3_ITEM_DESCRIPTION,    
  Z2_MATERIAL_GROUP    AS  Z3_MATERIAL_GROUP,       
  Z2_ASSIGNMENT      AS Z3_ASSIGNMENT,            
  FROM I_JAPFA_DEV.IND_SAPHANA_DEV_TN.BILLING_TAB;

  -- TO ADD NEW COLUMNS IN TABLE 
alter table I_JAPFA_DEV.IND.BILLING_TAB_TRANSF 
ADD COLUMN ZRE_BQ NUMBER(38,0),S1_BQ NUMBER(38,0), FINAL_QUANTITY NUMBER(38,0);

-- TO VALIDATE THE RESULT
select * from I_JAPFA_DEV.IND.BILLING_TAB_TRANSF;


CREATE OR REPLACE TABLE I_JAPFA_DEV.IND.BILLING_FINAL_QUANTITY_TRANSF_TAB AS
WITH
    -- Filter only relevant billing types early to reduce data volume
    changed_docs AS (
        SELECT 
            Z3_BILLING_DOCUMENT,
            Z3_ITEM,
            Z3_BILLING_TYPE,
            Z3_BILLING_DATE,
            Z3_BILLING_QTY_IN_SKU,
            Z3_MATERIAL,
            Z3_ITEM_DESCRIPTION,
            Z3_MATERIAL_GROUP,
            Z3_ASSIGNMENT
        FROM I_JAPFA_DEV.IND.BILLING_TAB_TRANSF
        WHERE Z3_BILLING_TYPE IN ('ZF2', 'ZRE', 'S1')
    ),

    -- ZF2 changes: direct and indirect
    zf2_direct AS (
        SELECT * FROM changed_docs WHERE Z3_BILLING_TYPE = 'ZF2'
    ),

    zf2_indirect_zre AS (
        SELECT 
            zf2.Z3_BILLING_DOCUMENT,
            zf2.Z3_ITEM,
            zf2.Z3_BILLING_TYPE,
            zf2.Z3_BILLING_DATE,
            zf2.Z3_BILLING_QTY_IN_SKU,
            zf2.Z3_MATERIAL,
            zf2.Z3_ITEM_DESCRIPTION,
            zf2.Z3_MATERIAL_GROUP,
            zf2.Z3_ASSIGNMENT
        FROM I_JAPFA_DEV.IND.BILLING_TAB_TRANSF zf2
        INNER JOIN changed_docs zre
            ON zre.Z3_BILLING_TYPE = 'ZRE'
           AND zf2.Z3_BILLING_DOCUMENT = zre.Z3_ASSIGNMENT
           AND zf2.Z3_MATERIAL = zre.Z3_MATERIAL
    ),

    zf2_indirect_s1 AS (
        SELECT 
            zf2.Z3_BILLING_DOCUMENT,
            zf2.Z3_ITEM,
            zf2.Z3_BILLING_TYPE,
            zf2.Z3_BILLING_DATE,
            zf2.Z3_BILLING_QTY_IN_SKU,
            zf2.Z3_MATERIAL,
            zf2.Z3_ITEM_DESCRIPTION,
            zf2.Z3_MATERIAL_GROUP,
            zf2.Z3_ASSIGNMENT
        FROM I_JAPFA_DEV.IND.BILLING_TAB_TRANSF zf2
        INNER JOIN changed_docs s1
            ON s1.Z3_BILLING_TYPE = 'S1'
           AND zf2.Z3_BILLING_DOCUMENT = s1.Z3_ASSIGNMENT
           AND zf2.Z3_MATERIAL = s1.Z3_MATERIAL
    ),

    zf2_changes AS (
        SELECT * FROM zf2_direct
        UNION   
        SELECT * FROM zf2_indirect_zre
        UNION   
        SELECT * FROM zf2_indirect_s1
    ),

    -- Pre-filter ZRE and S1 documents
    zre AS (
        SELECT 
            Z3_BILLING_DOCUMENT,
            Z3_MATERIAL,
            Z3_BILLING_QTY_IN_SKU,
            Z3_ASSIGNMENT
        FROM I_JAPFA_DEV.IND.BILLING_TAB_TRANSF
        WHERE Z3_BILLING_TYPE = 'ZRE'
    ),

    s1 AS (
        SELECT 
            Z3_BILLING_DOCUMENT,
            Z3_MATERIAL,
            Z3_BILLING_QTY_IN_SKU,
            Z3_ASSIGNMENT
        FROM I_JAPFA_DEV.IND.BILLING_TAB_TRANSF
        WHERE Z3_BILLING_TYPE = 'S1'
    )

SELECT
    zf2_changes.Z3_BILLING_DOCUMENT,
    zf2_changes.Z3_ITEM,
    zf2_changes.Z3_BILLING_TYPE,
    zf2_changes.Z3_BILLING_DATE,
    zf2_changes.Z3_BILLING_QTY_IN_SKU,
    zf2_changes.Z3_MATERIAL,
    zf2_changes.Z3_ITEM_DESCRIPTION,
    zf2_changes.Z3_MATERIAL_GROUP,
    zf2_changes.Z3_ASSIGNMENT,
    zre.Z3_BILLING_QTY_IN_SKU AS ZRE_BQ,
    s1.Z3_BILLING_QTY_IN_SKU AS S1_BQ,
    CASE 
        WHEN zre.Z3_BILLING_DOCUMENT IS NOT NULL 
            THEN zf2_changes.Z3_BILLING_QTY_IN_SKU - zre.Z3_BILLING_QTY_IN_SKU
        WHEN s1.Z3_BILLING_DOCUMENT IS NOT NULL 
            THEN zf2_changes.Z3_BILLING_QTY_IN_SKU - s1.Z3_BILLING_QTY_IN_SKU
        ELSE 
            zf2_changes.Z3_BILLING_QTY_IN_SKU
    END AS FINAL_QUANTITY
FROM zf2_changes
LEFT JOIN zre
    ON zf2_changes.Z3_BILLING_DOCUMENT = zre.Z3_ASSIGNMENT
   AND zf2_changes.Z3_MATERIAL = zre.Z3_MATERIAL
LEFT JOIN s1
    ON zf2_changes.Z3_BILLING_DOCUMENT = s1.Z3_ASSIGNMENT
   AND zf2_changes.Z3_MATERIAL = s1.Z3_MATERIAL;

-- TO VALIDATE THE RESULT
select * from I_JAPFA_DEV.IND.BILLING_FINAL_QUANTITY_TRANSF_TAB;

---------------------------------------------Ending of zone 3 silver layer------------------------------------------------


---------------------------------------------starting of zone 4 Gold Layer -----------------------------------------------

CREATE SCHEMA IND_BI;

CREATE OR REPLACE VIEW I_JAPFA_DEV.IND_BI.VIEW_BILLING_FINAL_RESULT AS SELECT * FROM I_JAPFA_DEV.IND.BILLING_FINAL_QUANTITY_TRANSF_TAB;

SELECT * FROM I_JAPFA_DEV.IND_BI.VIEW_BILLING_FINAL_RESULT


------------------------------------------Ending of zone 4 Gold Layer--------------------------------------------

