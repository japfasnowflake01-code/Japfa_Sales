-- ============================================================================
-- ELT MEDALLION ARCHITECTURE - PL_JAPFA

-- ============================================================================

-- ============================================================================
-- STEP 1: DATABASE & SCHEMA SETUP
-- ============================================================================

CREATE DATABASE IF NOT EXISTS PL_JAPFA;

-- Bronze Layer: Raw ingestion (IND_SAPHANA_DEV_TN acts as Bronze)
CREATE SCHEMA IF NOT EXISTS PL_JAPFA.IND_SAPHANA_DEV_TN;

-- Silver Layer: Cleaned and validated data (IND)
CREATE SCHEMA IF NOT EXISTS PL_JAPFA.IND;

-- Gold Layer: Business aggregations and analytics (IND_BI)
CREATE SCHEMA IF NOT EXISTS PL_JAPFA.IND_BI;

-- ============================================================================
-- STEP 2: BRONZE LAYER - Raw Data Ingestion
-- ============================================================================

-- Create Stage for CSV files
CREATE OR REPLACE STAGE PL_JAPFA.IND_SAPHANA_DEV_TN.BILLING_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV' 
        SKIP_HEADER = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        FIELD_DELIMITER = ','
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
    );

----------------------------------------------------------------
-- Step 1: Create a sequence for BATCH_ID
CREATE OR REPLACE SEQUENCE PL_JAPFA.IND_SAPHANA_DEV_TN.SEQ_BATCH_ID 
START = 1001 
INCREMENT = 1;


CREATE OR REPLACE TABLE PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T (
    BILLING_DOCUMENT NUMBER,
    ITEM NUMBER,
    BILLING_TYPE VARCHAR,
    BILLING_DATE DATE,
    BILLING_QTY_IN_SKU NUMBER,
    MATERIAL VARCHAR,
    ITEM_DESCRIPTION VARCHAR,
    MATERIAL_GROUP VARCHAR,
    ASSIGNMENT NUMBER,
    -- Metadata columns
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE VARCHAR,
    BATCH_ID NUMBER  
);

-- Step 2: Load data with same BATCH_ID for entire file
COPY INTO PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T (
    BILLING_DOCUMENT, ITEM, BILLING_TYPE, BILLING_DATE,
    BILLING_QTY_IN_SKU, MATERIAL, ITEM_DESCRIPTION,
    MATERIAL_GROUP, ASSIGNMENT, SOURCE_FILE, BATCH_ID
)
FROM (
    SELECT 
        $1::NUMBER, 
        $2::NUMBER, 
        $3::VARCHAR, 
        TRY_TO_DATE($4, 'MM/DD/YYYY'),
        $5::NUMBER, 
        $6::VARCHAR, 
        $7::VARCHAR, 
        $8::VARCHAR, 
        $9::NUMBER,
        METADATA$FILENAME,
        PL_JAPFA.IND_SAPHANA_DEV_TN.SEQ_BATCH_ID.NEXTVAL  -- All rows get same batch ID
    -- FROM @PL_JAPFA.IND_SAPHANA_DEV_TN.BILLING_STAGE/Sample_S1.csv
    FROM @PL_JAPFA.IND_SAPHANA_DEV_TN.BILLING_STAGE/delta_file/Manual_insert.csv
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"') 
FORCE = TRUE;



-- Create Stream on Bronze table for CDC
CREATE OR REPLACE STREAM PL_JAPFA.IND_SAPHANA_DEV_TN.BRONZE_STREAM 
ON TABLE PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T;


-- ============================================================================
-- STEP 3: SILVER LAYER - Cleaned & Validated Data
-- ============================================================================

-- Silver Table: Cleaned master data (SCD Type 1)
CREATE OR REPLACE TABLE PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T (
    BILLING_DOCUMENT NUMBER,
    ITEM NUMBER,
    BILLING_TYPE VARCHAR,
    BILLING_DATE DATE,
    BILLING_QTY_IN_SKU NUMBER,
    MATERIAL VARCHAR,
    ITEM_DESCRIPTION VARCHAR,
    MATERIAL_GROUP VARCHAR,
    ASSIGNMENT NUMBER,
    -- Derived/Enriched columns
    PRODUCT_CATEGORY VARCHAR,
    YEAR_MONTH VARCHAR,
    -- Metadata
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ,
    DATA_QUALITY_FLAG VARCHAR,
    
    PRIMARY KEY (BILLING_DOCUMENT, ITEM)
);

select * from PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T;    --891,   
select DISTINCT PRODUCT_CATEGORY from PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T;

-- ============================================================================
-- STEP 4: GOLD LAYER - Business Aggregations
-- ============================================================================

-- Gold Table: ZF2 transactions with returns/adjustments applied
CREATE OR REPLACE TABLE PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T (
    BILLING_DOCUMENT NUMBER,
    ITEM NUMBER,
    BILLING_TYPE VARCHAR,
    BILLING_DATE DATE,
    BILLING_QTY_IN_SKU NUMBER,
    MATERIAL VARCHAR,
    ITEM_DESCRIPTION VARCHAR,
    MATERIAL_GROUP VARCHAR,
    ASSIGNMENT NUMBER,
    RETURN_QTY NUMBER,
    FINAL_QTY NUMBER,
    T_FINAL_AMOUNT NUMBER,  -- New column added
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (BILLING_DOCUMENT, ITEM)
);

-- ============================================================================
-- STEP 5: DIMENSIONAL MODEL (Star Schema)
-- ============================================================================

-- Dimension: Product
CREATE OR REPLACE TABLE PL_JAPFA.IND.DIM_PRODUCT (
    MATERIAL VARCHAR PRIMARY KEY,
    ITEM_DESCRIPTION VARCHAR,
    MATERIAL_GROUP VARCHAR,
    PRODUCT_CATEGORY VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ
);

-- Dimension: Billing Type
CREATE OR REPLACE TABLE PL_JAPFA.IND.DIM_BILLING_TYPE (
    BILLING_TYPE VARCHAR PRIMARY KEY,
    BILLING_TYPE_DESC VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Date
CREATE OR REPLACE TABLE PL_JAPFA.IND.DIM_DATE (
    DATE_KEY DATE PRIMARY KEY,
    YEAR NUMBER,
    QUARTER NUMBER,
    MONTH NUMBER,
    MONTH_NAME VARCHAR,
    WEEK NUMBER,
    DAY NUMBER,
    DAY_NAME VARCHAR,
    YEAR_MONTH VARCHAR
);

-- Fact: Sales Transactions
CREATE OR REPLACE TABLE PL_JAPFA.IND.FACT_SALES (
    BILLING_DOCUMENT NUMBER,
    ITEM NUMBER,
    BILLING_DATE DATE,
    BILLING_QTY_IN_SKU NUMBER,
    MATERIAL VARCHAR,
    BILLING_TYPE VARCHAR,
    ASSIGNMENT NUMBER,
    RETURN_QTY NUMBER,
    FINAL_QTY NUMBER,
     T_FINAL_AMOUNT NUMBER,
    
    PRIMARY KEY (BILLING_DOCUMENT, ITEM),
    FOREIGN KEY (MATERIAL) REFERENCES PL_JAPFA.IND.DIM_PRODUCT(MATERIAL),
    FOREIGN KEY (BILLING_TYPE) REFERENCES PL_JAPFA.IND.DIM_BILLING_TYPE(BILLING_TYPE),
    FOREIGN KEY (BILLING_DATE) REFERENCES PL_JAPFA.IND.DIM_DATE(DATE_KEY)
);

-- ============================================================================
-- STEP 6: ETL STORED PROCEDURES
-- ============================================================================

-- Procedure 1: Bronze → Silver Transformation
CREATE OR REPLACE PROCEDURE PL_JAPFA.IND_SAPHANA_DEV_TN.SP_BRONZE_TO_SILVER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Check if stream has data
    var streamCheck = snowflake.createStatement({
        sqlText: `SELECT COUNT(*) FROM PL_JAPFA.IND_SAPHANA_DEV_TN.BRONZE_STREAM`
    });
    var streamResult = streamCheck.execute();
    streamResult.next();
    var streamCount = streamResult.getColumnValue(1);
    
    if (streamCount == 0) {
        return 'No new data in Bronze stream. Skipping Silver load.';
    }

    // Merge data from Bronze to Silver with data quality checks
    var mergeSilver = `
        MERGE INTO PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T AS TGT
        USING (
            SELECT 
                BILLING_DOCUMENT,
                ITEM,
                BILLING_TYPE,
                BILLING_DATE,
                BILLING_QTY_IN_SKU,
                MATERIAL,
                ITEM_DESCRIPTION,
                MATERIAL_GROUP,
                ASSIGNMENT,
                -- Derive product category
                CASE 
                    WHEN UPPER(ITEM_DESCRIPTION) LIKE '%BROILER%' THEN 'Broiler Feed'
                    WHEN UPPER(ITEM_DESCRIPTION) LIKE '%LAYER%' THEN 'Layer Feed'
                    WHEN UPPER(ITEM_DESCRIPTION) LIKE '%CATTLE%' OR UPPER(ITEM_DESCRIPTION) LIKE '%CALF%' THEN 'Cattle Feed'
                    WHEN UPPER(ITEM_DESCRIPTION) LIKE '%FISH%' THEN 'Fish Feed'
                    WHEN UPPER(ITEM_DESCRIPTION) LIKE '%PIG%' THEN 'Pig Feed'
                    WHEN UPPER(ITEM_DESCRIPTION) LIKE '%DESHI%' THEN 'Deshi Feed'
                    ELSE 'Other'
                END AS PRODUCT_CATEGORY,
                TO_CHAR(BILLING_DATE, 'YYYY-MM') AS YEAR_MONTH,
                -- Data quality flag
                CASE 
                    WHEN BILLING_DOCUMENT IS NULL THEN 'MISSING_DOC_ID'
                    WHEN BILLING_DATE IS NULL THEN 'INVALID_DATE'
                    WHEN BILLING_QTY_IN_SKU IS NULL OR BILLING_QTY_IN_SKU < 0 THEN 'INVALID_QUANTITY'
                    WHEN MATERIAL IS NULL THEN 'MISSING_MATERIAL'
                    ELSE 'VALID'
                END AS DATA_QUALITY_FLAG,
                METADATA$ACTION,
                METADATA$ISUPDATE
            FROM PL_JAPFA.IND_SAPHANA_DEV_TN.BRONZE_STREAM
        ) AS SRC
        ON TGT.BILLING_DOCUMENT = SRC.BILLING_DOCUMENT 
           AND TGT.ITEM = SRC.ITEM
        
        -- Handle deletes
        WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN
            DELETE
        
        -- Handle updates
        WHEN MATCHED AND SRC.METADATA$ACTION = 'INSERT' AND SRC.METADATA$ISUPDATE = TRUE THEN
            UPDATE SET
                BILLING_TYPE = SRC.BILLING_TYPE,
                BILLING_DATE = SRC.BILLING_DATE,
                BILLING_QTY_IN_SKU = SRC.BILLING_QTY_IN_SKU,
                MATERIAL = SRC.MATERIAL,
                ITEM_DESCRIPTION = SRC.ITEM_DESCRIPTION,
                MATERIAL_GROUP = SRC.MATERIAL_GROUP,
                ASSIGNMENT = SRC.ASSIGNMENT,
                PRODUCT_CATEGORY = SRC.PRODUCT_CATEGORY,
                YEAR_MONTH = SRC.YEAR_MONTH,
                DATA_QUALITY_FLAG = SRC.DATA_QUALITY_FLAG,
                UPDATED_AT = CURRENT_TIMESTAMP()
        
        -- Handle inserts
        WHEN NOT MATCHED AND SRC.METADATA$ACTION = 'INSERT' THEN
            INSERT (
                BILLING_DOCUMENT, ITEM, BILLING_TYPE, BILLING_DATE,
                BILLING_QTY_IN_SKU, MATERIAL, ITEM_DESCRIPTION,
                MATERIAL_GROUP, ASSIGNMENT, PRODUCT_CATEGORY,
                YEAR_MONTH, DATA_QUALITY_FLAG, UPDATED_AT
            )
            VALUES (
                SRC.BILLING_DOCUMENT, SRC.ITEM, SRC.BILLING_TYPE, SRC.BILLING_DATE,
                SRC.BILLING_QTY_IN_SKU, SRC.MATERIAL, SRC.ITEM_DESCRIPTION,
                SRC.MATERIAL_GROUP, SRC.ASSIGNMENT, SRC.PRODUCT_CATEGORY,
                SRC.YEAR_MONTH, SRC.DATA_QUALITY_FLAG, CURRENT_TIMESTAMP()
            );
    `;
    snowflake.createStatement({sqlText: mergeSilver}).execute();

    return 'Bronze to Silver completed. Records processed: ' + streamCount;
} catch (err) {
    return 'Error in Bronze to Silver: ' + err.message;
}
$$;

-- Procedure 2: Silver → Gold Transformation
CREATE OR REPLACE PROCEDURE PL_JAPFA.IND_SAPHANA_DEV_TN.SP_SILVER_TO_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Create Gold table with business logic:
    -- ZF2 transactions minus returns (ZRE/S1)
    CREATE OR REPLACE TABLE PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T AS
    SELECT
        zf2.BILLING_DOCUMENT,
        zf2.ITEM,
        zf2.BILLING_TYPE,
        zf2.BILLING_DATE,
        zf2.BILLING_QTY_IN_SKU,
        zf2.MATERIAL,
        zf2.ITEM_DESCRIPTION,
        zf2.MATERIAL_GROUP,
        zf2.ASSIGNMENT,
        COALESCE(returns.RETURN_QTY, 0) AS RETURN_QTY,
        zf2.BILLING_QTY_IN_SKU - COALESCE(returns.RETURN_QTY, 0) AS FINAL_QTY,
         -- T_FINAL_AMOUNT calculation logic
        CASE 
            WHEN COALESCE(returns.RETURN_QTY, 0) = 0 AND (zf2.BILLING_QTY_IN_SKU - COALESCE(returns.RETURN_QTY, 0)) != 0 
                THEN zf2.BILLING_QTY_IN_SKU - COALESCE(returns.RETURN_QTY, 0)
            WHEN (zf2.BILLING_QTY_IN_SKU - COALESCE(returns.RETURN_QTY, 0)) = 0 AND COALESCE(returns.RETURN_QTY, 0) != 0 
                THEN COALESCE(returns.RETURN_QTY, 0)
            WHEN COALESCE(returns.RETURN_QTY, 0) != 0 AND (zf2.BILLING_QTY_IN_SKU - COALESCE(returns.RETURN_QTY, 0)) != 0 
                THEN zf2.BILLING_QTY_IN_SKU - COALESCE(returns.RETURN_QTY, 0)
            ELSE 0
        END AS T_FINAL_AMOUNT,
          CURRENT_TIMESTAMP() AS CREATED_AT
    FROM PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T zf2
    LEFT JOIN (
        SELECT 
            ASSIGNMENT,
            MATERIAL,
            ITEM_DESCRIPTION,
            SUM(BILLING_QTY_IN_SKU) AS RETURN_QTY
        FROM PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T
        WHERE BILLING_TYPE IN ('ZRE', 'S1')
        GROUP BY ASSIGNMENT, MATERIAL, ITEM_DESCRIPTION
    ) returns
        ON zf2.BILLING_DOCUMENT = returns.ASSIGNMENT
        AND zf2.MATERIAL = returns.MATERIAL
        AND zf2.ITEM_DESCRIPTION = returns.ITEM_DESCRIPTION
    WHERE zf2.BILLING_TYPE = 'ZF2'
        AND zf2.DATA_QUALITY_FLAG = 'VALID';

    RETURN 'Silver to Gold completed successfully.';
END;
$$;

-- Procedure 3: Load Dimensional Tables

CREATE OR REPLACE PROCEDURE PL_JAPFA.IND_SAPHANA_DEV_TN.SP_LOAD_DIMENSIONS()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Load DIM_PRODUCT
    var dimProduct = `
        MERGE INTO PL_JAPFA.IND.DIM_PRODUCT AS TGT
        USING (
            SELECT DISTINCT 
                MATERIAL, 
                ITEM_DESCRIPTION, 
                MATERIAL_GROUP,
                PRODUCT_CATEGORY
            FROM PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T
            WHERE MATERIAL IS NOT NULL
        ) AS SRC
        ON TGT.MATERIAL = SRC.MATERIAL
        WHEN MATCHED THEN
            UPDATE SET
                ITEM_DESCRIPTION = SRC.ITEM_DESCRIPTION,
                MATERIAL_GROUP = SRC.MATERIAL_GROUP,
                PRODUCT_CATEGORY = SRC.PRODUCT_CATEGORY,
                UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (MATERIAL, ITEM_DESCRIPTION, MATERIAL_GROUP, PRODUCT_CATEGORY)
            VALUES (SRC.MATERIAL, SRC.ITEM_DESCRIPTION, SRC.MATERIAL_GROUP, SRC.PRODUCT_CATEGORY);
    `;
    snowflake.createStatement({sqlText: dimProduct}).execute();

    // Load DIM_BILLING_TYPE
    var dimBilling = `
        MERGE INTO PL_JAPFA.IND.DIM_BILLING_TYPE AS TGT
        USING (
            SELECT DISTINCT 
                BILLING_TYPE,
                CASE BILLING_TYPE
                    WHEN 'ZF2' THEN 'Sales Invoice'
                    WHEN 'ZRE' THEN 'Return/Credit Memo'
                    WHEN 'S1' THEN 'Sales Adjustment'
                    WHEN 'ZXP' THEN 'Export Sales'
                    ELSE 'Other'
                END AS BILLING_TYPE_DESC
            FROM PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T
        ) AS SRC
        ON TGT.BILLING_TYPE = SRC.BILLING_TYPE
        WHEN NOT MATCHED THEN
            INSERT (BILLING_TYPE, BILLING_TYPE_DESC)
            VALUES (SRC.BILLING_TYPE, SRC.BILLING_TYPE_DESC);
    `;
    snowflake.createStatement({sqlText: dimBilling}).execute();

    // Load DIM_DATE
    var dimDate = `
        MERGE INTO PL_JAPFA.IND.DIM_DATE AS TGT
        USING (
            SELECT DISTINCT
                BILLING_DATE AS DATE_KEY,
                YEAR(BILLING_DATE) AS YEAR,
                QUARTER(BILLING_DATE) AS QUARTER,
                MONTH(BILLING_DATE) AS MONTH,
                MONTHNAME(BILLING_DATE) AS MONTH_NAME,
                WEEK(BILLING_DATE) AS WEEK,
                DAY(BILLING_DATE) AS DAY,
                DAYNAME(BILLING_DATE) AS DAY_NAME,
                TO_CHAR(BILLING_DATE, 'YYYY-MM') AS YEAR_MONTH
            FROM PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T
            WHERE BILLING_DATE IS NOT NULL
        ) AS SRC
        ON TGT.DATE_KEY = SRC.DATE_KEY
        WHEN NOT MATCHED THEN
            INSERT (DATE_KEY, YEAR, QUARTER, MONTH, MONTH_NAME, WEEK, DAY, DAY_NAME, YEAR_MONTH)
            VALUES (SRC.DATE_KEY, SRC.YEAR, SRC.QUARTER, SRC.MONTH, SRC.MONTH_NAME, 
                    SRC.WEEK, SRC.DAY, SRC.DAY_NAME, SRC.YEAR_MONTH);
    `;
    snowflake.createStatement({sqlText: dimDate}).execute();

    return 'All dimensions loaded successfully.';
} catch (err) {
    return 'Error loading dimensions: ' + err.message;
}
$$;

-- Procedure 4: Load Fact Table
CREATE OR REPLACE PROCEDURE PL_JAPFA.IND_SAPHANA_DEV_TN.SP_LOAD_FACT()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Truncate and reload fact table from Gold
    TRUNCATE TABLE PL_JAPFA.IND.FACT_SALES;
    
    INSERT INTO PL_JAPFA.IND.FACT_SALES (
        BILLING_DOCUMENT, ITEM, BILLING_DATE, BILLING_QTY_IN_SKU,
        MATERIAL, BILLING_TYPE, ASSIGNMENT, RETURN_QTY, FINAL_QTY, T_FINAL_AMOUNT
    )
    SELECT 
        BILLING_DOCUMENT,
        ITEM,
        BILLING_DATE,
        BILLING_QTY_IN_SKU,
        MATERIAL,
        BILLING_TYPE,
        ASSIGNMENT,
        RETURN_QTY,
        FINAL_QTY,
        T_FINAL_AMOUNT
    FROM PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T;

    RETURN 'Fact table loaded successfully.';
END;
$$;

-- ============================================================================
-- STEP 7: CREATE BI VIEWS
-- ============================================================================

CREATE OR REPLACE PROCEDURE PL_JAPFA.IND_BI.SP_CREATE_BI_VIEWS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Sales by Billing Type
    CREATE OR REPLACE VIEW PL_JAPFA.IND_BI.VW_SALES_BY_BILLING_TYPE AS
    SELECT 
        bt.BILLING_TYPE,
        bt.BILLING_TYPE_DESC,
        COUNT(DISTINCT fs.BILLING_DOCUMENT) AS TOTAL_DOCUMENTS,
        SUM(fs.BILLING_QTY_IN_SKU) AS TOTAL_QUANTITY,
        SUM(fs.RETURN_QTY) AS TOTAL_RETURNS,
        SUM(fs.FINAL_QTY) AS TOTAL_FINAL_QTY,
         SUM(fs.T_FINAL_AMOUNT) AS TOTAL_FINAL_AMOUNT
    FROM PL_JAPFA.IND.FACT_SALES fs
    JOIN PL_JAPFA.IND.DIM_BILLING_TYPE bt 
        ON fs.BILLING_TYPE = bt.BILLING_TYPE
    GROUP BY bt.BILLING_TYPE, bt.BILLING_TYPE_DESC;

    -- Sales by Product
    CREATE OR REPLACE VIEW PL_JAPFA.IND_BI.VW_SALES_BY_PRODUCT AS
    SELECT 
        p.MATERIAL,
        p.ITEM_DESCRIPTION,
        p.PRODUCT_CATEGORY,
        p.MATERIAL_GROUP,
        COUNT(DISTINCT fs.BILLING_DOCUMENT) AS TOTAL_DOCUMENTS,
        SUM(fs.FINAL_QTY) AS TOTAL_FINAL_QTY,
          SUM(fs.T_FINAL_AMOUNT) AS TOTAL_FINAL_AMOUNT  -- Added
    FROM PL_JAPFA.IND.FACT_SALES fs
    JOIN PL_JAPFA.IND.DIM_PRODUCT p 
        ON fs.MATERIAL = p.MATERIAL
    GROUP BY p.MATERIAL, p.ITEM_DESCRIPTION, p.PRODUCT_CATEGORY, p.MATERIAL_GROUP;

    -- Sales by Date
    CREATE OR REPLACE VIEW PL_JAPFA.IND_BI.VW_SALES_BY_DATE AS
    SELECT 
        d.DATE_KEY,
        d.YEAR,
        d.QUARTER,
        d.MONTH,
        d.MONTH_NAME,
        d.YEAR_MONTH,
        COUNT(DISTINCT fs.BILLING_DOCUMENT) AS TOTAL_DOCUMENTS,
        SUM(fs.FINAL_QTY) AS TOTAL_FINAL_QTY,
         SUM(fs.T_FINAL_AMOUNT) AS TOTAL_FINAL_AMOUNT  -- Added
    FROM PL_JAPFA.IND.FACT_SALES fs
    JOIN PL_JAPFA.IND.DIM_DATE d 
        ON fs.BILLING_DATE = d.DATE_KEY
    GROUP BY d.DATE_KEY, d.YEAR, d.QUARTER, d.MONTH, d.MONTH_NAME, d.YEAR_MONTH;

    -- Sales Dashboard (Combined)
    CREATE OR REPLACE VIEW PL_JAPFA.IND_BI.VW_SALES_DASHBOARD AS
    SELECT 
        fs.BILLING_DOCUMENT,
        fs.ITEM,
        d.YEAR_MONTH,
        bt.BILLING_TYPE_DESC,
        p.PRODUCT_CATEGORY,
        p.MATERIAL_GROUP,
        fs.BILLING_QTY_IN_SKU,
        fs.RETURN_QTY,
        fs.FINAL_QTY,
        fs.T_FINAL_AMOUNT  -- Added
    FROM PL_JAPFA.IND.FACT_SALES fs
    JOIN PL_JAPFA.IND.DIM_DATE d ON fs.BILLING_DATE = d.DATE_KEY
    JOIN PL_JAPFA.IND.DIM_BILLING_TYPE bt ON fs.BILLING_TYPE = bt.BILLING_TYPE
    JOIN PL_JAPFA.IND.DIM_PRODUCT p ON fs.MATERIAL = p.MATERIAL;

    RETURN 'BI Views created successfully.';
END;
$$;

-- ============================================================================
-- STEP 8: MASTER ORCHESTRATION PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE PL_JAPFA.IND_SAPHANA_DEV_TN.SP_RUN_FULL_ETL()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    var results = [];

    // Step 1: Bronze → Silver
    var step1 = snowflake.execute({
        sqlText: `CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_BRONZE_TO_SILVER();`
    });
    step1.next();
    results.push('Step 1: ' + step1.getColumnValue(1));

    // Step 2: Silver → Gold
    var step2 = snowflake.execute({
        sqlText: `CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_SILVER_TO_GOLD();`
    });
    step2.next();
    results.push('Step 2: ' + step2.getColumnValue(1));

    // Step 3: Load Dimensions
    var step3 = snowflake.execute({
        sqlText: `CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_LOAD_DIMENSIONS();`
    });
    step3.next();
    results.push('Step 3: ' + step3.getColumnValue(1));

    // Step 4: Load Fact
    var step4 = snowflake.execute({
        sqlText: `CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_LOAD_FACT();`
    });
    step4.next();
    results.push('Step 4: ' + step4.getColumnValue(1));

    // Step 5: Create BI Views
    var step5 = snowflake.execute({
        sqlText: `CALL PL_JAPFA.IND_BI.SP_CREATE_BI_VIEWS();`
    });
    step5.next();
    results.push('Step 5: ' + step5.getColumnValue(1));

    return 'Full ETL completed successfully.\n' + results.join('\n');
} catch (err) {
    return 'Error during ETL: ' + err.message;
}
$$;

-- ============================================================================
-- STEP 9: AUTOMATED TASK SCHEDULING
-- ============================================================================

CREATE OR REPLACE TASK PL_JAPFA.IND_SAPHANA_DEV_TN.TASK_RUN_ETL_PIPELINE
    WAREHOUSE = COMPUTE_WH 
    SCHEDULE = '11 SECONDS' 
    -- SCHEDULE = 'USING CRON 0 */1 * * * America/New_York'  -- Every hour
    WHEN SYSTEM$STREAM_HAS_DATA('PL_JAPFA.IND_SAPHANA_DEV_TN.BRONZE_STREAM')
AS
    CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_RUN_FULL_ETL();

-- Enable the task
ALTER TASK PL_JAPFA.IND_SAPHANA_DEV_TN.TASK_RUN_ETL_PIPELINE RESUME;

ALTER TASK PL_JAPFA.IND_SAPHANA_DEV_TN.TASK_RUN_ETL_PIPELINE SUSPEND;


SHOW TASKS IN ACCOUNT;

    CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_RUN_FULL_ETL();


-- ============================================================================
-- STEP 10: MANUAL EXECUTION & MONITORING
-- ============================================================================

-- Execute full ETL manually
-- CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_RUN_FULL_ETL();


-- View BI summary

 
 SELECT * FROM PL_JAPFA.IND_BI.VW_SALES_BY_BILLING_TYPE;
 SELECT * FROM PL_JAPFA.IND_BI.VW_SALES_BY_PRODUCT ORDER BY TOTAL_FINAL_QTY DESC LIMIT 10;
 SELECT * FROM PL_JAPFA.IND_BI.VW_SALES_BY_DATE order by date_key asc;

SELECT * FROM PL_JAPFA.IND_SAPHANA_DEV_TN.BRONZE_STREAM;



-- SELECT APPROX_PERCENTILE(BILLING_QTY_IN_SKU, 0.7) AS mH  FROM PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T;
SELECT *  FROM PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T;
DELETE  FROM PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T where batch_id = 1892; 

UPDATE PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T
SET BILLING_QTY_IN_SKU = 600 WHERE BATCH_ID = 1111;

SELECT * FROM  PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T;
SELECT * FROM  PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T;

SELECT * FROM  PL_JAPFA.IND.FACT_SALES;
SELECT * FROM  PL_JAPFA.IND.DIM_PRODUCT;
SELECT * FROM  PL_JAPFA.IND.DIM_BILLING_TYPE;
SELECT * FROM  PL_JAPFA.IND.DIM_DATE;

SELECT * FROM  PL_JAPFA.IND.DIM_DATE;

select current_timestamp();

truncate PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T;
truncate PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T;
truncate PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T;

-- truncate Fact and Dim
truncate PL_JAPFA.IND.FACT_SALES;
truncate PL_JAPFA.IND.DIM_PRODUCT;
truncate PL_JAPFA.IND.DIM_BILLING_TYPE;
truncate PL_JAPFA.IND.DIM_DATE;


-- truncate BI summary
truncate PL_JAPFA.IND_BI.VW_SALES_BY_BILLING_TYPE;
truncate PL_JAPFA.IND_BI.VW_SALES_BY_PRODUCT;
truncate PL_JAPFA.IND_BI.VW_SALES_BY_DATE;




CREATE OR REPLACE PROCEDURE PL_JAPFA.IND_SAPHANA_DEV_TN.SP_TRUNCATE_ALL_WITH_CASCADE()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    var results = [];
    
    // Disable foreign key checks is not available in Snowflake
    // Instead, truncate in reverse dependency order
    
    var tables = [
        {name: 'PL_JAPFA.IND.FACT_SALES', layer: 'FACT'},
        {name: 'PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T', layer: 'GOLD'},
        {name: 'PL_JAPFA.IND.ZSD_BAS_SALESDOC_SILVER_T', layer: 'SILVER'},
        {name: 'PL_JAPFA.IND.DIM_PRODUCT', layer: 'DIMENSION'},
        {name: 'PL_JAPFA.IND.DIM_BILLING_TYPE', layer: 'DIMENSION'},
        {name: 'PL_JAPFA.IND.DIM_DATE', layer: 'DIMENSION'},
        {name: 'PL_JAPFA.IND_SAPHANA_DEV_TN.ZSD_BAS_SALESDOC_BRONZE_T', layer: 'BRONZE'}
    ];
    
    for (var i = 0; i < tables.length; i++) {
        try {
            // Get row count before truncate
            var countSQL = `SELECT COUNT(*) FROM ${tables[i].name}`;
            var countResult = snowflake.execute({sqlText: countSQL});
            countResult.next();
            var rowCount = countResult.getColumnValue(1);
            
            // Truncate
            var truncateSQL = `TRUNCATE TABLE ${tables[i].name}`;
            snowflake.execute({sqlText: truncateSQL});
            
            results.push(`[${tables[i].layer}] ${tables[i].name} - Deleted ${rowCount} rows`);
        } catch (err) {
            results.push(`[ERROR] ${tables[i].name}: ${err.message}`);
        }
    }
    
    return 'All tables truncated successfully.\n\n' + results.join('\n');
    
} catch (err) {
    return 'Error: ' + err.message;
}
$$;

CALL PL_JAPFA.IND_SAPHANA_DEV_TN.SP_TRUNCATE_ALL_WITH_CASCADE();

SELECT CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', CURRENT_TIMESTAMP()) AS time_in_ist;    --2025-10-29 04:33:37.286
select  CURRENT_TIMESTAMP(); --2025-10-28 23:04:21.912 -0700

show stages;
List @~;
DESCRIBE FILE FORMAT PL_JAPFA.IND_SAPHANA_DEV_TN.BILLING_STAGE;


-- Select HLL(Column_Name) from tableName;   -- count distinct value from column records of the table
   SELECT APPROX_TOP_K(BILLING_DOCUMENT,3)   -- this give the result in array formate like below where  here 3 means top  3 records
-- [
--   [
--     60000746,  --BILLING_DOCUMENT
--     22         -- count how many time this number present in the  that column 
--   ],
--   [
--     60001027,
--     17
--   ],
--   [
--     70000045,
--     14
--   ]
-- ]

SELECT * FROM  PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T;

SELECT *  FROM  PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T;


SELECT 
    RETURN_QTY,
    FINAL_QTY,
    CASE 
        WHEN RETURN_QTY = 0 AND FINAL_QTY != 0 THEN FINAL_QTY
        WHEN FINAL_QTY = 0 AND RETURN_QTY != 0 THEN RETURN_QTY
        WHEN RETURN_QTY != 0 AND FINAL_QTY != 0 THEN FINAL_QTY
        ELSE 0
    END AS T_FINAL_Amount
FROM PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T;

SELECT * FROM PL_JAPFA.IND_BI.ZSD_BAS_SALESDOC_GOLD_T;