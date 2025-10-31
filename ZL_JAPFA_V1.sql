
CREATE DATABASE IF NOT EXISTS ZL_JAPFA;
DROP DATABASE ZL_JAPFA;

-- Bronze Layer: Raw ingestion (IND_SAPHANA_DEV_TN acts as Bronze)
CREATE SCHEMA IF NOT EXISTS ZL_JAPFA.IND_SAPHANA_DEV_TN;

-- Silver Layer: Cleaned and validated data (IND)
CREATE SCHEMA IF NOT EXISTS ZL_JAPFA.IND;

-- Gold Layer: Business aggregations and analytics (IND_BI)
CREATE SCHEMA IF NOT EXISTS ZL_JAPFA.IND_BI;


CREATE OR REPLACE STAGE ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STAGE
    FILE_FORMAT = (
        TYPE = 'CSV' 
        SKIP_HEADER = 1 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        FIELD_DELIMITER = ','
        NULL_IF = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL = TRUE
       
    );

-- Create Batch ID Sequence for ZIBSIS
CREATE OR REPLACE SEQUENCE ZL_JAPFA.IND_SAPHANA_DEV_TN.SEQ_ZIBSIS_BATCH_ID 
START = 1 
INCREMENT = 1;

-- Bronze Table: ZIBSIS (Raw staging data)
CREATE OR REPLACE TABLE ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS (
    BUKRS NUMBER(4,0),           -- Company Code
    HKONT NUMBER(20,0),          -- General Ledger Account
    SHKZG VARCHAR(1),           -- Debit/Credit Indicator (H=Credit, S=Debit)
    DMBTR NUMBER(15,2),         -- Amount in Local Currency
    BUDAT DATE,                 -- Posting Date
    -- Metadata columns
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE VARCHAR,
    BATCH_ID NUMBER
);

-- Copy data from stage to Bronze table

-- COPY INTO ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS (
--     BUKRS, HKONT, SHKZG, DMBTR, BUDAT, 
--     SOURCE_FILE, BATCH_ID 
-- )
-- FROM (
--     SELECT 
--         $1::NUMBER(4,0),           -- BUKRS
--         $2::NUMBER(20,0),          -- HKONT
--         $3::VARCHAR(1),            -- SHKZG
--         $4::NUMBER(15,2),          -- DMBTR
--         CASE  
--             WHEN TRY_TO_DATE($5, 'DD/MM/YYYY') IS NOT NULL 
--             THEN TRY_TO_DATE($5, 'DD/MM/YYYY')     
--             ELSE TRY_TO_DATE($5, 'MM/DD/YYYY')   
--         END,                       -- BUDAT
--         METADATA$FILENAME,
--         ZL_JAPFA.IND_SAPHANA_DEV_TN.SEQ_ZIBSIS_BATCH_ID.NEXTVAL
--     FROM @ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STAGE/ZFinance_CS.csv
-- )
-- FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"') 
-- ON_ERROR = 'CONTINUE';

COPY INTO ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS (
    BUKRS, HKONT, SHKZG, DMBTR, BUDAT, 
    SOURCE_FILE, BATCH_ID 
)
FROM (
    SELECT 
        $1::NUMBER(4,0),           -- BUKRS
        $2::NUMBER(20,0),          -- HKONT
        $3::VARCHAR(1),            -- SHKZG
        $4::NUMBER(15,2),          -- DMBTR
        $5::DATE,     -- BUDAT
        METADATA$FILENAME,
        ZL_JAPFA.IND_SAPHANA_DEV_TN.SEQ_ZIBSIS_BATCH_ID.NEXTVAL
    FROM @ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STAGE/ZFinance_SS.csv
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"') 
ON_ERROR = 'CONTINUE';






-- Verify Bronze data
SELECT *  FROM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS;
truncate ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS;

SELECT 
    BUKRS,
    COUNT(*) AS RECORD_COUNT,
    MIN(BUDAT) AS MIN_DATE,
    MAX(BUDAT) AS MAX_DATE
FROM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS
GROUP BY BUKRS;

-- Create Stream on Bronze table for CDC
CREATE OR REPLACE STREAM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM 
ON TABLE ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS;

drop stream ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM;

SELECT * FROM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM;

-- Silver Table: FINANCIALS (Filtered data based on business rules)
CREATE OR REPLACE TABLE ZL_JAPFA.IND.FINANCIALS (
    BUKRS NUMBER(4,0),
    HKONT NUMBER(20,0),
    SHKZG VARCHAR(1),
    DMBTR NUMBER(15,2),
    BUDAT DATE,
    -- Derived columns
    ACCOUNT_CATEGORY VARCHAR(50),
    -- Metadata
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ,
    
    PRIMARY KEY (BUKRS, HKONT, BUDAT, SHKZG, DMBTR)
);

-- Initial Load: Bronze → Silver with Filters
INSERT INTO ZL_JAPFA.IND.FINANCIALS (
    BUKRS, HKONT, SHKZG, DMBTR, BUDAT, ACCOUNT_CATEGORY, UPDATED_AT
)
SELECT 
    BUKRS,
    HKONT,
    SHKZG,
    DMBTR,
    BUDAT,
    CASE 
        WHEN HKONT BETWEEN '21900000' AND '21999999' THEN 'Cash In Hand'
        WHEN HKONT BETWEEN '10500000' AND '10599999' THEN 'Term Loan'
        WHEN HKONT BETWEEN '10400000' AND '10499999' THEN 'Working Capital Loan'
        ELSE 'Other'
    END AS ACCOUNT_CATEGORY,
    CURRENT_TIMESTAMP()
FROM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS
WHERE BUKRS = '1000'
    AND (
        (HKONT BETWEEN '21900000' AND '21999999') OR  -- Cash In Hand
        (HKONT BETWEEN '10500000' AND '10599999') OR  -- Term Loan
        (HKONT BETWEEN '10400000' AND '10499999')     -- Working Capital Loan
    )
    AND SHKZG IN ('H', 'S');

-- Verify Silver data
SELECT * FROM ZL_JAPFA.IND.FINANCIALS;

SELECT 
    ACCOUNT_CATEGORY,
    SHKZG,
    COUNT(*) AS RECORD_COUNT,
    SUM(DMBTR) AS TOTAL_AMOUNT
FROM ZL_JAPFA.IND.FINANCIALS
GROUP BY ACCOUNT_CATEGORY, SHKZG
ORDER BY ACCOUNT_CATEGORY, SHKZG;

-- Gold Table: FT_FINANCIALS (Daily Summary)
CREATE OR REPLACE TABLE ZL_JAPFA.IND_BI.FT_FINANCIALS (
    DATE DATE PRIMARY KEY,
    CASH_IN_HAND NUMBER(15,2),
    TERM_LOAN NUMBER(15,2),
    WORKING_CAPITAL_LOAN NUMBER(15,2),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ
);

-- Initial Load: Silver → Gold with Aggregation Logic
INSERT INTO ZL_JAPFA.IND_BI.FT_FINANCIALS (
    DATE, CASH_IN_HAND, TERM_LOAN, WORKING_CAPITAL_LOAN, UPDATED_AT
)
SELECT 
    BUDAT AS DATE,
    -- Cash In Hand: SUM(DMBTR where SHKZG='S') - SUM(DMBTR where SHKZG='H')
    COALESCE(SUM(CASE 
        WHEN ACCOUNT_CATEGORY = 'Cash In Hand' AND SHKZG = 'S' THEN DMBTR
        WHEN ACCOUNT_CATEGORY = 'Cash In Hand' AND SHKZG = 'H' THEN -DMBTR
        ELSE 0 
    END), 0) AS CASH_IN_HAND,
    -- Term Loan: SUM(DMBTR where SHKZG='S') - SUM(DMBTR where SHKZG='H')
    COALESCE(SUM(CASE 
        WHEN ACCOUNT_CATEGORY = 'Term Loan' AND SHKZG = 'S' THEN DMBTR
        WHEN ACCOUNT_CATEGORY = 'Term Loan' AND SHKZG = 'H' THEN -DMBTR
        ELSE 0 
    END), 0) AS TERM_LOAN,
    -- Working Capital Loan: SUM(DMBTR where SHKZG='S') - SUM(DMBTR where SHKZG='H')
    COALESCE(SUM(CASE 
        WHEN ACCOUNT_CATEGORY = 'Working Capital Loan' AND SHKZG = 'S' THEN DMBTR
        WHEN ACCOUNT_CATEGORY = 'Working Capital Loan' AND SHKZG = 'H' THEN -DMBTR
        ELSE 0 
    END), 0) AS WORKING_CAPITAL_LOAN,
    CURRENT_TIMESTAMP()
FROM ZL_JAPFA.IND.FINANCIALS
GROUP BY BUDAT
ORDER BY BUDAT;

-- Verify Gold data
SELECT * FROM ZL_JAPFA.IND_BI.FT_FINANCIALS 
ORDER BY DATE DESC 
LIMIT 10;

-- Summary statistics
SELECT 
    COUNT(*) AS TOTAL_DAYS,
    SUM(CASH_IN_HAND) AS TOTAL_CASH_IN_HAND,
    SUM(TERM_LOAN) AS TOTAL_TERM_LOAN,
    SUM(WORKING_CAPITAL_LOAN) AS TOTAL_WORKING_CAPITAL_LOAN,
    AVG(CASH_IN_HAND) AS AVG_CASH_IN_HAND,
    AVG(TERM_LOAN) AS AVG_TERM_LOAN,
    AVG(WORKING_CAPITAL_LOAN) AS AVG_WORKING_CAPITAL_LOAN
FROM ZL_JAPFA.IND_BI.FT_FINANCIALS;

-- Procedure 1: Bronze → Silver Transformation
CREATE OR REPLACE PROCEDURE ZL_JAPFA.IND_SAPHANA_DEV_TN.SP_ZIBSIS_BRONZE_TO_SILVER()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    // Check if stream has data
    var streamCheck = snowflake.createStatement({
        sqlText: `SELECT COUNT(*) FROM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM`
    });
    var streamResult = streamCheck.execute();
    streamResult.next();
    var streamCount = streamResult.getColumnValue(1);
    
    if (streamCount == 0) {
        return 'No new data in ZIBSIS stream. Skipping Silver load.';
    }

    // Merge data from Bronze to Silver with filters
    var mergeSilver = `
        MERGE INTO ZL_JAPFA.IND.FINANCIALS AS TGT
        USING (
            SELECT 
                BUKRS,
                HKONT,
                SHKZG,
                DMBTR,
                BUDAT,
                CASE 
                    WHEN HKONT BETWEEN '21900000' AND '21999999' THEN 'Cash In Hand'
                    WHEN HKONT BETWEEN '10500000' AND '10599999' THEN 'Term Loan'
                    WHEN HKONT BETWEEN '10400000' AND '10499999' THEN 'Working Capital Loan'
                    ELSE 'Other'
                END AS ACCOUNT_CATEGORY,
                METADATA$ACTION,
                METADATA$ISUPDATE
            FROM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM
            WHERE BUKRS = '1000'
                AND (
                    (HKONT BETWEEN '21900000' AND '21999999') OR
                    (HKONT BETWEEN '10500000' AND '10599999') OR
                    (HKONT BETWEEN '10400000' AND '10499999')
                )
                AND SHKZG IN ('H', 'S')
        ) AS SRC
        ON TGT.BUKRS = SRC.BUKRS 
           AND TGT.HKONT = SRC.HKONT
           AND TGT.BUDAT = SRC.BUDAT
           AND TGT.SHKZG = SRC.SHKZG
           AND TGT.DMBTR = SRC.DMBTR
        
        -- Handle deletes
        WHEN MATCHED AND SRC.METADATA$ACTION = 'DELETE' THEN
            DELETE
        
        -- Handle updates
        WHEN MATCHED AND SRC.METADATA$ACTION = 'INSERT' AND SRC.METADATA$ISUPDATE = TRUE THEN
            UPDATE SET
                ACCOUNT_CATEGORY = SRC.ACCOUNT_CATEGORY,
                UPDATED_AT = CURRENT_TIMESTAMP()
        
        -- Handle inserts
        WHEN NOT MATCHED AND SRC.METADATA$ACTION = 'INSERT' THEN
            INSERT (BUKRS, HKONT, SHKZG, DMBTR, BUDAT, ACCOUNT_CATEGORY, UPDATED_AT)
            VALUES (SRC.BUKRS, SRC.HKONT, SRC.SHKZG, SRC.DMBTR, SRC.BUDAT, 
                    SRC.ACCOUNT_CATEGORY, CURRENT_TIMESTAMP());
    `;
    snowflake.createStatement({sqlText: mergeSilver}).execute();

    return 'ZIBSIS Bronze to Silver completed. Records processed: ' + streamCount;
} catch (err) {
    return 'Error in Bronze to Silver: ' + err.message;
}
$$;

-- Procedure 2: Silver → Gold Transformation
CREATE OR REPLACE PROCEDURE ZL_JAPFA.IND_SAPHANA_DEV_TN.SP_ZIBSIS_SILVER_TO_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Recreate Gold table with daily aggregations
    CREATE OR REPLACE TABLE ZL_JAPFA.IND_BI.FT_FINANCIALS AS
    SELECT 
        BUDAT AS DATE,
        -- Cash In Hand: SUM(DMBTR where SHKZG='S') - SUM(DMBTR where SHKZG='H')
        COALESCE(SUM(CASE 
            WHEN ACCOUNT_CATEGORY = 'Cash In Hand' AND SHKZG = 'S' THEN DMBTR
            WHEN ACCOUNT_CATEGORY = 'Cash In Hand' AND SHKZG = 'H' THEN -DMBTR
            ELSE 0 
        END), 0) AS CASH_IN_HAND,
        -- Term Loan: SUM(DMBTR where SHKZG='S') - SUM(DMBTR where SHKZG='H')
        COALESCE(SUM(CASE 
            WHEN ACCOUNT_CATEGORY = 'Term Loan' AND SHKZG = 'S' THEN DMBTR
            WHEN ACCOUNT_CATEGORY = 'Term Loan' AND SHKZG = 'H' THEN -DMBTR
            ELSE 0 
        END), 0) AS TERM_LOAN,
        -- Working Capital Loan: SUM(DMBTR where SHKZG='S') - SUM(DMBTR where SHKZG='H')
        COALESCE(SUM(CASE 
            WHEN ACCOUNT_CATEGORY = 'Working Capital Loan' AND SHKZG = 'S' THEN DMBTR
            WHEN ACCOUNT_CATEGORY = 'Working Capital Loan' AND SHKZG = 'H' THEN -DMBTR
            ELSE 0 
        END), 0) AS WORKING_CAPITAL_LOAN
        -- CURRENT_TIMESTAMP() AS CREATED_AT,
        -- CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM ZL_JAPFA.IND.FINANCIALS
    GROUP BY BUDAT
    ORDER BY BUDAT;

    RETURN 'ZIBSIS Silver to Gold completed successfully.';
END;
$$;

-- Procedure 3: Master Orchestration
CREATE OR REPLACE PROCEDURE ZL_JAPFA.IND_SAPHANA_DEV_TN.SP_RUN_ZIBSIS_ETL()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
try {
    var results = [];

    // Step 1: Bronze → Silver
    var step1 = snowflake.execute({
        sqlText: `CALL ZL_JAPFA.IND_SAPHANA_DEV_TN.SP_ZIBSIS_BRONZE_TO_SILVER();`
    });
    step1.next();
    results.push('Step 1: ' + step1.getColumnValue(1));

    // Step 2: Silver → Gold
    var step2 = snowflake.execute({
        sqlText: `CALL ZL_JAPFA.IND_SAPHANA_DEV_TN.SP_ZIBSIS_SILVER_TO_GOLD();`
    });
    step2.next();
    results.push('Step 2: ' + step2.getColumnValue(1));

    return 'ZIBSIS Full ETL completed successfully.\n' + results.join('\n');
} catch (err) {
    return 'Error during ZIBSIS ETL: ' + err.message;
}
$$;

-- Create Task for automated ETL pipeline
CREATE OR REPLACE TASK ZL_JAPFA.IND_SAPHANA_DEV_TN.TASK_RUN_ZIBSIS_ETL_PIPELINE
    WAREHOUSE = COMPUTE_WH 
    -- SCHEDULE = 'USING CRON 0 */2 * * * America/New_York'  -- Every 2 hours
       SCHEDULE = '11 SECONDS' 
    WHEN SYSTEM$STREAM_HAS_DATA('ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM')
AS
    CALL ZL_JAPFA.IND_SAPHANA_DEV_TN.SP_RUN_ZIBSIS_ETL();

-- Enable the task
ALTER TASK ZL_JAPFA.IND_SAPHANA_DEV_TN.TASK_RUN_ZIBSIS_ETL_PIPELINE RESUME;

SHOW TASKS IN ACCOUNT;
-- View stream contents
SELECT * FROM ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM;

-- Manual ETL execution
CALL ZL_JAPFA.IND_SAPHANA_DEV_TN.SP_RUN_ZIBSIS_ETL();





select * from ZL_JAPFA.IND.FINANCIALS;
truncate ZL_JAPFA.IND.FINANCIALS;

select * from ZL_JAPFA.IND_BI.FT_FINANCIALS;
truncate ZL_JAPFA.IND_BI.FT_FINANCIALS;

select * from ZL_JAPFA.IND_SAPHANA_DEV_TN.ZIBSIS_STREAM;