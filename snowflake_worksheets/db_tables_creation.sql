create database health_insights_db;
create schema raw;
create schema analytics;

CREATE FILE FORMAT parquet_format
TYPE = PARQUET
COMPRESSION = AUTO;

SELECT $1
FROM @stg_datasus/procedures.csv;

create table procedures_name 
(
    id integer,
    cod_proced varchar(10),
    name_proced varchar(200)
);


select  $1
from @stg_datasus/procedures.csv
;

------------------------------------------------------------------------------
----------------------- Celaning Procedure Docs File -------------------------
------------------------------------------------------------------------------

CREATE OR REPLACE FILE FORMAT csv_procedure
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ';'
SKIP_HEADER = 1;

-- Creating clean table

CREATE OR REPLACE TABLE procedures_name AS
SELECT 
    SPLIT_PART(TRIM(REPLACE($1, '"', '')), ' ', 1) AS COD_PROCEDURE,
    SPLIT_PART(
        TRIM(SUBSTR(TRIM(REPLACE($1, '"', '')), POSITION(' ' IN TRIM(REPLACE($1, '"', ''))) + 1)),
        ';',
        1
    ) AS NAME_PROCEDURE
    
FROM @stg_datasus/procedures.csv (FILE_FORMAT => csv_procedure);


select * from procedures_name;

-- Testing Cotrex Translate to create a English version of the column

SELECT SNOWFLAKE.CORTEX.TRANSLATE(name_procedure, 'pt', 'en') FROM procedures_name LIMIT 10;

-- Applying the translation

ALTER TABLE procedures_name
ADD COLUMN NAME_PROCEDURE_EN STRING;

UPDATE procedures_name
SET NAME_PROCEDURE_EN = SNOWFLAKE.CORTEX.TRANSLATE(NAME_PROCEDURE, 'pt', 'en');

select * from procedures_name;

------------------------------------------------------------------------------
------------------------- Creating sih_data Table ----------------------------
------------------------------------------------------------------------------


CREATE OR REPLACE TABLE HEALTH_INSIGHTS_DB.RAW.sih_data AS
SELECT *
FROM @HEALTH_INSIGHTS_DB.RAW.STG_S3_DATASUS/sih_data_parquet
(
  FILE_FORMAT => 'PARQUET_FORMAT',
  PATTERN => '.*\\.parquet$'
);

select * from sih_data limit 10;




------------------------------------------------------------------------------
------------------------- Creating cities_data Table -------------------------
------------------------------------------------------------------------------



CREATE OR REPLACE TABLE HEALTH_INSIGHTS_DB.RAW.cities_data AS
SELECT *
FROM @HEALTH_INSIGHTS_DB.RAW.STG_S3_DATASUS/docs/cities_data
(
  FILE_FORMAT => 'PARQUET_FORMAT',
  PATTERN => '.*\\.parquet$'
);

select * from cities_data limit 10;


SELECT city_code
FROM stg_cities_data;
--LIMIT 10;

SELECT MUNIC_RES
FROM STG_SIH_DATA
LIMIT 10;

SELECT DISTINCT PACIENT_CITY_NAME, HOSPITAL_CITY_NAME, STATE_CODE 
FROM INT_SIH_DATA;

SELECT 
    $1:"city_id"::STRING AS city_code,
    $1:"city_name"::STRING AS city_name,
    $1:"microrregiao"."mesorregiao"."UF"."sigla"::STRING AS state_code,
    $1:"microrregiao"."mesorregiao"."UF"."regiao"."nome"::STRING AS region_name
FROM HEALTH_INSIGHTS_DB.RAW.cities_data
limit 10;


------------------------------------------------------------------------------
------------------------- Creating icd_data Table ----------------------------
------------------------------------------------------------------------------

select $1,$2
from @HEALTH_INSIGHTS_DB.RAW.STG_S3_DATASUS/docs/icd_data.csv
limit 10;

CREATE OR REPLACE FILE FORMAT csv_icd
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;


SELECT $1, $2
FROM @HEALTH_INSIGHTS_DB.RAW.STG_S3_DATASUS/docs/icd_data.csv
(FILE_FORMAT => 'csv_icd')
LIMIT 10;


CREATE OR REPLACE TABLE ICD_DATA AS
SELECT
    $1::STRING AS ICD_CODE,
    $2::STRING AS ICD_DESCRIPTION
FROM @HEALTH_INSIGHTS_DB.RAW.STG_S3_DATASUS/docs/icd_data.csv
(FILE_FORMAT => 'csv_icd');


SELECT DISTINCT ICD_CODE
FROM HEALTH_INSIGHTS_DB.ANALYTICS.INT_SIH_DATA;

-- Checking for Nulls in ICD column to decide it's relevance

SELECT
  COUNT(CASE WHEN icd_code IS NULL THEN 1 END) AS null_coluna1,
  COUNT(CASE WHEN icd_description IS NULL THEN 1 END) AS null_coluna2,
  
  COUNT(*) AS total_rows
FROM HEALTH_INSIGHTS_DB.ANALYTICS.INT_SIH_DATA;

select 
    $1:city_id::STRING AS city_code,
    $1:city_name::STRING AS city_name
    $1:UF
from HEALTH_INSIGHTS_DB.RAW.cities_data;

select $1
from HEALTH_INSIGHTS_DB.RAW.cities_data
limit 10;











