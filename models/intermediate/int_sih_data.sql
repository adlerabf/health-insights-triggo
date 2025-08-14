{{ config(
    materialized='table'
) }}

WITH sih AS (
    SELECT *
    FROM {{ ref('stg_sih_data') }}
),

cities AS (
    SELECT 
        city_code,
        city_name,
        state_code,
        region_name
    FROM {{ ref('stg_cities_data') }}
),

procedures AS (
    SELECT 
        procedure_code,
        procedure_name
    FROM {{ ref('stg_procedures_name') }}
),

icd AS (
    SELECT 
        icd_code,
        icd_description
    FROM {{ ref('stg_icd_data') }}
)

SELECT
    /* Admissions Surrogate key */
    MD5(
        CONCAT_WS(
            '-',
            s.admission_id,
            s.state_code,
            TO_CHAR(s.admission_date, 'YYYYMMDD'),
            s.hospital_city_code,
            COALESCE(s.procedure_code, 'NOPROC')
        )
    ) AS admission_sk,

    s.admission_id,
    s.state_code,
    s.admission_date,
    s.discharge_date,
    s.length_of_stay,
    s.patient_age,
    s.patient_gender,
    s.death_flag,

    /* Patient surrogate/business key */
    MD5(
        CONCAT_WS(
            '-',
            s.patient_age,
            s.patient_gender,
            s.patient_city_code
        )
    ) AS patient_id,  

    /* Patient locality */
    s.patient_city_code,
    c_res.city_name     AS patient_city_name,
    c_res.state_code    AS patient_state_code,
    c_res.region_name   AS patient_region_name,

    /* Hospital locality */
    s.hospital_city_code,
    c_mov.city_name     AS hospital_city_name,
    c_mov.state_code    AS hospital_state_code,
    c_mov.region_name   AS hospital_region_name,

    /* Procedures */
    s.procedure_code,
    p.procedure_name,

    /* ICD Data */
    s.icd_code,
    icd.icd_description,

    /* Costs */
    s.total_cost_brl,
    s.total_cost_usd

FROM sih s
LEFT JOIN cities c_res 
    ON s.patient_city_code = LEFT(TO_VARCHAR(c_res.city_code), 6)
LEFT JOIN cities c_mov 
    ON s.hospital_city_code = LEFT(TO_VARCHAR(c_mov.city_code), 6)
LEFT JOIN procedures p 
    ON s.procedure_code = p.procedure_code
LEFT JOIN icd 
    ON s.icd_code = icd.icd_code




{# SELECT
    /* Admissions Surrogate key */
    MD5(
        CONCAT_WS(
            '-',
            s.admission_id,
            s.state_code,
            TO_CHAR(s.admission_date, 'YYYYMMDD')
        )
    ) AS admission_sk,

    s.admission_id,
    s.state_code,
    s.admission_date,
    s.discharge_date,
    s.length_of_stay,
    s.patient_age,
    s.patient_gender,
    s.death_flag,

    /* Patient locality */
    s.patient_city_code,
    c_res.city_name     AS patient_city_name,
    c_res.state_code    AS patient_state_code,
    c_res.region_name   AS patient_region_name,

    /* Hospital locality */
    s.hospital_city_code,
    c_mov.city_name     AS hospital_city_name,
    c_mov.state_code    AS hospital_state_code,
    c_mov.region_name   AS hospital_region_name,

    /* Procedures */
    s.procedure_code,
    p.procedure_name,

    /* ICD Data */
    s.icd_code,
    icd.icd_description,

    /* Costs */
    s.total_cost_brl,
    s.total_cost_usd

FROM sih s
LEFT JOIN cities c_res 
    ON s.patient_city_code = LEFT(TO_VARCHAR(c_res.city_code), 6)
LEFT JOIN cities c_mov 
    ON s.hospital_city_code = LEFT(TO_VARCHAR(c_mov.city_code), 6)
LEFT JOIN procedures p 
    ON s.procedure_code = p.procedure_code
LEFT JOIN icd 
    ON s.icd_code = icd.icd_code #}




-- old version of the code
{# WITH sih AS (
    SELECT *
    FROM {{ ref('stg_sih_data') }}
),

cities AS (
    SELECT city_code, city_name
    FROM {{ ref('stg_cities_data') }}
),

procedures AS (
    SELECT procedure_code, procedure_name
    FROM {{ ref('stg_procedures_name') }}
),

icd AS (
    SELECT ICD_CODE, ICD_DESCRIPTION
    FROM {{ ref('stg_icd_data') }}
)

SELECT
    s.N_AIH AS admission_id,
    s.UF_CODE AS state_code,
    TO_DATE(s.DT_INTER, 'YYYYMMDD') AS admission_date,
    TO_DATE(s.DT_SAIDA, 'YYYYMMDD') AS discharge_date,
    TRY_TO_NUMBER(s.DIAS_PERM) AS length_of_stay,
    TRY_TO_NUMBER(s.IDADE) AS patient_age,
    CASE s.SEXO
      WHEN '1' THEN 'Male'
      WHEN '3' THEN 'Female'
      ELSE 'Unknown'
    END AS patient_sex,
    c_res.city_name AS patient_city_name,
    c_mov.city_name AS hospital_city_name,
    TRY_TO_NUMBER(s.VAL_TOT) AS total_cost_brl,
    TRY_TO_NUMBER(s.US_TOT) AS total_cost_usd,
    s.PROC_REA AS procedure_code,
    p.procedure_name,
    TRIM(s.DIAG_PRINC) AS icd_code,
    icd.ICD_DESCRIPTION,
    CASE s.MORTE 
      WHEN '0' THEN 'No'
      WHEN '1' THEN 'Yes'
      ELSE 'Unknown'
    END AS death_flag

FROM sih s
-- note: MUNIC_RES and MUNIC_MOV have only the first 6 numbers of the city code
-- thus we use LEFT to match with the city_code
LEFT JOIN cities c_res ON s.MUNIC_RES = LEFT(TO_VARCHAR(c_res.city_code), 6)
LEFT JOIN cities c_mov ON s.MUNIC_MOV = LEFT(TO_VARCHAR(c_mov.city_code), 6)
LEFT JOIN procedures p ON s.PROC_REA = p.procedure_code
LEFT JOIN icd ON TRIM(s.DIAG_PRINC) = icd.ICD_CODE #}

