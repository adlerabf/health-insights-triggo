{{ config(
    materialized='table'
) }}

WITH sih AS (
    SELECT *
    FROM {{ ref('int_sih_data') }}
),

-- Join com dim_date para admission_date
admission_dates AS (
    SELECT d.date_id AS admission_date_key, d.full_date AS admission_date
    FROM {{ ref('dim_date') }} d
),

-- Join com dim_date para discharge_date
discharge_dates AS (
    SELECT d.date_id AS discharge_date_key, d.full_date AS discharge_date
    FROM {{ ref('dim_date') }} d
),

-- Demographics
demographics AS (
    SELECT demographic_key, patient_id
    FROM {{ ref('dim_demographics') }}
),

-- Locations
locations AS (
    SELECT location_key AS patient_location_key, city_code AS patient_city_code
    FROM {{ ref('dim_location') }}
),
hospital_locations AS (
    SELECT location_key AS hospital_location_key, city_code AS hospital_city_code
    FROM {{ ref('dim_location') }}
),

-- Procedures
procedures AS (
    SELECT procedure_key, procedure_code
    FROM {{ ref('dim_procedure') }}
),

-- Diseases
diseases AS (
    SELECT disease_key, icd_code
    FROM {{ ref('dim_disease') }}
)

SELECT
    s.admission_id,
    d.demographic_key,
    p.procedure_key,
    dis.disease_key,
    loc.patient_location_key,
    h_loc.hospital_location_key,
    a.date_id AS admission_date_key,
    disch.date_id AS discharge_date_key,
    s.length_of_stay,
    s.total_cost_brl,
    s.total_cost_usd,
    s.death_flag
FROM sih s

-- Demographics
LEFT JOIN demographics d
    ON s.patient_id = d.patient_id

-- Procedure
LEFT JOIN procedures p
    ON s.procedure_code = p.procedure_code

-- Disease
LEFT JOIN diseases dis
    ON s.icd_code = dis.icd_code

-- Patient location
LEFT JOIN locations loc
    ON s.patient_city_code = LEFT(TO_VARCHAR(loc.patient_city_code), 6)

-- Hospital location
LEFT JOIN hospital_locations h_loc
    ON s.hospital_city_code = LEFT(TO_VARCHAR(h_loc.hospital_city_code), 6)

-- Admission date
LEFT JOIN {{ ref('dim_date') }} a
    ON s.admission_date = a.full_date

-- Discharge date
LEFT JOIN {{ ref('dim_date') }} disch
    ON s.discharge_date = disch.full_date



{# {{ config(
    materialized='table'
) }}

WITH fact AS (
    SELECT
        sih.admission_id,
        sih.patient_id,
        sih.procedure_code,
        proc.procedure_name,
        sih.icd_code,
        dis.icd_description,
        sih.patient_city_name,
        loc.city_name,
        loc.state_code,
        demo.patient_age,
        demo.patient_sex,
        sih.admission_date,
        dt.full_date,
        dt.year,
        dt.month,
        dt.day,
        dt.quarter,
        dt.month_name,
        dt.day_name,
        sih.total_cost_brl,
        sih.total_cost_usd
    FROM {{ ref('int_sih_data_clean') }} sih
    LEFT JOIN {{ ref('dim_procedure') }} proc
        ON sih.procedure_code = proc.procedure_code
    LEFT JOIN {{ ref('dim_disease') }} dis
        ON sih.icd_code = dis.icd_code
    LEFT JOIN {{ ref('dim_location') }} loc
        ON sih.patient_city_name = loc.city_name
    LEFT JOIN {{ ref('dim_demographics') }} demo
        ON sih.patient_id = demo.patient_id
    LEFT JOIN {{ ref('dim_date') }} dt
        ON sih.admission_date = dt.full_date
)

SELECT * FROM fact #}
