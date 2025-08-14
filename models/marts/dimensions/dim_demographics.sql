{{ config(
    materialized='table'
) }}

WITH age_groups AS (
    SELECT DISTINCT
        patient_id,
        patient_age,
        patient_gender,
        CASE 
            WHEN patient_age < 12 THEN 'Child'
            WHEN patient_age < 18 THEN 'Teenager'
            WHEN patient_age < 60 THEN 'Adult'
            ELSE 'Elderly'
        END as age_group
    FROM {{ ref('int_sih_data') }}
)

SELECT
    patient_id,
    patient_age,
    patient_gender,
    age_group,
    MD5(CONCAT(patient_id, '_', patient_gender)) AS demographic_key  -- cria surrogate key
FROM age_groups
ORDER BY patient_age, patient_gender





{# {{ config(
    materialized='table'
) }}

WITH age_groups AS (
    SELECT DISTINCT
        patient_id,
        patient_age,
        patient_sex,
        CASE 
            WHEN patient_age < 12 THEN 'Child'
            WHEN patient_age < 18 THEN 'Teenager'
            WHEN patient_age < 60 THEN 'Adult'
            ELSE 'Elderly'
        END as age_group
    FROM {{ ref('int_sih_data_clean') }}
)

SELECT
    patient_id,
    patient_age,
    patient_sex,
    age_group
FROM age_groups
ORDER BY patient_age, patient_sex #}

