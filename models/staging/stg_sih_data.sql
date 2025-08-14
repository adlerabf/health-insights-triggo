SELECT
    $1:N_AIH::STRING AS admission_id,
    $1:UF_CODE::STRING AS state_code,
    TO_DATE($1:DT_INTER::STRING, 'YYYYMMDD') AS admission_date,
    TO_DATE($1:DT_SAIDA::STRING, 'YYYYMMDD') AS discharge_date,
    TRY_TO_NUMBER($1:DIAS_PERM::STRING) AS length_of_stay,
    TRY_TO_NUMBER($1:IDADE::STRING) AS patient_age,
    
    CASE $1:SEXO::STRING
        WHEN '1' THEN 'Male'
        WHEN '3' THEN 'Female'
        ELSE 'Unknown'
    END AS patient_gender,

    CASE $1:MORTE::STRING
        WHEN '0' THEN 'No'
        WHEN '1' THEN 'Yes'
        ELSE 'Unknown'
    END AS death_flag,

    TRIM($1:DIAG_PRINC::STRING) AS icd_code,
    $1:PROC_REA::STRING AS procedure_code,
    TO_VARCHAR($1:MUNIC_RES::STRING) AS patient_city_code,
    TO_VARCHAR($1:MUNIC_MOV::STRING) AS hospital_city_code,
    TRY_TO_NUMBER($1:VAL_TOT::STRING) AS total_cost_brl,
    TRY_TO_NUMBER($1:US_TOT::STRING) AS total_cost_usd
FROM {{ source('raw', 'sih_data') }}


-- old version of the code
{# SELECT 
    $1:N_AIH::STRING AS N_AIH,
    $1:UF_CODE::STRING AS UF_CODE,
    $1:DT_INTER::STRING AS DT_INTER,
    $1:DT_SAIDA::STRING AS DT_SAIDA,
    $1:DIAS_PERM::STRING AS DIAS_PERM,
    $1:IDADE::STRING AS IDADE,
    $1:SEXO::STRING AS SEXO,
    $1:MORTE::STRING AS MORTE,
    $1:DIAG_PRINC::STRING AS DIAG_PRINC,
    $1:PROC_REA::STRING AS PROC_REA,
    $1:MUNIC_RES::STRING AS MUNIC_RES,
    $1:MUNIC_MOV::STRING AS MUNIC_MOV,
    $1:VAL_TOT::STRING AS VAL_TOT,
    $1:US_TOT::STRING AS US_TOT
FROM {{ source('raw', 'sih_data') }} #}

