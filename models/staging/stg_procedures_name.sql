with source as (
    select
        cod_procedure::string as procedure_code,
        name_procedure_en::string as procedure_name
    from {{ source('raw', 'procedures_name') }}
),

renamed as (
    select
        procedure_code,
        procedure_name
    from source
)

select * from renamed