SELECT 
    $1:"city_id"::STRING AS city_code,
    $1:"city_name"::STRING AS city_name,
    $1:"microrregiao"."mesorregiao"."UF"."sigla"::STRING AS state_code,
    $1:"microrregiao"."mesorregiao"."UF"."regiao"."nome"::STRING AS region_name
from {{ source('raw', 'cities_data') }}
