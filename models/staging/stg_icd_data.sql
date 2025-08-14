select *
from {{ source('raw', 'icd_data') }}