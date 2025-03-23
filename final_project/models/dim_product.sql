with stg_products as (
    select *, 'fudgemart_v3' as source_column
    from {{ source("fudgemart_v3", "fm_products") }}
)
select
    {{
        dbt_utils.generate_surrogate_key(
            ["product_id"]
        )
    }} as productkey,
    stg_products.*
from stg_products

