with stg_plans as (
    select *, 'fudgeflix_v3' as source_column
    from {{ source("fudgeflix_v3", "ff_plans") }}
)
select
    {{
        dbt_utils.generate_surrogate_key(
            ["plan_id"]
        )
    }} as plankey,
    stg_plans.*
from stg_plans