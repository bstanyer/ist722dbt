-- Combine fm_customers and ff_accounts
with
    stg_customers as (select * from raw.fudgemart_v3.fm_customers),

    stg_accounts as (select * from raw.fudgeflix_v3.ff_accounts),

    combined_data as (
        select
            md5(
                cast(
                    coalesce(
                        cast(customer_id as text), '_dbt_utils_surrogate_key_null_'
                    ) as text
                )
            ) as customerkey,
            customer_id as customer_id,
            customer_email as email,
            customer_firstname as firstname,
            customer_lastname as lastname,
            customer_address as address,
            customer_city as city,
            customer_state as state,
            customer_zip as zip,
            customer_phone as phone,
            customer_fax as fax,
            'fudgemart_v3' as source_table
        from stg_customers

        union all

        select
            md5(
                cast(
                    coalesce(
                        cast(account_id as text), '_dbt_utils_surrogate_key_null_'
                    ) as text
                )
            ) as customerkey,
            account_id as customer_id,
            account_email as email,
            account_firstname as firstname,
            account_lastname as lastname,
            account_address as address,
            null as city,
            null as state,
            customer_zip as zip,
            null as phone,
            null as fax,
            'fudgeflix_v3' as source_table
        from stg_accounts
    )

select *
from combined_data
