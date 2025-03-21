 with
    stg_customers as (
        select
            md5(cast(customer_id as text)) as source_key,
            'customers' as source_table,
            customer_id as business_key,
            customer_address,
            customer_email,
            customer_firstname,
            customer_lastname,
            customer_zip
        from {{ source("fudgemart_v3", "fm_customers") }}
    ),
    stg_accounts as (
        select
            md5(cast(account_id as text)) as source_key,
            'accounts' as source_table,
            account_id as business_key,
            account_address,
            account_email,
            account_firstname,
            account_lastname,
            customer_zip,
            account_plan_id
        from {{ source("fudgeflix_v3", "ff_Accounts") }}
    ),
 
    combined_data as (
        select
            source_key,
            source_table,
            business_key,
            customer_address,
            customer_email,
            customer_firstname,
            customer_lastname,
            customer_zip,
            null as account_plan_id  -- placeholder for missing columns in customers
        from stg_customers
 
        union all
 
        select
            source_key,
            source_table,
            business_key,
            account_address as customer_address,
            account_email as customer_email,
            account_firstname as customer_firstname,
            account_lastname as customer_lastname,
            customer_zip,
            account_plan_id
        from stg_accounts
    )
 
select
    row_number() over (order by source_key) as dimension_key,  -- Generates unique surrogate key
    *
from combined_data