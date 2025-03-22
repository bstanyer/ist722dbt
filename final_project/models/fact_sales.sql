with stg_orders as (
    select
        order_id,  
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customerkey, 
        replace(to_date(order_date)::varchar, '-', '')::int as orderdatekey
    from {{source('fudgemart_v3', 'fm_orders')}}
),
stg_order_details as (
    select 
        od.order_id,
        {{ dbt_utils.generate_surrogate_key(['od.product_id']) }} as productkey,
        sum(od.order_qty) as quantity,
        sum(od.order_qty * p.product_retail_price) as retailpriceamount,
        sum(od.order_qty * p.product_wholesale_price) as wholesalepriceamount,
        sum(od.order_qty * (p.product_retail_price - p.product_wholesale_price)) as soldamount
    from {{source('fudgemart_v3', 'fm_order_details')}} od
    join {{source('fudgemart_v3', 'fm_products')}} p
      on od.product_id = p.product_id
    group by od.order_id, od.product_id, p.product_retail_price, p.product_wholesale_price
),
stg_account_plan as (
    select
        account_id,  
        {{ dbt_utils.generate_surrogate_key(['account_id']) }} as customerkey, 
        replace(to_date(account_opened_on)::varchar, '-', '')::int as accountopenedkey
    from {{source('fudgeflix_v3', 'ff_accounts')}}
),
stg_account_plan_details as (
    select 
        ab.ab_account_id,
        {{ dbt_utils.generate_surrogate_key(['ab.ab_plan_id']) }} as productkey,
        count(ab.ab_account_id) as quantity,
        sum(pl.plan_price) as soldamount
    from {{source('fudgeflix_v3', 'ff_account_billing')}} ab
    join {{source('fudgeflix_v3', 'ff_plans')}} pl
      on ab.ab_plan_id = pl.plan_id
    group by ab.ab_account_id, ab.ab_plan_id
)

select * 
from (
    select 
        'fudgemart_v3' as source_table,
        o.order_id as id,
        null as account_id,
        o.customerkey,
        o.orderdatekey,
        od.productkey,
        od.quantity,
        od.retailpriceamount,
        od.wholesalepriceamount,
        od.soldamount,
        null as accountopenedkey,
        null as account_quantity,
        null as account_soldamount
    from stg_orders o
    join stg_order_details od on o.order_id = od.order_id

    UNION ALL

    select 
        'fudgeflix_v3' as source_table,
        null as order_id,
        a.account_id as id,
        a.customerkey,
        null as orderdatekey,
        ad.productkey,
        null as quantity,
        null as retailpriceamount,
        null as wholesalepriceamount,
        null as soldamount,
        a.accountopenedkey,
        ad.quantity as account_quantity,
        ad.soldamount as account_soldamount
    from stg_account_plan a
    join stg_account_plan_details ad on a.account_id = ad.ab_account_id
) sub



