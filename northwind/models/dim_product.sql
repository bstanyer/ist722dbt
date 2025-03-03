with stg_products as (
    select * from {{source('northwind' ,'Products')}}
),
stg_categories as (
    select * from {{source('northwind', 'Categories')}}
),
stg_supplier as (
    select * from {{source('northwind', 'Suppliers')}}
)
select 
    {{dbt_utils.generate_surrogate_key(['p.productid']) }} 
        as productkey, 
    p.productid,
    p.productname,
    {{dbt_utils.generate_surrogate_key(['p.supplierid']) }} 
        as supplierkey,
    c.categoryname,
    c.description as categorydescription

from stg_products p
    left join stg_supplier s on p.supplierid = s.supplierid
    left join stg_categories c on p.categoryid = c.categoryid
