with stg_order_details as
(
    select 
        orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        quantity, 
        (quantity * unitprice) as extendedpriceamount,
        (extendedpriceamount * discount) as discountamount,
        (extendedpriceamount - discountamount) as soldamount
    from {{source('northwind','Order_Details')}}
),
stg_orders as 
(
    select
        orderid,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    from {{source('northwind','Orders')}}
)
select 
    od.orderid,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    od.productkey,
    od.quantity,
    od.extendedpriceamount,
    od.discountamount,
    od.soldamount
from stg_order_details od
    left join stg_orders o on od.orderid = o.orderid
