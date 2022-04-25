--北京牛肉销售明细
select  a.sales_time,
    a.order_time,
    a.shipped_time,
    order_no,
    business_type_code,
    business_type_name,
    channel_code,
    channel_name,
    customer_no,
    customer_name,
    goods_code,
    goods_name,
    sum(sales_qty) qty,
    sum(sales_value) sales_value,
    sum(profit) profit,
    sum(profit) /sum(sales_value) as profit_rate
from csx_dw.dws_sale_r_d_detail a
where sdt>='20210601' and sdt<'20211001' 
and classify_small_code='B030103'
and province_name like '北京市'
group by  a.sales_time,
    a.order_time,
    a.shipped_time,
    order_no,
    business_type_code,
    business_type_name,
    channel_code,
    channel_name,
    customer_no,
    customer_name,
    goods_code,
    goods_name