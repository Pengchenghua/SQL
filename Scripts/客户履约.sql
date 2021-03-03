select
a.order_kind,
a.contract_location_code,
a.contract_location_name,
a.location_code,
a.location_name,
a.channel_code,
a.channel,
a.customer_no,
a.customer_name,
a.child_customer_no,
a.child_customer_name,
a.order_sku,
a.order_amt,
a.order_qty,
receive_sku,
receive_qty,
send_qty,
sign_qty,
sales_value 
from 
(select
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
   customer_no,
customer_name,
child_customer_no,
child_customer_name,
count(goods_code )as order_sku,
sum(purchase_qty *price ) as order_amt,
sum(purchase_qty) order_qty
from
    csx_dw.dws_sync_r_d_order_merge as a 
 where
    (sdt>='20200601' or sdt = '19990101')
    and order_time >= '2020-06-01 00:00:00'
    and delivery_date<regexp_replace(to_date(CURRENT_TIMESTAMP() ),'-','') 
    and source_sys = 'qyg'
    and return_flag != 'X'
group by 
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
customer_no,
customer_name,
child_customer_no,
child_customer_name
)a 
left join 
(select
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
customer_no,
customer_name,
child_customer_no,
child_customer_name,
count(case when delivery_date < regexp_replace(to_date(delivery_time),'-','')  then goods_code end   )as receive_sku,
--sum(purchase_qty *price ) as order_amt,
--sum(purchase_qty) order_qty
sum(case when delivery_date < regexp_replace(to_date(delivery_time),'-','')  then send_qty end ) as receive_qty,
sum(send_qty)send_qty,
sum(sign_qty )sign_qty,
sum(sales_value) as sales_value 
from csx_dw.dws_sync_r_d_order_merge as dm
where
    (sdt>='20200601' or sdt = '19990101')
    and order_time >= '2020-06-01 00:00:00'
    and delivery_date<regexp_replace(to_date(CURRENT_TIMESTAMP() ),'-','') 
    and source_sys = 'qyg'
    and return_flag != 'X'
group by 
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
customer_no,
customer_name,
child_customer_no,
child_customer_name
) b  on a.order_kind=b.order_kind and a.contract_location_code =b.contract_location_code 
and a.location_code =b.location_code and a.channel =b.channel and a.customer_no =b.customer_no
and a.child_customer_no =b.child_customer_no
;
select
a.order_kind,
a.contract_location_code,
a.contract_location_name,
a.location_code,
a.location_name,
a.channel_code,
a.channel,
a.customer_no,
a.customer_name,
count(a.child_customer_no ) as child_customer_num,
-- a.child_customer_name,
a.order_sku,
a.order_amt,
a.order_qty,
receive_sku,
receive_qty,
send_qty,
sign_qty,
sales_value 
from 
(select
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
   customer_no,
customer_name,
child_customer_no,
child_customer_name,
count(goods_code )as order_sku,
sum(purchase_qty *price ) as order_amt,
sum(purchase_qty) order_qty
from
    csx_dw.dws_sync_r_d_order_merge as a 
 where
    (sdt>='20200601' or sdt = '19990101')
    and order_time >= '2020-06-01 00:00:00'
    and delivery_date<regexp_replace(to_date(CURRENT_TIMESTAMP() ),'-','') 
    and source_sys = 'qyg'
    and return_flag != 'X'
group by 
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
customer_no,
customer_name,
child_customer_no,
child_customer_name
)a 
left join 
(select
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
customer_no,
customer_name,
child_customer_no,
child_customer_name,
count(case when delivery_date <= regexp_replace(to_date(delivery_time),'-','')  then goods_code end   )as receive_sku,
--sum(purchase_qty *price ) as order_amt,
--sum(purchase_qty) order_qty
sum(case when delivery_date <= regexp_replace(to_date(delivery_time),'-','')  then send_qty end ) as receive_qty,
sum(send_qty)send_qty,
sum(sign_qty )sign_qty,
sum(sales_value) as sales_value 
from csx_dw.dws_sync_r_d_order_merge as dm
where
    (sdt>='20200601' or sdt = '19990101')
    and order_time >= '2020-06-01 00:00:00'
    and delivery_date<regexp_replace(to_date(CURRENT_TIMESTAMP() ),'-','') 
    and source_sys = 'qyg'
    and return_flag != 'X'
group by 
order_kind,
contract_location_code,
contract_location_name,
location_code,
location_name,
channel_code,
channel,
customer_no,
customer_name,
child_customer_no,
child_customer_name
) b  on a.order_kind=b.order_kind and a.contract_location_code =b.contract_location_code 
and a.location_code =b.location_code and a.channel =b.channel and a.customer_no =b.customer_no
and a.child_customer_no =b.child_customer_no
;

select  location_code,concat(location_code ,' ' ,shop_name ) as full_name  ,dist_name,province_code,province_name
from csx_dw.csx_shop where sdt='current' and table_type =1
and 
order by  cast (dist_code as int)
;

SELECT * FROM csx_dw.dws_sync_r_d_order_merge as dm 
where  -- (
regexp_replace(to_date(order_time ),'-','')>='20200610' and (sdt='19900101' or sdt<='20200610')
--or delivery_time >='2020-06-01 00:00:00') 
and return_flag !='X' AND customer_no ='106287'
 and delivery_date<='20200610'
--and (case when division_code ='10' then 'U01' else department_code end)='U01' 
;
select  location_code,concat(location_code ,' ' ,shop_name ) as full_name  ,dist_name,province_code,province_name,dist_code,location_uses,location_uses_code 
from csx_dw.csx_shop where sdt='current' and table_type =1 
--and dist_code in ('${layer1}')
order by  cast (dist_code as int),location_code ;

select DISTINCT business_division_code ,business_division_name ,'0' level,'0' upper_layer from csx_dw.dws_basic_w_a_category_m where sdt='20200610' and division_code !=''
union all 
select DISTINCT case when division_code='10' then 'U01' else coalesce(purchase_group_code,category_large_code) end purchase_group_code ,
case when division_code='10' then '加工课' else coalesce(purchase_group_name,category_large_name )end purchase_group_name,'2' level,business_division_code as upper_layer 
from csx_dw.dws_basic_w_a_category_m where sdt='20200610' and category_large_code !=''
;

select sdt,channel ,channel_name ,city_code ,city_name ,
from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200601' and sdt<='20200610';