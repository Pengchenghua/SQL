-- 食用油销售查询

select 
substr(sdt,1,6) as mon,
channel_code,
channel_name,
business_type_code,
business_type_name,
region_code,
region_name,
province_code,
province_name,
goods_code,
goods_name,
brand_name,
sum(sales_qty) as qty,
sum(sales_value) as sales_value,
sum(profit) profit
from csx_dw.dws_sale_r_d_detail 
where sdt>='20210101' 
and sdt<'20210912'
and classify_middle_code='B0603'
group by 
substr(sdt,1,6),
channel_code,
channel_name,
business_type_code,
business_type_name,
region_code,
region_name,
province_code,
province_name,
goods_code,
goods_name,
brand_name
;
select * from csx_dw.dws_basic_w_a_manage_classify_m where sdt='current' and classify_middle_name like '%油%';