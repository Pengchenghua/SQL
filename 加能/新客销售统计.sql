
--根据业务属性关联
--新客销售统计
select mon, region_code,region_name,province_name,a.business_type_code,a.business_type_name,a.customer_no,sales_value,first_order_date
from
(select substr(sdt,1,6) mon,region_code,region_name,province_name,channel_code,business_type_code,business_type_name,customer_no,sum(sales_value) sales_value
from csx_dw.dws_sale_r_d_detail
where substr(sdt,1,6) in    ('202101','202102','202201','202202')
and channel_code in ('1','7','9')
and business_type_code in ('1','2','6')
-- and province_name='安徽省'
group by region_code,region_name,province_name,channel_code,business_type_code,business_type_name,customer_no,substr(sdt,1,6)
)a 
 left join 
(select channel_code,business_type_code,customer_no,first_order_date, substr(first_order_date,1,6) first_mon
from csx_dw.dws_crm_w_a_customer_business_active
where sdt='20220221'
  --  and substr(first_order_date,1,6) in    ('202101','202102','202201','202202')
) b on  a.customer_no=b.customer_no and mon=first_mon and a.business_type_code=b.business_type_code
where b.customer_no is not null 
-- and a.business_type_code='6'
;

--按照关联销售
select mon, region_code,region_name,province_name,a.business_type_code,a.business_type_name,a.customer_no,sales_value,first_order_date
from
(select substr(sdt,1,6) mon,region_code,region_name,province_name,channel_code,business_type_code,business_type_name,customer_no,sum(sales_value) sales_value
from csx_dw.dws_sale_r_d_detail
where substr(sdt,1,6) in    ('202101','202102','202201','202202')
and channel_code in ('1','7','9')
and business_type_code in ('1','2','6')
-- and province_name='安徽省'
group by region_code,region_name,province_name,channel_code,business_type_code,business_type_name,customer_no,substr(sdt,1,6)
)a 
join 
(select customer_no,first_order_date, substr(first_order_date,1,6) first_mon
from csx_dw.dws_crm_w_a_customer_active
where sdt='20220221'
  --  and substr(first_order_date,1,6) in    ('202101','202102','202201','202202')
) b on  a.customer_no=b.customer_no and mon=first_mon
;