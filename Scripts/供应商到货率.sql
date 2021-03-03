select distinct sdt from csx_dw.ads_sale_r_m_dept_sale_mon_report where sdt>='20200101' and sdt<='20200115'  ;
SELECT * FROM csx_dw.csx_partner_list where sdt>='202001' and customer_no ='104708';
select * from csx_dw.dws_crm_w_a_customer_m where sdt='20200602' and customer_no ='111321';


select sum(a.sales_value) sales_value
from 
(select *
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200501' and sdt<'20200601'
--and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
-- and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
and channel ='1'
 )a
join (select * from csx_dw.goods_m where sdt='current' 
and division_code in('12','13','14')
) b on b.goods_id=a.goods_code
 left join (select distinct customer_no from csx_dw.csx_partner_list where sdt='202005') c on a.customer_no=c.customer_no
where c.customer_no is null;
 ;
 
 --明细 5月大客户业绩（不含合伙人，不含BBC）
select a.channel_name,a.province_name,
a.dc_code,a.dc_name,a.customer_no,d.customer_name,
d.attribute,d.first_category,d.sales_name,d.work_no,
a.sales_date,b.division_code,b.division_name,b.category_large_code,b.category_large_name,b.department_id,b.department_name,
sum(a.sales_value) sales_value,
sum(a.profit) profit
from 
(select *
from csx_dw.dws_sale_r_d_customer_sale
where sdt>='20200501' and sdt<'20200601'
and sales_type in ('qyg','sapqyg','sapgc','sc','bbc')
-- and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
and channel ='1'
 )a
join (select * from csx_dw.goods_m where sdt='current' 
--and division_code in('12','13','14','15')
) b on b.goods_id=a.goods_code
left join (select distinct customer_no from csx_dw.csx_partner_list where sdt='202005') c on a.customer_no=c.customer_no
join (select customer_no,customer_name,sales_province,work_no,first_category,attribute,
sales_name from csx_dw.dws_crm_w_a_customer_m where sdt='20200601') d on d.customer_no=a.customer_no
where c.customer_no is null 
group by a.channel_name,a.province_name,
a.dc_code,a.dc_name,a.customer_no,d.customer_name,
d.attribute,d.first_category,d.sales_name,d.work_no,
a.sales_date,b.division_code,b.division_name,b.category_large_code,b.category_large_name,b.department_id,b.department_name;