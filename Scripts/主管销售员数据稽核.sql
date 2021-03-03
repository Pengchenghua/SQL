
select a.*,b.sales_name,POSITION,b.province_name,sales_city_manager_name,sales_supervisor_name from csx_dw.report_big_cust_sale_v2 a 
JOIN 
(SELECT * from csx_dw.sale_org_m WHERE sdt ='20190802' and province_name like '上海%' )b 
on a.sales_id =b.sales_id
where   a.sdt ='20190802' AND dim_date ='M';
-- 查询销售人员
SELECT * from csx_dw.sale_org_m WHERE sdt ='20190802' and sales_supervisor_name='张丽琴';