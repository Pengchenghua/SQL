--名称与供应商名称一致
with temp_entry as 
(select 
    b.company_code,
    b.company_name,
    b.city_code,
    b.city_name,
    customer_name,
    sum(sales_value) sales 
from csx_dw.dws_sale_r_d_detail a 
 join 
 (select shop_id,company_code,company_name,city_code,city_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')b on a.dc_code=b.shop_id
where sdt>='20210601' 
and sdt<'20211001'
group by  b.company_code,
    b.company_name,
    b.city_code,
    b.city_name,
    customer_name),
temp_sale as 
(select company_code,
    company_name,
    b.city_code,
    b.city_name ,
    supplier_name,
    sum(price*receive_qty) as entry_amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select shop_id,company_code,company_name,city_code,city_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current')b on a.receive_location_code=b.shop_id
where sdt>='20210601'
and sdt<'20211001'
group by company_code,company_name,b.city_code,b.city_name,a.supplier_name
)
select a.*,b.supplier_name,entry_amt
from temp_entry a 
join 
temp_sale b on a.company_code=b.company_code and a.city_code=b.city_code and a.customer_name=b.supplier_name
;



    -- 日配业务	1
	-- 福利业务	2
	-- 批发内购	3
	-- 城市服务商	4
	-- 省区大宗	5
	-- BBC	6
	-- 大宗一部	7
	-- 大宗二部	8
	-- 商超	9



-- 名称与供应商名称雷同
-- 下单频次、销售额、课组数组
  SET hive.execution.engine=spark; 


-- 供应商排名
drop table  csx_tmp.tem_sale_jj;
create temporary table csx_tmp.tem_sale_jj as 
select company_code,
    company_name,
    city_code,
    city_name ,
    supplier_name,
    department_name,
    entry_amt,
    dense_rank()over(partition by supplier_name,company_code,city_code order by entry_amt desc) as aa 
from(
select company_code,
    company_name,
    b.city_code,
    b.city_name ,
    supplier_name,
    a.department_name,
    sum(price*receive_qty) as entry_amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select shop_id,
    company_code,
    company_name,
    city_code,
    city_name 
from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt='current')b on a.receive_location_code=b.shop_id
    where sdt>='20210701'
and sdt<'20211001'
group by company_code,
    company_name,
    b.city_code,
    b.city_name,
    a.supplier_name,
    department_name
    ) a 
;

drop table  csx_tmp.temp_sale_jj_01;
create temporary table csx_tmp.temp_sale_jj_01 as 

select  
    company_code,
    company_name,
    city_code,
    city_name,
    supplier_name,
    sum(coalesce(entry_amt,0)) entry_amt,
    concat_ws(',',collect_set(if(aa<3,department_name,''))) as dept_id
from csx_tmp.tem_sale_jj
group by company_code,
    company_name,
    city_code,
    city_name,
    supplier_name
    ;
  
    -- concat_ws(',', collect_list(department_name))as dept_id --未去重
   -- concat_ws(',',collect_set(department_name)) as dept_id --列转行 数据去重
 --   ;
-- 销售-- 名称与供应商名称雷同
-- 下单频次、销售额、课组数组
  SET hive.execution.engine=spark; 
  
    -- concat_ws(',', collect_list(department_name))as dept_id --未去重
   -- concat_ws(',',collect_set(department_name)) as dept_id --列转行 数据去重
 --   ;
drop table csx_tmp.temp_sale_kk;
create temporary table csx_tmp.temp_sale_kk as 
 select 
    company_code,
    company_name,
    city_code,
    city_name,
    customer_name,
    department_name,
    sdt,
    sum(sales) sales,
    sum(day_sale) day_sale,
    sum(fl_sale) fl_sale,
    sum(dz_sale)dz_sale
from (
select 
    sdt,
    b.company_code,
    b.company_name,
    b.city_code,
    b.city_name,
    customer_name,
    department_name,
    sum(sales_value) sales,
    sum(if(business_type_code='1',sales_value,0)) as day_sale,
    sum(if(business_type_code in('2','6'),sales_value,0)) as fl_sale,
    sum(if(business_type_code in('5'),sales_value,0)) as dz_sale
from csx_dw.dws_sale_r_d_detail a 
 join 
 (select shop_id,
    company_code,
    company_name,
    city_code,
    city_name 
from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt='current')b on a.dc_code=b.shop_id
where sdt>='20210701' 
    and sdt<'20211001'
group by  b.company_code,
    b.company_name,
    b.city_code,
    b.city_name,
    customer_name,
    sdt,
    department_name
  ) a  
 group by company_code,
    company_name,
    city_code,
    city_name,
    customer_name,
    department_name,
    sdt;
 
 drop table  csx_tmp.temp_sale_kk_01;
create temporary table csx_tmp.temp_sale_kk_01 as 
    
select  
    company_code,
    company_name,
    city_code,
    city_name,
    customer_name,
    count(distinct sdt) as sale_sdt,
    sum(coalesce(sales,0)) sales,
    sum(coalesce(day_sale,0)) day_sale,
    sum(coalesce(fl_sale,0)) fl_sale,
    sum(coalesce(dz_sale,0))dz_sale,
    concat_ws(',',collect_set(if(aa<3,department_name,''))) as dept_id
from (
select *,dense_rank()over(partition by customer_name,company_code,city_code order by sales desc) as aa 
from   csx_tmp.temp_sale_kk
) a 
group by company_code,
    company_name,
    city_code,
    city_name,
    customer_name
    ;
   


-- select * from csx_tmp.temp_sale_kk_01 where customer_name='南平市诚安贸易有限公司';

 select a.*,b.dept_id_2 from    
(select  a.company_code a_company_code,
a.company_name  a_company_name,
a.city_code  a_city_code,
a.city_name a_city_name, 
a.customer_name  a_customer_name,
sale_sdt,
a.sales  a_sales, 
a.day_sale a_day_sale,
a.fl_sale a_fl_sale,
a.dz_sale a_dz_sale,
a.dept_id   a_dept_id,
b.*
from csx_tmp.temp_sale_kk_01   a 
join 
csx_tmp.temp_sale_jj_01 b on a.company_code=b.company_code 
    and a.customer_name=b.supplier_name
    and a.city_code=b.city_code
)a
left join
(
select * from (    
select 
a.company_code a_company_code,
a.company_name  a_company_name,
a.city_code  a_city_code,
a.city_name a_city_name, 
a.customer_name  a_customer_name,
a.sales  a_sales,
a.day_sale a_day_sale,
a.fl_sale a_fl_sale,
a.dz_sale a_dz_sale,
a.dept_id   a_dept_id,
b.*
from csx_tmp.temp_sale_kk_01   a 
join 
    csx_tmp.temp_sale_jj_01 b on a.company_code=b.company_code 
and a.customer_name=b.supplier_name
and a.city_code=b.city_code
) aa 
lateral view explode(split(a_dept_id,',')) tl as a_dept_id_1        -- 提取各课组
lateral view explode(split(dept_id,',')) tl as dept_id_2            -- 提取课组
where a_dept_id_1=dept_id_2 and dept_id_2!=''
) b on a.a_company_code=b.a_company_code and a.a_city_code=b.a_city_code and a.a_customer_name=b.a_customer_name;