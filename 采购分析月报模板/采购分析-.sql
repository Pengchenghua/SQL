set sdt='20210101';
set edt='20210731';
create table csx_tmp.temp_fin_sup as 
SELECT  substr(sdt,1,6) as mon,
        coalesce(j.sales_region_code,d.sales_region_code) as region_code,
        coalesce(j.sales_region_name ,d.sales_region_name) as region_name,
        a.order_code,
        coalesce(j.province_code,d.province_code) province_code,
        coalesce(j.province_name,d.province_name) province_name,
        source_type_name,
       CASE
			WHEN a.super_class='1'
				THEN '供应商订单'
			WHEN a.super_class='2'
				THEN '供应商退货订单'
			WHEN a.super_class='3'
				THEN '配送订单'
			WHEN a.super_class='4'
				THEN '返配订单'
				ELSE a.super_class
		END super_class_name  ,
       receive_location_code,
       receive_location_name,
       goods_code,
       b.goods_name,
       unit_name,
       standard,
       brand_name,
       department_id,
       department_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       b.category_large_code,
       b.category_large_name,
       supplier_code,
       supplier_name,
       shipped_location_code,
       shipped_location_name,
       local_purchase_flag,
       receive_business_type,
       shipped_business_type,
       receive_qty,
       receive_amt,
       shipped_qty,
       shipped_amt,
       a.receive_close_date,
       coalesce(j.purpose,d.purpose) as purpose
FROM csx_dw.ads_supply_order_flow a 
left join 
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current' 	
	and  table_type=1 
	and purpose  in ('01','02','03','07','08') 
)j on a.receive_location_code=j.shop_id
LEFT JOIN 
(select goods_id,
        goods_name,
        unit_name,
        standard,
        brand_name,
        division_code,
        division_name,
        department_id,
        department_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        category_large_code,
        category_large_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
LEFT JOIN
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current' 	
	and  table_type=1 
	and purpose  in ('01','02','03','07','08') 
) d on a.shipped_location_code=d.shop_id
WHERE ( ( sdt>='20210101' or sdt='19990101')
	and a.super_class in ('1','2')
	and  ( shipped_status in ('6','7','8') or a.receive_status='2') )
	and ((a.receive_close_date>=${hiveconf:sdt} AND receive_close_date<=${hiveconf:edt})
     OR (shipped_date >=${hiveconf:sdt} AND shipped_date<=${hiveconf:edt})
     )
     ;
     
     

drop table csx_tmp.temp_entry_00 ;
create table csx_tmp.temp_entry_00 as
select   order_no,
    dc_code,
    goods_code,
    division_code,
    division_name,
    supplier_code,
    sum(receive_qty) receive_qty,
    sum( receive_amt) receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(shipped_amt) shipped_amt
from 
(
select origin_order_code order_no,
    receive_location_code as dc_code,
    goods_code,
    supplier_code,
    sum(receive_qty) receive_qty,
    sum(price/(1+tax_rate/100)*receive_qty) as receive_amt,
    0 shipped_qty,
    0 shipped_amt
from csx_dw.dws_wms_r_d_entry_batch
where sdt>='20210101' 
    and regexp_replace( to_date(receive_time ),'-','')<='20210731'
    and  regexp_replace( to_date(receive_time ),'-','')>='20210701'
    and order_type_code like 'P%'
    and business_type !='02'
    and receive_status in ('1','2')
   group by receive_location_code,goods_code,origin_order_code,supplier_code
union all 
select origin_order_no order_no, 
    shipped_location_code as dc_code,
    goods_code,
    supplier_code,
    0 receive_qty,
    0 receive_amt,
    sum(shipped_qty) shipped_qty,
    sum(price/(1+tax_rate/100)*shipped_qty) as shipped_amt
from csx_dw.dws_wms_r_d_ship_detail
where regexp_replace( to_date(send_time),'-','') >='20210701' 
    and  regexp_replace( to_date(send_time),'-','') <='20210731'
    and order_type_code like 'P%'
    and business_type_code in ('05')
    and status in ('6','7','8')
    group by shipped_location_code,goods_code,origin_order_no,supplier_code
) a 
join 
(select goods_id,division_code,division_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id

group by  
    order_no,
    dc_code,
    goods_code,
    supplier_code,
    division_code,
    division_name
;

-- 关联采购订单&DC类型&复用供应商

create temporary table csx_tmp.temp_entry_01 as 
select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    j.company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    order_no,
    dc_code,
    goods_code,
    case when purpose ='07' then '20' when yh_reuse_tag='是' then '21' when  division_code in ('11','10') then '11' when  division_code in ('12','13','14','15') then '12' end division_code ,
    case when purpose ='07' then 'BBC' when yh_reuse_tag='是' then '复用供应商' when  division_code in ('11','10') then '生鲜' when  division_code in ('12','13','14','15') then '食百' end  division_name,
    j.supplier_code,
    source_type,
    source_type_name,
    yh_reuse_tag
from 
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    company_code,
    city_code,
    city_name,
    province_code,
    province_name,
    purpose,
    a.order_no,
    dc_code,
    goods_code,
    division_code,
    division_name,
    supplier_code,
    source_type,
    source_type_name 
from csx_tmp.temp_entry_00 a 
join 
(select  order_code,source_type,source_type_name 
    from csx_dw.dws_scm_r_d_header_item_price 
    where super_class in ('2','1')  
    and source_type in ('1','10')
    group by  order_code,source_type,source_type_name
)b on a.order_no=b.order_code
join 
(select sales_province_code,
    sales_province_name,
    sales_region_code,
    sales_region_name,
    shop_id,
    company_code,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '' else city_code end  city_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '' else city_name end  city_name,
    case when shop_id in ('W0H4') then '900001' when shop_id in ('W0G1','W0J8')  then '900002' else province_code end province_code,
    case when shop_id in ('W0H4') then '大宗二' when shop_id in ('W0G1','W0J8')  then '大宗一' else  province_name  end province_name,
    purpose
from csx_dw.dws_basic_w_a_csx_shop_m
 where sdt='current' 	
	and  table_type=1 
	and purpose  in ('01','02','03','07','08') 
) d on a.dc_code=d.shop_id
) j 
left join  
(select company_code,supplier_code,yh_reuse_tag from csx_tmp.ads_fr_r_m_supplier_reuse where months='202107' ) s on j.company_code=s.company_code and j.supplier_code=s.supplier_code
;




select * from csx_tmp.temp_entry_01 a 
join 
();

show create table csx_dw.dws_wms_r_d_entry_batch;

-- source_type,source_type_name  
--1	采购导入
--2	直送客户
--3	一键代发
--4	项目合伙人
--5	无单入库
--8	云超物流采购
--10	智能补货
--11	商超直送
--13	云超门店采购
--14	临时地采
--15	联营直送
--16	永辉生活


