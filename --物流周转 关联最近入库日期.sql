--物流周转 关联最近入库日期
create temporary table csx_tmp.temp_entry_max as 
select receive_location_code as dc_code,
    a.goods_code,
    sdt,
    sum(a.receive_qty) qty ,
    sum(price*a.receive_qty) receive_amt
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select receive_location_code as dc_code,
    goods_code,
    max(sdt) max_sdt
from csx_dw.dws_wms_r_d_entry_detail 
where sdt>='20190101' 
group by receive_location_code,
    goods_code
) b on a.receive_location_code =b.dc_code and a.goods_code=b.goods_code and a.sdt=b.max_sdt
group by receive_location_code, a.goods_code,sdt
;

drop table if exists  csx_tmp.temp_turn_01 ;
create table csx_tmp.temp_turn_01 as 
select a.sdt,
    sales_region_code,
    sales_region_name,
    a.province_code,
    a.province_name,
    city_code , 
    city_name ,
    a.dc_code,
    a.dc_name,
    goods_id ,
    goods_name ,
    standard ,
    unit_name ,
    brand_name ,
    dept_id ,
    dept_name ,
    business_division_code ,
    business_division_name ,
    division_code ,
    division_name ,
    classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    goods_status_id ,
    goods_status_name ,
    final_qty ,
    final_amt ,
    sales_30day ,
    qty_30day ,
    inv_sales_days ,
    days_turnover_30 ,
    period_inv_amt_30day ,
    cost_30day ,
    max_sale_sdt ,
    no_sale_days ,
   b.sdt as entry_max_sdt,
   qty   as entry_qty,
   receive_amt mc_entry_amt ,
    coalesce(datediff(date_sub(CURRENT_DATE,1),from_unixtime(unix_timestamp(entry_max_sdt,'yyyyMMdd'),'yyyy-MM-dd')),9999) as entry_date,
   dc_uses 
from csx_tmp.ads_wms_r_d_goods_turnover a 
JOIN 
(SELECT shop_id,sales_region_code,sales_region_name FROM CSX_DW.dws_basic_w_a_csx_shop_m WHERE SDT='current' and purpose in ('01','02','07')) c on a.dc_code=c.shop_id
left join 
 csx_tmp.temp_entry_max  b on  a.dc_code=b.dc_code and a.goods_id=b.goods_code
where a.sdt='20210912'
    and a.dc_code like 'W%'
    
;

set hive.support.quoted.identifiers=none;
select `(dc_uses)?+.+`, coalesce(datediff(date_sub(CURRENT_DATE,1),from_unixtime(unix_timestamp(entry_max_sdt,'yyyyMMdd'),'yyyy-MM-dd')),9999) as entry_date,dc_uses
from csx_tmp.temp_turn_01 where division_code in ('12','13','14','15')
;


-- purpose 	purpose_name
-- 01	大客户物流
-- 02	商超物流
-- 03	工厂
-- 06	合伙人物流
-- 07	BBC物流
-- 08	代加工工厂