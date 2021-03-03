select  a.mon,a.shipped_location_code ,a.shipped_location_name ,qty,amt,b.wms_qty,b.wms_amt,amt-wms_amt from 
(select substr(sdt,1,6) mon,shipped_location_code ,shipped_location_name ,sum(shipped_qty)qty,sum(amount )amt 
from csx_dw.dws_wms_r_d_shipped_order_all_detail
where sdt>='20200301' and sys ='new' 
    and shipped_location_code ='W0A3'
group by substr(sdt,1,6) ,shipped_location_code ,shipped_location_name
)a 
left join 
(select substr(sdt,1,6) mon,shipped_location_code ,shipped_location_name ,sum(shipped_qty )wms_qty,sum(amount )wms_amt from csx_dw.wms_shipped_order 
where sdt>='20200301' and sys ='new' 
group by substr(sdt,1,6),shipped_location_code ,shipped_location_name
)b on a.shipped_location_code = b.shipped_location_code and a.mon=b.mon

;
select * from csx_ods.source_basic_w_a_md_dic where sdt='20200812';

refresh  csx_dw.dws_wms_r_d_shipped_order_all_detail ;
select distinct business_type ,business_type_code,shipped_type,shipped_type_code from csx_dw.wms_shipped_order 
where sdt>='20200701' and sdt<='20200731'and sys ='new' 
and shipped_location_code ='W0A3'
;

select shipped_location_code,
    goods_code,
    sum(coalesce(shipped_qty,0)) shipped_qty,
    sum(amount )shipped_amt 
from csx_dw.wms_shipped_order 
where business_type not in('18','19')
;


select  a.mon,a.order_no,a.shipped_location_code ,a.shipped_location_name ,qty,amt,b.wms_qty,b.wms_amt,amt-wms_amt from 
(select substr(sdt,1,6) mon,order_no ,shipped_location_code ,shipped_location_name ,sum(shipped_qty )qty,sum(amount )amt 
from csx_dw.wms_shipped_order
where sdt>='20200701' and sdt<='20200731'and sys ='new' 
and shipped_location_code ='W0A3' 
group by substr(sdt,1,6) ,shipped_location_code ,shipped_location_name,order_no 
)a 
left join 
(select substr(sdt,1,6) mon,order_no ,shipped_location_code ,shipped_location_name ,sum(shipped_qty )wms_qty,sum(amount )wms_amt from csx_dw.dws_wms_r_d_shipped_order_all_detail 
where sdt>='20200701' and sdt<='20200731'and sys ='new' 
and shipped_location_code ='W0A3'
group by substr(sdt,1,6),shipped_location_code ,shipped_location_name,order_no
)b on a.shipped_location_code = b.shipped_location_code and a.mon=b.mon and a.order_no=b.order_no
-- and order_no ='OM200311003105'
;

select * from csx_ods.source_basic_w_a_d where sdt='20200809';


select distinct shop_code,shop_name ,purchase_group_name,shop_purchase_group_name 
from dws_basic_w_a_csx_product_info
where sdt='current'  
and purchase_group_name != shop_purchase_group_name ;

select province_code ,
province_name,
is_factory_goods_code,
workshop_code ,
workshop_name ,
sum(sales_qty )qty,sum(sales_value )sale,sum(profit )profit 
from csx_dw.dws_sale_r_d_customer_sale a 
where province_code in ('32','23','24')
and sdt>='20200701' and sdt<='20200731'
-- and is_factory_goods_code =1
group by province_code ,
province_name,
is_factory_goods_code,
workshop_code ,
workshop_name ;

select *from csx_dw.csx_shop where sdt='current' and location_type_code ='2' and zone_id ='3';
select * from csx_ods.source_wms_r_d_bills_config where sdt='20200809';
select * from  csx_dw.dws_sync_r_d_order_merge as dm where sdt='20200811' and customer_name is null;
refresh dws_sync_r_d_order_merge ;


select shipped_location_code,
    goods_code,
    sum(shipped_qty) shipped_qty, 
from csx_dw.wms_shipped_order 
where send_sdt >= '20200801'
    and send_sdt <='20200811'
    and business_type_code !='18'
    ;

select a.*,b.* from 
(select distinct receive_location_code ,
    receive_location_name ,
    outside_order_code ,
    business_type 
from csx_dw.wms_entry_order 
where sdt>='20200101' 
    and business_type like '货到%'
)a
left join 
(select distinct shipper_code ,
    shipper_name ,
    order_no ,
    business_type ,
    send_sdt
from csx_dw.wms_shipped_order 
where send_sdt>='20200101' 
    -- and business_type like '客户%'
)b on a.outside_order_code=b.order_no
;

select * from csx_dw.dws_sale_r_d_customer_sale where sdt>='20200101' and order_no ='OM200709000189';

SELECT * FROM  csx_tmp.temp_turnover;

select * from csx_dw.dws_basic_w_a_category_m where sdt='current';

select * from csx_dw.ads_supply_order_flow ;

select shipped_location_code,send_sdt,
count(DISTINCT  order_no  ),
      count(DISTINCT case when send_sdt <= regexp_replace(to_date(plan_date ),'-','') then order_no end )
from csx_dw.wms_shipped_order
where regexp_replace(to_date(create_time),'-','')>='20200701' 
    and regexp_replace(to_date(create_time),'-','')<='20200731'
    and send_sdt >= '${plan_sdt}'
    and business_type_code !='73'
    and source_system ='BBC'
    and shipped_location_code ='W0B6'
    group by shipped_location_code,send_sdt
;

