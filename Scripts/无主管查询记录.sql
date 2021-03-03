select
	*
from
	csx_dw.wms_entry_order
where
	receive_location_code = 'W0A8'
	and sdt = '20201127'
	--and category_middle_code = '110508'
	and goods_code ='128'
;
select
	*
from
	csx_dw.dws_wms_r_d_entry_order_all_detail 
where
	receive_location_code = 'W0A8'
	and sdt = '20201127'
	--and category_middle_code = '110508'
	and goods_code ='128'
;
	
select
	
	receive_location_code ,
	0 qty,
	0 amt,
	sum(shipped_qty) shipped_qty ,
	sum(amount) shipped_amt,
	0 sales_cost,
	0 sales_qty,
	0 sales_value

from
	csx_dw.wms_shipped_order a 
where
  shipped_location_code = 'W053'
	and send_sdt >= '20200101'
	-- and shipped_area_code='BZ02'
	and category_middle_code = '110508'
	and a.shipped_type like '调拨%'
group by 	goods_code ,
	goods_name ,
	category_small_code ,
	category_small_name,
	receive_location_code;
	
select * from csx_dw.dws_basic_w_a_csx_supplier_m ;
select * from csx_dw.dws_sale_r_d_customer_sale where sdt='20201130';

select
	receive_location_code dc_code,
	supplier_code,
	business_type ,
	goods_code,
	sum(receive_qty)entry_qty,
	sum(amount)receive_amt,
	0 shipped_qty ,
	0 shipped_amt
from csx_dw.wms_entry_order
where
sdt >= '20201101'
and sdt <= '20201130'
and receive_location_code ='W0A8'
and entry_type  like '%采购%'
group by 
	receive_location_code ,
	supplier_code,
	business_type ,
	goods_code
union all 
select
	shipped_location_code dc_code,
	supplier_code,
	business_type ,
	goods_code,
	0 entry_qty,
	0 receive_amt,
	sum(shipped_qty) shipped_qty ,
	sum(amount) shipped_amt
from   csx_dw.wms_shipped_order 
where
send_sdt >= '20201101'
and send_sdt <= '20201130'
and shipped_location_code ='W0A8'
and shipped_type like '%采购%'
group by 
	shipped_location_code ,
	supplier_code,
	business_type ,
	goods_code
;



select
	*
from
	csx_dw.wms_shipped_order
where
--	goods_code = '195'
--	and supplier_code = '20036173'
	 sdt >= '20201101'
	and order_no ='OU201127000440'
--	 and status = 9
	 ;
	 
with entry01 as (
	select
		sdt,
		goods_code ,
		sum(receive_qty) wms_qty_01,
		sum(price*receive_qty)
	from
		csx_dw.dwd_wms_r_d_entry_order_detail
	where
		sdt >= '20201101'
		and sdt <= '20201130'
		and receive_location_code = 'W0A8'
	group by
		sdt,
		goods_code
),
entry_02 as 
(select sdt,
goods_code,
	sum(receive_qty) wms_qty,
	sum(price*receive_qty)
from
	csx_dw.wms_entry_order
where
	sdt >= '20201101'
	and sdt <= '20201130'
	and receive_location_code = 'W0A8'
group by sdt,
goods_code)
select a.sdt,a.goods_code,wms_qty_01-wms_qty,wms_qty_01,wms_qty from entry01 a left  join entry_02 b on a.sdt=b.sdt and a.goods_code=b.goods_code
and wms_qty_01-wms_qty !=0
and a.goods_code='128';

select * from csx_tmp.dws_csms_province_month_sale_plan_tmp where month='202012';


-- 无主管
select sdt, province_code ,province_name ,customer_no,customer_name,sales_name,supervisor_name,sum(sales_value)sales
from csx_dw.dws_sale_r_d_detail where sdt>='20210112' and channel_code in('1' ,'7','9')
and province_code in ('15')
and (supervisor_name ='' )
group by province_code ,province_name,customer_no,customer_name,sales_name,supervisor_name,sdt;


-- 无主管
select province_code ,province_name ,customer_no,customer_name,sales_name,supervisor_name,sum(sales_value)sales
from  csx_dw.dws_sale_r_d_customer_sale where sdt>='20201201' and channel ='1' 
and province_code in ('15','24')
and (supervisor_name ='' or supervisor_name is null or supervisor_name like '虚拟%'or supervisor_name like '%城市%')
group by province_code ,province_name,customer_no,customer_name,sales_name,supervisor_name;

select province_code ,province_name ,customer_no,customer_name,sales_name,supervisor_name,sum(sales_value)sales
from  csx_dw.dws_sale_r_d_detail where sdt>='20210101'  
and province_name like '陕西%'
and (supervisor_name ='' or supervisor_name is null or supervisor_name like '虚拟%'or supervisor_name like '%城市%')
group by province_code ,province_name,customer_no,customer_name,sales_name,supervisor_name;

refresh csx_dw.dws_sync_r_d_order_merge ;


select
	dc_code,
	goods_code,
	sum(case when sdt>'20201001' then sales_value end ) as sale_30day,
	sum(case when sdt>=regexp_replace(to_date(date_sub('2020-11-30',60)),'-','') then sales_value end ) as sale_60day,
	max(sdt)max_sdt
from
	csx_dw.dws_sale_r_d_customer_sale
where
	sdt >= '20200101'
	and sdt <= '20201130'
	and dc_code = 'W0A8'
group by 
dc_code,
	goods_code;
	
select * from csx_dw.dws_crm_w_a_customer_20200924 where sdt='current' and sales_name like '何江军%' and customer_no ='107079';


SELECT
	goods_code ,
	sum(amount)amt
from
	csx_dw.wms_entry_order a
where sdt>='20201101' and sdt <='20201130'
and a.department_id='A02'
and receive_location_code ='W0A7'
and entry_type_code like 'P%'
-- and business_type like '%供应商%'
-- and supplier_code ='20045719'
-- and entry_type ='采购入库'
and receive_status =2
group by goods_code 
;


 select     receive_location_code ,order_code ,goods_code,sdt from 
( select receive_location_code ,order_code ,goods_code ,count(goods_code) sku,sdt FROM 
 csx_dw.wms_entry_order
   WHERE  sdt>='19990101'
         -- and order_no='OU201121000277'
        --  AND status<>9 
        group by receive_location_code ,order_code ,goods_code,sdt
 ) a where sku>1
 ;
 
