
-- 有库存SKU数汇总
drop table if exists csx_tmp.temp_hight_sku_00;
create temporary table csx_tmp.temp_hight_sku_00 as 
select
    dc_type,
    dc_uses,
    dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
	dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	count(distinct goods_id) as inv_sku,
	sum(final_qty) as final_qty,
	sum(final_amt) as final_amt,
	sum(cost_30day)as cost_30day,
	sum(sales_30day) as sales_30day,
	sum(qty_30day) as qty_30day,
	sum(period_inv_qty_30day) as period_inv_qty_30day,
	sum(period_inv_amt_30day) as period_inv_amt_30day,
	sum(period_inv_amt_30day)/sum(cost_30day) as days_turnover_30
from
	csx_tmp.ads_wms_r_d_goods_turnover
where
	sdt = '20201026'
--	and dc_code = 'W0A3'
    and period_inv_amt_30day != 0 
    and dept_id in ('H01', 'A01', 'A02', 'A03', 'A04', 'A10', 'H04', 'H05', 'H06','A05', 'A06', 'A07', 'A08', 'A09')
	group by 
	    dc_type,
        dc_uses,
	    dept_id,
	    dept_name,
	    business_division_code,
	    business_division_name,
	    division_code,
	    division_name,
	    dist_code ,
	    dist_name ,
	    dc_code,
	    dc_name 
	;
	
-- 高周转SKU数汇总
drop table if exists csx_tmp.temp_hight_sku_01;
create temporary table csx_tmp.temp_hight_sku_01 as 
select
    dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
	dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	count(distinct goods_id) as inv_sku,
	sum(final_qty) as final_qty,
	sum(final_amt) as final_amt,
	sum(cost_30day)as cost_30day,
	sum(sales_30day) as sales_30day,
	sum(qty_30day) as qty_30day,
	sum(period_inv_qty_30day) as period_inv_qty_30day,
	sum(period_inv_amt_30day) as period_inv_amt_30day,
	sum(period_inv_amt_30day)/sum(cost_30day) as days_turnover_30
from
	csx_tmp.ads_wms_r_d_goods_turnover
where
	sdt = '20201026'
	--and dc_code = 'W0A3'
	--and period_inv_amt_30day != 0
	and final_amt > 0
	and ((dept_id in ('H01', 'A01', 'A02', 'A03', 'A04', 'A10', 'H04', 'H05', 'H06')   and days_turnover_30 >=30) 
	    or  (dept_id in ('A05', 'A06', 'A07', 'A08', 'A09') and days_turnover_30 >=45)) 
	group by 
	    dc_type,
        dc_uses,
	    dept_id,
	    dept_name,
	    business_division_code,
	    business_division_name,
	    division_code,
	    division_name,
	    dist_code ,
	    dist_name ,
	    dc_code,
	    dc_name 
	;

--高库存又入库
drop table if exists csx_tmp.temp_hight_sku_03;
create temporary table csx_tmp.temp_hight_sku_03 as 
select
    a.dc_type,
    a.dc_uses,
	a.dist_code ,
	a.dist_name ,
	a.dc_code,
	a.dc_name ,
	a.dept_id,
	a.dept_name,
	a.business_division_code,
	a.business_division_name,
	a.division_code,
	a.division_name,
	count(distinct a.goods_id) as inv_sku,
	sum(final_qty) as final_qty,
	sum(final_amt) as final_amt,
	sum(receive_qty)receive_qty,
	sum(receive_amt)receive_amt
from 	csx_tmp.ads_wms_r_d_goods_turnover a 
join 
(select a.dc_code,goods_id,receive_qty,receive_amt
from
	csx_tmp.ads_wms_r_d_goods_turnover a
 join 
 (select receive_location_code as dc_code,goods_code ,sum(receive_qty)receive_qty,sum(amount)receive_amt
    from csx_dw.wms_entry_order 
    where sdt>='20201020' and entry_type!='客退入库' and business_type_code not in ('03','54') and receive_status=2
  group by 
  receive_location_code,goods_code 
  ) j on a.dc_code=j.dc_code and a.goods_id=j.goods_code
where
	sdt = '20201020'
	--and dc_code = 'W0A3'
    --and period_inv_amt_30day > 0
	and final_amt > 0
	and ((dept_id in ('H01', 'A01', 'A02', 'A03', 'A04', 'A10', 'H04', 'H05', 'H06')
	and days_turnover_30 >30)
	or (dept_id in ('A05', 'A06', 'A07', 'A08', 'A09')
	and days_turnover_30 >45)) 
)b on a.dc_code=b.dc_code and a.goods_id=b.goods_id 
    and a.sdt='20201026'
    and a.final_amt>200
    --and a.no_sale_days>=7
    --and a.entry_days>=7
group by
    a.dc_type,
    a.dc_uses,
	a.dist_code ,
	a.dist_name ,
	a.dc_code,
	a.dc_name ,
	a.dept_id,
	a.dept_name,
	a.business_division_code,
	a.business_division_name,
	a.division_code,
	a.division_name
;
	
	
-- 未销售商品
drop table if exists csx_tmp.temp_hight_sku_02;
create temporary table csx_tmp.temp_hight_sku_02 as 
select
     dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
    dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	count(distinct goods_id) as inv_sku,
	sum(final_qty) as final_qty,
	sum(final_amt) as final_amt,
	sum(cost_30day)as cost_30day,
	sum(sales_30day) as sales_30day,
	sum(qty_30day) as qty_30day,
	sum(period_inv_qty_30day) as period_inv_qty_30day,
	sum(period_inv_amt_30day) as period_inv_amt_30day,
	case when sum(cost_30day)=0 and sum(period_inv_amt_30day)!=0 then 9999 else   sum(period_inv_amt_30day)/sum(cost_30day)end as days_turnover_30
from csx_tmp.ads_wms_r_d_goods_turnover 
where sdt='20201103' 
	--and dc_code ='W0A3'
	--and no_sale_days >=30
	and final_amt >0
	--and entry_days >=3
	and ((dept_id in ('H02','H03','H05','H07','H08') and no_sale_days >3)
		or (dept_id in ('H01','A01','A02','A03','A04','A10','H04','H06') and no_sale_days >60) 
		or (dept_id ('A05','A06','A07','A08','A09') and no_sale_days >90))
group by 
     dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
    dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name;

-- 汇总数据	
select
    a.dc_type,
    a.dc_uses,
    a.dist_code ,
	a.dist_name ,
	a.dc_code,
	a.dc_name ,
	a.dept_id,
	a.dept_name,
	a.business_division_code,
	a.business_division_name,
	a.division_code,
	a.division_name,
	a.inv_sku,
	a.final_qty,
	a.final_amt,
	a.cost_30day,
	a.sales_30day,
    a.qty_30day,
	a.period_inv_qty_30day,
	a.period_inv_amt_30day,
	a.days_turnover_30,
	b.inv_sku as  hight_sku,
	b.final_amt as  hight_amt,
	c.inv_sku as  no_sale_sku,
	c.final_amt as  no_sale_inv_amt,
	d.inv_sku hight_entry_sku,
	d.receive_amt hight_entry_amt
from 
(select
    dc_type,
    dc_uses,
    dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
	dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	inv_sku,
	final_qty,
	final_amt,
	cost_30day,
	sales_30day,
    qty_30day,
	period_inv_qty_30day,
	period_inv_amt_30day,
	days_turnover_30
from csx_tmp.temp_hight_sku_00
where dc_type !=''
) a 	
left join 
csx_tmp.temp_hight_sku_01 b on a.dc_code=b.dc_code and a.dept_id=b.dept_id
left join 
csx_tmp.temp_hight_sku_02 c on a.dc_code=c.dc_code and a.dept_id=c.dept_id
left join 
 csx_tmp.temp_hight_sku_03 d on a.dc_code=d.dc_code and a.dept_id=d.dept_id
;


(select
    dc_type,
    dc_uses,
    dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
	dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	inv_sku,
	final_qty,
	final_amt,
	cost_30day,
	sales_30day,
    qty_30day,
	period_inv_qty_30day,
	period_inv_amt_30day,
	days_turnover_30,
	0 hight_sku,
	0 hight_amt,
	0 no_sale_sku,
	0 no_sale_inv_amt,
	0 hight_entry_sku,
	0 hight_entry_amt
from csx_tmp.temp_hight_sku_00
) a 
-- 高库存
union all 
select
    dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
	dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	inv_sku,
	final_qty,
	final_amt,
	cost_30day,
	sales_30day,
	qty_30day,
	period_inv_qty_30day,
	period_inv_amt_30day,
    days_turnover_30
from  csx_tmp.temp_hight_sku_01
-- 未销售
union all 
select
    dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
    dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	inv_sku,
	final_qty,
	final_amt,
    cost_30day,
    sales_30day,
    qty_30day,
    period_inv_qty_30day,
    period_inv_amt_30day,
    days_turnover_30
from  csx_tmp.temp_hight_sku_02
;

-- 未销售明细
select
     dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	a.dc_code,
	dc_name ,
    dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	a.goods_id ,
	goods_name ,
	c.unit_name ,
	qualitative_period,
	final_qty,
	final_amt,
	DMS,
	inv_sales_days ,
--	cost_30day,
--	sales_30day,
--	qty_30day,
--	period_inv_qty_30day,
--	period_inv_amt_30day,
--	days_turnover_30,
	no_sale_days ,
	max_sale_sdt ,
	entry_sdt ,
	entry_value ,
	entry_qty,
	entry_days ,
	if (sales_return_tag='1','可退','不可退') as return_note,
	product_status_name
from csx_tmp.ads_wms_r_d_goods_turnover a
join 
(select goods_id,qualitative_period,unit_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_id =c.goods_id
join 
(select shop_code ,sales_return_tag,product_code,product_status_name from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') d 
	on a.goods_id =d.product_code and a.dc_code =d.shop_code 
where   a.sdt='20201103'
	-- sdt='20201103' 
	--and dc_code ='W0A3'
	--and no_sale_days >=30
	and final_amt >0
	--and entry_days >=3
	and ((division_code in ('10','11') and no_sale_days >3)
		or (division_code in ('12') and no_sale_days >60) 
		or (division_code in ('13') and no_sale_days >90))

;
refresh csx_dw.wms_shipped_day_report ;

select * from csx_ods.source_wms_r_d_bills_config where sdt='20201103' and final_amt =0.01;

-- 高周转明细=2000
select
    dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
    dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	a.goods_id ,
	goods_name ,
	brand_name ,
	b.unit_name ,
	
	qualitative_period,
	inv_sales_days ,
	final_qty,
	final_amt,
	dms ,
	no_sale_days ,
	max_sale_sdt ,
	entry_sdt ,
	entry_value ,
	entry_qty,
	entry_days,
	if (sales_return_tag='1','可退','不可退') as return_note,
	product_status_name
from
	 csx_tmp.ads_wms_r_d_goods_turnover a
	join 
(select goods_id,qualitative_period,unit_name,brand_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_id =b.goods_id
join 
(select shop_code ,sales_return_tag,product_code,product_status_name from csx_dw.dws_basic_w_a_csx_product_info where sdt='current') d 
	on a.goods_id =d.product_code and a.dc_code =d.shop_code 
where
	sdt = '20201101'
	--and dc_code = 'W0A3'
	--and period_inv_amt_30day != 0
	 and dept_id in ('H01', 'A01', 'A02', 'A03', 'A04', 'A10', 'H04', 'H05', 'H06','A05', 'A06', 'A07', 'A08', 'A09') 
	and ( (final_amt > 3000	and entry_days>15 
			and ((dept_id in ('H01', 'A01', 'A02', 'A03', 'A04', 'A10', 'H04', 'H05', 'H06') and inv_sales_days >=60)
	    	or  (dept_id in ('A05', 'A06', 'A07', 'A08', 'A09') and inv_sales_days >=90)))
	    or (final_amt>3000 and final_qty>1 and( dms<=0.01 or dms is null ) )
	  )
	;
	

-- 入库大金额

select
     dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
    dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	a.goods_id ,
	goods_name ,
	unit_name ,
	qualitative_period,
	final_qty,
	final_amt,
	cost_30day,
	sales_30day,
	qty_30day,
	period_inv_qty_30day,
	period_inv_amt_30day,
	days_turnover_30,
	no_sale_days ,
	max_sale_sdt ,
	entry_sdt ,
	entry_value ,
	entry_qty,
	entry_days 	
from csx_tmp.ads_wms_r_d_goods_turnover a
join 
(select goods_id,qualitative_period,unit_name ni from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_id =b.goods_id
where sdt='20201101' 
	--and dc_code ='W0A3'
	and entry_value >=5000
	and dc_type ='仓库'
	and entry_sdt='20201101'
	--and dept_id in ('H01','A01','A02','A03','A04','A10','H04','H05','H06','A05','A06','A07','A08','A09')
;


-- 工厂期末库存商品>5000
select
    dc_type,
    dc_uses,
	dist_code ,
	dist_name ,
	dc_code,
	dc_name ,
    dept_id,
	dept_name,
	business_division_code,
	business_division_name,
	division_code,
	division_name,
	a.goods_id ,
	goods_name ,
	unit_name,
	qualitative_period,
	final_qty,
	final_amt,
	entry_sdt ,
	entry_value ,
	entry_qty,
	entry_days 	
from
	csx_tmp.ads_wms_r_d_goods_turnover a
	join 
(select goods_id,qualitative_period,unit_name n from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_id =b.goods_id
where
	sdt = '20201101'
	--and dc_code = 'W0A3'
	--and period_inv_amt_30day != 0
	and final_amt >= 5000
	and dc_type ='工厂'
	--and dept_id in ('H01','A01','A02','A03','A04','A10','H04','H05','H06','A05','A06','A07','A08','A09') 
	;
	

SELECT sale_order_code,
         local_purchase_order_code,
          product_code,
          qty AS pick_qty
   FROM csx_ods.source_scm_r_a_scm_local_purchase_request --地采请求表
   WHERE sdt='20201027'
     AND status =2
     AND qty !=0;
     
    select sdt from csx_ods.source_wms_r_d_task group by sdt;
    select * from csx_tmp.ads_wms_r_d_warehouse_sales A where sdt> ='20200101' and warehouse_sales_qty is  null ;