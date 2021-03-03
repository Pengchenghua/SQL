
with temp_t1 as 
(select a.order_code,
	case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end as order_class,
	source_type_name,
	received_order_code,
	shipped_order_code
	
from csx_dw.dws_scm_r_d_header_item_price a
	where sdt>='20201101'
	group by 	a.order_code,
	case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end,
	source_type_name,
	received_order_code,
	shipped_order_code),
temp_t2 as 
(
	select
		sdt
		, posting_date
		, receive_location_code as dc_code
		, order_code
		, supplier_code
		, goods_code
		, unit
		, sum(receive_qty) as purchase_qty
		, sum(amount) as purchase_amt
		, 0 return_qty
		, 0 return_amt
	from
		csx_dw.wms_entry_order a
	join 
	(select
		wms_order_no
		,product_code
		,to_date(posting_time) posting_date
	from
		csx_dw.dwd_cas_r_d_accounting_credential_item
		where source_order_type IN ('采购订单')
		and sdt>='20201101'
		and to_date(posting_time) between '2020-12-01' and '2020-12-31'
		and move_type in ('101A','102A','105A')
	group by
		wms_order_no, 
		to_date(posting_time) ,
		product_code) b on a.order_code=b.wms_order_no and a.goods_code=b.product_code
	where
		sdt >= '20201101'
		and sdt <= '20201231'
--		and a.department_id='A02'
--		and receive_location_code ='W0A7'
 		and (entry_type_code like 'P%' or entry_type_code like 'T%' )
		and receive_status = 2
	group by
		sdt
		, receive_location_code
		, supplier_code
		, goods_code
		, unit
		, order_code
		,entry_type
		,entry_type_code
		,posting_date
union all
	select
		send_sdt as sdt
		, posting_date
		, shipped_location_code as dc_code
		, order_no order_code
		, supplier_code
		, goods_code
		, unit
		, 0 purchase_qty
		, 0 purchase_amt
		, sum(coalesce(shipped_qty, 0)) as return_qty
		, sum(amount) as return_amt
	from
		 csx_dw.wms_shipped_order a 
		 join 
	(select
		wms_order_no
		,product_code
		,to_date(posting_time) posting_date
	from
		csx_dw.dwd_cas_r_d_accounting_credential_item
		where source_order_type IN ('采购订单')
		and sdt>='20201101'
		and to_date(posting_time) between '2020-12-01' and '2020-12-31'
		and move_type in ('103A','104A','106A')
	group by
		wms_order_no, 
		to_date(posting_time) ,
		product_code) b on a.order_no=b.wms_order_no and a.goods_code=b.product_code
		where
		send_sdt >= '20201101'
		and send_sdt <= '20201231'
		and status in (6, 7, 8)
		and substr(shipped_type_code,1,2) in('P0','RP','T0','')
	group by
		send_sdt
		, shipped_type
		, shipped_type_code
		, shipped_location_code
		, supplier_code
		, goods_code
		, unit
		, order_no
		, posting_date)
select * from temp_t2 as  t2
join 
temp_t1 as t1 on t1.received_order_code=t2.order_code  
 
;
with temp_t1 as 	
(select
		location_code,
		wms_order_no
		,product_code
		,to_date(posting_time) as posting_date	,
		sum(case when move_type in ('101A','102A','105A') then qty end)  as entry_qty,
		sum(case when move_type in ('101A','102A','105A') then amt end ) as entry_amt,
		sum(case when move_type in ('103A','104A','106A') then qty end)  as shipped_qty,
		sum(case when move_type in ('103A','104A','106A') then amt end ) as shipped_amt		
	from
		csx_dw.dwd_cas_r_d_accounting_credential_item
		where source_order_type IN ('采购订单')
		and sdt>='20201101'
		and to_date(posting_time) between '2020-12-01' and '2020-12-31'
		and move_type in ('101A','102A','105A','103A','104A','106A')
	group by
		location_code,
		wms_order_no, 
		to_date(posting_time) ,
		product_code),
temp_t2 as 
(select p.order_code ,
		case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end super_class_name,p.received_order_code,p.source_type_name  
		from csx_dw.dws_scm_r_d_header_item_price p where sdt>='20200101'
		group by
		p.order_code ,
		case when super_class='1' then '供应商订单'
		when super_class='2' then '供应商退货订单'
		when  super_class='3' then '配送订单'
		when  super_class='4' then '返配订单'
		else super_class end ,
		p.received_order_code,
		p.source_type_name ) 
select sum(entry_amt) from temp_t1 
join 
temp_t2  on temp_t1.wms_order_no=temp_t2.received_order_code ;


select sum(receive_qty*price) from csx_dw.dws_wms_r_d_entry_detail 
where sdt>='20201201' and sdt<='20201231'
and (order_type_code like 'P%'OR order_type_code like 'T%');




select level_id,
    sales_months,
    zone_id,
    zone_name,
    channel,
    channel_name,
    province_code,
    province_name,
    city_group_code ,
    city_group_name ,
    manager_no,
    case when province_name like '%合计' then '' else  manager_name end manager_name,
    all_cust_count,
    all_daily_sale,
    all_plan_sale,
    all_month_sale,
     real_month_sale,
    all_sales_fill_rate,
   real_sales_fill_rate,
    all_last_month_sale,
    all_mom_sale_growth_rate,
    all_plan_profit,
    all_month_profit,
    all_month_profit_fill_rate,
    all_month_profit_rate,
    old_cust_count,
    old_daily_sale,
    old_plan_sale,
    old_month_sale,
    old_sales_fill_rate,
    old_last_month_sale,
    old_mom_sale_growth_rate,
    old_month_profit,
    old_month_profit_rate,
    new_plan_sale_cust_num,
    new_cust_count,
    new_cust_count_fill,
    new_daily_sale,
    new_plan_sale,
    new_month_sale,
    new_month_sale_fill_rate,
    new_last_month_sale,
    new_mom_sale_growth_rate,
    new_month_profit,
    new_month_profit_rate,
    update_time,
    sdt
 from csx_tmp.ads_sale_r_d_zone_supervisor_fr
where sdt='20210112' 
and zone_id ='3' 
and channel='1'
-- and province_code ='24'
and level_id in ('1','0','2','3')
 order by 
 zone_id,case when 	province_code='00' then 0  when province_code='32' then 1 when province_code='24' then 2 when province_code='23' then 3 else cast(province_code as int) end ,level_id asc ,
 case when manager_name like '虚%' then 998
      when manager_name='' then 999
 else 1 end asc;