
set orderedate ='2021-03-11';
select  regexp_replace(to_date(date_sub(${hiveconf:orderedate},30)),'-','') ;
select * from csx_tmp.tmp_sale;
show create table csx_dw.ads_supply_order_flow ;
create table csx_tmp.tmp_sale as 
select
	purchase_org_code,
	purchase_org_name,
	location_code,
	location_name,
    super_class ,
	order_code,
	link_order_code,     
	receive_location_code                             ,
	receive_location_name                             ,
	settle_location_code                              ,
	settle_location_name                              ,
	shipped_location_code                             ,
	shipped_location_name                             ,
	supplier_code                                     ,
	supplier_name                                     ,
	a.goods_code                                        ,
	bar_code                                          ,
	goods_name                                        ,
	spec                                              ,
	pack_qty                                          ,
	unit                                              ,
	category_code                                     ,
	category_name                                     ,
	classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
	purchase_group_code                               ,
	purchase_group_name                               ,
	category_large_code                               ,
	category_large_name                               ,
	category_middle_code                              ,
	category_middle_name                              ,
	a.category_small_code                               ,
	category_small_name                               ,
	tax_code                                          ,
	tax_rate                                          ,
	order_price                   ,
	order_qty                                         ,
	order_amt                                         ,
	receive_qty                                       ,
	receive_amt                                       ,
    no_tax_receive_amt,
	shipped_date ,
    shipped_qty                                  ,
    shipped_amt                                 ,
    no_tax_shipped_amt,	
	source_type                                                                   ,
    source_type_name ,
    order_status ,
	local_purchase_flag,	
	receive_date ,
	order_create_date,
    order_update_date ,
    avg_sales_qty,
    order_create_by
from
(
select
	purchase_org_code,
	purchase_org_name,
	location_code,
	location_name,
	case
		when super_class='1'
			then '供应商订单'
		when super_class='2'
			then '供应商退货订单'
		when super_class='3'
			then '配送订单'
		when super_class='4'
			then '返配订单'
	end super_class ,
	order_code,
	link_order_code,     
	receive_location_code                             ,
	receive_location_name                             ,
	settle_location_code                              ,
	settle_location_name                              ,
	shipped_location_code                             ,
	shipped_location_name                             ,
	supplier_code                                     ,
	supplier_name                                     ,
	goods_code                                        ,
	bar_code                                          ,
	goods_name                                        ,
	spec                                              ,
	pack_qty                                          ,
	unit                                              ,
	category_code                                     ,
	category_name                                     ,
	classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
	purchase_group_code                               ,
	purchase_group_name                               ,
	category_large_code                               ,
	category_large_name                               ,
	category_middle_code                              ,
	category_middle_name                              ,
	a.category_small_code                               ,
	category_small_name                               ,
	tax_code                                          ,
	tax_rate                                          ,
	order_amt/ order_qty as order_price                   ,
	order_qty   as order_qty                                         ,
	order_amt   as order_amt                                         ,
	receive_qty as receive_qty                                       ,
	receive_amt as receive_amt                                       ,
	receive_amt/(1+tax_rate/100) as no_tax_receive_amt,
	shipped_date ,
	shipped_qty   as   shipped_qty                                  ,
	shipped_amt      as shipped_amt                                 ,
	shipped_amt/(1+tax_rate/100) as no_tax_shipped_amt,	
	source_type                                                                   ,
	concat(cast(source_type as string) ,' ', source_type_name)as source_type_name ,
    case
		when order_status=1
			then '已创建'
		when order_status=2
			then '已发货'
		when order_status=3
			then '部分入库'
		when order_status=4
			then '已完成'
		when order_status=5
			then '已取消'
	end order_status ,
	if(local_purchase_flag='0','否','是') as local_purchase_flag,	
	receive_date ,
	to_date(order_create_time) order_create_date,
	to_date(order_update_time) order_update_date,
	order_create_by 
from
	csx_dw.ads_supply_order_flow a 
join 
(select shop_code,product_code from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' and joint_purchase_flag=1) m on a.location_code=m.shop_code and a.goods_code=m.product_code
left outer join
(select classify_large_code ,
    classify_large_name ,
    classify_middle_code ,
    classify_middle_name ,
    classify_small_code ,
    classify_small_name ,
    category_small_code 
from csx_dw.dws_basic_w_a_manage_classify_m 
where sdt='current') b on a.category_small_code=b.category_small_code
where
	sdt  = regexp_replace(to_date(${hiveconf:orderedate}),'-','')
) a 
left join 
(select dc_code,goods_code,sum(sales_qty)/30 as avg_sales_qty 
    from csx_dw.dws_sale_r_d_detail where sdt  >=  regexp_replace(to_date(date_sub(${hiveconf:orderedate},30)),'-','') 
        and sdt<=  regexp_replace(to_date(${hiveconf:orderedate}),'-','')
    group by  dc_code,goods_code) b on a.location_code=b.dc_code and a.goods_code=b.goods_code




