select 
  t1.credential_no,
  t1.region_code,
  t1.region_name,
  t1.province_code,
  t1.province_name,
  t1.city_group_code,
  t1.city_group_name,  
--   t1.customer_no,
--   t4.customer_name,
  t1.goods_code,
  t5.goods_name,
  goods_type_name,
  product_code,
  product_name,
  t1.sales_qty,
  t1.sales_value,
  t1.sales_cost,
  t1.profit,
  t1.cost_price,
  t1.purchase_price,
  t1.middle_office_price,
  t1.sales_price,
  t3.fact_price,
  t1.goods_group_sales_value,
  t1.goods_group_profit
from 
   ( select 
      split(id, '&')[0] as credential_no,
      region_code,
      region_name,
      province_code,
      province_name,
	  city_group_code,
  	  city_group_name,
      goods_code,
      goods_name,
      sales_qty,
      sales_value,
      sales_cost,
      profit,
	   purchase_price_flag,
      case when sales_type<>'fanli' then cost_price end as cost_price,
      case when purchase_price_flag='1' and sales_type<>'fanli' then purchase_price end as purchase_price,
      case when sales_type<>'fanli' then middle_office_price end as middle_office_price,
      case when sales_type<>'fanli' then sales_price end as sales_price,
      sum(sales_value) over(partition by goods_code) as goods_group_sales_value,
      sum(profit) over(partition by goods_code) as goods_group_profit
    from csx_dw.dws_sale_r_d_detail
    where sdt >= '20210301' and sdt <'20210401' 
       -- and channel_code in ('1', '7', '9')
        and classify_middle_code='B0304'
  )t1 
  left outer join 
  (
    select
	  t2.goods_code,
	  t2.credential_no,
	  goods_type_name,
      product_code,
      product_name,
      sum(fact_price) as fact_cost,
	  sum(qty),
	  sum(t3.fact_price*t2.qty)/sum(case when t3.fact_price is not null then t2.qty end) fact_price
	from 
	(
	  select
	  	goods_code,
	  	credential_no,
	  	source_order_no,
	  	sum(qty) as qty
	  from csx_dw.dws_wms_r_d_batch_detail
	  where sdt >='20200101'
	  and move_type in ('107A', '108A')    --销售出库 108 退货入库
	  group by goods_code, credential_no, source_order_no
    )t2 
    left outer join 
    (
      select 
      	goods_code,
      	order_code,
      	goods_type_code,
      	goods_type_name,
      	product_code,
      	product_name,
        sum(fact_values)/sum(fact_qty) as fact_price
      from csx_dw.dws_mms_r_a_factory_order 
      where sdt >= '20200101' 
        and mrp_prop_key in('3061','3010')
       group by goods_code,
        order_code,
        goods_type_code,
      	product_code,
      	goods_type_name,
      	product_name
    )t3 on t2.source_order_no = t3.order_code and t2.goods_code = t3.goods_code
	group by  t2.goods_code,
	  t2.credential_no,
	  goods_type_name,
      product_code,
      product_name
  )t3 on t1.goods_code = t3.goods_code and t1.credential_no = t3.credential_no
  left join 
  (select goods_id,goods_name,classify_small_code,classify_small_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current')t5 on t1.goods_code=t5.goods_id
  ;



show create table csx_dw.dws_mms_r_a_factory_order;  
  --select * from csx_tmp.ads_fr_r_m_end_post_inventory where months='202012';
  
  select distinct vendor_id from dw.inv_sap_setl_dly_fct where vendor_id='G1933' and sdt>='20210101';