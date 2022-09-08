drop table  csx_tmp.temp_order ;
CREATE table csx_tmp.temp_order as 
SELECT credential_no,
       a.location_code,
       source_order_no,
       purchase_batch_no,
       wms_batch_no,
       wms_order_no,
       a.goods_code,
       a.qty,
       a.amt,
       a.price,
       b.business_type
FROM csx_dw.dws_cas_r_d_account_credential_detail a 
LEFT JOIN 
csx_dw.dws_scm_r_d_order_detail b on a.source_order_no=b.order_code and a.goods_code=b.goods_code
left join 
(SELECT credential_no as purchase_crdential_no,
       batch_no as purchase_batch_no,
       product_code,
       qty,
       amt,
       price
FROM csx_dw.dwd_cas_r_d_accounting_stock_log_item
WHERE sdt>='20210101'
  AND in_out_type='PURCHASE_IN') c on a.credential_no=c.purchase_crdential_no and a.goods_code=c.product_code
WHERE a.sdt>='20210101'
    and a.sdt<='202208016'
  AND move_type='101A'
 -- and b.business_type=1
;



drop table csx_tmp.tmp_sale_detal ;
create table csx_tmp.tmp_sale_detal as 
    select 
      sdt,
	  split(id, '&')[0] as credential_no,
	  order_no,
      region_code,
      region_name,
      province_code,
      province_name,
	  city_group_code,
	  city_group_name,
	  business_type_name,
	  dc_code, 
      customer_no,
      customer_name,
      goods_code,
      goods_name,
	  is_factory_goods_desc,
      sales_qty,
      sales_value,
      excluding_tax_sales ,
      excluding_tax_cost , 
      excluding_tax_profit ,
      sales_cost,
      profit,
	  front_profit,
	  purchase_price_flag,
      cost_price,
      case when purchase_price_flag='1' then purchase_price end as purchase_price,
      middle_office_price,
      sales_price,
      division_code,
      classify_large_code
    from csx_dw.dws_sale_r_d_detail 
    where sdt>='20220701' and sdt<'20220801'
-- 	and channel_code in ('1', '7', '9')
-- 	and business_type_code ='4'
	and sales_type<>'fanli'
	and division_code in('11','10','12')
--	and province_code ='24'
;


-- 查找销售批次号
drop table csx_tmp.temp_batch_sale;
create table csx_tmp.temp_batch_sale as 
select 
    province_code,
    province_name,
    a.credential_no,
    order_no,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    sales_price,
    a.sales_cost,
    a.sales_value,
    a.sales_qty,
    a.profit,
    excluding_tax_sales ,
    excluding_tax_cost , 
    excluding_tax_profit ,
    b.batch_no,
    qty,
    amt,
    amt_no_tax,
    price
FROM csx_tmp.tmp_sale_detal a
left join
(SELECT credential_no,
       batch_no,
       product_code,
       qty,
       amt,
       amt_no_tax ,
       price
FROM csx_dw.dwd_cas_r_d_accounting_stock_log_item
WHERE sdt>='20210101'
  AND in_out_type='SALE_OUT') b on a.credential_no=b.credential_no and a.goods_code=b.product_code;
  
 
 -- 根据批次号查找采购入库凭证 
 drop table  csx_tmp.temp_batch_sale_01;
 create table  csx_tmp.temp_batch_sale_01 as 
 select 
    province_code,
    province_name,
    a.credential_no,
    order_no,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    sales_price,
    a.sales_cost,
    a.sales_value,
    a.sales_qty,
    excluding_tax_sales ,
    excluding_tax_cost , 
    excluding_tax_profit ,
    a.qty,
    a.amt,
    a.amt_no_tax,
    a.price,
    a.profit,
    purchase_crdential_no,
    a.batch_no,
    b.qty as pur_qty,
    b.amt as pur_amt,
    b.amt_no_tax as pur_amt_no_tax,
    b.price as pur_price
from  csx_tmp.temp_batch_sale a 
 left join
(SELECT credential_no as purchase_crdential_no,
       batch_no as purchase_batch_no,
       product_code,
       qty,
       amt,
       amt_no_tax,
       price
FROM csx_dw.dwd_cas_r_d_accounting_stock_log_item
WHERE sdt>='20210101'
  AND in_out_type='PURCHASE_IN') b on a.batch_no=b.purchase_batch_no and a.goods_code=b.product_code
  ;
  
-- select * from csx_tmp.temp_batch_sale_01 where sales_qty>0;
 
  -- 根据成品批次号查找领料凭证号 
drop table  csx_tmp.temp_batch_sale_02;
create table  csx_tmp.temp_batch_sale_02 as 
select a.*,transfer_crdential_no,transfer_qty,transfer_amt,transfer_price,transfer_amt_no_tax 
from csx_tmp.temp_batch_sale a 
 left join
(SELECT credential_no as transfer_crdential_no,
       batch_no as transfer_batch_no,
       product_code,
       qty as   transfer_qty,
       amt as   transfer_amt,
       amt_no_tax as transfer_amt_no_tax,
       price as transfer_price
FROM csx_dw.dwd_cas_r_d_accounting_stock_log_item
WHERE sdt>='20210101'
    and sdt<='20220816'
  AND in_out_type='FINISHED'
  and in_or_out=0
  ) b on a.batch_no=b.transfer_batch_no and a.goods_code=b.product_code
where  sales_qty>0
;


-- select * from csx_tmp.temp_batch_sale_02 where sales_qty>0 and transfer_batch_no is not null;

-- 根据领料凭证号查找原料批次号
drop table  csx_tmp.temp_batch_sale_03;
create table  csx_tmp.temp_batch_sale_03 as 
select  a.transfer_crdential_no,
    goods_code,
    transfer_qty,
    transfer_amt,
    transfer_price,
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    meta_amt_no_tax,
    meta_amt/sum(meta_amt)over(partition by transfer_crdential_no ) as ratio
from
    (select transfer_crdential_no,
        goods_code,
        transfer_qty,
        transfer_amt,
        transfer_price
    from csx_tmp.temp_batch_sale_02
      where transfer_crdential_no is not null 
    group by transfer_crdential_no,
        goods_code,
        transfer_qty,
        transfer_amt,
        transfer_price
    ) a 
 left join
(SELECT credential_no as meta_crdential_no,
       batch_no as meta_batch_no,
       product_code,
       sum(qty) as meta_qty,
       sum(amt) as meta_amt,
       sum(amt_no_tax) meta_amt_no_tax
FROM csx_dw.dwd_cas_r_d_accounting_stock_log_item
WHERE sdt>='20210101'
  AND in_out_type='FINISHED'
  and in_or_out=1
  group by credential_no ,
       batch_no ,
       product_code
  ) b on a.transfer_crdential_no=b.meta_crdential_no 

;


-- 判断是否基地

drop table csx_tmp.tmp_purchase_jd ;
CREATE table csx_tmp.tmp_purchase_jd as
select batch_no,business_type,source_order_no,wms_batch_no,
       wms_order_no from 
(select distinct batch_no
from 
(select distinct meta_batch_no batch_no  from  csx_tmp.temp_batch_sale_03 a 
union all 
select distinct batch_no  from csx_tmp.temp_batch_sale_01
where purchase_crdential_no is not null 
) a
)a
left join 
(SELECT  
       purchase_batch_no,
       a.business_type,
       source_order_no,
       wms_batch_no,
       wms_order_no
FROM  csx_tmp.temp_order a
    group by  purchase_batch_no,
       a.business_type,
       source_order_no,
       wms_batch_no,
       wms_order_no
) b on a.batch_no = b.purchase_batch_no
where business_type=1

;




-- 计算占比，根据销售凭证号计算占比
drop table csx_tmp.tmp_batch_sale_03 ;
create table csx_tmp.tmp_batch_sale_03 as 
select a.*, 
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    meta_amt/sum(meta_amt)over(partition by credential_no,a.goods_code ) as ratio
from csx_tmp.temp_batch_sale_02 a 
join 
(select a.transfer_crdential_no,
    goods_code,
    transfer_qty,
    transfer_amt,
    transfer_price,
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    ratio
from csx_tmp.temp_batch_sale_03 a 
join
 csx_tmp.tmp_purchase_jd b on a.meta_batch_no=b.batch_no
where b.batch_no is not null
  and b.business_type=1
  ) b on b.transfer_crdential_no =a.transfer_crdential_no and a.goods_code=b.goods_code
 ;


-- 工厂商品
drop table csx_tmp.temp_puracse;
create table csx_tmp.temp_puracse as 
select a.*, 
    meta_batch_no,
    source_order_no,
    product_code,
    product_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    meta_qty,
    meta_amt,
    meta_amt_no_tax,
    meta_amt/sum(meta_amt)over(partition by credential_no,a.goods_code ) as ratio,
    produt_ratio,
    order_qty,
    order_amt,
    business_type
from csx_tmp.temp_batch_sale_02 a 
left join 
(SELECT a.transfer_crdential_no,
       goods_code,
       transfer_qty,
       transfer_amt,
       transfer_price,
       meta_batch_no,
       source_order_no,
       product_code,
       goods_name as product_name,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       meta_qty,
       meta_amt,
       meta_amt_no_tax,
       order_qty,
       order_amt,
       ratio as produt_ratio,
       business_type
FROM csx_tmp.temp_batch_sale_03 a
left JOIN
  (SELECT DISTINCT meta_batch_no batch_no,
                   business_type,
                   source_order_no,
                   wms_batch_no,
                   wms_order_no,
                   order_qty,
                   order_amt
   FROM csx_tmp.temp_batch_sale_03 a
   LEFT JOIN
     (SELECT  purchase_batch_no,
                      a.business_type,
                      source_order_no,
                      wms_batch_no,
                      wms_order_no,
                       sum(a.qty) order_qty,
                       sum(a.amt) order_amt
      FROM csx_tmp.temp_order a
      group by purchase_batch_no,
                      a.business_type,
                      source_order_no,
                      wms_batch_no,
       wms_order_no
    ) b ON a.meta_batch_no = b.purchase_batch_no
) b ON a.meta_batch_no=b.batch_no
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          classify_small_code,
          classify_small_name
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') g ON a.product_code=g.goods_id
WHERE b.batch_no IS NOT NULL
) b on b.transfer_crdential_no =a.transfer_crdential_no and a.goods_code=b.goods_code
  where a.classify_large_code='B02'
    and b.transfer_crdential_no is not null


;


select a.*,
(a.sales_price* qty ) * a.produt_ratio as product_sale_amt,
(a.sales_price/(1+tax_rate) * qty ) * a.produt_ratio as product_sale_amt_no_tax,
amt * produt_ratio as product_cost_amt,
a.amt_no_tax * produt_ratio as product_cost_amt_no_tax,
(sales_price*qty*produt_ratio-a.amt * a.produt_ratio) product_profit,
(a.sales_price/(1+tax_rate)*qty*produt_ratio-a.amt_no_tax * a.produt_ratio) product_profit_no_tax,
if(b.credential_no is not null ,'含有基地订单','') as note 
from csx_tmp.temp_puracse a
left join 
(select distinct credential_no from csx_tmp.temp_puracse where business_type=1) b on a.credential_no=b.credential_no
join
(select goods_id,tax_rate/100 tax_rate from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id
where business_type=1;


 
 select 
    province_code,
    province_name,
    a.credential_no,
    order_no,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    tax_rate,
    sales_price,
    a.sales_cost,
    a.sales_value,
    a.sales_qty,
    a.profit,
    excluding_tax_sales ,
    excluding_tax_cost , 
    excluding_tax_profit ,
    a.qty,
    a.amt,
    a.amt_no_tax,
    a.price,
    purchase_crdential_no,
    a.batch_no,
    -- qty as pur_qty,
    -- amt as pur_amt,
    -- pur_amt_no_tax,
    -- price as pur_price,
    source_order_no,
    wms_batch_no,
    wms_order_no,
    order_qty,
    order_amt,
    a.sales_price*qty as cb_sale,
   ( a.sales_price/(1+tax_rate))*a.qty as no_tax_sale,
   a.amt as cb_cost,
   a.amt_no_tax as no_tax_cost,
   ( a.sales_price/(1+tax_rate))*a.qty-amt_no_tax as cb_no_tax_profit,
    a.sales_price*qty-amt as cb_profit,
   ( (a.sales_price/(1+tax_rate))*a.qty-amt_no_tax)/( a.sales_price/(1+tax_rate)*a.qty) as cb_no_tax_profit_rate,
    (a.sales_price*qty-amt)/(a.sales_price*qty) as cb_profit_rate
from csx_tmp.temp_batch_sale_01 a
left join 
(SELECT  purchase_batch_no,
         a.business_type,
         source_order_no,
         wms_batch_no,
         wms_order_no,
         goods_code,
         sum(a.qty) order_qty,
         sum(a.amt) order_amt
      FROM csx_tmp.temp_order a
      group by purchase_batch_no,
                      a.business_type,
                      source_order_no,
                      wms_batch_no,
                      wms_order_no,
                      a.goods_code
    ) b on a.batch_no=b.purchase_batch_no and b.goods_code=a.goods_code
join
(select goods_id,tax_rate/100 tax_rate from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id
where business_type=1
 ;


 
 
 
-- 分割线
select round(sum(sales_value*ratio),4) as sale_amt,round(sum(profit*ratio),4) as profit_amt,round(sum(sales_cost*ratio) ,4) sale_cost from 
(
select a.*, 
    meta_batch_no,
    source_order_no,
    product_code,
    product_name,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    meta_qty,
    meta_amt,
    meta_amt/sum(meta_amt)over(partition by credential_no,a.goods_code ) as ratio,
    order_qty,
    order_amt,
    business_type
from csx_tmp.temp_batch_sale_02 a 
left join 
(SELECT a.transfer_crdential_no,
       goods_code,
       transfer_qty,
       transfer_amt,
       transfer_price,
       meta_batch_no,
       source_order_no,
       product_code,
       goods_name as product_name,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       meta_qty,
       meta_amt,
       order_qty,
      order_amt,
       ratio,
       business_type
FROM csx_tmp.temp_batch_sale_03 a
left JOIN
  (SELECT DISTINCT meta_batch_no batch_no,
                   business_type,
                   source_order_no,
                   order_qty,
                    order_amt
   FROM csx_tmp.temp_batch_sale_03 a
   LEFT JOIN
     (SELECT  purchase_batch_no,
                      a.business_type,
                      source_order_no,
                       sum(a.qty) order_qty,
                       sum(a.amt) order_amt
      FROM csx_tmp.temp_order a
      group by purchase_batch_no,
                      a.business_type,
                      source_order_no
    ) b ON a.meta_batch_no = b.purchase_batch_no
) b ON a.meta_batch_no=b.batch_no
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          classify_small_code,
          classify_small_name
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') g ON a.product_code=g.goods_id
WHERE b.batch_no IS NOT NULL
) b on b.transfer_crdential_no =a.transfer_crdential_no and a.goods_code=b.goods_code
  where a.classify_large_code='B02'
    and b.transfer_crdential_no is not null
) b on b.transfer_crdential_no =a.transfer_crdential_no and a.goods_code=b.goods_code
) a ;



select credential_no,goods_code,goods_name,sum(sales_value*ratio) as sale_amt,sum(profit*ratio) as profit_amt from csx_tmp.tmp_batch_sale_03 where credential_no='PZ20220729292690' and goods_code='852410';

select round(sum(sales_value*ratio),4) as sale_amt,round(sum(profit*ratio),4) as profit_amt,round(sum(sales_cost*ratio) ,4) sale_cost from csx_tmp.tmp_batch_sale_03 


where credential_no='PZ20220729292690' and goods_code='852410'
;

select * from  csx_tmp.temp_order where credential_no=  'PZ20220525146086' and goods_

select * from csx_tmp.temp_batch_sale_02 where sales_qty>0 and transfer_batch_no is not null;

select * from csx_tmp.temp_batch_sale_03 where   goods_code='560';

select a.*,b.* from csx_tmp.temp_order a 
join 
csx_tmp.temp_batch_sale_01 b on a.batch_no=b.purchase_batch_no and a.goods_code=b.goods_code
where a.goods_code='560'
;



-- 采购订单入库销售
select 
   sum(a.sales_cost),
   sum(a.sales_value),
   sum(a.sales_qty),
   sum(a.profit)
from (
select a.credential_no,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    sales_price,
    a.sales_cost,
    a.sales_value,
    a.sales_qty,
    a.profit
from   csx_tmp.temp_batch_sale_01 a 
join 
(
select distinct meta_batch_no batch_no from  csx_tmp.temp_batch_sale_03 a 
 join 
(SELECT distinct 
       purchase_batch_no,
       a.business_type
FROM  csx_tmp.temp_order a
) b on a.meta_batch_no = b.purchase_batch_no
where business_type=1
) b on a.batch_no=b.batch_no 
) a 

;

select a.*,b.* from csx_tmp.temp_order a 
join 
csx_tmp.temp_batch_sale_03 b on a.purchase_batch_no=b.meta_batch_no and a.goods_code=b.goods_code
;

select * from csx_tmp.temp_batch_sale_02 a where a.location_code='W080' AND a.goods_code='472';

select * from csx_tmp.temp_batch_sale_01  where purchase_batch_no='CB20220706031488';


select sum(transfer_amt) from(
select transfer_crdential_no,
    goods_code,
    max(transfer_qty) qty,
    max(transfer_amt) transfer_amt,
    max(transfer_price) transfer_price
    from 
(select a.transfer_crdential_no,
    goods_code,
    transfer_qty,
    transfer_amt,
    transfer_price,
    meta_batch_no,
    product_code,
    meta_qty,
    meta_amt,
    meta_price,
    ratio
from csx_tmp.temp_batch_sale_03 a 
join
 csx_tmp.tmp_purchase_jd b on a.meta_batch_no=b.batch_no
where b.batch_no is not null
  and b.business_type=1
  ) b
  group by transfer_crdential_no,
    goods_code
 )  a ;


select 
   sum(a.sales_cost),
   sum(a.sales_value),
   sum(a.sales_qty),
   sum(a.profit)
from (
select a.credential_no,
    a.goods_code,
    a.goods_name,
    division_code,
    classify_large_code,
    sales_price,
    a.sales_cost,
    a.sales_value,
    a.sales_qty,
    a.profit
from   csx_tmp.temp_batch_sale_01 a 
join 
(
select distinct meta_batch_no batch_no from  csx_tmp.temp_batch_sale_03 a 
 join 
(SELECT distinct 
       purchase_batch_no,
       a.business_type
FROM  csx_tmp.temp_order a
) b on a.meta_batch_no = b.purchase_batch_no
where business_type=1
) b on a.batch_no=b.batch_no 
) a 

;