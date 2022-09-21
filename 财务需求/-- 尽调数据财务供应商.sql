-- 尽调数据财务供应商
select distinct a.source_type_name,a.business_type_name FROM   csx_tmp.report_fr_r_m_financial_purchase_detail a where a.source_type_name='云超物流采购';

-- 销售单号	销售日期	sku编码	sku名称	管理品类	不含税销售额	不含税毛利额	毛利率	客户编码	客户名称

select 
    order_no,
    sdt,
    a.goods_code, 
    b.goods_name,
    b.unit_name,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
    sum(a.excluding_tax_sales) sale_amt,
     sum(a.excluding_tax_profit)  profit,
    if(sum(coalesce(excluding_tax_sales,0))=0,0, sum(excluding_tax_profit)  /sum(excluding_tax_sales)) profit_rate,
    a.customer_no,
    a.customer_name
from csx_dw.dws_sale_r_d_detail  a 
join
(select goods_id,
    goods_name,
    unit_name,
    product_purchase_level ,
    product_purchase_level_name ,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
from  csx_dw.dws_basic_w_a_csx_product_m
 where sdt='current') b on a.goods_code=b.goods_id
where 1=1
    and sdt>='20200101'
    and sdt<='20220630'
and (case when a.province_code not in ('15','2') and a.goods_code in ('1456612' ,'1456631') then '02' else product_purchase_level end ='03')
group by 
    order_no,
    sdt,
    a.goods_code,
    b.goods_name,
    b.unit_name,
    b.classify_large_code,
    b.classify_large_name,
    b.classify_middle_code,
    b.classify_middle_name,
    b.classify_small_code,
    b.classify_small_name,
     a.customer_no,
    a.customer_name;



    SET hive.execution.engine=mr;
CREATE  table csx_tmp.temp_supplier_fis_01 as 
SELECT 
       substr(receive_sdt,1,4) yesr,
       substr(receive_sdt,1,6) mon,
       receive_sdt,
       purchase_order_code,
       supplier_code,
       supplier_name,
       goods_code,
       goods_name,
       sales_region_name,
       sales_province_name,
       sum(receive_qty) qty,
       sum(receive_amt) receive_amt,
       sum(no_tax_receive_amt) no_tax_receive_amt,
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt,
       sum(no_tax_shipped_amt) no_tax_shipped_amt
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail
WHERE sdt<='20220630'
  AND sdt>='20200101'
  and (business_type_name not in ('云超配送','云超配送退货') 
  and supplier_name not like '%永辉%')
  and source_type_name !='云超物流采购'
 GROUP BY purchase_order_code,
       receive_sdt,
       supplier_code,
       supplier_name,
       goods_code,
       goods_name,
       sales_region_name,
       sales_province_name,
       substr(receive_sdt,1,4),
        substr(receive_sdt,1,6)
        distribute by 1
       ;
       
       
 drop table       csx_tmp.temp_supplier_fis_02 ;
CREATE  table csx_tmp.temp_supplier_fis_02 as 
SELECT 
       substr(receive_sdt,1,4) yesr,
       substr(receive_sdt,1,6) mon,
       receive_sdt,
       purchase_order_code,
       supplier_code,
       supplier_name,
       b.classify_large_code,
       b.classify_large_name,
       b.classify_middle_code,
       b.classify_middle_name,
       b.classify_small_code,
       b.classify_small_name,
       goods_code,
       goods_name,
       sales_region_name,
       sales_province_name,
       sum(receive_qty) qty,
       sum(receive_amt) receive_amt,
       sum(no_tax_receive_amt) no_tax_receive_amt,
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt,
       sum(no_tax_shipped_amt) no_tax_shipped_amt,
       b.department_id,
       b.department_name
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail a
join 
(SELECT goods_id,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') b on a.goods_code=goods_id
WHERE sdt<='20220630'
  AND sdt>='20200101'
  and (source_type_name  in ('云超物流采购') 
         or supplier_name  like '%永辉%')
  -- and source_type_name ='云超物流采购'
 GROUP BY receive_sdt,
       purchase_order_code,
       supplier_code,
       supplier_name,
      b.classify_large_code,
      b.classify_large_name,
      b.classify_middle_code,
      b.classify_middle_name,
      b.classify_small_code,
      b.classify_small_name,
       goods_code,
       goods_name,
       sales_region_name,
       sales_province_name,
       substr(receive_sdt,1,4),
        substr(receive_sdt,1,6),
        b.department_id,
        b.department_name
        
       ;
       
select * from csx_tmp.temp_supplier_fis_01 distribute by 1;

select supplier_type_name from csx_tmp.report_fr_r_m_financial_purchase_detail
WHERE sdt<='20220831'
  AND sdt>='20200101';



  SELECT 
       
       supplier_code,
       supplier_name,
       b.supplier_type_name,
       sdt
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail a
join 
(select vendor_id,supplier_type,case when supplier_type='0' then '0_空'  
    when supplier_type='1' then '1_代理商'
    when supplier_type='2' then '2_生产厂商'
    else supplier_type end supplier_type_name
from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id
WHERE
  ( business_type_name  in ('云超配送','云超配送退货') 
    or supplier_name  like '%永辉%'
    )
    and   source_type_name ='云超物流采购'
 GROUP BY supplier_code,
       supplier_name,
       b.supplier_type_name
union all 

;

drop table csx_tmp.csx_supplier_01 ;

CREATE temporary table csx_tmp.csx_supplier_01 as 
SELECT a.supplier_code,
       vendor_name,
       supplier_type_name,
       min_sdt,
       yh_reuse_tag
FROM
(
SELECT supplier_code,
       vendor_name,
       b.supplier_type_name,
       min(sdt) min_sdt
FROM    csx_dw.dws_wms_r_d_entry_detail a
join 
(select vendor_id,vendor_name,supplier_type,case when supplier_type='0' then '0_空'  
    when supplier_type='1' then '1_代理商'
    when supplier_type='2' then '2_生产厂商'
    else supplier_type end supplier_type_name
from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id

WHERE
   ( business_type_name   in ('云超配送','云超配送退货') 
    or supplier_name   like '%永辉%'
    )
   -- and   source_type_name ='云超物流采购'
 GROUP BY supplier_code,
       vendor_name,
       b.supplier_type_name
) a 
LEFT JOIN
(select supplier_code,yh_reuse_tag from csx_tmp.ads_fr_r_m_supplier_reuse 
    where yh_reuse_tag='是'
group by supplier_code,yh_reuse_tag) c on a.supplier_code=c.supplier_code
       ;


select * from  csx_tmp.csx_supplier_01;

CREATE temporary table csx_tmp.csx_supplier_02 as 
SELECT a.supplier_code,
       vendor_name,
       supplier_type_name,
       min_sdt,
       yh_reuse_tag
FROM
(
SELECT supplier_code,
       vendor_name,
       b.supplier_type_name,
       min(sdt) min_sdt
FROM    csx_dw.dws_wms_r_d_entry_detail a
join 
(select vendor_id,vendor_name,supplier_type,case when supplier_type='0' then '0_空'  
    when supplier_type='1' then '1_代理商'
    when supplier_type='2' then '2_生产厂商'
    else supplier_type end supplier_type_name
from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id

WHERE
   ( business_type_name not in ('云超配送','云超配送退货') 
    and supplier_name not like '%永辉%'
    )
   -- and   source_type_name ='云超物流采购'
 GROUP BY supplier_code,
       vendor_name,
       b.supplier_type_name
) a 
LEFT JOIN
(select supplier_code,yh_reuse_tag from csx_tmp.ads_fr_r_m_supplier_reuse 
    where yh_reuse_tag='是'
group by supplier_code,yh_reuse_tag) c on a.supplier_code=c.supplier_code
       ;

select supplier_code ,aa from 
(select  supplier_code,count(*)aa from 
(select supplier_code,yh_reuse_tag from csx_tmp.ads_fr_r_m_supplier_reuse where months='202208'
group by supplier_code,yh_reuse_tag
)a
group by supplier_code
)a where aa>0
;



SELECT *
FROM    csx_dw.dws_wms_r_d_entry_detail a
join 
(select vendor_id,vendor_name,supplier_type,case when supplier_type='0' then '0_空'  
    when supplier_type='1' then '1_代理商'
    when supplier_type='2' then '2_生产厂商'
    else supplier_type end supplier_type_name
from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') b on a.supplier_code=b.vendor_id

WHERE
  ( business_type_name   in ('云超配送','商超直送','云超配送退货','商超直送退供出库') 
    or  vendor_name    like '%永辉%')
     and supplier_code='117035CQ'
 GROUP BY supplier_code,
       vendor_name,
       b.supplier_type_name

   
   

select * from  csx_tmp.csx_supplier_02;

-- 供应商返利
    select substr(regexp_replace(settle_date,'-',''),1,6) mon,supplier_code,supplier_name,invoice_name,sum(net_value)net_value,sum(value_tax_total)bill_total_amount
from  csx_dw.dwd_gss_r_d_settle_bill  
group by substr(regexp_replace(settle_date,'-',''),1,6) ,supplier_code,supplier_name,invoice_name

;

-- 外部供应商
 insert overwrite directory '/tmp/pengchenghua/bb' row format delimited fields terminated by ','     
select yesr `年份`,
       mon `月份`,
       receive_sdt `入库日期`,
       purchase_order_code `采购订单号`,
       supplier_code `供应商编码`,
       supplier_name `供应商名称`,
       goods_code `商品编码`,
        goods_name as  `商品名称`,
       sales_region_name `大区名称`,
       sales_province_name `省区名称`,
       qty as `入库量`,
       receive_amt as `入库额`,
       shipped_qty as `出库量`,
       shipped_amt as `出库额`
from csx_tmp.temp_supplier_fis_01
distribute by 1


;




-- 供应商复用统计
CREATE  table csx_tmp.temp_supplier_reuse_01 as 
SELECT 
       substr(receive_sdt,1,4) yesr,
       concat(substr(receive_sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(receive_sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3) ,2,0)) as q,
       supplier_code,
       yh_reuse_tag,
       sum(receive_qty) qty,
       sum(receive_amt) receive_amt,
       sum(no_tax_receive_amt) no_tax_receive_amt,
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt,
       sum(no_tax_shipped_amt) no_tax_shipped_amt,
       sum(receive_amt-shipped_amt) net_amt,
       sum(no_tax_receive_amt-no_tax_shipped_amt) net_amt_no
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail
WHERE sdt<='20220831'
  AND sdt>='20200101'
  and (business_type_name not in ('云超配送','云超配送退货') 
  and supplier_name not like '%永辉%')
  and source_type_name !='云超物流采购'
 GROUP BY concat(substr(receive_sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(receive_sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3) ,2,0)),
       supplier_code,
       yh_reuse_tag,
       substr(receive_sdt,1,4)
     ;
        
     select yesr,q,
        count(distinct supplier_code),
        sum(net_amt_no) net_amt_no,
        count(distinct case when yh_reuse_tag='是' then  supplier_code end ) reuse_supplier_no,
        sum(distinct case when yh_reuse_tag='是' then  net_amt_no end )reuse_amt
     from  csx_tmp.temp_supplier_reuse_01
     group by yesr,q;   

insert overwrite directory '/tmp/pengchenghua/aa' row format delimited fields terminated by ',' 
select yesr	
,q	
,supplier_code
,vendor_name
,yh_reuse_tag	
,qty	
,receive_amt	
,no_tax_receive_amt	
,shipped_qty	
,shipped_amt	
,no_tax_shipped_amt	
,net_amt	
,net_amt_no	

from csx_tmp.temp_supplier_reuse_01 a 
left join
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current')b on a.supplier_code=b.vendor_id;


select q,suppler_code,supplier_name,no_amt,dense_rank()over(partition by q order by no_amt desc) aa
from 
(select concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd')/3),2,0))) as q,
    supplier_code,
    supplier_name,
    sum(amount_no_tax) no_amt
from  csx_dw.dws_wms_r_d_entry_detail 
where sdt>='20210101' and sdt<='20220630'
and receive_status=2
and order_type_code like 'P%'
AND (business_type!='2' or supplier_name not like '%永辉%')
group by supplier_code,
    supplier_name,
    concat(substr(sdt,1,4),lpad(ceil(month(sdt)/3),2,0)) 
)a

;

create table csx_tmp.report_fina_po_order_detail as 
select a.yesr,
       a.mon,
       a.purchase_order_code,
       a.supplier_code,
       a.supplier_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       goods_code,
       a.goods_name,
       a.sales_region_name,
       a.sales_province_name,
       a.qty,
       a.receive_amt,
       a.no_tax_receive_amt,
       a.shipped_qty,
       a.shipped_amt,
       a.no_tax_shipped_amt,
       department_id,
       department_name
from   csx_tmp.temp_supplier_fis_01 a 
left join 
(SELECT goods_id,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current')b on a.goods_code=goods_id
left join 
(select vendor_id,vendor_name,supplier_type 
    from csx_dw.dws_basic_w_a_csx_supplier_m 
    where sdt='current')c on a.supplier_code=c.vendor_id
;

drop table  csx_tmp.supplier_rankt;
create table csx_tmp.supplier_rankt as 
select concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0)) as q,
    supplier_code,
    supplier_name,
    sum(amount_no_tax) no_amt
from  csx_tmp.re 
where sdt>='20210101' and sdt<='20220630'
and receive_status=2
and order_type_code like 'P%'
AND (business_type!='02' and supplier_name not like '%永辉%')
group by supplier_code,
    supplier_name,
    concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0) )

;

select * from 
(select *, dense_rank()over(partition by q,supplier_type order by no_amt desc)  aa from csx_tmp.supplier_rankt a 
left join 
(select vendor_id,vendor_name,supplier_type 
    from csx_dw.dws_basic_w_a_csx_supplier_m 
    where sdt='current') b on a.supplier_code=b.vendor_id
) a 
where aa<21;

drop table  csx_tmp.supplier_rank;
create table csx_tmp.supplier_rank as 
select concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0)) as q,
    supplier_code,
    supplier_name,
    sum( no_tax_receive_amt ) no_amt,
    sum( no_tax_shipped_amt ) no_shipped_amt
from csx_tmp.report_fr_r_m_financial_purchase_detail 
where sdt>='20210101' and sdt<='20220630'
-- and status=2
and  source_type_code  not in ('8')
AND ( business_type_name not in ('云超配送退货','云超配送') and supplier_name not like '%永辉%')
group by supplier_code,
    supplier_name,
    concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0) )

;
 
select * 
from 
(select *, 
    dense_rank()over(partition by supplier_type order by no_amt desc)  aa 
from csx_tmp.supplier_rank a 
left join 
(select vendor_id,vendor_name,supplier_type 
    from csx_dw.dws_basic_w_a_csx_supplier_m 
    where sdt='current') b on a.supplier_code=b.vendor_id
) a 
where aa<21

;


select supplier_code,supplier_name,sum(amount) 
from csx_dw.dws_wms_r_d_entry_detail 
where sdt>='20220101' and supplier_name like '%易丰%'
group by supplier_code,supplier_name

;

select customer_no,customer_name,sum(sales_value) sale
from csx_dw.dws_sale_r_d_detail where sdt>='20220901'
and customer_no='123629' 
group by
customer_no,customer_name;


drop table csx_tmp.supplier_entry_top ;
create table csx_tmp.supplier_entry_top as 
SELECT 
       a.supplier_code,
       a.supplier_name,
       supplier_type_name,
       (qty) qty,
       (receive_amt) receive_amt,
       (no_tax_receive_amt) no_tax_receive_amt,
       (shipped_qty) shipped_qty,
       (shipped_amt) shipped_amt,
       (no_tax_shipped_amt) no_tax_shipped_amt,
       rank()over(partition by supplier_type_name order by receive_amt desc) aa
FROM (
SELECT 
       a.supplier_code,
       a.supplier_name,
       b.supplier_type_name,
       sum(receive_qty) qty,
       sum(receive_amt) receive_amt,
       sum(no_tax_receive_amt) no_tax_receive_amt,
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt,
       sum(no_tax_shipped_amt) no_tax_shipped_amt
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail a
 join 
csx_tmp.vendor b on a.supplier_code=b.supplier_code
WHERE sdt<='20220630'
  AND sdt>='20210101'
  and (business_type_name not in ('云超配送','云超配送退货') 
  and a.supplier_name not like '%永辉%')
  and source_type_name !='云超物流采购'
 GROUP BY a.supplier_code,
       a.supplier_name,
       b.supplier_type_name
     )a
       ;

drop table csx_tmp.supplier_entry_top20;
create table csx_tmp.supplier_entry_top20 as 
SELECT 
       q,
       a.supplier_code,
       a.supplier_name,
       a.supplier_type_name,
       (a.qty) qty,
       (a.receive_amt) receive_amt,
       (a.no_tax_receive_amt) no_tax_receive_amt,
       (a.shipped_qty) shipped_qty,
       (a.shipped_amt) shipped_amt,
       (a.no_tax_shipped_amt) no_tax_shipped_amt,
       aa
FROM (
SELECT 
       concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0)) as q,
       a.supplier_code,
       a.supplier_name,
       sum(receive_qty) qty,
       sum(receive_amt) receive_amt,
       sum(no_tax_receive_amt) no_tax_receive_amt,
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt,
       sum(no_tax_shipped_amt) no_tax_shipped_amt,
       aa
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join 
 (select * from csx_tmp.supplier_entry_top where aa<21) b on a.supplier_code=b.supplier_code 
WHERE sdt<='20220630'
  AND sdt>='20210101'
  and (business_type_name not in ('云超配送','云超配送退货') 
  and supplier_name not like '%永辉%')
  and source_type_name !='云超物流采购'
 GROUP BY a.supplier_code,
       a.supplier_name,
       b.supplier_type_name,
        concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0))
       ) a 
   join 
 (select * from csx_tmp.supplier_entry_top where aa<21) b on a.supplier_code=b.supplier_code  ;
  
  
  select * from csx_tmp.supplier_entry_top20;
  
  
  
SELECT 
       concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0)) as q,
       a.supplier_code,
       a.supplier_name,
       supplier_type_name,
       sum(receive_qty) qty,
       sum(receive_amt) receive_amt,
       sum(no_tax_receive_amt) no_tax_receive_amt,
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt,
       sum(no_tax_shipped_amt) no_tax_shipped_amt,
       aa
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join 
 (select supplier_code,supplier_name,supplier_type_name,aa from csx_tmp.supplier_entry_top where aa<21) b on a.supplier_code=b.supplier_code 
WHERE sdt<='20220630'
  AND sdt>='20210101'
  and (business_type_name not in ('云超配送','云超配送退货') 
  and a.supplier_name not like '%永辉%')
  and source_type_name !='云超物流采购'
 GROUP BY a.supplier_code,
       a.supplier_name,
       b.supplier_type_name,
       aa,
        concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0))
       


       SELECT 
       a.supplier_code,
       a.supplier_name,
       supplier_type_name,
       (qty) qty,
       (receive_amt) receive_amt,
       (no_tax_receive_amt) no_tax_receive_amt,
       (shipped_qty) shipped_qty,
       (shipped_amt) shipped_amt,
       (no_tax_shipped_amt) no_tax_shipped_amt,
       rank()over(order by receive_amt desc) aa
FROM 
csx_tmp.supplier_entry_top a
;




-- 供应商TOP20
SELECT 
       concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0)) as q,
       a.supplier_code,
       a.supplier_name,
       supplier_type_name,
       sum(receive_qty) qty,
       sum(receive_amt) receive_amt,
       sum(no_tax_receive_amt) no_tax_receive_amt,
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt,
       sum(no_tax_shipped_amt) no_tax_shipped_amt,
       aa
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join 
 (select supplier_code,supplier_name,aa,supplier_type_name from 
 (SELECT 
       a.supplier_code,
       a.supplier_name,
       supplier_type_name,
       (receive_amt) receive_amt,
       rank()over(order by receive_amt desc) aa
FROM 
csx_tmp.supplier_entry_top a) a where aa<21) b on a.supplier_code=b.supplier_code 
WHERE sdt<='20220630'
  AND sdt>='20210101'
  and (business_type_name not in ('云超配送','云超配送退货') 
  and a.supplier_name not like '%永辉%')
  and source_type_name !='云超物流采购'
 GROUP BY a.supplier_code,
       a.supplier_name,
       b.supplier_type_name,
       aa,
        concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0))
;


-- 集采商品入库明细
-- 大区处理

-- 集采商品入库明细
-- 大区处理
drop table csx_tmp.temp_dc_new ;
create  TABLE csx_tmp.temp_dc_new as 
select case when region_code!='10' then '大区'else '平台' end dept_name,
    region_code,
    region_name,
    sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    case when performance_province_name like'平台%' then '00' else   sales_region_code end sales_region_code,
    case when performance_province_name like'平台%' then '平台' else  sales_region_name end sales_region_name,
    shop_id ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
    case when c.dc_code is not null then '1' else '0' end as is_purchase_dc ,
    enable_date
from csx_dw.dws_basic_w_a_csx_shop_m a 
left join 
(select a.code as province_code,a.name as province_name,b.code region_code,b.name region_name 
from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql a 
 left join 
(select code,name,parent_code from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql where level=1)  b on a.parent_code=b.code
 where level=2) b on a.performance_province_code=b.province_code
 left join 
 (select dc_code,regexp_replace(to_date(enable_time),'-','') enable_date from csx_ods.source_basic_w_a_conf_supplychain_location where sdt='20220718') c on a.shop_id=c.dc_code
 where sdt='current'    
      and table_type=1 
    ;
    
    

drop table  csx_tmp.jd_temp_purchase_01 ;
create  table csx_tmp.jd_temp_purchase_01 as 
SELECT substr(receive_sdt,1,4) yesr,
       substr(receive_sdt,1,6) mon,
       receive_sdt,
       purchase_order_code,
       supplier_code,
       supplier_name,
       c.classify_large_code,
       c.classify_large_name,
       c.classify_middle_code,
       c.classify_middle_name,
       c.classify_small_code,
       c.classify_small_name,
       goods_code,
       goods_name,
       a.sales_region_name,
       a.sales_province_name,
       (receive_qty) qty,
       (receive_amt) receive_amt,
       (no_tax_receive_amt) no_tax_receive_amt,
       (shipped_qty) shipped_qty,
       (shipped_amt) shipped_amt,
       (no_tax_shipped_amt) no_tax_shipped_amt,
       c.department_id,
       c.department_name
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
 join 
 csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
join csx_tmp.temp_dc_new  d on a.dc_code=d.shop_id 
join 
(SELECT goods_id,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name
FROM csx_dw.dws_basic_w_a_csx_product_m
WHERE sdt='current') c on a.goods_code=c.goods_id
WHERE months <= '202208'
    and months >= '202201'
    and is_purchase_dc=1
    and joint_purchase_flag=1 
    and b.classify_small_code IS NOT NULL
  and source_type_name not in  ('城市服务商','联营直送','项目合伙人')
 ;
  insert overwrite directory '/tmp/pengchenghua/aa' row format delimited fields terminated by ','     

  select * from csx_tmp.temp_purchase_01 
  ;