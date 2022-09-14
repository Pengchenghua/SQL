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
       
       
       
CREATE temporary table csx_tmp.temp_supplier_fis_02 as 
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
       sum(shipped_qty) shipped_qty,
       sum(shipped_amt) shipped_amt
FROM   csx_tmp.report_fr_r_m_financial_purchase_detail
WHERE sdt<='20220831'
  AND sdt>='20200101'
  and (business_type_name  in ('云超配送','云超配送退货') 
         or supplier_name  like '%永辉%')
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
