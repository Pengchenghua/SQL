
--共性商品查询，采购入库
drop table  csx_tmp.temp_sku_01 ;
create table csx_tmp.temp_sku_01 as 
SELECT    sales_region_code,
          sales_region_name,
          b.province_code,
          b.province_name,
          a.receive_location_code,
          a.goods_code,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
sum(price*a.receive_qty)/10000 as amt 
FROM csx_dw.dws_wms_r_d_entry_detail a
JOIN
  (SELECT shop_id,
          province_code,
          province_name,
          purchase_org_name,
          purpose_name,
          case when province_name='安徽省' then '1' else  sales_region_code end sales_region_code,
          case when province_name='安徽省' then '华东大区' else sales_region_name end sales_region_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     and purchase_org !='P620'
     and sales_province_name not in ('广东省','河南省','湖北省')
     AND purpose IN ('01',
                     '02',
                     '03',
                     '04',
                     '05',
                     '07',
                     '08')) b on a.receive_location_code=b.shop_id
join 
(select goods_id,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' ) c on a.goods_code =c.goods_id
WHERE sdt>='20210201'
  AND sdt<'20210801'
  and a.receive_status in (1,2)
  AND order_type_code LIKE 'P%'
  group by  sales_region_code,
          sales_region_name,
          b.province_code,
          b.province_name,
          a.receive_location_code,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,a.goods_code
          
          ;
          

drop table  csx_tmp.temp_sku_06;
create temporary table csx_tmp.temp_sku_06 as 
select sales_region_code,
          sales_region_name,
          province_code,
          province_name,
          goods_code,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          amt,
          sum(amt)over(partition by sales_region_code ) as total_amt,
          -- count( province_code)over(partition by goods_code,sales_region_code ) as aa,
          size( collect_set(province_name)over(partition by goods_code,sales_region_code )) as aa,
          concat_ws('|', collect_set(province_name)over(partition by goods_code,sales_region_code )) as bb,
         -- count( province_code)over(partition by goods_code ) as cc,
         size( collect_set(province_name)over(partition by goods_code )) as cc,
          concat_ws('|', collect_set(province_name)over(partition by goods_code )) as dd,
         -- count( sales_region_name)over(partition by goods_code ) as gg,
         size( collect_set(sales_region_code)over(partition by goods_code )) as gg,
          concat_ws('|', collect_set(sales_region_code)over(partition by goods_code )) as hh
from 
(select  sales_region_code,
          sales_region_name,
          province_code,
          province_name,
          goods_code,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          sum(amt) amt
from  csx_tmp.temp_sku_01 
group by 
          sales_region_code,
          sales_region_name,
          province_code,
          province_name,
          goods_code,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name
)a ;


-- 大区总SKU\总入库额

create temporary table csx_tmp.temp_sku_03 as 
select sales_region_code,sales_region_name, classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          count(distinct goods_code) sku,
          sum(amt) amt
from  csx_tmp.temp_sku_06
group by sales_region_code,sales_region_name, classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name
union all 
select '00'sales_region_code, ' 'sales_region_name, classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          count(distinct goods_code) sku,
          sum(amt) amt
from  csx_tmp.temp_sku_06
          group by  classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name
;
   
 
-- 大区共性商品统计 
drop table csx_tmp.temp_sku_04;
 create temporary table csx_tmp.temp_sku_04 as 
 select sales_region_code,
        sales_region_name, 
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          count(distinct goods_code) sku,
          sum(amt) amt
 from 
 (select a.* from  
    csx_tmp.temp_sku_06 a 
     join 
    (SELECT 
          count(distinct province_code) as prov_cnt,
          sales_region_code,
          sales_region_name
          from csx_tmp.temp_sku_01
    group by sales_region_code,
          sales_region_name ) b on a.sales_region_code =b.sales_region_code and aa=prov_cnt
) a 
group by sales_region_code,
        sales_region_name, 
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name;
          
          
-- 全国共性商品统计 
drop table csx_tmp.temp_sku_05;
 create temporary table csx_tmp.temp_sku_05 as 
 select '00'sales_region_code,
        '' sales_region_name, 
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          count(distinct goods_code) sku,
          sum(amt) amt
 from 
 (select a.* from  
    csx_tmp.temp_sku_06 a 
    join 
    (SELECT 
          count(distinct sales_region_code) as prov_cnt
          from csx_tmp.temp_sku_01
     ) b on gg=prov_cnt
) a 
group by 
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name
;



drop table  csx_tmp.tmp_sku_00;
create table csx_tmp.tmp_sku_00 as 
select    a.sales_region_code,
          a.sales_region_name, 
          a.classify_large_code,
          a.classify_large_name,
          a.classify_middle_code,
          a.classify_middle_name,
          a.sku as all_sku,
          b.sku,
          a.amt as all_amt ,
          b.amt,
          b.amt/a.amt as ratio
from  csx_tmp.temp_sku_03 a 
left join 
(select sales_region_code,
        sales_region_name, 
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          sku,amt 
from  csx_tmp.temp_sku_04 
union all 
select  sales_region_code,
        sales_region_name, 
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          sku,amt 
from  csx_tmp.temp_sku_05
)b on a.sales_region_code=b.sales_region_code and a.classify_large_code=b.classify_large_code and a.classify_middle_code=b.classify_middle_code
;

--select * from csx_tmp.tmp_sku_00 ;

show create table csx_dw.dws_sss_r_a_customer_accounts;


drop table  csx_tmp.temp_sku_goods_01;

create table csx_tmp.temp_sku_goods_01 as 
select    a.sales_region_code,
          a.sales_region_name,
          a.province_code,
          a.province_name,
          a.goods_code,
          goods_name,
          a.classify_large_code,
          a.classify_large_name,
          a.classify_middle_code,
          a.classify_middle_name,
          amt,
          total_amt,
          aa,
          bb,
          cc,
          dd,
          gg,
          hh ,
          d.prov_cnt ,
          c.prov_cnt region_cnt,
          if(aa=d.prov_cnt ,'省区共性','省区非共性') as note,
          if(gg=c.prov_cnt ,'全国共性','全国非共性') as note1,
          if(cc=j.prov_cnt ,'全国共性','全国非共性') as note2
from  csx_tmp.temp_sku_06 a 
join 
(select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_code=b.goods_id
left join 
    (SELECT 
          count(distinct province_code) as prov_cnt,
          sales_region_code,
          sales_region_name
          from csx_tmp.temp_sku_01
    group by sales_region_code,
          sales_region_name ) d on a.sales_region_code =d.sales_region_code and aa=d.prov_cnt
 left join 
    (SELECT 
          count(distinct sales_region_code) as prov_cnt
          from csx_tmp.temp_sku_01
     ) c on gg=c.prov_cnt
  left join 
    (SELECT 
          count(distinct province_code) as prov_cnt
          from csx_tmp.temp_sku_01
     ) j on cc=j.prov_cnt         ;
          
          
          select * from  csx_tmp.temp_sku_goods_01 ;