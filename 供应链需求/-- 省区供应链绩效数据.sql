-- 省区供应链绩效数据
--省区供应链满足率
drop table  csx_tmp.vendor_sku01;
create temporary table csx_tmp.vendor_sku01
as
select 
  c.province_name,
  city_name city_group_name,
  a.shop_id_in,
  a.shop_name,
  sdt,order_code,
  a.vendor_id,
  b.vendor_name,
  goodsid,
  classify_middle_code,
  classify_middle_name,
  classify_large_code,
  classify_large_name ,
  plan_qty,
  plan_amount,
  receive_qty,
  amount
from 
(
  select 
  sdt,
  order_code,
  supplier_code vendor_id,
  goods_code goodsid,
  receive_location_code shop_id_in,
  receive_location_name shop_name,
  max(case when return_flag='Y' then -1*plan_qty else plan_qty end) plan_qty,
  max(case when return_flag='Y' then -1*plan_qty*price else plan_qty*price end) plan_amount,
  sum(case when return_flag='Y' then -1*receive_qty else receive_qty end) receive_qty,
  sum(case when return_flag='Y' then -1*receive_qty*price else receive_qty*price end) amount
  from csx_dw.dws_wms_r_d_entry_detail a
  where --sdt>=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','') and
   sdt>='20220101' and sdt<='20220131'
  and receive_status=2  --已关业务
  and order_type_code LIKE 'P%' 
  and order_type_code<>'P02' --剔除调拨
  group by sdt,order_code,
  supplier_code,
  goods_code,receive_location_code,receive_location_name)a 
join 
(select shop_id,province_name,cs.city_name from csx_dw.dws_basic_w_a_csx_shop_m cs where sdt='current'and cs.purpose in ('01','02','03')  --1仓库2工厂3	门店
)c on a.shop_id_in =c.shop_id
left join 
(select vendor_id,vendor_name from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current' and frozen='0')b 
on lpad(a.vendor_id,10,'0')=lpad(b.vendor_id,10,'0')
left join 
(select goods_id,
    classify_middle_code,
    classify_middle_name,
    classify_large_code,
    classify_large_name 
from csx_dw.dws_basic_w_a_csx_product_m
where sdt='current') m on a.goodsid=m.goods_id;


-----月满足率 取 sku 、qty、金额满足率的 均值 
select 
    province_name,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    classify_large_code,
    classify_large_name,
    classify_middle_management,
sum(receive_sku) receive_sku,
sum(sku) order_sku,
sum(receive_sku)/sum(sku) as sku_rate,
sum(receive_qty) receive_qty,
sum(plan_qty) plan_qty,
sum(receive_qty)/sum(plan_qty) as qty_rate,
sum(amount) as receive_amount,
sum(plan_amount) as plan_amount,
sum(amount)/sum(plan_amount) as amount_rate
from 
(select
    a.province_name,
    a.city_group_name,
    a.vendor_id,
    a.vendor_name,
    a.sdt,
    a.order_code,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_large_code,
    a.classify_large_name,
    b.classify_middle_management,
    count(distinct a.goodsid) sku,
    count(distinct case when receive_qty<>0 and receive_qty is not null then a.goodsid else null end) receive_sku,
    sum(coalesce(plan_qty,0)) plan_qty,
    sum(coalesce(plan_amount,0)) plan_amount,
    sum(receive_qty) receive_qty,sum(amount) amount
from   csx_tmp.vendor_sku01 a 
left join 
 csx_tmp.report_scm_r_d_classify_province_person b on a.classify_middle_code=b.classify_middle_code and a.city_group_name=b.city_name
group by a.province_name,
    a.city_group_name,
    a.vendor_id,
    a.vendor_name,
    a.sdt,
    a.order_code,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_large_code,
    a.classify_large_name,
    b.classify_middle_management
) a
where province_name ='福建省'
group by province_name,
    city_group_name,
    classify_middle_code,
    classify_middle_name,
    classify_large_code,
    classify_large_name,
    classify_middle_management;


    show create table csx_tmp.report_sale_r_d_zone_classify_sale_fr ;


-- 1、含税销售收入（指部门实际B端销售额，不含城市服务商、不含M端、不含省区大宗、批发内购)
-- 2、毛利率指标说明：B端自营综合毛利额不含税，不含城市服务商。
select a.province_code,
       a.province_name,
       a.city_name,
       a.classify_large_code,
       a.classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       b.classify_middle_management,
       sales_value,
       profit,
       profit/sales_value profit_rate,
       b_daily_sales_value,
       b_daily_profit,
       b_daily_profit/b_daily_sales_value as b_daily_profit_rate,
       b_welfare_sales_value,
       b_welfare_profit,
       b_welfare_profit/b_welfare_sales_value b_welfare_profit_rate,
       bbc_sales_value,
       bbc_profit,
       bbc_profit/bbc_sales_value bbc_profit_rate
 from (
    select province_code,
       province_name,
       city_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       sum(sales_value) sales_value,    -- 自营销售额
       sum(profit)profit,               --自营毛利额
        -- 日配
       sum(case when business_type_code in ('1') then sales_value end ) b_daily_sales_value,   
       sum(case when business_type_code in ('1' ) then profit end ) b_daily_profit,
       sum(case when business_type_code in ('2') then sales_value end ) b_welfare_sales_value,
       sum(case when business_type_code in ('2' ) then profit end ) b_welfare_profit,
       sum(case when business_type_code in ('6') then sales_value end ) bbc_sales_value,
       sum(case when business_type_code in ('6') then profit end ) bbc_profit
 from csx_dw.dws_sale_r_d_detail 
 where sdt='20220101' and sdt<='20220131' 
        and business_type_code !='4'
        and channel_code in ('1','7')
        and province_code='15'
 group by 
    province_code,
    province_name,
    city_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name
 ) a 
 left join 
 csx_tmp.report_scm_r_d_classify_province_person b on a.classify_middle_code=b.classify_middle_code and a.city_name=b.city_name
;
select * from csx_dw.dws_sale_w_a_order_channel_relation;


select * from csx_dw.dws_wms_r_d_accounting_stock_m where sdt='20220209' and  goods_code='8708';


	DROP table csx_tmp.report_scm_r_d_classify_province_person;
CREATE  TABLE `csx_tmp.report_scm_r_d_classify_province_person`(
   sales_months string COMMENT '销售月份',
   province_name string comment '省区名称',
   city_name string comment '城市名称',
  `classify_large_code` string comment '管理一级', 
  `classify_large_name` string comment '管理一级', 
  `classify_middle_code` string comment '管理二级', 
  `classify_middle_name` string comment '管理二级', 
   classify_large_management string comment '管理一级负责人',
   classify_middle_management string comment '管理二级负责人',
   sales_target decimal(38,6) comment '销售目标额',
   daliy_sales_target decimal(38,6) comment '日配销售目标额',
   update_time TIMESTAMP comment '数据更新时间'
  )comment'省区供应链管理品类绩效对应负责人'
  partitioned by(sdt string comment'日期分区')
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS TEXTFILE
  
  ;
  