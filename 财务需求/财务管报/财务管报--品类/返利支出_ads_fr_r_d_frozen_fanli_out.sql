set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');

drop table if  exists csx_tmp.temp_fanli_01;
create temporary table if not exists csx_tmp.temp_fanli_01 as 
select case when channel_code in ('1','7','9') then '1' else channel_code end channel_code,
    case when channel_code in ('1','7','9') then '大' else channel_name end channel_name,
       case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '21'
     when channel_code='2' then '22' else business_type_code end business_type_code,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
     when channel_code='2' then '非代加工' else business_type_name end business_type_name,
     province_code,
     province_name,
     city_group_code,
     city_group_name,
     classify_large_code,
     classify_large_name,
     classify_middle_code,
     classify_middle_name,
     classify_small_code,
     classify_small_name,
     sum(sales_value) as sales_value,
     sum(excluding_tax_sales) as no_tax_sale_value
     from csx_dw.dws_sale_r_d_detail 
where sales_type='fanli'
    and sdt >=${hiveconf:sdate}
    and sdt<= ${hiveconf:edt}
    and classify_middle_code in ('B0304','B0305')
group by 
    case when channel_code in ('1','7','9') then '1' else channel_code end ,
    case when channel_code in ('1','7','9') then '大' else channel_name end ,
       case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '21'
     when channel_code='2' then '22' else business_type_code end ,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
     when channel_code='2' then '非代加工' else business_type_name end ,
     classify_large_code,
     classify_large_name,
     classify_middle_code,
     classify_middle_name,
     classify_small_code,
     classify_small_name,
     province_code,
     province_name,
     city_group_code,
     city_group_name
     ;
     
-- create  table csx_tmp.ads_fr_r_d_frozen_fanli_out  as 
insert overwrite table csx_tmp.ads_fr_r_d_frozen_fanli_out  partition(months)
select   substr(${hiveconf:edt},1,6), 
        case when channel_code is null then '00'
             else channel_code
        end channel_code,
        case when channel_code is null then '合计'
             else channel_name 
        end channel_name,
        case when business_type_code is null then '00' 
             else  business_type_code 
        end business_type_code,
        case when business_type_name is null and channel_code is null then '合计'  
             when business_type_name is null then channel_name 
             else business_type_name 
        end business_type_name,
        case when classify_large_code is null and business_type_name is null then '00' 
             when classify_large_code is null then '00'
             else classify_large_code 
        end classify_large_code,
        case when classify_large_name is null and business_type_name is null then '00' 
             when classify_large_name is null then '合计'
             else classify_large_name 
        end classify_large_name,
        case when classify_middle_code is null and classify_large_code is null then '00' 
             when classify_middle_code is null then '00'
             else classify_middle_code 
        end classify_middle_code,
        case when classify_middle_name is null and classify_large_code is null then '合计' 
             when classify_middle_name is null then '合计'
             else classify_middle_name
             end classify_middle_name,
        case when classify_small_code is null and classify_middle_name is null then '00' 
             when classify_small_code is null then '00'
             else classify_small_code 
        end classify_small_code,
        case when classify_small_code is null and classify_middle_code is null then '合计'
             when classify_small_code is null then classify_middle_name 
             else classify_small_name 
        end classify_small_name, 
    no_tax_sale_value,
    sales_value,
    current_timestamp(),
     substr(${hiveconf:edt},1,6) 
from (
select channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name, 
    sum(no_tax_sale_value) no_tax_sale_value,
    sum(sales_value) sales_value
from  csx_tmp.temp_fanli_01
group by channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name
grouping sets (( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),
        ( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  -- 业务中类合计
        ( channel_code, 
    channel_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),  -- 渠道三级分类
        ( channel_code, 
    channel_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  --渠道+二级分类合计
        ( 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),   --三级分类汇总
        (
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name),  --二级分类汇总
        ( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name),  -- 一级分类汇总
         (channel_code,
        channel_name , classify_large_code, 
    classify_large_name),())
) a ;



DROP table csx_tmp.ads_fr_r_d_frozen_fanli_out;
CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_fanli_out`(
      sales_months string comment '销售月份',
      `channel_code` string comment '渠道', 
      `channel_name` string comment '渠道', 
      `business_type_code` string comment '销售类型', 
      `business_type_name` string comment '销售类型', 
      `classify_large_code` string comment '管理一级分类', 
      `classify_large_name` string comment '管理一级分类', 
      `classify_middle_code` string comment '管理二级分类', 
      `classify_middle_name` string comment '管理二级分类', 
      `classify_small_code` string comment '管理三级', 
      `classify_small_name` string comment '管理三级', 
      `no_tax_sale_value` decimal(38,6) comment '未税返利额', 
      `sales_value` decimal(38,6) comment '含税额',
      update_time timestamp comment '更新日期'
      )comment '冻品财务报表——管理类型返利'
    partitioned by(months string comment '月分区')
    STORED AS parquet 



CREATE TABLE `csx_tmp.ads_fr_r_d_frozen_fanli_out`(
      sales_months string comment '销售月份',
	  `channel_code` string comment '渠道', 
	  `channel_name` string comment '渠道', 
	  `business_type_code` string comment '销售类型', 
	  `business_type_name` string comment '销售类型', 
	  `classify_large_code` string comment '管理一级分类', 
	  `classify_large_name` string comment '管理一级分类', 
	  `classify_middle_code` string comment '管理二级分类', 
	  `classify_middle_name` string comment '管理二级分类', 
	  `classify_small_code` string comment '管理三级', 
	  `classify_small_name` string comment '管理三级', 
	  `no_tax_sale_value` decimal(38,6) comment '未税返利额', 
	  `sales_value` decimal(38,6) comment '含税额',
      update_time timestamp comment '更新日期'
      )comment '冻品财务报表——返利'
	partitioned by(months string comment '月分区')
	STORED AS parquet 