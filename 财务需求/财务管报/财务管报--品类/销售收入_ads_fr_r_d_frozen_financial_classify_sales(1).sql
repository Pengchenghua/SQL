-- 关联销售调整表 csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost 
-- 关联返利支出 csx_tmp.ads_fr_r_d_frozen_fanli_out
set edate = '${enddate}';
set edt =regexp_replace(${hiveconf:edate},'-','');
set sdate=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','');
set hive.exec.dynamic.partition.mode=nonstrict;
--  drop table  csx_tmp.ads_fr_r_d_frozen_financial_classify_sales ;
  drop table if exists temp_classify_sales;
 create temporary table if not exists csx_tmp.temp_classify_sales as  
-- insert overwrite table csx_tmp.ads_fr_r_d_frozen_financial_classify_sales partition(months)
   select substr(${hiveconf:edt},1,6), 
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
        sales_cost,
        sales_value,
        profit,
        no_tax_sales_cost,
        no_tax_sales,
        no_tax_profit,
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
        sum(sales_cost) as sales_cost,
        sum(sales_value) as sales_value,
        sum(profit) as profit,
        sum(no_tax_sales_cost) as no_tax_sales_cost,
        sum(no_tax_sales) as no_tax_sales,
        sum(no_tax_profit) as no_tax_profit,
        grouping__id
    from 
    (
    select case when channel_code in ('1','7','9') then '1' when channel_code in ('5','6') then '4' else  channel_code end channel_code,
    case when channel_code in ('1','7','9') then '大'  when channel_code in ('5','6') then '大宗'  else  channel_name end channel_name ,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '21'
        when channel_code='2' then '22' else business_type_code  end business_type_code,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
        when channel_code='2' then '非代加工' else business_type_name end business_type_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_cost) as sales_cost,
    sum(sales_value) as sales_value,
    sum(profit) as profit,
    sum(excluding_tax_cost) as no_tax_sales_cost,
    sum(excluding_tax_sales) as no_tax_sales,
    sum(excluding_tax_profit) as no_tax_profit
from csx_dw.dws_sale_r_d_detail 
where sdt >=${hiveconf:sdate}
    and sdt<= ${hiveconf:edt}
    and classify_middle_code in ('B0304','B0305')
group by
   case when channel_code in ('1','7','9') then '1' when channel_code in ('5','6') then '4' else  channel_code end ,
    case when channel_code in ('1','7','9') then '大'  when channel_code in ('5','6') then '大宗'  else  channel_name end  ,
   case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '21'
     when channel_code='2' then '22' else business_type_code  end ,
    case when channel_code ='2' and dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
     when channel_code='2' then '非代加工' else business_type_name end ,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
   ) a 
   group by 
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name
  grouping sets (
    ( channel_code, 
    channel_name, 
    business_type_code, 
    business_type_name, 
    classify_large_code, 
    classify_large_name, 
    classify_middle_code, 
    classify_middle_name, 
    classify_small_code, 
    classify_small_name),
    (channel_code, 
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
    )a

  ;
 
 --  csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost  销售调整成本
 --   csx_tmp.ads_fr_r_d_frozen_fanli_out  返利支出
 insert overwrite table csx_tmp.ads_fr_r_d_frozen_financial_classify_sales partition(months)
 select  substr(${hiveconf:edt},1,6),
        channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        sum(sales_cost) as sales_cost,
        sum(sales_value) as sales_value,
        sum(profit) as profit,
        sum(profit)/sum(sales_value) as profit_rate,
        sum(no_tax_sales_cost) as no_tax_sales_cost,
        sum(no_tax_sales) as no_tax_sales,
        sum(no_tax_profit) as no_tax_profit,
        sum(no_tax_profit) /sum(no_tax_sales)  as no_tax_profit_rate,
        sum(adj_no_tax_sum_value) as adj_no_tax_sum_value,
        sum(adj_sum_value) as adj_sum_value,
        sum(no_tax_rebate_out_value) as no_tax_rebate_out_value,    --未税返利支出金额
        sum(rebate_out_value ) as rebate_out_value ,             --含税返利支出金额
        sum(no_tax_rebate_in_value) as no_tax_rebate_in_value,       --未税返利收入金额
        sum(rebate_in_value) as rebate_in_value,               --含税税返利收入金额
        sum(no_tax_profit)+sum(adj_no_tax_sum_value)+(sum(no_tax_rebate_in_value)-sum(no_tax_rebate_out_value)) as no_tax_net_profit,            --综合毛利=定价毛利+调整成本+（后台收入-后台支出）
        sum(profit)+sum(adj_sum_value)+(sum(rebate_in_value)-sum(rebate_out_value)) as no_tax_net_profit ,          --综合毛利=定价毛利+调整成本+（后台收入-后台支出）
        (sum(no_tax_profit)+sum(adj_no_tax_sum_value)+(sum(no_tax_rebate_in_value)-sum(no_tax_rebate_out_value)))/sum(no_tax_sales) as no_tax_net_profit_rate,
        (sum(profit)+sum(adj_sum_value)+(sum(rebate_in_value)-sum(rebate_out_value)))/sum(no_tax_sales) as net_profit_rate,
        current_timestamp(),
        substr(${hiveconf:edt},1,6)
  from 
  ( select  channel_code,
        channel_name,
        business_type_code,
        business_type_name,
        classify_large_code,
        classify_large_name,
        classify_middle_code,
        classify_middle_name,
        classify_small_code,
        classify_small_name,
        sales_cost,
        sales_value,
        profit,
        no_tax_sales_cost,
        no_tax_sales,
        no_tax_profit,
        0 adj_no_tax_sum_value,
        0 adj_sum_value,
        0 no_tax_rebate_out_value,    --未税返利支出金额
        0 rebate_out_value ,             --含税返利支出金额
        0 no_tax_rebate_in_value,       --未税返利收入金额
        0 rebate_in_value               --含税税返利收入金额
  from  csx_tmp.temp_classify_sales
 union all 
 --关联销售调整数据
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
        0 sales_cost,
        0 sales_value,
        0 profit,
        0 no_tax_sales_cost,
        0 no_tax_sales,
        0 no_tax_profit,
        coalesce(adj_no_tax_sum_value,0) as adj_no_tax_sum_value,
        coalesce(adj_sum_value,0) adj_sum_value,
        0 no_tax_rebate_out_value,    --未税返利支出金额
        0 rebate_out_value ,             --含税返利支出金额
        0 no_tax_rebate_in_value,       --未税返利收入金额
        0 rebate_in_value     
    from  csx_tmp.ads_fr_r_d_frozen_adjust_sale_cost 
 union all 
 --关联返利支出数据
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
        0 sales_cost,
        0 sales_value,
        0 profit,
        0 no_tax_sales_cost,
        0 no_tax_sales,
        0 no_tax_profit,
        0 adj_no_tax_sum_value,
        0 adj_sum_value,
        coalesce(no_tax_sale_value,0) as no_tax_rebate_out_value,    --未税返利支出金额
        coalesce(sales_value,0) as rebate_out_value ,             --含税返利支出金额
        0 no_tax_rebate_in_value,       --未税返利收入金额
        0 rebate_in_value     
    from csx_tmp.ads_fr_r_d_frozen_fanli_out
    ) a 
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
        ;
        


	CREATE TABLE csx_tmp.ads_fr_r_d_frozen_financial_classify_sales(
        sales_monts string comment '销售日期',
	  channel_code string comment '渠道', 
	  channel_name string comment '渠道', 
	  business_type_code string comment '销售业务', 
	  business_type_name string comment '销售业务', 
	  classify_large_code string comment '管理一级分类', 
	  classify_large_name string comment '管理一级分类', 
	  classify_middle_code string comment '管理二级分类', 
	  classify_middle_name string comment '管理二级分类', 
	  classify_small_code string comment '管理三级', 
	  classify_small_name string comment '管理三级', 
	  sales_cost decimal(38,6) comment '销售成本含税', 
	  sales_value decimal(38,6) comment '销售额含税', 
	  profit decimal(38,6) comment '毛利额含税', 
      profit_rate decimal(38,6) comment '毛利率',
	  no_tax_sales_cost decimal(38,6)comment '未税成本', 
	  no_tax_sales decimal(38,6)comment '未税销售', 
	  no_tax_profit decimal(38,6) comment '未税毛利额', 
      no_tax_profit_rate decimal(38,6) comment '未税毛利率', 
      adj_no_tax_sum_value decimal(38,6) comment '未税调整成本',
      adj_sum_value decimal(38,6) comment '含税调整成本',
      no_tax_rebate_out_value decimal(38,6) comment '返利支出未税额',  
      rebate_out_value decimal(38,6) comment '返利支出含税额',        
      no_tax_rebate_in_value decimal(38,6) comment '返利收入未税额',   
      rebate_in_value decimal(38,6) comment '返利收入含税额', 
      no_tax_net_profit decimal(38,6) comment '净毛利额未税',
      net_profit decimal(38,6) comment '净毛利额含税',
      net_profit_rate decimal(38,6) comment '净毛利率',
	  update_time timestamp  comment '更新时间'
      )comment '冻品财报-品类销售收入'
      partitioned by(months string comment '月分区')
	STORED AS parquet 
    ;