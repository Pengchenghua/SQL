 --冻品管报调整成本+盘点&报损

-- 盘点—+报损
drop table csx_tmp.tmp_loss_01;
create temporary table csx_tmp.tmp_loss_01 
as 
    SELECT location_code  as dc_code,
               location_name,
               company_code,
               company_name,
               goods_code ,
               goods_name ,
               unit,
               price_no_tax,
               credential_no,
               posting_time,
               purchase_group_code,
               purchase_group_name,
               move_type,
               reservoir_area_code,
               wms_biz_type_code,
               wms_order_no,
               wms_biz_type_name,
               cost_center_code,
               cost_center_name,
               coalesce( case when move_type IN ('117B') then -1*qty when move_type IN ('117A') then  qty end,0) loss_qty,
               coalesce( case when move_type IN ('117B') then -1*amt_no_tax when move_type IN ('117A') then  amt_no_tax end,0) no_tax_loss_amt,
               coalesce( case when move_type IN ('117B') then -1*amt when move_type IN ('117A') then amt end,0)  loss_amt,
               coalesce( case when move_type IN ('115B') then -1*qty when move_type IN ('115A') then  qty end,0) pd_profit_qty,   --盘盈数量
               coalesce( case when move_type IN ('115B') then -1*amt_no_tax when move_type IN ('115A') then  amt_no_tax end,0)  no_tax_pd_profit_amt, --未税盘盈金额
               coalesce( case when move_type IN ('115B') then -1*amt when move_type IN ('115A') then amt end,0) pd_profit_amt,       --含税盘盈金额
               coalesce( case when move_type IN ('116B') then -1*qty when move_type IN ('116A') then  qty end,0) pd_loss_qty,        --盘亏数量
               coalesce( case when move_type IN ('116B') then -1*amt_no_tax when move_type IN ('116A') then  amt_no_tax end,0)  no_tax_pd_loss_amt,  --盘亏未税金额
               coalesce( case when move_type IN ('116B') then -1*amt when move_type IN ('116A') then amt end,0) pd_loss_amt  --盘亏含税金额
        FROM csx_dw.dws_cas_r_d_account_credential_detail
        WHERE sdt>=${hiveconf:sdate}
          AND sdt<=${hiveconf:edt}
          and move_type in ('117A','117B','115A','115B','116A','116B')
          --  and wms_biz_type_code in (35, 36, 37, 38, 39, 40, 41, 64, 66, 76, 77, 78)
;


  
drop  table if exists csx_tmp.temp_cbgb_tz_v11;
create temporary table if not exists csx_tmp.temp_cbgb_tz_v11 
as 
select 
    dc_code,
    case when dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
        when dc_code in ('W0J8','W0G1') then '大宗一部'
        when dc_code in ('W0H4') then '大宗二部'
        else ''
        end dc_type,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(adj_ddfkc_no) adj_ddfkc_no,
    sum(a.adj_ddfkc) adj_ddfkc,
    sum(a.adj_cgth_no) adj_cgth_no,
    sum(adj_cgth) adj_cgth,
    sum(a.adj_gc_xs_no) adj_gc_xs_no,
    sum(adj_gc_xs) adj_gc_xs,
    sum(a.adj_gc_db_no) adj_gc_db_no,
    sum(adj_gc_db) adj_gc_db,
    sum(a.adj_gc_qt_no) adj_gc_qt_no,
    sum(adj_gc_qt) adj_gc_qt,
    sum(a.adj_sg_no) adj_sg_no,
    sum(adj_sg) adj_sg,
    sum(a.adj_bj_xs_no) adj_bj_xs_no,
    sum(adj_bj_xs) adj_bj_xs,
    sum(a.adj_bj_db_no) adj_bj_db_no,
    sum(adj_bj_db) adj_bj_db,
    sum(a.adj_bj_qt_no) adj_bj_qt_no,
    sum(adj_bj_qt) adj_bj_qt,
    sum(loss_qty) loss_qty,
    sum(no_tax_loss_amt) no_tax_loss_amt,
    sum(loss_amt) loss_amt,
    sum(pd_profit_qty) pd_profit_qty,   --盘盈数量
    sum(no_tax_pd_profit_amt) no_tax_pd_profit_amt, --未税盘盈金额
    sum(pd_profit_amt) pd_profit_amt,       --含税盘盈金额
    sum(pd_loss_qty) pd_loss_qty,        --盘亏数量
    sum(no_tax_pd_loss_amt) no_tax_pd_loss_amt,  --盘亏未税金额
    sum(pd_loss_amt) pd_loss_amt,
    sum(adj_ddfkc_no+adj_cgth_no+adj_gc_xs_no+adj_gc_db_no+adj_gc_qt_no+adj_sg_no+adj_bj_xs_no+adj_bj_db_no+adj_bj_qt_no+(pd_profit_amt-no_tax_loss_amt-no_tax_pd_loss_amt)) as total_no_tax_amt,
    sum(adj_ddfkc+adj_cgth+adj_gc_xs+adj_gc_db+adj_gc_qt+adj_sg+adj_bj_xs+adj_bj_db+adj_bj_qt+(pd_profit_amt-loss_amt-pd_loss_amt)) as total_amt

from
(select a.dc_code,
    a.goods_code,
    sum(coalesce(adj_ddfkc_no_tax,0)) adj_ddfkc_no,
    sum(coalesce(a.adj_ddfkc,0)) adj_ddfkc,
    sum(coalesce(a.adj_cgth_no_tax,0)) adj_cgth_no,
    sum(coalesce(adj_cgth,0)) adj_cgth,
    sum(coalesce(a.adj_gc_xs_no_tax,0)) adj_gc_xs_no,
    sum(coalesce(adj_gc_xs,0)) adj_gc_xs,
    sum(coalesce(a.adj_gc_db_no_tax,0)) adj_gc_db_no,
    sum(coalesce(adj_gc_db,0)) adj_gc_db,
    sum(coalesce(a.adj_gc_qt_no_tax,0)) adj_gc_qt_no,
    sum(coalesce(adj_gc_qt,0)) adj_gc_qt,
    sum(coalesce(a.adj_sg_no_tax,0)) adj_sg_no,
    sum(coalesce(adj_sg,0)) adj_sg,
    sum(coalesce(a.adj_bj_xs_no_tax,0)) adj_bj_xs_no,
    sum(coalesce(adj_bj_xs,0)) adj_bj_xs,
    sum(coalesce(a.adj_bj_db_no_tax,0)) adj_bj_db_no,
    sum(coalesce(adj_bj_db,0)) adj_bj_db,
    sum(coalesce(a.adj_bj_qt_no_tax,0)) adj_bj_qt_no,
    sum(coalesce(adj_bj_qt,0)) adj_bj_qt,
    0 loss_qty,
    0 no_tax_loss_amt,
    0 loss_amt,
    0 pd_profit_qty,   --盘盈数量
    0 no_tax_pd_profit_amt, --未税盘盈金额
    0 pd_profit_amt,       --含税盘盈金额
    0 pd_loss_qty,        --盘亏数量
    0 no_tax_pd_loss_amt,  --盘亏未税金额
    0 pd_loss_amt  
from 
csx_dw.dws_sync_r_d_data_relation_cas_sale_adjustment  a 
where  sdt >= ${hiveconf:sdate}
 and sdt<=${hiveconf:edt}
group by  a.dc_code,
    a.goods_code
union all 
select  a.dc_code,
    a.goods_code,
    0 adj_ddfkc_no,
    0 adj_ddfkc,
    0 adj_cgth_no,
    0 adj_cgth,
    0 adj_gc_xs_no,
    0 adj_gc_xs,
    0 adj_gc_db_no,
    0 adj_gc_db,
    0 adj_gc_qt_no,
    0 adj_gc_qt,
    0 adj_sg_no,
    0 adj_sg,
    0 adj_bj_xs_no,
    0 adj_bj_xs,
    0 adj_bj_db_no,
    0 adj_bj_db,
    0 adj_bj_qt_no,
    0 adj_bj_qt,
    loss_qty,
    no_tax_loss_amt,
    loss_amt,
    pd_profit_qty,   --盘盈数量
    no_tax_pd_profit_amt, --未税盘盈金额
    pd_profit_amt,       --含税盘盈金额
    pd_loss_qty,        --盘亏数量
    no_tax_pd_loss_amt,  --盘亏未税金额
    pd_loss_amt  
from csx_tmp.tmp_loss_01  a
) a 
join 
(select goods_id,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    from csx_dw.dws_basic_w_a_csx_product_m 
    where sdt='current' 
        and classify_middle_code in ('B0304','B0305')
    ) b on a.goods_code=b.goods_id
left join 

-- W0G1\W0J8 大宗一  W0H4 大宗二
(SELECT shop_id,
       sales_province_code ,
       sales_province_name ,
       province_code,
       province_name,
       city_group_code ,
       city_group_name ,
       city_code,
       city_name,
       purpose,
       purpose_name
    FROM csx_dw.dws_basic_w_a_csx_shop_m
        WHERE sdt = 'current'
            and table_type=1 
        --    and purchase_org ='P620'
    ) d on d.shop_id=a.dc_code
-- and c.joint_purchase_flag=1
group by   dc_code,
    case when dc_code in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5','W0X4') then '代加工'
        when dc_code in ('W0J8','W0G1') then '大宗一部'
        when dc_code in ('W0H4') then '大宗二部'
        else ''
        end,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name
    ;


-- 计算渠道占比
drop table  csx_tmp.temp_tz_01;
create temporary table csx_tmp.temp_tz_01 as 
 select 
    dc_type,
    channel_code,
    channel_name,
    a.business_type_code,
    a.business_type_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    no_tax_sale_amt,
    sum(no_tax_sale_amt)over(partition by dc_type ) as sum_sales_amt,
    no_tax_sale_amt/(sum(no_tax_sale_amt)over(partition by business_type_name,dc_type )) as classify_ratio,
    no_tax_sale_amt/(sum(no_tax_sale_amt)over(partition by dc_type )) as dc_type_ratio,
    no_tax_sale_amt/(sum(no_tax_sale_amt)over()) as ratio
from 

(select 
    case when a.business_type_code in ('1','2','3','4','5','6','22') then '-'
        else a.business_type_name end dc_type,
    channel_code,
    channel_name,
    a.business_type_code,
    a.business_type_name,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    sum(excluding_tax_sales) as no_tax_sale_amt
 from csx_tmp.temp_fina_sale_00 a 
   where business_type_code !='4'
   group by 
    case when a.business_type_code in ('1','2','3','4','5','6','22') then '-'
        else a.business_type_name end ,
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name,
    a.business_type_code,
    a.business_type_name,
    channel_code,
    channel_name
)a

 ;
  



drop table  csx_tmp.temp_tz_02;
create table csx_tmp.temp_tz_02 as 
select a.*,b.total_no_tax_amt,a.dc_type_ratio*b.total_no_tax_amt  as no_tax_apportion_value,
a.dc_type_ratio*b.total_value  as apportion_value
from csx_tmp.temp_tz_01 a 
left join 
( select case when   dc_type='' then '-' else dc_type end dc_type ,
   sum(total_no_tax_amt) total_value,
   sum(total_amt) total_no_tax_amt
from  csx_tmp.temp_cbgb_tz_v11  
 group by  case when   dc_type='' then '-' else dc_type end
 )  b on a.dc_type=b.dc_type 
 ;
 

insert overwrite table `csx_tmp.ads_fr_r_d_frozen_adjust_apportion` partition(months)
select substr(${hiveconf:edt},1,6)months,
dc_type,
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
        case when classify_large_name is null and business_type_name is null then '合计' 
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
    no_tax_sale_amt,
    no_tax_apportion_amt,
    no_tax_sale_amt/sum(no_tax_sale_amt)over(partition by channel_name)*3.00 as sales_ratio,
        apportion_amt,
    current_timestamp(),
   substr(${hiveconf:edt},1,6)
from 
(
select dc_type,channel_code,
channel_name,
business_type_code,
business_type_name,
classify_large_code,
classify_large_name,
classify_middle_code,
classify_middle_name,
classify_small_code,
classify_small_name,
sum(no_tax_sale_amt) no_tax_sale_amt,
sum(no_tax_apportion_value) no_tax_apportion_amt,
sum(apportion_value) apportion_amt

 from  csx_tmp.temp_tz_02
 group by dc_type,
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
grouping sets 
(( channel_code, 
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
