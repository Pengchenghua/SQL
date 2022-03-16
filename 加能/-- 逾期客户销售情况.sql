-- 逾期客户销售情况
drop table csx_tmp.temp_aa;
create temporary table csx_tmp.temp_aa as 
select sdt,order_no,province_code,province_name,
    customer_no,
    customer_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    goods_code,
    goods_name,
    classify_large_code,
    classify_large_name,classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    sum(sales_value) sales_value,
    sum(profit) profit
from csx_dw.dws_sale_r_d_detail
where customer_no in ('123067',
'122635',
'123079',
'118816',
'119053',
'112948',
'111881',
'113544',
'111000',
'113896',
'115297',
'115575',
'117945',
'115211',
'119686',
'124432',
'122422',
'122968')
group by   first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    sdt,
    order_no,
    province_code,
    province_name,
    customer_no,
    customer_name,
    goods_code,
    goods_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name;
    
    
select province_code,
    a.province_name,
    a.customer_no,
    customer_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    goods_code,
    goods_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    max(sdt) max_sdt,
    min(sdt) min_sdt,
    count(distinct sdt) sale_days,
    count(distinct order_no) sales_cn,
    sum(sales_value) sales_value,
    sum(profit) profit,
    ac_all,
    over_value,
    note
from  csx_tmp.temp_aa a 
left join 
(select customer_no,
        comp_code,
        province_name,
        ac_all,
        ac_all-ac_wdq as over_value,
        case when ac_3y>1 then '3年以上'
            when ac_2y>1 and ac_3y=0 then '2年~3年'
            when ac_365d>1 and ac_2y+ac_3y=0 then '1年~2年'
            when ac_180d>1 and ac_2y+ac_3y+ac_365d=0 then '180天~365天'
            when ac_120d >1 and  ac_2y+ac_3y+ac_365d+ac_180d=0  then '120天~180天'
            when ac_90d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d=0  then '90天~120天'
            when ac_60d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d+ac_90d=0  then '60天~90天'
            when ac_30d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d+ac_90d+ac_60d=0  then '60天~90天'
            when ac_15d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d+ac_90d+ac_60d+ac_30d=0  then '15天~30天'
            else '15天以上' end note
from csx_tmp.ads_fr_r_d_account_receivables_scar 
where sdt='20220314'
    and customer_no in ('123067',
'122635',
'123079',
'118816',
'119053',
'112948',
'111881',
'113544',
'111000',
'113896',
'115297',
'115575',
'117945',
'115211',
'119686',
'124432',
'122422',
'122968')
)b  on a.customer_no=b.customer_no and a.province_name=b.province_name
group by province_code,
    a.province_name,
    a.customer_no,
    customer_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    goods_code,
    goods_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
     ac_all,
    over_value,
    note
    ;
    
    
    
select province_code,
    a.province_name,
    a.customer_no,
    customer_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
    max(sdt) max_sdt,
    min(sdt) min_sdt,
    count(distinct sdt) sale_days,
    count(distinct order_no) sales_cn,
    sum(sales_value) sales_value,
    sum(profit) profit,
    ac_all,
    over_value,
    note
from  csx_tmp.temp_aa a 
left join 
(select customer_no,
        comp_code,
        province_name,
        ac_all,
        ac_all-ac_wdq as over_value,
        case when ac_3y>1 then '3年以上'
            when ac_2y>1 and ac_3y=0 then '2年~3年'
            when ac_365d>1 and ac_2y+ac_3y=0 then '1年~2年'
            when ac_180d>1 and ac_2y+ac_3y+ac_365d=0 then '180天~365天'
            when ac_120d >1 and  ac_2y+ac_3y+ac_365d+ac_180d=0  then '120天~180天'
            when ac_90d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d=0  then '90天~120天'
            when ac_60d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d+ac_90d=0  then '60天~90天'
            when ac_30d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d+ac_90d+ac_60d=0  then '60天~90天'
            when ac_15d>1 and  ac_2y+ac_3y+ac_365d+ac_180d+ac_120d+ac_90d+ac_60d+ac_30d=0  then '15天~30天'
            else '15天以上' end note
from csx_tmp.ads_fr_r_d_account_receivables_scar 
where sdt='20220314'
    and customer_no in ('123067',
'122635',
'123079',
'118816',
'119053',
'112948',
'111881',
'113544',
'111000',
'113896',
'115297',
'115575',
'117945',
'115211',
'119686',
'124432',
'122422',
'122968')
)b  on a.customer_no=b.customer_no and a.province_name=b.province_name
group by province_code,
    a.province_name,
    a.customer_no,
    customer_name,
    first_category_code,
    first_category_name,
    second_category_code,
    second_category_name,
     ac_all,
    over_value,
    note