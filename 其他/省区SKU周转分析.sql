set shop = ('W0A3','W0Q9','W0N1','W0R9','W0A5','W0N0','W0W7','W0A2','W0F4','W0A8','W0K1','W0K6','W0L3','W0A7','W0A6','W0Q2','W0P8','W0F7');

set last_sdt=('20200731','20200831','20200930','20201031','20201130','20201231');
set b_sdt=('20210131','20210228','20210331','20210430','20210531','2021630');

with temp00 as (
select substr(sdt,1,6) as mon,province_code,
    province_name,
    sum(final_amt) as final_amt,
    count(distinct goods_id) sku,
    sum(period_inv_amt_30day)/sum(cost_30day) as turn_days
 from csx_tmp.ads_wms_r_d_goods_turnover 
where sdt in ${hiveconf:b_sdt} and dc_code in ${hiveconf:shop}
    and division_code  in ('12','13','14')
group by  substr(sdt,1,6) ,
    province_code,
    province_name),
temp01 as 
    (
select substr(sdt,1,6) as mon,province_code,
    province_name,
    sum(final_amt) as final_amt,
    count(distinct goods_id) sku,
    sum(period_inv_amt_30day)/sum(cost_30day) as turn_days
 from csx_tmp.ads_wms_r_d_goods_turnover 
where sdt in ${hiveconf:b_sdt} and dc_code in ${hiveconf:shop}
    and division_code  in ('12','13','14')
    and  (entry_sdt <'20210101'or entry_sdt>='20200101')
    and  days_turnover_30 >90
group by  substr(sdt,1,6) ,
    province_code,
    province_name
    ),
temp02 as 
    (
select substr(sdt,1,6) as mon,province_code,
    province_name,
    sum(final_amt) as final_amt,
    count(distinct goods_id) sku,
    sum(period_inv_amt_30day)/sum(cost_30day) as turn_days
 from csx_tmp.ads_wms_r_d_goods_turnover 
where sdt in ${hiveconf:b_sdt} and dc_code in ${hiveconf:shop}
    and  division_code  in ('12','13','14')
    and  entry_sdt >'20210101' 
    and  days_turnover_30 >90
group by  substr(sdt,1,6) ,
    province_code,
    province_name
    )
select a.*,b.sku,b.final_amt,c.sku,c.final_amt from temp00 a 
left join 
temp01 b on a.mon=b.mon and a.province_code = b.province_code 
left join 
temp02 c on a.mon=c.mon and a.province_code = c.province_code
;

