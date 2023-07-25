SET edate                                   = date_sub(current_date(),1);
SET sdate                                   = trunc(date_sub(current_date(),1),'MM');
set years=regexp_replace(trunc(add_months(${hiveconf:sdate},-12),'YY'), '-', '');

-- 取出库 配送、直送单据号
drop table if exists csx_tmp.p_shipped_data;
create TEMPORARY TABLE csx_tmp.p_shipped_data as 
select origin_order_no
from csx_dw.wms_shipped_order 
where send_sdt >= ${hiveconf:years}
and send_sdt <= regexp_replace(${hiveconf:edate}, '-', '')
and business_type_code  in ('19')
group by origin_order_no
;

select mon,province_code,province_name,dc_code,dc_name,channel_name,qty,sale 
from 
(SELECT substr(sdt,1,6)mon,
       dc_code,
       dc_name,
       channel_name,
       sum(sales_qty)qty,
       sum(sales_value)sale
FROM csx_dw.dws_sale_r_d_customer_sale a 
left join 
csx_tmp.p_shipped_data b on a.origin_order_no=b.origin_order_no
WHERE sdt>='20200101'
    and sdt<'20200801'
    and b.origin_order_no is null 
  AND is_self_sale=1
  and a.dc_code like 'W%'
  and a.customer_no not in ('104444','102998')
group by 
    substr(sdt,1,6),
       dc_code,
       dc_name,
       channel_name
)a 
join 
(select shop_id,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' )b on a.dc_code=b.shop_id
 ;
