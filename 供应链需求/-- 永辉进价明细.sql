-- 永辉进价明细
SELECT 
order_sts,
apply_order_type,
place_id,
shop_name,
goods_id,
goods_name,
pur_price	,
approve_qty,
total_amt,
div_id,
catg_l_id	,
catg_l_name	,
catg_m_id	,
catg_m_name	,
catg_s_id	,
catg_s_name	,
pur_group_id,
pur_group_name,
vendor_id,
vendor_name,
po_order_no,
recpt_time

FROM 
dwd.dwd_scm_order_trace_bpm_di a 
join
(select shop_code,shop_name from csx_dim.csx_dim_shop where sdt='current' and  shop_code in ('9300','9473','9018') ) b on a.place_id=b.shop_code
where sdt>='20220901'
    and div_id in('12','13')
    and recpt_time !='0'
;

-- '9KG0','90E9','9473'	,'9018'