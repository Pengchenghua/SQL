-- 云超入库查询
select province_name,goods_id,goods_name,goods_unit,
    division_code,
    division_name,
    category_large_name,
    category_large_code,
    category_middle_code,
    category_middle_name,
    sum(pur_price*recpt_qty)total_amt,
    sum(recpt_qty) 
from   dwd.dwd_scm_order_trace_bpm_di a 
join 
(select division_code,
    division_name,
    category_large_name,
    category_large_code,
    category_middle_code,
    category_middle_name,
    category_small_code
from csx_dim.csx_dim_basic_category where sdt='current' and   ( category_middle_name like '%调理%' or category_middle_name like '%预制%') ) b on a.catg_s_id=b.category_small_code
join 
(select shop_code,shop_name,province_name from csx_dim.csx_dim_shop where sdt='current' and purchase_org in('P001','P002','P020')) c on a.place_id=c.shop_code
where sdt>='20221101' and sdt<='20221130' 
   and apply_order_type like 'ZN%' 
   and recpt_inv_loc='0001'
   and order_sts='70'
    group by province_name,goods_id,goods_name,
    division_code,
    division_name,
    goods_unit,
    category_large_name,
    category_large_code,
    category_middle_code,
    category_middle_name
;

select * from  dwd.dwd_scm_order_trace_bpm_di where sdt>='20221227'