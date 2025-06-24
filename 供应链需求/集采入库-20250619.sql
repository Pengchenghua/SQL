
 -- 集采入库
 select substr(sdt,1,6) s_month,
  performance_region_name,
  performance_province_name,
  basic_performance_city_name,
  case
    when classify_middle_code in ('B0305','B0302','B0301','B0201','B0202','B0303','B0306' ) then classify_middle_name 
    when classify_large_code in ('B01') then '干货加工'
	else '食百' end  as new_classify_middle_name,
  classify_middle_name,
  classify_small_name,
  is_central_tag, --品类+供应商集采
  supplier_code,
  supplier_name,
  sum(receive_amt) receive_amt
 from 
    csx_analyse.csx_analyse_scm_purchase_order_flow_di a 
      join 
(select shop_code,basic_performance_region_name,basic_performance_province_code,	basic_performance_province_name	,basic_performance_city_code,purpose,purpose_name,	basic_performance_city_name 
 from csx_dim.csx_dim_shop 
    where sdt='current'
    and purpose in ('01','03')
)c  on a.dc_code=c.shop_code
 where sdt>='20250101' and sdt<='20250617'
  AND order_code like 'IN%'
  and is_central_tag=1
group by performance_region_name,
   performance_province_name,
   dc_code,
   case when classify_middle_code in ('B0305', 'B0302', 'B0301', 'B0201', 'B0202', 'B0303', 'B0306') then classify_middle_name
        when classify_large_code in ('B01') then '干货加工'
	 else '食百' end,
   csx_purchase_level_code,
   is_central_tag,
   classify_small_name,
   classify_middle_name,
    supplier_code,
    supplier_name
    ,basic_performance_city_name,
     substr(sdt,1,6)
