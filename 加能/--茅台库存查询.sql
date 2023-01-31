--茅台库存查询
select performance_province_name,dc_code,shop_name,goods_code,goods_name,unit_name,sum(qty) qty,sum(amt) amt from csx_dws.csx_dws_cas_accounting_stock_m_df A 
join
(select shop_code,shop_name,performance_province_code,performance_province_name from csx_dim.csx_dim_shop where sdt='current' and performance_province_code ='15') b on a.dc_code=b.shop_code
where sdt='20221215' and company_code='2115' and reservoir_area_code not in ('PD01','PD02','TS01','CY01')
and goods_name like '%茅台%'

group by performance_province_name,dc_code,shop_name,goods_code,goods_name,unit_name
having sum(qty)!=0