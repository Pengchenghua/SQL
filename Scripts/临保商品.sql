select a.*,b.qualitative_period,a.entry_days/b.qualitative_period  from csx_tmp.ads_wms_r_d_goods_turnover a 
join 
(select goods_id ,qualitative_period from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') b on a.goods_id =b.goods_id
where sdt='20201021' 
	and business_division_code ='12'
	and division_code ='12'
	and (a.entry_days/b.qualitative_period )>0.8
	and a.final_amt !=0