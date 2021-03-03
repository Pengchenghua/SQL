--米面粮油   dc
select
	province_code,
	province_name,
	dc_code,
	dc_name,
	goods_code,
	bar_code,
	goods_name,
	unit,
	department_id,
	department_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name ,
	qty ,
	amt
from
	(
	select
		province_code,
		province_name,
		dc_code,
		dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name,
		sum(qty)qty,
		sum(amt)amt
	from
		csx_dw.wms_accounting_stock_m a
	join (
		select
			shop_id,
			province_code,
			province_name
		from
			csx_dw.shop_m
		where
			sdt = 'current') b on
		regexp_replace(a.dc_code,
		'(^E)',
		'9')= regexp_replace(b.shop_id,
		'(^E)',
		'9')
	where
		sdt = '20200209'
		and (category_middle_code in('110119',
		'110120',
		'110123',
		'110132',
		'124005',
		'125701')
		or category_large_code in('1240'))
		and reservoir_area_code not in ('PD01',
		'PD02',
		'TS01',
		'B999',
		'B997')
	GROUP by
		province_code,
		province_name,
		dc_code,
		dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name ) a
where
	qty>0 ;
	
--春节 粮油米面库存情况 
--米面粮油   dc
select
	province_code,
	province_name,
	--dc_code,
	--dc_name,
	goods_code,
	bar_code,
	goods_name,
	unit,
	department_id,
	department_name,
	category_large_code,
	category_large_name,
	category_middle_code,
	category_middle_name ,
	qty ,
	amt
from
	(
	select
		province_code,
		province_name,
		--dc_code,
		--dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name,
		sum(qty)qty,
		sum(amt)amt
	from
		csx_dw.wms_accounting_stock_m a
	join (
		select
			shop_id,
			province_code,
			province_name
		from
			csx_dw.shop_m
		where
			sdt = 'current') b on
		regexp_replace(a.dc_code,
		'(^E)',
		'9')= regexp_replace(b.shop_id,
		'(^E)',
		'9')
	where
		sdt = '20200209'
		and (category_middle_code in('110119',
		'110120',
		'110123',
		'110132',
		'124005',
		'125701')
		or category_large_code in('1240'))
		and reservoir_area_code not in ('PD01',
		'PD02',
		'TS01',
		'B999',
		'B997')
	GROUP by
		province_code,
		province_name,
		--dc_code,
		--dc_name,
		goods_code,
		bar_code,
		goods_name,
		unit,
		department_id,
		department_name,
		category_large_code,
		category_large_name,
		category_middle_code,
		category_middle_name ) a
where
	qty>0 ;
	
	-- 商贸局数据应急
	select category_large_code ,category_large_name,category_middle_code ,category_middle_name ,goods_code ,goods_name ,unit,standard,sum(qty )qty 
	from csx_dw.dws_wms_r_d_accounting_stock_m where sdt='20200608'AND  reservoir_area_code not in ('PD01',
        'PD02',
        'TS01',
        'B999',
        'B997')
	group by category_large_code ,category_large_name,goods_code,unit,standard,goods_name,category_middle_code ,category_middle_name;