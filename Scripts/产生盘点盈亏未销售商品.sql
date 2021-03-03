create TEMPORARY  TABLE csx_dw.temp_pd_ifno
as 
select 
substr(sdt,1,6)smonth,dc_code,
dc_name,
goods_code,
move_type,
case when move_type ='115A' then '盘点过账-盘盈'else '盘点过账-盘亏' end move_name,
sum(case when move_type ='116A' then -1*qty else qty end)qty,
sum(case when move_type ='116A' then -1*amt else amt end)amt 
from  csx_dw.wms_accounting_stock_operation_item_m
where sdt>='20191101' and sdt<='20191130'
and move_type in ('115A','116A') 
--and substr(reservoir_area_code,1,2)='PD'
group by substr(sdt,1,6),dc_code,
dc_name,
goods_code,
move_type,case when move_type ='115A' then '盘点过账-盘盈'else '盘点过账-盘亏' end;

select dc_code ,dc_name ,goods_code ,goods_name ,unit ,bar_code ,brand_name ,division_code ,division_name ,department_id ,department_name ,sum(qty)qty,sum(amt)amt 
from csx_dw.wms_accounting_stock_m where sdt='20191130' and sys ='new' and SUBSTRING(reservoir_area_code,1,2) not in('PD','TS') 
group by dc_code ,dc_name ,goods_code ,goods_name ,unit ,bar_code ,brand_name ,division_code ,division_name ,department_id ,department_name ;

select a.*,c.* from csx_dw.temp_pd_ifno a 
join 
(select dc_code ,dc_name ,goods_code ,goods_name ,unit ,bar_code ,brand_name ,division_code ,division_name ,department_id ,department_name ,sum(qty)qty,sum(amt)amt 
from csx_dw.wms_accounting_stock_m where sdt='20191130' and sys ='new' and SUBSTRING(reservoir_area_code,1,2) not in('PD','TS') 
group by dc_code ,dc_name ,goods_code ,goods_name ,unit ,bar_code ,brand_name ,division_code ,division_name ,department_id ,department_name )c 
on a.dc_code=c.dc_code and a.goods_code=c.goods_code
left join 
(select shop_id ,goods_code from csx_dw.sale_goods_m1 a where sdt>='20191101' and sdt<='20191130') b on a.dc_code=b.shop_id and a.goods_code=b.goods_code 
where b.goods_code is  null 
