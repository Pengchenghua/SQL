CREATE temporary  table b2b_tmp.temp_gs
as
select goodsid,shop_id,tax_code,
coalesce(y.taxrate*1.00/100,0))taxrate
from 
(select goodsid,shop_id,tax_code from dw.shop_goods_fct a 
where sdt=regexp_replace(date_sub(current_date,1),'-','') and shop_id like 'W%')x 
left join b2b.dim_ztax y on x.tax_code=y.rt_taxcode;

drop table b2b_tmp.temp_gs01;
CREATE table b2b_tmp.temp_gs01
as
select sdt,goodsid,shop_id,tax_code,cycle_unit_price,y.taxrate,
x.cycle_unit_price*(1+coalesce(y.taxrate*1.00/100,0))unit_price
from 
(select sdt,goodsid,shop_id,cycle_unit_price,tax_code from dw.shop_goods_fct a 
where sdt>='20191001' and shop_id like 'W%')x 
left join b2b.dim_ztax y on x.tax_code=y.rt_taxcode;



CREATE temporary table b2b_tmp.temp_ly
as
select substr(sdt,1,6)smonth,plant,goodsid
from csx_ods.mseg_ecc_dtl_fct 
where sdt>='20191001'
and movetype in('631','632') and stor_loc<>''and quant_b<>0
group by substr(sdt,1,6),plant,goodsid;


-- 盘点/财务盘点查询结果表表头依次是月份、物流地点，物流地点名称，仓位编码，仓位名称，课组编码，课组名称，商品编码，商品名称，移动类型，移动类型备注，报损数量，报损金额


--旧系统盘点所有数据
--Z03盘亏Z04盘盈
drop table b2b_tmp.temp_pd;
CREATE table b2b_tmp.temp_pd
as
select 
substr(a.sdt,1,6)smonth,a.plant,stor_loc,a.goodsid,move_type,move_name,
sum(pd_qty)pd_qty,
sum(pd_qty*coalesce(unit_price,0)) pd_amt
from 
(select sdt,plant,stor_loc,goodsid,movetype move_type,
case when movetype='Z03' then '盘亏'else '盘盈'end move_name,
sum(quant_b)pd_qty
from csx_ods.mseg_ecc_dtl_fct 
where sdt>='20191001'and movetype in ('Z03','Z04') and stor_loc<>'B999'
group by sdt,plant,stor_loc,goodsid,case when movetype='Z03' then '盘亏'else '盘盈'end )a 
left join b2b_tmp.temp_gs01 b on (a.plant=b.shop_id and a.goodsid=b.goodsid and a.sdt=b.sdt)
group by substr(a.sdt,1,6),a.plant,stor_loc,a.goodsid,move_type,move_name;

--盘点数据提取
select 
a.smonth,a.plant,b.shop_name,stor_loc,''stor_name,dept_id,dept_name,a.goodsid,c.goodsname,move_type,move_name,
pd_qty,pd_amt
 from (select * from b2b_tmp.temp_pd where stor_loc<>'B999')a 
join b2b.dim_shops_current b on a.plant=b.shop_id 
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') goodsname,dept_id,dept_name 
from dim.dim_goods where edate='9999-12-31')c on a.goodsid=c.goodsid 
order by a.smonth,a.plant,dept_id;

--新系统盘点数据
--新系统盘点数据中包含link_wms_move_type='201A'期初入库，link_wms_move_type in('119A','120A')原料成本转换，其本质不属于盘点，要筛选出
--link_wms_move_type in ('110A','111A')
select substr(sdt,1,6)smonth,dc_code,
dc_name,reservoir_area_code,reservoir_area_name,department_id,
department_name,
goods_code,
goods_name,
move_type,move_name,
sum(case when move_type like '%B' then -1*qty else qty end)qty,
sum(case when move_type like '%B' then -1*amt else amt end)amt 
from  csx_dw.wms_accounting_stock_operation_item_m
where sdt>='20191001' 
and substr(move_type,1,3) in ('110','111') 
and substr(link_wms_move_type,1,3) in ('110','111') 
and substr(reservoir_area_code,1,2)<>'PD'
group by substr(sdt,1,6),dc_code,
dc_name,reservoir_area_code,reservoir_area_name,department_id,
department_name,
goods_code,
goods_name,move_type,move_name;

---财务盘点数据712过账盘盈711过账盘亏,盘点过账只发生在盘点仓
CREATE temporary table b2b_tmp.temp_pdcw
as
select 
a.smonth,a.plant,a.goodsid,stor_loc,movetype move_type,
case when movetype='711' then '盘亏过账'else '盘盈过账'end move_name,
pd_qty,
pd_value*(1+coalesce(taxrate,0)/100)pd_amt
from 
(select substr(sdt,1,6)smonth,plant,goodsid,
sum(quant_b)pd_qty,
sum(rthfees)pd_value
from csx_ods.mseg_ecc_dtl_fct 
where sdt>='20191001'and movetype in ('711','712') and stor_loc='B999'
group by substr(sdt,1,6),plant,goodsid)a 
left join (select * from b2b_tmp.temp_gs01 where sdt=regexp_replace(date_sub(current_date,1),'-','')) b on (a.plant=b.shop_id and a.goodsid=b.goodsid);

--财务盘点数据提取

select 
a.smonth,a.plant,b.shop_name,stor_loc,'盘点仓'stor_name,dept_id,dept_name,a.goodsid,c.goodsname,move_type,move_name,
pd_qty,pd_amt
 from b2b_tmp.temp_pdcw  a 
join b2b.dim_shops_current b on a.plant=b.shop_id 
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') goodsname,dept_id,dept_name 
from dim.dim_goods where edate='9999-12-31')c on a.goodsid=c.goodsid 
order by a.smonth,a.plant,dept_id;

--新系统财务盘点数据115A：盘点过账-盘盈，116A：盘点过账-盘亏
select 
substr(sdt,1,6)smonth,dc_code,
dc_name,reservoir_area_code,reservoir_area_name,department_id,
department_name,
goods_code,
goods_name,
move_type,move_name,
sum(case when move_type like '%B' then -1*qty else qty end)qty,
sum(case when move_type like '%B' then -1*amt else amt end)amt 
from  csx_dw.wms_accounting_stock_operation_item_m
where sdt>='20191001' and sdt<'20191101'
and substr(move_type,1,3) in ('115','116') and substr(reservoir_area_code,1,2)='PD'
group by goods_code,
goods_name,department_id,
department_name,dc_code,
dc_name,move_type,move_name,reservoir_area_code,
reservoir_area_name;



--报损数据的查询结果表表头依次是月份、物流地点，物流地点名称，仓位编码，仓位名称，课组编码，课组名称，商品编码，商品名称，报损数量，报损金额
--报损的数据
drop table b2b_tmp.temp_bs;
CREATE temporary table b2b_tmp.temp_bs
as
select 
a.smonth,a.plant,stor_loc,a.goodsid,pd_qty,
pd_value*(1+coalesce(taxrate,0))pd_amt
from 
(select substr(sdt,1,6)smonth,plant,stor_loc,goodsid,
sum(case when movetype='Z45' then quant_b else -1*quant_b end)pd_qty,
sum(case when movetype='Z45' then rthfees else -1*rthfees end)pd_value
from csx_ods.mseg_ecc_dtl_fct 
where sdt>='20191001'and movetype in ('Z45','Z46') 
group by substr(sdt,1,6),plant,stor_loc,goodsid)a 
left join (select * from b2b_tmp.temp_gs01 where sdt=regexp_replace(date_sub(current_date,1),'-','')) b on (a.plant=b.shop_id and a.goodsid=b.goodsid);


--报损数据提取
select 
a.smonth,a.plant,b.shop_name,stor_loc,''stor_name,dept_id,dept_name,a.goodsid,c.goodsname,
pd_qty,pd_amt
 from (select * from b2b_tmp.temp_bs where stor_loc<>'B999')a 
join b2b.dim_shops_current b on a.plant=b.shop_id 
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') goodsname,dept_id,dept_name 
from dim.dim_goods where edate='9999-12-31')c on a.goodsid=c.goodsid 
order by a.smonth,a.plant,dept_id;

--联营数据提取
select 
a.smonth,a.plant,b.shop_name,stor_loc,dept_id,dept_name,a.goodsid,c.goodsname,
pd_qty,pd_value,pd_amt
 from b2b_tmp.temp_bs a 
join b2b.dim_shops_current b on a.plant=b.shop_id 
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') goodsname,dept_id,dept_name 
from dim.dim_goods where edate='9999-12-31')c on a.goodsid=c.goodsid 
join b2b_tmp.temp_ly e on (a.smonth=e.smonth and a.plant=e.plant and a.goodsid=e.goodsid)
order by a.smonth,a.plant,dept_id;



--新系统报损数据提取
select substr(sdt,1,6)smonth,dc_code,
dc_name,reservoir_area_code,
reservoir_area_name,department_id,
department_name,
goods_code,
goods_name,
sum(case when move_type like '%B' then -1*qty else qty end)qty,
sum(case when move_type like '%B' then -1*amt else amt end)amt 
from  csx_dw.wms_accounting_stock_operation_item_m
where sdt>='20191001' and sdt<'20191101'
and substr(move_type,1,3) in ('117')
group by substr(sdt,1,6)smonth,dc_code,
dc_name,reservoir_area_code,
reservoir_area_name,department_id,
department_name,
goods_code,
goods_name;




---食品用品用Z03 Z04 生鲜直接用过账711 712，新系统直接都使用已过账的




