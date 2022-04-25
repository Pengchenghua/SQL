--彩食鲜的库存数据
--2090930的数据每个月提取最后一天的数据，9月30日以后每天提取
CREATE TEMPORARY  table b2b_tmp.temp_csx_inv
as
select a.*,shop_name,province_name from 
(select * from dw.inv_sap_setl_dly_fct 
where sdt='20190930')a 
join (select csx_dw.shop_m where sdt='current')b on a.shop_id=b.shop_id 


--入库的旧系统数据,每天更新近两个月的数据
CREATE TEMPORARY  table b2b_tmp.temp_csx_purin
as
select a.*,b.shop_name shop_name_in,province_name
from 
(select * 
from b2b.ord_orderflow_t 
where sdt>='20190101'and substr(pur_org,1,2)='P6')a 
join (select csx_dw.shop_m where sdt='current')b on a.shop_id_in=b.shop_id;

--盘点明细数据
drop table b2b_tmp.temp_gs01;
CREATE table b2b_tmp.temp_gs01
as
select sdt,goodsid,shop_id,tax_code,cycle_unit_price,y.taxrate,
x.cycle_unit_price*(1+coalesce(y.taxrate*1.00/100,0))unit_price
from 
(select sdt,goodsid,shop_id,cycle_unit_price,tax_code from dw.shop_goods_fct a 
where sdt>='20190101' and shop_id like'W%')x 
left join b2b.dim_ztax y on x.tax_code=y.rt_taxcode;


--业务盘点数据  增量更新，每天更新近两个月的，注每笔盘点都会有两笔记录，一笔是正常仓位的盘点，另一笔记录在B999库，所以实际使用时要筛选stor_loc<>'B999'
--Z03为盘亏Z04为盘盈，Z03记为负数，Z04记为正
drop table b2b_tmp.temp_pd;
CREATE temporary table b2b_tmp.temp_pd
as
select a*,pd_qty*coalesce(cycle_unit_price,0) pd_value,
pd_qty*coalesce(unit_price,0) pd_amt
from 
(select 
pstng_date,mat_doc,movetype,plant,goodsid,stor_loc,sold_to,vendor,
move_plant,quant_b pd_qty,rthfees,coorder,oi_ebeln,vbeln_im,sdt
from csx_ods.mseg_ecc_dtl_fct 
where sdt>='20190101'and movetype in ('Z03','Z04') 
and plant like 'W%')a 
left join b2b_tmp.temp_gs01 b on (a.plant=b.shop_id and a.goodsid=b.goodsid and a.sdt=b.sdt)
group by substr(a.sdt,1,6),a.plant,stor_loc,movetype,a.goodsid;

--财务盘点数据 增量更新，每天更新近两个月的，财务盘点数据发生在B999仓每月盘点一次，711记为负数，712记为正数
insert into b2b_tmp.temp_pd
select a.*,pd_qty,rthfees pd_value,
rthfees*(1+coalesce(taxrate,0)/100)pd_amt
from 
(select pstng_date,mat_doc,movetype,plant,goodsid,stor_loc,sold_to,vendor,
move_plant,quant_b pd_qty,rthfees,coorder,oi_ebeln,vbeln_im,sdt
from csx_ods.mseg_ecc_dtl_fct 
where sdt>='20190101'and movetype in ('711','712') 
and plant like 'W%')a 
left join (select * from b2b_tmp.temp_gs01 where sdt=regexp_replace(date_sub(current_date,1),'-','')) b 
on (a.plant=b.shop_id and a.goodsid=b.goodsid);



---报损数据，增量更新，每天更新近两个月的,实际使用时要筛选stor_loc<>'B999'，Z45记为正，Z46记为负
insert into b2b_tmp.temp_pd
select a.*,pd_qty,rthfees pd_value,
sum(pd_value*(1+coalesce(taxrate,0)))pd_amt
from 
(select pstng_date,mat_doc,movetype,plant,goodsid,stor_loc,sold_to,vendor,
move_plant,quant_b pd_qty,rthfees,coorder,oi_ebeln,vbeln_im,sdt
from csx_ods.mseg_ecc_dtl_fct 
where sdt>='20190101'and movetype in ('Z45','Z46')
and plant like 'W%')a 
left join (select * from b2b_tmp.temp_gs01 where sdt=regexp_replace(date_sub(current_date,1),'-','')) b on (a.plant=b.shop_id and a.goodsid=b.goodsid);

