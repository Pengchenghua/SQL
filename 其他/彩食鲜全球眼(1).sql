 
 --生鲜商品除成品外销售现状

 
drop table b2b_tmp.temp_hsale;
CREATE temporary table b2b_tmp.temp_hsale
as
select 'sale'gtype,a.* from 
(select goods_code,sum(sales_qty)qty,sum(excluding_tax_sales)untax_sale,sum(sales_value)sale
from  csx_dw.sale_b2b_item 
where sdt>=regexp_replace(date_sub(current_date,30),'-','')
and sales_type in('qyg','gc','anhui')and substr(category_large_code,1,2)='11'
group by goods_code)a 
left join (select distinct goodsid from csx_ods.marc_ecc where mat_type='成品')b on a.goods_code=b.goodsid 
where b.goodsid is null;

--工厂生产订单领用
insert into b2b_tmp.temp_hsale
select 'recip'gtype,a.goodsid goods_code,qty,untax_sale,untax_sale*(1+coalesce(c.taxrate,0)/100)sale
from 
(select plant shop_id,goodsid,
sum(case when movetype='Z93' then quant_b else -1*quant_b end)qty, --领用数量
sum(case when movetype='Z93' then rthfees else -1*rthfees end)untax_sale --领用金额
 from csx_ods.mseg_ecc_dtl_fct a
where sdt>regexp_replace(date_sub(current_date,30),'-','')
and pstng_date>regexp_replace(date_sub(current_date,30),'-','')
and  movetype in('Z93','Z94') 
group by plant,goodsid
union all 
select location_code shop_id,product_code goodsid,
sum(case when status=0 then qty else -1*qty end) qty,
sum(case when status=0 then qty*unit_price else -1*qty*unit_price end) untax_sale
from csx_ods.factory_mr_receive_return_ods where 
sdt=regexp_replace(date_sub(current_date,1),'-','')
and to_date(order_time)>=date_sub(current_date,30)
and to_date(order_time)<=date_sub(current_date,1)
group by location_code,product_code
)a
join (select goodsid from dim.dim_goods where edate='9999-12-31' and dept_id like 'H%')x on a.goodsid=x.goodsid 
left join 
(SELECT shop_id,goodsid,tax_code from dw.shop_goods_fct 
where sdt=regexp_replace(date_sub(current_date,1),'-','') and shop_id like 'W%')b on (a.shop_id=b.shop_id and a.goodsid=b.goodsid)
left join b2b.dim_ztax c on b.tax_code=c.rt_taxcode;




CREATE temporary table b2b_tmp.temp_hsale1
as
select goods_code,qty,untax_sale,sale,
row_number() OVER(ORDER BY sale desc)rno,
sum(sale)over(order by sale desc)/sale_t zb_sale
from (select goods_code,sum(qty)qty,sum(untax_sale)untax_sale,sum(sale)sale from b2b_tmp.temp_hsale group by goods_code) a 
join (select sum(sale)sale_t from b2b_tmp.temp_hsale)b on 1=1;



--筛选销售前80%的商品
drop table b2b_tmp.temp_purprice;
CREATE temporary table b2b_tmp.temp_purprice
as
select stype,province_name,shop_id_in,goodsid,pur_doc_id,sdt,pur_qty_in,tax_pur_val_in,
tax_pur_val_in/pur_qty_in pur_price
from 
(select * from b2b.ord_orderflow_t 
where sdt>=regexp_replace(date_sub(current_date,30),'-','') and (substr(goods_catg,1,2)='11' or goods_catg is null)
and shop_id_in like 'W%'
 and  pur_qty_in>0
union all 
select * from b2b.ord_orderflow_t 
where sdt>=regexp_replace(date_sub(current_date,30),'-','') and (substr(goods_catg,1,2)='11' or goods_catg is null)
and shop_id_in not like 'W%' and pur_doc_id like '40%'
 and  pur_qty_in>0
 )a 
join (select * from  b2b_tmp.temp_hsale1 where zb_sale<=0.8)b on a.goodsid=b.goods_code
join (select shop_id,province_name,case when sales_belong_flag='1_云超' then '云超' else '彩食鲜'end stype
from csx_dw.shop_m 
where sdt='current' and sales_belong_flag in ('1_云超','4_企业购','5_彩食鲜'))c on a.shop_id_in=c.shop_id;

drop table b2b_tmp.temp_purprice1;
CREATE temporary table b2b_tmp.temp_purprice1
as
select a.stype,a.province_name,shop_id_in,a.goodsid,pur_doc_id,a.sdt,pur_qty_in,tax_pur_val_in,
pur_price
from 
b2b_tmp.temp_purprice  a 
join (select stype,province_name,goodsid,min(sdt)sdt from b2b_tmp.temp_purprice 
group by stype,province_name,goodsid)b 
on (a.stype=b.stype and a.province_name=b.province_name and a.goodsid=b.goodsid and a.sdt=b.sdt);

drop table b2b_tmp.temp_purprice2;
CREATE temporary table b2b_tmp.temp_purprice2
as
select a.stype,a.province_name,a.goodsid,a.pur_qty_in,tax_pur_val_in,
pur_price
 from b2b_tmp.temp_purprice1 a 
join (select stype,province_name,goodsid,max(pur_qty_in)pur_qty_in from b2b_tmp.temp_purprice1 
group by stype,province_name,goodsid)b 
on (a.stype=b.stype and a.province_name=b.province_name and a.goodsid=b.goodsid and a.pur_qty_in=b.pur_qty_in);



set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.purprice_globaleye partition (sdt) 
select a.stype,a.province_name,a.goodsid,goodsname,b.dept_id,dept_name,
avg(pur_price)pur_price,regexp_replace(date_sub(current_date,1),'-','')sdt
from b2b_tmp.temp_purprice2 a 
join (select goodsid,goodsname,dept_id,dept_name,catg_m_id,catg_m_name from dim.dim_goods where edate='9999-12-31')b 
on a.goodsid=b.goodsid
group by a.stype,a.province_name,a.goodsid,goodsname,b.dept_id,dept_name
order by b.dept_id;

--impala刷新一下
INVALIDATE METADATA csx_dw.purprice_globaleye

/*
--建表
drop table csx_dw.purprice_globaleye;
create table csx_dw.purprice_globaleye
(
stype string comment  '物流类型',
province_name string comment  '省份',
goodsid string comment  '商品编码',
goodsname string comment  '商品名称',
dept_id string comment  '课组编码',
dept_name string comment  '课组名称',
pur_price decimal(26,4) comment '进货价格'
)
comment '彩食鲜进价全球眼'
partitioned by (sdt string comment '过账日期分区')
row format delimited
stored as parquet;


select * from csx_dw.purprice_globaleye 
where sdt='${SDATE}' ${if(len(kz)==0,"","AND dept_id = '"+kz+"'")}
order by province_name,stype
