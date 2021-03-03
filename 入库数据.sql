CREATE temporary table b2b_tmp.temp_purprice
as
select 
a.goodsid,goodsname,b.unit_name,b.dept_id,b.dept_name,
sum(pur_qty_in)pur_qty,sum(tax_pur_val_in)pur_amt,
sum(tax_pur_val_in)/sum(pur_qty_in) pur_price
from 
(
--旧系统物流入库数据
select shop_id_in,goodsid,sum(pur_qty_in)pur_qty_in,sum(tax_pur_val_in)tax_pur_val_in from b2b.ord_orderflow_t 
where sdt>='20190101'and sdt<'20200101'
and shop_id_in like 'W%' 
 and  pur_qty_in>0 and ordertype not in ('返配','退货') and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%' 
 and substr(pur_org,1,2)='P6'
 group by shop_id_in,goodsid
union all --旧系统门店入库(只挑选直送)
select shop_id_in,goodsid,sum(pur_qty_in)pur_qty_in,sum(tax_pur_val_in)tax_pur_val_in from b2b.ord_orderflow_t 
where sdt>='20190101'and sdt<'20200101'
and shop_id_in not like 'W%' and pur_doc_id like '40%' and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
 and  pur_qty_in>0 and substr(pur_org,1,2)='P6'
 group by shop_id_in,goodsid
 union all --新系统入库数据
 select location_code shop_id_in,product_code goodsid,sum(receive_qty) pur_qty_in,sum(amount) tax_pur_val_in
from 
(select distinct order_code,regexp_replace(to_date(receive_time),'-','')sdate from csx_ods.wms_entry_order_header_ods a 
where sdt>='20191015' and entry_type LIKE 'P%'
and to_date(receive_time)>='2019-10-01' and return_flag<>'Y' and receive_status<>0)a 
join 
(select distinct order_code,product_code,location_code,receive_qty,price,amount from csx_ods.wms_entry_order_item_ods 
where sdt>='20191015' and receive_qty>0
and to_date(update_time)>='2019-10-01')b 
on a.order_code=b.order_code )a 
join (select shop_id,shop_name,province_name
from csx_dw.shop_m 
where sdt='current' and sales_belong_flag in ('4_企业购','5_彩食鲜') )c on a.shop_id_in=c.shop_id
join 
(select goodsid,regexp_replace(regexp_replace(goodsname,'\n',''),'\r','') as goodsname,unit,unit_name,
dept_id,dept_name
from dim.dim_goods where edate='9999-12-31' and dept_id in ('H02','H03'))b on a.goodsid=b.goodsid
group by 
a.goodsid,goodsname,b.unit_name,b.dept_id,b.dept_name;