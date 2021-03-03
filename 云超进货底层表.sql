--云超进货底层表
CREATE temporary table b2b_tmp.temp_pur
as
select a.shop_id_in,a.goodsid,round(sum(pur_order_total_value)/sum(pur_order_qty),2) last_storage_price
from 
(select * from b2b.ord_orderflow_t 
where sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-3),'-','') ,1,6),'01')
 and  pur_order_qty<>0 and pur_order_total_value<>0 and 
 shop_id_in in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149')
and pur_grp like 'H%')a 
join 
(select shop_id_in,goodsid,max(sdt)sdt from b2b.ord_orderflow_t 
where sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-3),'-','') ,1,6),'01')
 and  pur_order_qty<>0 and pur_order_total_value<>0 and 
 shop_id_in in ('9337','9109','9300','9272','9241','9012','9423','9340','9344','9448','9149')
and pur_grp like 'H%'
group by shop_id_in,goodsid)b on (a.shop_id_in=b.shop_id_in and a.goodsid=b.goodsid and a.sdt=b.sdt)
group by a.shop_id_in,a.goodsid;