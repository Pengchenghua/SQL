--供应商入库情况
select  supplier_code,vendor_name,sum(qty)qty,sum(amt) amt from 
(select  regexp_replace(supplier_code,'(^0*)','')as supplier_code,goods_code,sum(receive_qty)qty,sum(receive_qty*price) amt from csx_dw.wms_entry_order_all_m a 
where sdt>='20190101' and sdt<='20191231' and 
entry_type in ('P01')
and sys='new'
group by supplier_code,goods_code
union all 
select  regexp_replace(vendor_id,'(^0*)','')as supplier_code,goodsid as goods_code,sum(recpt_qty)qty,sum(tax_pur_val_in) amt from b2b.ord_orderflow_t  a 
where sdt>='20190101' and sdt<='20191231' 
and pur_doc_type in ('ZN01','ZNR1')
and pur_org like 'P6%'
group by  regexp_replace(vendor_id,'(^0*)',''),goodsid
) a 
join (select * from dim.dim_goods where edate ='9999-12-31') b on a.goods_code=b.goodsid
JOIN (SELECT * from dim.dim_vendor where edate='9999-12-31') c on a.supplier_code=vendor_id
and b.div_id in ('12','13','14')
group by supplier_code,vendor_name;

SELECT * from csx_dw.wms_entry_order_all_m where regexp_replace(supplier_code,'(^0*)','')='200419';
select * from b2b.ord_orderflow_t where  regexp_replace(vendor_id,'(^0*)','')='200419' and shop_id='W0A8';
