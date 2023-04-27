-- 直送订单对应销售明细
--供应商20031467金寨县锦天农业开发有限公司  对应客户Z008077合肥市霖升商贸有限公司   订单日期+供应商编码/名称+入库单+商品编码+商品名称+入库金额（含税）+入库数量+出库金额（含税）+出库数量+客户编码/名称+直送单号+销售金额（含税）+销售数量+退货金额（含税）+退货数量+毛利额+毛利率
--供应商20031467金寨县锦天农业开发有限公司  对应客户Z008077合肥市霖升商贸有限公司   订单日期+供应商编码/名称+入库单+商品编码+商品名称+入库金额（含税）+入库数量+出库金额（含税）+出库数量+客户编码/名称+直送单号+销售金额（含税）+销售数量+退货金额（含税）+退货数量+毛利额+毛利率
 create table csx_analyse_tmp.csx_analyse_tmp_entry_shipped_sale as 
 with entry as (
  select
    sdt,
    order_code,
    original_order_code,
    link_order_code,
    entry_type,
    a.business_type_code,
    b.business_type_name,
    goods_code,
    goods_name,
    supplier_code,
    supplier_name,
    receive_dc_code,
    receive_dc_name,
    receive_amt,
    receive_qty,
    shipped_qty,
    shipped_qty * price as shipped_amt
  from
    csx_dws.csx_dws_wms_entry_detail_di a 
    left join 
    (select order_type_code,
        business_type_code,
        business_type_name 
    from csx_ods.csx_ods_csx_data_config_wms_entry_business_type_config_df	
        where sdt=regexp_replace(to_date(date_add(current_timestamp(),-1)),'-','') 
         --   and order_type_code like 'P%'
) b ON a.business_type_code=b.business_type_code  and entry_type=order_type_code
  where 
   sdt>='20190101' 
   and supplier_code= '20031467'
    ),
 shipped as (
  select
    sdt,
    order_code,
    original_order_code,
    link_order_code,
    shipped_type,
    a.business_type_code,
    b.business_type_name,
    goods_code,
    goods_name,
    supplier_code,
    supplier_name,
    shipped_qty,
    shipped_amt
  from
    csx_dws.csx_dws_wms_shipped_detail_di a 
    left join 
    (select order_type_code,
        business_type_code,
        business_type_name 
    from csx_ods.csx_ods_csx_data_config_wms_shipped_business_type_config_df	
        where sdt=regexp_replace(to_date(date_add(current_timestamp(),-1)),'-','') 
         --   and order_type_code like 'P%'
) b ON a.business_type_code=b.business_type_code    and shipped_type=order_type_code
  where sdt>='20000101' 
    and supplier_code='20031467'
    and status!=9
    and shipped_type like 'P%'
  --  order_code = 'IN210619002057'
    ),
     sale as
    (select sdt,order_code,original_order_code,customer_code,customer_name,sub_customer_code,sub_customer_name,goods_code  ,
        refund_order_flag,
        sale_qty,
        sale_amt
    from csx_dws.csx_dws_sale_detail_di
        where 
             sdt>='20190101' and sub_customer_code='Z008077'  
         and refund_order_flag=0
    ),
      sale_return as
    (select sdt,order_code,original_order_code,customer_code,customer_name,sub_customer_code,sub_customer_name,goods_code  ,
        refund_order_flag,
        sale_qty as return_qty,
        sale_amt as return_amt
    from csx_dws.csx_dws_sale_detail_di
        where 
             sdt>='20190101' 
             and sub_customer_code='Z008077'  
         and refund_order_flag=1
    )
 
select
    a.sdt,
    a.order_code,
    a.original_order_code,
    a.link_order_code,
    a.entry_type,
    a.business_type_code,
    a.business_type_name,
    a.goods_code,
    a.goods_name,
    a.supplier_code,
    a.supplier_name,
    a.receive_dc_code,
    a.receive_dc_name,
    a.receive_amt,
    a.receive_qty,
    coalesce(shipped_qty,0)shipped_qty,
    coalesce(shipped_amt,0)shipped_amt,
    coalesce(shipped_business_type_code,'')shipped_business_type_code,
    coalesce(shipped_business_type_name,'')shipped_business_type_name,
    coalesce(b.customer_code,'')customer_code,
    coalesce(b.customer_name,'')customer_name,
    coalesce(b.sub_customer_code,'')sub_customer_code,
    coalesce(b.sub_customer_name,'')sub_customer_name,
    coalesce(sale_qty,0)sale_qty,
    coalesce(sale_amt,0)sale_amt,
    coalesce(return_qty, 0)return_qty,
    coalesce(return_amt ,0)return_amt
    from 
(select
    entry.sdt,
    entry.order_code,
    entry.original_order_code,
    entry.link_order_code,
    entry.entry_type,
    entry.business_type_code,
    entry.business_type_name,
    entry.goods_code,
    entry.goods_name,
    entry.supplier_code,
    entry.supplier_name,
    entry.receive_dc_code,
    entry.receive_dc_name,
    entry.receive_amt,
    entry.receive_qty,
    shipped.shipped_qty,
    shipped.shipped_amt,
    shipped.business_type_code as shipped_business_type_code,
    shipped.business_type_name as shipped_business_type_name
  from entry 
  left join shipped on shipped.link_order_code=entry.order_code and entry.goods_code=shipped.goods_code
  )a 
  left join 
  (select 
  sale.sdt,
  sale.order_code,
  sale.original_order_code,
  sale.customer_code,
  sale.customer_name,
  sale.sub_customer_code,
  sale.sub_customer_name,
  sale.goods_code  ,
  sale.sale_qty,
  sale.sale_amt,
  return_qty,
  return_amt
from   sale
  left join sale_return on sale_return.original_order_code=sale.order_code and sale.goods_code=sale_return.goods_code
  )b
   on b.order_code=a.link_order_code and a.goods_code=b.goods_code
   ;
   
   select * from csx_analyse_tmp.csx_analyse_tmp_entry_shipped_sale;