-- 临时地采商品成本销售跟踪  CB20230627058958 CB20230627056571    PURCHASE_IN 采购订单  SALE_OUT 销售出库  ON_WAY  出库在途

-- 查找采购订单号 
select * from csx_analyse.csx_analyse_scm_purchase_order_flow_di where order_code='IN230627018509'

-- 查找采购入库批次
select * from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di  where sdt>='20230401' and link_wms_order_no='IN230627005052';

-- 查找销售凭证单号
select * from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di  where sdt>='20230401' and batch_no='CB20230627056574' and goods_code ='1279161';

-- 通过出库批次查找入库单号
select * from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di  where sdt>='20230401' and link_wms_order_no='BG23062700004516'  and goods_code ='1279161';


-- 通过凭证单号查找多批次销售成本
select * from csx_dwd.csx_dwd_cas_accounting_stock_log_item_di  where sdt>='20230401' and credential_no='PZ20230627420239'  and goods_code ='1279161';

-- 根据凭证号，查找销售单号
select * from csx_dws.csx_dws_sale_detail_di where sdt>='20230401' and id like 'PZ20230627420239%' and goods_code ='1279161'


select * from csx_dws.csx_dws_wms_shipped_detail_di where order_code='CO23062700020267'
-- original_order_code  地采源单号销售单