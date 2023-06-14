-- 临时地采商品成本销售跟踪
-- 查找采购入库批次
select * from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di  where sdt>='20230401' and link_wms_order_no='IN230508031295';

-- 查找销售凭证单号
select * from   csx_dwd.csx_dwd_cas_accounting_stock_log_item_di  where sdt>='20230401' and batch_no='CB20230509070357' and in_out_type='SALE_OUT'

-- 通过凭证单号查找多批次销售成本
select * from csx_dwd.csx_dwd_cas_accounting_stock_log_item_di  where sdt>='20230401' and credential_no='PZ20230509439952'  and goods_code ='911669';

-- 根据凭证号，查找销售单号
select * from csx_dws.csx_dws_sale_detail_di where sdt>='20230401' and id like 'PZ20230509439952%' and goods_code ='911669'