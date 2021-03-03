
SET sdate='2019-10-01';
SET edate='2019-10-31';
set sdt='2019-11-03';
SELECT c.prov_code,
c.prov_name,
        location_code,
       c.shop_name,
       product_code,
       b.goodsname,
       b.dept_id,
       b.dept_name,
       b.catg_l_id,
       b.catg_l_name,
       b.catg_m_id,
       b.catg_m_name,
       b.catg_s_id,
       b.catg_s_name,
       b.unit_name,
       supplier_code,
       supplier_name,
       sum(coalesce(enter_qty,0)) enter_qty,
      sum(coalesce(enter_amt,0))enter_amt,
      sum(coalesce(transfer_in_qty,0))transfer_in_qty,
      sum(coalesce(transfer_in_amt,0))transfer_in_amt,
      sum(coalesce(transfer_out_qty,0))transfer_out_qty,
      sum(coalesce(transfer_out_amt,0))transfer_out_amt,
      sum(coalesce(out_qty,0))out_qty,
      sum(coalesce(out_amt,0))out_amt
FROM
  (SELECT a.product_code,
          a.location_code,
          a.supplier_code,
          a.supplier_name,
          sum(CASE
                  WHEN move_type IN ('101A') THEN txn_qty
              END)AS enter_qty,-- 入库数理
 sum(CASE
         WHEN move_type IN ('101A') THEN txn_amt
     END)AS enter_amt,-- 入库金额
     sum(CASE
                  WHEN move_type IN ('102A') THEN txn_qty
              END)AS transfer_in_qty,-- 调拨入库数量
 sum(CASE
         WHEN move_type IN ('102A') THEN txn_amt
     END)AS  transfer_in_amt,-- 调拨入库金额
 sum(CASE
         WHEN move_type IN ('103A')THEN txn_qty
     END)AS out_qty,-- 退货出库量
 sum(CASE
         WHEN move_type IN ('103A') THEN txn_amt
     END)AS out_amt,-- 退货出库
      sum(CASE
         WHEN move_type IN ('104A')THEN txn_qty
     END)AS  transfer_out_qty,-- 调拨出库量
 sum(CASE
         WHEN move_type IN ('104A') THEN txn_amt
     END)AS  transfer_out_amt,-- 调拨出库
 sum(CASE
         WHEN move_type IN ('117A') THEN txn_qty
     END)AS loss_qty,-- 报损量
 sum(CASE
         WHEN move_type IN ('117A') THEN txn_amt
     END)AS loss_amt,-- 报损额
 sum(CASE
         WHEN move_type IN ('116A') THEN txn_qty
     END)stock_loss_qty,-- 盘亏量
 sum(CASE
         WHEN move_type IN ('116A') THEN txn_amt
     END)stock_loss_amt,-- 盘亏额
 sum(CASE
         WHEN move_type IN ('115A') THEN txn_qty
     END)stock_profit_qty,-- 盘盈量
 sum(CASE
         WHEN move_type IN ('115A') THEN txn_amt
     END)stock_profit_amt -- 盘盈额
     from csx_ods.wms_accounting_stock_detail_view_ods a
---(select *,row_number()over(partition by id order by sdt asc) as mm from csx_ods.wms_accounting_stock_detail_view_ods where sdt>='20191001' and sdt<='20191031') a  
    where sdt=regexp_replace(${hiveconf:edate},'-','') 
    -- and a.location_code='W0G7' and a.product_code='1051645'
     AND regexp_replace(to_date(a.update_time),'-','') >= regexp_replace(${hiveconf:sdate},'-','')
     AND regexp_replace(to_date(a.update_time),'-','') <= regexp_replace(${hiveconf:edate},'-','')
     and a.supplier_code!='D001'
   GROUP BY a.product_code,
            a.location_code,
            a.supplier_code,
            a.supplier_name)a
LEFT JOIN dim.dim_goods_latest b ON a.product_code=b.goodsid
LEFT JOIN dim.dim_shop_latest c ON a.location_code=c.shop_id
where b.bd_id='11' and coalesce(enter_amt,0)+coalesce(transfer_in_amt,0)+coalesce(transfer_out_amt,0)+coalesce(out_amt,0)!=0
group  by 
c.prov_code,
c.prov_name,
        location_code,
       c.shop_name,
       product_code,
       b.goodsname,
       b.dept_id,
       b.dept_name,
       b.catg_l_id,
       b.catg_l_name,
       b.catg_m_id,
       b.catg_m_name,
       b.catg_s_id,
       b.catg_s_name,
       b.unit_name,
       supplier_code,
       supplier_name
;