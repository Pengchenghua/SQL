
SELECT
    b.prov_code,
    b.prov_name,
a.shop_id,
b.shop_name,
 a.goodsid,
    b.goodsname,
    b.bar_code,
    b.unit,
    b.brand_name,
  a.vendor_id,
  b.vendor_name,
b.prod_area,
  b.div_id,
  b.div_name,
    b.catg_l_id,
    b.catg_l_name,
    b.catg_m_id,
    b.catg_m_name,
    b.catg_s_id,
    b.catg_s_name,
    sum(qty)qty,
    sum(sales_cost)sales_cost,
    sum(sale)sale,
    sum(profit)profit
FROM
    (
    SELECT
        shop_id,
        a.goodsid,
        a.vendor_id,
        sum(a.sales_qty)qty,
        sum(cost_amt) sales_cost,
        sum(a.tax_value + a.sales_val - a.subtatal_5) sale,
        sum(a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt) profit
    FROM
        dw.sale_sap_dtl_fct a
    WHERE
        a.bill_type IN ('','S1', 'S2','ZF1','ZF2','ZR1','ZR2','ZFP','ZFP1')
        AND sdt >= '20201101'
        AND a.sdt <= '20201220'
        and shop_id in ('9893','9839','9700','9598','9549','9539','9531','9530','9496','9448','9440','9439','9387','9350','9330','9296','9250','9199','9165','9163','9144','9139','9129','9116','90V3','90T4','9078','9067','9064','9049','9047','9046','9045','9039','9038','9029','9026','9024','9023','9022','9019','9018','9017','9015','9014','9012','9011','9009','9007')
       -- AND a.div_id IN ('11','10')
    GROUP BY
        shop_id,
        a.goodsid,
        a.vendor_id)a
JOIN 
(select * from dim.dim_shop_goods_latest ) b  ON
    a.shop_id = b.shop_id
    AND a.goodsid = b.goodsid
group by
    b.prov_code,
    b.prov_name,
a.shop_id,
b.shop_name,
 a.goodsid,
    b.goodsname,
    b.bar_code,
    b.unit,
    b.brand_name,
a.vendor_id,
b.vendor_name,
b.prod_area,
 b.div_id,
  b.div_name,
    b.catg_l_id,
    b.catg_l_name,
    b.catg_m_id,
    b.catg_m_name,
    b.catg_s_id,
    b.catg_s_name
;	