create temporary table if not exists temp.p_invt_1
as 
select shop_id,goodsid, sum(inv_qty)inv_qty,sum(period_inv_amt)period_inv_amt from csx_dw.inv_sap_setl_dly_fct 
where sdt='20191024' and (sales_dist between '600000' and '690000' and  sales_dist not in ('613000','612000')) and catg_l_id between '1200' and '1499'
and inv_place not in ('B997','B9999') 
GROUP BY shop_id,goodsid
;

create temporary table if not exists temp.p_invt_2
as 
SELECT a.product_code goodsid,
                    a.location_code as shop_id,
                    -- a.shipper_code,
                    a.reservoir_area_code,
                    a.reservoir_area_name,
                    sum(after_qty) inv_qty,
                    sum(after_amt) period_inv_amt,
                   -- sum(after_price) qm_price,
                    posting_time
   FROM
     (SELECT product_code,
             location_code,
             shipper_code,
             after_qty,
             after_amt,
             after_price,
             regexp_replace(to_date(posting_time), '-', '')posting_time,
             id,
             reservoir_area_code,
             reservoir_area_name
      FROM csx_ods.wms_accounting_stock_detail_view_ods
      WHERE sdt = '20191024' ) a
   JOIN
     (SELECT product_code,
             location_code,
             shipper_code,
             max(id)max_id,
             reservoir_area_code,
             reservoir_area_name
      FROM csx_ods.wms_accounting_stock_detail_view_ods
      WHERE 
         regexp_replace(to_date(update_time),'-','')<='20190924'
         AND regexp_replace(to_date(posting_time),'-','')<='20190924'
         
        AND sdt = '20191024'
      GROUP BY product_code,
               location_code,
               shipper_code,
               reservoir_area_code,
               reservoir_area_name) b ON a.product_code = b.product_code
   AND a.location_code = b.location_code
   AND a.shipper_code = b.shipper_code
   AND A.reservoir_area_code = b.reservoir_area_code
   AND a.id = b.max_id
   GROUP BY a.product_code,
            a.location_code,
            a.shipper_code,
            a.reservoir_area_code,
            a.reservoir_area_name,posting_time
            ;
-- ��ѯ����
drop table if  exists temp.p_invt_3;
create temporary table if not exists temp.p_invt_3
as 
select a.shop_id,a.goods_code goodsid,sum(a.sales_value)sale_30day,sum(a.sales_qty)qty_30day,sum(a.sales_sales_cost)sales_sales_cost
from csx_dw.sale_goods_m a 
where a.sdt<=regexp_replace(to_date(date_sub(current_date(),1)),'-','') 
and  a.sdt>=regexp_replace(to_date(date_sub(current_date(),31)),'-','') 
and a.category_code in ('12','13','14')
group by a.shop_id,a.category_small_code,a.goods_code
;

select province_name, shop_id,shop_name,a.goodsid,goodsname,catg_l_id,catg_l_name,catg_m_id,catg_m_name,catg_s_id,catg_s_name,firm_g1_id,firm_g1_name,dept_id,dept_name
,inv_qty,period_inv_amt,sale_30day,qty_30day,sales_sales_cost
from 
(
select b.province_name, a.shop_id,b.shop_name,a.goodsid,c.goodsname,catg_l_id,catg_l_name,catg_m_id,catg_m_name,catg_s_id,catg_s_name,firm_g1_id,firm_g1_name,dept_id,dept_name
,sum(inv_qty)inv_qty,sum(period_inv_amt)period_inv_amt,sum(sale_30day)sale_30day,sum(qty_30day)qty_30day,sum(sales_sales_cost)sales_sales_cost
from 
(
SELECT shop_id,goodsid,inv_qty,period_inv_amt, 0 sale_30day,0 qty_30day,0 sales_sales_cost FROM  temp.p_invt_1
UNION ALL
SELECT shop_id,goodsid,sum(inv_qty)inv_qty,sum(period_inv_amt)period_inv_amt, 0 sale_30day,0 qty_30day,0 sales_sales_cost from temp.p_invt_2 where  reservoir_area_code not in ('PD01','TS01') group by shop_id,goodsid
union  ALL 
SELECT a.shop_id,a.goodsid,0 inv_qty,0 period_inv_amt,sale_30day,qty_30day,sales_sales_cost FROM temp.p_invt_3 a
) a 
left join
(select shop_id, shop_name,province_code,province_name from csx_dw.shop_m where sdt='current')b on a.shop_id=b.shop_id
left join
(select goodsid,goodsname,catg_l_id,catg_l_name,catg_m_id,catg_m_name,catg_s_id,catg_s_name,firm_g1_id,firm_g1_name,dept_id,dept_name from dim.dim_goods_latest)c
on a.goodsid=c.goodsid
group by b.province_name, a.shop_id,b.shop_name,a.goodsid,c.goodsname,catg_l_id,catg_l_name,catg_m_id,catg_m_name,catg_s_id,catg_s_name,firm_g1_id,firm_g1_name,dept_id,dept_name
)a
where catg_l_id between '1200' and '1499'