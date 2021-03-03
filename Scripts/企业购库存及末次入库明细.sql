select
	province_code,
	province_name,
	dc_code,
	dc_name,
	goods_code,
	goods_name,
	goods_bar_code,
	unit_name,
	dept_id,
	dept_name,
	category_middle_code,
	category_middle_name,
	vendor_code,
	vendor_name,
	inventory_qty,
	inventory_amt,
	in_val/in_qty as price,
	 in_qty,
    in_val,
    max_sdt
	from 
(select
	province_code,
	province_name,
	dc_code,
	dc_name,
	goods_code,
	goods_name,
	goods_bar_code,
	unit_name,
	dept_id,
	dept_name,
	category_middle_code,
	category_middle_name,
	vendor_code,
	vendor_name,
	inventory_qty,
	inventory_amt
from
	csx_dw.dc_sale_inventory
where
	sdt = '20200315'
	and dc_code='W0A6' 
	and inventory_amt !=0
	and category_small_code BETWEEN '12000000' and '14999999'
)a
LEFT join (	
SELECT a.receive_location_code shop_id,
        a.goods_code goodsid,
       in_qty,
     receive_amt as  in_val,
      sdt as  max_sdt
FROM
  (select receive_location_code,goods_code,sdt,sum(receive_qty)in_qty,sum(amount)receive_amt from csx_dw.wms_entry_order_all_m group by receive_location_code,goods_code,sdt)a join
  (select receive_location_code,goods_code,max(sdt) max_sdt from csx_dw.wms_entry_order_all_m  group by receive_location_code,goods_code)b 
  on a.receive_location_code=b.receive_location_code and a.goods_code=b.goods_code and a.sdt=b.max_sdt
)b  on a.dc_code=b.shop_id and a.goods_code=b.goodsid;

select a.*,b.in_qty,b.in_val,b.max_sdt from csx_dw.temp_supp_inv a 
left join 
(select * from  temp.p_sale_02)b on a.dc_code=b.shop_id and a.goods_code=b.goodsid;

select * from csx_dw.wms_entry_order_all_m where goods_code='1005223' and receive_location_code='W0A6' and sdt='20190611';


SELECT a.shop_id,
          a.goodsid,
          sum(a.pur_qty_in)in_qty,
          sum(a.tax_pur_val_in)in_val,
          plan_delivery_date max_sdt
   FROM b2b.ord_orderflow_t a
   join 
   (SELECT a.shop_id,
          a.goodsid,
          max(a.plan_delivery_date)max_sdt
     FROM b2b.ord_orderflow_t a
   WHERE a.ordertype IN ('配送',
                         '直送',
                         '直通',
                         '货到即配',
                         'UD')
     AND a.delivery_finish_flag='X' and a.shop_id = 'W0A6' and  goodsid='1005223'
   GROUP BY a.shop_id,
            a.goodsid )b on a.shop_id=b.shop_id and a.goodsid=b.goodsid and a.plan_delivery_date=b.max_sdt 
    GROUP BY a.shop_id,plan_delivery_date,
          a.goodsid
