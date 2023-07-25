select 
  a.sdt as `开票日期`,
  case when b.province_code = '320000' then 'W088'
   when b.province_code = '340000' then 'W080'
   when b.province_code = '330000' then 'W0F5' 
   when b.province_code = '350000' then 'W053' 
   when b.province_code = '110000' then 'W048' 
   when b.province_code = '500000' then 'W039' 
   when b.province_code = '440000' then 'W0J7' 
   when b.province_code = '510000' then 'W079'
  end as `工厂DC code`,
  case when b.province_code = '320000' then '江苏彩食鲜配送中心'
   when b.province_code = '340000' then '安徽彩食鲜配送中心'
   when b.province_code = '330000' then '江苏彩食鲜杭州分包中心' 
   when b.province_code = '350000' then '福建彩食鲜物流配送中心' 
   when b.province_code = '110000' then '北京彩食鲜物流配送中心' 
   when b.province_code = '500000' then '重庆彩食鲜物流配送中心' 
   when b.province_code = '440000' then '广东彩食鲜深圳物流配送中心' 
   when b.province_code = '510000' then '四川彩食鲜配送中心'
  end as `工厂`,
  coalesce(a.sap_dc_code, a.dc_code) as `发货DC code`,
  coalesce(a.sap_dc_name, a.dc_name) as `发货地点`,
  if(a.channel = 'M端', '商超（对内）', a.channel) as `销售渠道`,
  a.customer_no as `编码`,
  a.customer_name as `名称`,
  b.province_code as `省区`,
  b.sales_belong_flag as `业态`,
  a.department_code as `课组编码`,
  a.department_name as `课组名称`,
  a.category_large_code as `大类编码`,
  a.category_large_name as `大类名称`,
  a.category_middle_code as `中类编码`,
  a.category_middle_name as `中类名称`,
  a.category_small_code as `小类编码`,
  a.category_small_code as `小类名称`,
  a.goods_code as `商品编码`,
  a.goods_name as `商品名称`,
  case when a.province_code = '340000' 
    then if(d.goods_code is not null, '工厂产品', '非工厂产品')
    else if(c.mat_type = '成品', '工厂产品', '非工厂产品')
  end as `商品属性`,
  a.storage_location as `库存地点`,
  a.return_flag as `退货标识`,
  a.sales_qty as `数量`,
  a.sales_cost as `含税成本`,
  a.sales_value as `含税销售`
from
(
  select 
    *
  from csx_dw.sale_item_m 
  where sdt >= '20191001' and sales_type in ('qyg', 'gc', 'anhui_md') 
    and (customer_no like 'S%' or channel in ('M端', '商超（对内）', '商超（对外）'))
)a left outer join 
(
  select distinct 
    shop_id,
    province_code,
    sales_belong_flag
  from csx_dw.shop_m 
  where sdt = 'current' 
)b on  regexp_replace(a.customer_no, 'S', '') = b.shop_id
left outer join csx_ods.marc_ecc c on a.sap_origin_dc_code = c.shop_id and a.goods_code = c.goodsid
left outer join 
(
  select distinct goods_code from csx_dw.factory_bom 
  where sdt = 'current'
)d on a.goods_code = d.goods_code and a.province_code = '340000'; 