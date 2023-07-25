-- 本报表销售数据以DC省区,与省区销售有偏差
-- 1、	周转天数：期间库存金额/期间库存成本	新逻辑：
-- 2、	未销售： 末次销售天数>30天且库存额>0元且入库天数>3; 	1、增加30天周转
-- 3、	用品高库存: 周转>45天,库存额>2000元 ，入库天数>7天; 	2、销售成本剔除：一件代发、直送、配送
-- 4、	食品高库存: 周转大于30天，库存额>2000元，入库天数>7天；	3、末次入库剔除：客退入库、直送、货到即配、地采
-- 5、	生鲜高库存 : 周转天数>15天且入库天数>3天且库存额>500元 ；
-- 6、	SKU数:期末有库存或期间有销售商品数
-- 7、	动销SKU ：期间有销售的商品
-- 8、	动销率：动销SKU/SKU数
-- 9、	负库存 ：库存数量或库存额<0
-- 10、	末次销售日期:起始日期为2019年1月1日
-- 11、	旧逻辑：末次入库日期:入库类型：不含客退入库
-- 12、	DMS默认为0.01

select a.dist_code,
       a.dist_name,
       goods_id,
       goods_name,
       unit_name,
       dept_id,
       dept_name,
       final_qty,
       final_amt,
       period_inv_amt_30day,
       cost_30day,
       turn_day,
       row_number()over(partition by dist_code order by final_amt desc) as row_id
from
(
select a.dist_code,
       a.dist_name,
       goods_id,
       goods_name,
       unit_name,
       dept_id,
       dept_name,
       sum(a.final_qty) as final_qty,
       sum(a.final_amt) as final_amt,
       sum(a.period_inv_amt_30day) as period_inv_amt_30day,
       sum(a.cost_30day) as cost_30day,
       if (sum(a.cost_30day)!=0,sum(a.period_inv_amt_30day)/sum(a.cost_30day),9999) as turn_day
from csx_tmp.ads_wms_r_d_goods_turnover a 
 join
(
SELECT sales_region_code,
       sales_region_name,
       shop_id,
       purpose_name,
       location_uses_name,
       location_type_name ,
       province_code,
       province_name
FROM csx_dw.dws_basic_w_a_csx_shop_m
WHERE sdt='current'
  AND purpose_name !='合伙人物流'
  and location_type_name ='仓库'
  AND sales_region_code='4'
  AND table_type=1
  ) b on a.dc_code=b.shop_id
where sdt='20210204' 
and ((a.days_turnover_30>45 and a.final_amt>2000 and a.entry_days>7 and a.division_code='13') 
    or (a.days_turnover_30>30 and a.final_amt>2000 and a.entry_days>7 and a.division_code='12'))
group by  a.dist_code,
       a.dist_name,
       goods_id,
       goods_name,
       unit_name,
       dept_id,
       dept_name
)a ;