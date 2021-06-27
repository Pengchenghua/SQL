-- 高周转
--逻辑说明：期末库存量大于0，干货周转30天以上且库存金额大于2000，水果、蔬菜周转大于5天以上且库存金额大于500，生鲜其他课周转大于15天且金额大于2000，食品部周转大于45天且金额大于2000，用品类周转大于60天且金额大于3000  

SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
    --   a.division_code,
    --   a.division_name,
       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       classify_middle_code,
       classify_middle_name,
       a.dept_id,
       a.dept_name,
       coalesce(final_amt/final_qty) as cost,
       a.final_qty,
       a.final_amt,
       a.days_turnover_30,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
     AND sales_region_code='3') c ON a.dc_code=c.shop_id
WHERE sdt='20210623'
  AND a.final_qty>a.entry_qty
  AND ( (category_large_code='1101' and days_turnover_30>45 AND final_amt>2000)
    or (dept_id in ('H02','H03') and days_turnover_30>5 and a.final_amt>500 )
    OR (dept_id IN ('H04','H05','H06','H07','H08','H09','H10','H11') AND days_turnover_30>15 and a.final_amt>2000) 
    or (division_code ='12' and days_turnover_30>45 and final_amt>2000 )
    or (division_code in ('13','14','15')  and days_turnover_30>60 and final_amt>3000))
    and final_qty>0 ;



--盘点盈亏数据
--盘点盈亏明细
--逻辑说明 盘点盈亏金额±500以上

    SELECT sales_province_code,
       sales_province_name,
       shop_id,
       shop_name,
       a.goods_code,
       goods_name,
       unit_name,
       standard ,
       classify_middle_code,
       classify_middle_name,
       department_id,
       department_name,
       qty,
       amt,
       fina_qty,
       fina_amt
from 
(SELECT sales_province_code,
       sales_province_name,
       shop_id,
       shop_name,
       goods_code,
       c.goods_name,
       unit_name,
       standard ,
       classify_middle_code,
       classify_middle_name,
       department_id,
       department_name,
       sum(qty)qty,
       sum(amt)amt
FROM csx_dw.ads_wms_r_d_pd_detail_days a
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
     AND sales_region_code='3') b ON a.dc_code=b.shop_id
JOIN
(select goods_id,goods_name,unit_name,standard from csx_dw.dws_basic_w_a_csx_product_m where sdt='current') c on a.goods_code=c.goods_id
WHERE sdt>='20210401'
  AND sdt<'20210601'
  AND substr(move_type,1,3) IN ('115','116')
GROUP BY sales_province_code,
         sales_province_name,
         shop_id,
         shop_name,
         goods_code,
         c.goods_name,
         unit_name,
         standard ,
         classify_middle_code,
         classify_middle_name,
         department_id,
         department_name
) a 
LEFT JOIN 
(select dc_code,goods_code,sum(qty)fina_qty,sum(amt) fina_amt 
from csx_dw.dws_wms_r_d_accounting_stock_m 
    where sdt='20210531'  
    and reservoir_area_code not IN('PD01','PD02','TS01')
    GROUP BY dc_code,goods_code) b on a.shop_id=b.dc_code and a.goods_code=b.goods_code
where amt>500 or amt <-500;


-- 食百盘点

   SELECT sales_province_code,
       sales_province_name,
       shop_id,
       shop_name,
       a.goods_code,
       goods_name,
       unit_name,
       standard ,
       classify_middle_code,
       classify_middle_name,
       department_id,
       department_name,
       qty,
       amt
    --   fina_qty,
    --   fina_amt
from 
(SELECT sales_province_code,
       sales_province_name,
       shop_id,
       shop_name,
       goods_code,
       c.goods_name,
       unit_name,
       standard ,
       classify_middle_code,
       classify_middle_name,
       department_id,
       department_name,
       sum(qty)qty,
       sum(amt)amt
FROM csx_dw.ads_wms_r_d_pd_detail_days a
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
     AND sales_region_code='3') b ON a.dc_code=b.shop_id
JOIN
(select goods_id,goods_name,unit_name,standard from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and division_code in ('12','13','14')) c on a.goods_code=c.goods_id
WHERE sdt>='20210101'
  AND sdt<'20210624'
  AND substr(move_type,1,3) IN ('115','116')
GROUP BY sales_province_code,
         sales_province_name,
         shop_id,
         shop_name,
         goods_code,
         c.goods_name,
         unit_name,
         standard ,
         classify_middle_code,
         classify_middle_name,
         department_id,
         department_name
) a 
-- LEFT JOIN 
-- (select dc_code,goods_code,sum(qty)fina_qty,sum(amt) fina_amt 
-- from csx_dw.dws_wms_r_d_accounting_stock_m 
--     where sdt='20210531'  
--     and reservoir_area_code not IN('PD01','PD02','TS01')
--     GROUP BY dc_code,goods_code) b on a.shop_id=b.dc_code and a.goods_code=b.goods_code
where amt>500 or amt <-500;
  
  
  -- 未销售商品
--逻辑说明：期末库存量大于0，干货未销售30天以上，生鲜其他课水果、蔬菜、肉禽未销售5天以上，食品部大30天，用品类大于60天

SELECT a.dist_code,
       a.dist_name,
       a.dc_code,
       a.dc_name,
    --   a.division_code,
    --   a.division_name,

       a.goods_id,
       b.bar_code,
       b.goods_name,
       b.unit_name,
       b.standard,
       classify_middle_code,
       classify_middle_name,
       a.dept_id,
       a.dept_name,
       a.final_qty,
       a.final_amt,
       a.no_sale_days,
       a.max_sale_sdt,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          goods_name,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          unit_name,
          standard,
          bar_code
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
JOIN
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
     AND sales_region_code='3') c ON a.dc_code=c.shop_id
WHERE sdt='20210623'
  AND a.final_qty>a.entry_qty
  AND ((category_large_code='1101'
        AND (a.no_sale_days>30 or a.max_sale_sdt='' )
        AND a.entry_days>30)
       OR (dept_id IN ('H02',
                       'H03')
           AND (a.no_sale_days>5 or a.max_sale_sdt='' )
           AND a.entry_days>30)
       OR (dept_id IN ('H04',
                       'H05',
                       'H06',
                       'H07',
                       'H08',
                       'H09',
                       'H10',
                       'H11')
            AND (a.no_sale_days>30 or a.max_sale_sdt=''  )
        AND a.entry_days>30)
       OR (division_code ='12'
            AND ( a.no_sale_days>30 or a.max_sale_sdt='' )
        AND a.entry_days>30)
       OR (division_code IN ('13',
                             '14',
                             '15')
           AND (a.no_sale_days>60 or a.max_sale_sdt='' )
        AND a.entry_days>30))
  AND final_qty>0 
  and a.final_amt>2000;






--临保商品
--逻辑说明 ： 未次入库天数大于保质期天数


--临保商品
--逻辑说明 ： 未次入库天数大于保质期天数
SELECT sales_province_code,
       sales_province_name,
       dc_code,
       dc_name,
       a.goods_id,
       bar_code,
       a.goods_name,
       a.unit_name,
       standard,
       classify_middle_code,
       classify_middle_name,
       department_id,
       department_name,
       final_amt,
       final_qty,
       case when extent_date>1 then 1 else extent_date end extent_date,
       qualitative_period,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt,
       a.no_sale_days,
       a.max_sale_sdt
FROM
(
SELECT dc_code,
       dc_name,
       a.goods_id,
       bar_code,
       a.goods_name,
       a.unit_name,
       b.standard,
       classify_middle_code,
       classify_middle_name,
       department_id,
       department_name,
       final_amt,
       final_qty,
       qualitative_period,
       a.entry_days,
       a.entry_qty,
       a.entry_sdt,
       coalesce(entry_days/qualitative_period ,0) as extent_date,
       a.no_sale_days,
       a.max_sale_sdt
FROM csx_tmp.ads_wms_r_d_goods_turnover a
JOIN
  (SELECT goods_id,
          bar_code,
          goods_name,
          standard,
          classify_large_code,
          classify_large_name,
          classify_middle_code,
          classify_middle_name,
          department_id,
          department_name,
          qualitative_period
   FROM csx_dw.dws_basic_w_a_csx_product_m
   WHERE sdt='current') b ON a.goods_id=b.goods_id
WHERE sdt='20210623'
  -- AND dc_code='W0A7'
  AND business_division_code='12'
  -- AND a.entry_days>b.qualitative_period
  AND a.final_qty>0
  )a 
  join 
  (SELECT sales_province_code,
          sales_province_name,
          shop_id,
          shop_name
   FROM csx_dw.dws_basic_w_a_csx_shop_m
   WHERE sdt='current'
     AND table_type=1
     AND purpose IN ('01')
     AND sales_region_code='3') c ON a.dc_code=c.shop_id
  where round(extent_date,2)>0.75
  ;


--逾期客户
 -- 逾期率>80% 应收金额>5000元
    select * from csx_dw.dws_sss_r_a_customer_accounts 
    where sdt='20210623' 
    and province_name in ('四川省','重庆市','贵州省') 
    and round(overdue_amount/receivable_amount,2)>0.8 
    and receivable_amount>0 ;




-----返利数据			
select
		customer_no,---客户编码
		customer_name,---客户名称
		region_code,
		region_name,
		second_category_name,
		province_code,---省区编码
		province_name,---省区
		channel_code,---渠道
		channel_name,---渠道
		business_type_code,
		business_type_name,
		sales_type,
		count(distinct mon ) as sales_mon,
		count(distinct sdt) as sales_sdt,
		sum(sales_value) sales_value,---销售额
		sum(profit) profit---毛利额
		from 
(select	substr(sdt,1,6) as mon,
        sdt,
		customer_no,---客户编码
		customer_name,---客户名称
		second_category_name,
		region_code,
		region_name,
		province_code,---省区编码
		province_name,---省区
		channel_code,---渠道
		channel_name,---渠道
		business_type_code,
		business_type_name,
		sales_type,
		sum(sales_value) sales_value,---销售额
		sum(profit) profit---毛利额
from csx_dw.dws_sale_r_d_detail
where sdt>='20210401' and sales_type ='fanli' and region_name ='华西大区'
    and business_type_code='1'
group by 
	    substr(sdt,1,6),
        sdt,
	    customer_no,---客户编码
	    second_category_name,
		customer_name,---客户名称
		region_code,
		region_name,
		province_code,---省区编码
		province_name,---省区
		channel_code,---渠道
		channel_name,---渠道
		business_type_code,
		business_type_name,
		sales_type
)a where sales_value<0
group by customer_no,---客户编码
		customer_name,---客户名称
		second_category_name,
		region_code,
		region_name,
		province_code,---省区编码
		province_name,---省区
		channel_code,---渠道
		channel_name,---渠道
		business_type_code,
		business_type_name,
		sales_type;