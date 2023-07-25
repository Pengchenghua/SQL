
set
mapreduce.job.queuename = caishixian;
SET sdate='2020-01-01';


SET edate='2020-01-08';


DROP TABLE IF EXISTS temp.factory_stock_00;

--select * from temp.factory_stock_01;
CREATE
TEMPORARY TABLE temp.factory_stock_00 AS

select a.*,b.*,c.province_code,c.province_name from 
(select * from csx_dw.csx_product_info where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','') )a 
left join 
(select distinct factory_location_code, workshop_code,goods_code from csx_dw.factory_bom 
    where sdt=regexp_replace(${hiveconf:edate},'-','')  )b
on  a.product_code=b.goods_code and a.shop_code=b.factory_location_code
LEFT JOIN
(select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current') c 
on regexp_replace(a.shop_code,'(^E)','9')=c.shop_id
;

DROP TABLE IF EXISTS temp.factory_stock_01;


CREATE
TEMPORARY TABLE temp.factory_stock_01 AS

SELECT dc_code,province_code,province_name,workshop_code,workshop_name,
      a.goods_code,
       division_code,
       final_qty,
       final_amt,
       period_qty,
       period_amt,
       receive_amt,
       if(c.goods_code is not null, '是', '否') as label
       from 
(
SELECT dc_code,
      a.goods_code,
       division_code,
       sum(CASE
               WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN qty
           END) AS final_qty,
       sum(CASE
               WHEN sdt=regexp_replace(${hiveconf:edate},'-','') THEN amt
           END) AS final_amt,
       sum(qty) AS period_qty,
       sum(amt) AS period_amt
FROM csx_dw.wms_accounting_stock_m a 
WHERE sdt>=regexp_replace(${hiveconf:sdate},'-',
                                            '')
  AND sdt<=regexp_replace(${hiveconf:edate},'-',
                                            '')
  --and a.dc_code='W048'
  AND reservoir_area_code NOT IN ('B999',
                                  'B997',
                                  'PD01',
                                  'PD02',
                                  'TS01')
GROUP BY dc_code,
         a.goods_code,
         division_code )a
LEFT JOIN
(select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current')b 
on regexp_replace(a.dc_code,'(^E)','9')=b.shop_id
LEFT OUTER JOIN
(select  DISTINCT factory_location_code, workshop_code,workshop_name, goods_code 
    from csx_dw.factory_bom where sdt=regexp_replace(${hiveconf:edate},'-',''))c 
    on a.goods_code=c.goods_code and a.dc_code=c.factory_location_code
LEFT JOIN
(select receive_location_code,goods_code,sum(receive_qty*price) as receive_amt 
from csx_dw.wms_entry_order_m where sdt=regexp_replace(${hiveconf:edate},'-','')
    GROUP BY receive_location_code,goods_code)d  on a.dc_code=d.receive_location_code and a.goods_code=d.goods_code
;

DROP TABLE IF EXISTS temp.factory_sale;

drop table  if exists temp.factory_sale;
CREATE
TEMPORARY TABLE temp.factory_sale AS
select 
     a.shop_id,
     b.province_code,
     a.province_name,
      channel,
      channel_name,
      customer_no,
      a.goods_code,
      bd_id,bd_name,
      sale,
      profit,
      mom_sale,
      mom_profit,
      yoy_sale,
      yoy_profit
from (
	SELECT
		a.shop_id,	
      a.province_code,
	  a.province_name,
    CASE WHEN channel IN ('1','7','3') THEN '1'	else a.channel END channel,
		CASE WHEN channel IN ('1','7','3') THEN '大'	else a.channel_name END channel_name,
	customer_no,
	goods_code,
	case when category_code in ('12','13','14') then '12' 
	    when category_code in ('10','11') then '11'
	    else '15' end bd_id,
	case when category_code in ('12','13','14') then '食品用品部' 
	    when category_code in ('10','11') then '生鲜部'
	    else '易耗品' end bd_name,
 		sum(CASE when sdt >= regexp_replace(${hiveconf:sdate},'-','') and sdt <= regexp_replace(${hiveconf:edate},'-','') then sales_value end ) sale,
		sum(CASE when sdt >=  regexp_replace(${hiveconf:sdate},'-','') and sdt <= regexp_replace(${hiveconf:edate},'-','') then profit end) profit,
		sum(CASE when sdt >=  regexp_replace(add_months(${hiveconf:sdate},-1),'-','') 
            and sdt <= regexp_replace(add_months(${hiveconf:edate},-1),'-','') then sales_value end ) mom_sale,
		sum(CASE when sdt >=  regexp_replace(add_months(${hiveconf:sdate},-1),'-','') 
           and sdt <= regexp_replace(add_months(${hiveconf:edate},-1),'-','') then profit end)  mom_profit,
		sum(CASE when sdt >=  regexp_replace(add_months(${hiveconf:sdate},-12),'-','') 
            and sdt <= regexp_replace(add_months(${hiveconf:edate},-12),'-','') then sales_value end ) yoy_sale,
		sum(CASE when sdt >=  regexp_replace(add_months(${hiveconf:sdate},-12),'-','') 
            and sdt <= regexp_replace(add_months(${hiveconf:edate},-12),'-','') then profit end)  yoy_profit
		from
			csx_dw.sale_goods_m1 a 
		where
			sdt >=  regexp_replace(add_months(${hiveconf:sdate},-12),'-','') 
            and sdt <= regexp_replace(${hiveconf:edate},'-','')
			and province_name like '重庆%'
		group by 	a.shop_id,	
      a.province_code,
	  a.province_name,
    CASE WHEN channel IN ('1','7','3') THEN '1'	else a.channel END ,
		CASE WHEN channel IN ('1','7','3') THEN '大'	else a.channel_name END ,
	customer_no,
	goods_code,
	case when category_code in ('12','13','14') then '12' 
	    when category_code in ('10','11') then '11'
	    else '15' end ,
	case when category_code in ('12','13','14') then '食品用品部' 
	    when category_code in ('10','11') then '生鲜部'
	    else '易耗品' end
)a
LEFT JOIN 
 (select `limit` province_code,province from csx_ods.sys_province_ods)b on a.province_name=b.province
;

--select * from temp.factory_sale where shop_id='W039' and goods_code='988738';

-- 关联销售工厂商品
DROP TABLE IF EXISTS temp.factory_stock_02;

CREATE
TEMPORARY TABLE temp.factory_stock_02 AS
 select 
     a.shop_id,
     a.province_code,
     a.province_name,workshop_code,workshop_name,
      channel,
      channel_name,
      customer_no,
      a.goods_code,
      bd_id,bd_name,
      sale,
      profit,
      mom_sale,
      mom_profit,
      yoy_sale,
      yoy_profit,
 if(c.goods_code is NOT NULL,'是','否') as label
from (select * from  temp.factory_sale) a 
LEFT OUTER JOIN
(select  province_code, workshop_code,workshop_name, goods_code from csx_dw.factory_bom 
  where sdt= regexp_replace(${hiveconf:edate},'-','') 
GROUP BY   province_code,workshop_code,workshop_name, goods_code)c 
on a.goods_code=c.goods_code and a.province_code=c.province_code
;

--select * from temp.factory_stock_02;

drop table if exists temp.factory_stock_03;
CREATE temporary table temp.factory_stock_03
as 
SELECT province_code,province_name,
       goods_code,workshop_code,
          workshop_name,
       sum(sale_sku)sale_sku,
     --  sum(all_sku) as all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit)as yoy_profit,
       sum(final_qty) AS final_qty,
       sum(final_amt) AS final_amt,
       sum(period_qty) AS period_qty,
       sum(period_amt) AS period_amt,
       coalesce(sum(period_amt)/sum(sale-profit),0) as day_turnover,
       sum(negative_sku) as negative_sku,
       sum(receive_amt)as receive_amt
FROM
  (SELECT a.province_code,province_name,a.goods_code,workshop_code,
          workshop_name,
          count(DISTINCT CASE
                             WHEN coalesce(sale,0) !=0 THEN a.goods_code
                         END) AS sale_sku,
          0 AS negative_sku,
          sum(sale) AS sale,
          sum(profit) AS profit,
          sum(A.mom_sale) AS mom_sale,
          sum(A.mom_profit)as mom_profit,
          sum(A.yoy_sale) as yoy_sale,
          sum(A.yoy_sale)AS yoy_profit,
          0 AS final_qty,
          0 AS final_amt,
          0 AS period_qty,
          0 AS period_amt,
          0 as receive_amt
   FROM temp.factory_stock_02 A
   GROUP BY a.province_code,province_name,a.goods_code,workshop_code,
            workshop_name
   UNION ALL 
   SELECT a.province_code,province_name ,a.goods_code,workshop_code,
          workshop_name,
         0 AS sale_sku,
         0 as negative_sku,
         0 AS sale,
         0 AS profit,
         0 AS mom_sale,
         0 as mom_profit,
         0 as yoy_sale,
         0 AS yoy_profit,
         sum(final_qty) AS final_qty,
         sum(final_amt) AS final_amt,
         sum(period_qty) AS period_qty,
         sum(period_amt) AS period_amt,
         sum(receive_amt) as receive_amt
   FROM temp.factory_stock_01 a
    GROUP BY a.province_code,province_name,a.goods_code,workshop_code,
          workshop_name
    union all 
    SELECT province_code,province_name,goods_code,workshop_code,
          workshop_name,
          0 AS sale_sku,
           count(DISTINCT case when profit <0  then goods_code end ) AS negative_sku,
          0 AS sale,
          0 AS profit,
          0 AS mom_sale,
          0 as mom_profit,
          0 as yoy_sale,
          0 AS yoy_profit,
          0 AS final_qty,
          0 AS final_amt,
          0 AS period_qty,
          0 AS period_amt,
          0 as receive_amt
   FROM (select a.province_code,province_name,a.goods_code,workshop_code,
          workshop_name,
           sum(profit) profit from temp.factory_stock_02 a
   GROUP BY a.province_code,province_name,a.goods_code,workshop_code,
          workshop_name
            )a GROUP BY province_code,province_name,goods_code,workshop_code,
          workshop_name) a
GROUP BY province_code,province_name,goods_code,workshop_code,
          workshop_name;
 
-- select * from  temp.factory_stock_04 where province_name like '重庆%';
 -- 计算工厂、车间销售额
 drop table if exists temp.factory_stock_04;
CREATE temporary table temp.factory_stock_04
as 
 SELECT province_code,province_name,
       workshop_code,workshop_name,
       sum(sale_sku) AS sale_sku,
       count(DISTINCT  goods_code  ) all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       sum(final_amt) AS final_amt,
       sum(period_qty) AS period_qty,
       sum(period_amt) AS period_amt,
       coalesce(sum(period_amt)/sum(sale-profit),0) as day_turnover,
       sum(negative_sku) as negative_sku,
       sum(receive_amt)as receive_amt
FROM
temp.factory_stock_03 where workshop_code is not null
GROUP BY province_code,province_name,
       workshop_code,workshop_name
       union all 
     SELECT province_code,province_name,
      '00' workshop_code,'工厂'workshop_name,
       sum(sale_sku) AS sale_sku,
       count(DISTINCT  goods_code  ) all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       sum(final_amt) AS final_amt,
       sum(period_qty) AS period_qty,
       sum(period_amt) AS period_amt,
       coalesce(sum(period_amt)/sum(sale-profit),0) as day_turnover,
       sum(negative_sku) as negative_sku,
       sum(receive_amt)as receive_amt
FROM
temp.factory_stock_03 where workshop_code is not null
GROUP BY province_code,province_name
       
;

 -- 计算部类业绩
 drop table if exists temp.factory_stock_05;
CREATE temporary table temp.factory_stock_05
as 
 SELECT province_code,province_name,
       bd_id,bd_name,
       sum(sale_sku) AS sale_sku,
       count(DISTINCT  goods_code  ) all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       sum(final_amt) AS final_amt,
       sum(period_qty) AS period_qty,
       sum(period_amt) AS period_amt,
       coalesce(sum(period_amt)/sum(sale-profit),0) as day_turnover,
       sum(negative_sku) as negative_sku,
       sum(receive_amt)as receive_amt
FROM
temp.factory_stock_03 where workshop_code is not null
GROUP BY province_code,province_name,
       bd_id,bd_name
       union all 
     SELECT province_code,province_name,
      '00' bd_id,'物流'bd_name,
       sum(sale_sku) AS sale_sku,
       count(DISTINCT  goods_code  ) all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       sum(final_amt) AS final_amt,
       sum(period_qty) AS period_qty,
       sum(period_amt) AS period_amt,
       coalesce(sum(period_amt)/sum(sale-profit),0) as day_turnover,
       sum(negative_sku) as negative_sku,
       sum(receive_amt)as receive_amt
FROM
temp.factory_stock_03 where workshop_code is not null
GROUP BY province_code,province_name
       
;

--计算商超/大销售
drop table if exists temp.factory_stock_06;
CREATE temporary table temp.factory_stock_06
as 
 SELECT province_code,province_name,
       channel,channel_name,
       sum(sale_sku) AS sale_sku,
       count(DISTINCT  goods_code  ) all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       sum(final_amt) AS final_amt,
       sum(period_qty) AS period_qty,
       sum(period_amt) AS period_amt,
       coalesce(sum(period_amt)/sum(sale-profit),0) as day_turnover,
       sum(negative_sku) as negative_sku,
       sum(receive_amt)as receive_amt
FROM
temp.factory_stock_03 where workshop_code is not null
GROUP BY province_code,province_name,
        channel,channel_name
       union all 
     SELECT province_code,province_name,
      '00' channel,'全渠道'channel_name,
       sum(sale_sku) AS sale_sku,
       count(DISTINCT  goods_code  ) all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       sum(final_amt) AS final_amt,
       sum(period_qty) AS period_qty,
       sum(period_amt) AS period_amt,
       coalesce(sum(period_amt)/sum(sale-profit),0) as day_turnover,
       sum(negative_sku) as negative_sku,
       sum(receive_amt)as receive_amt
FROM
temp.factory_stock_03 where workshop_code is not null
GROUP BY province_code,province_name
;

-- 计算车间数
drop table if exists temp.factory_cust_01;
CREATE temporary table temp.factory_cust_01
as 
select a.province_code,workshop_code,workshop_name,shop_dept_cust,big_dept_cust,round(big_dept_cust/big_cust,4) sale_cust_ratio ,big_cust from
(select province_code,workshop_code,workshop_name,
  count( distinct case when channel_name ='商超' then customer_no end) as shop_dept_cust,
  count( distinct case when channel_name ='大' then customer_no end) as big_dept_cust  from temp.factory_stock_02
group by  province_code,workshop_code,workshop_name)a
left join 
(select province_code,
  count( distinct case when channel_name ='大' then customer_no end) as big_cust  from temp.factory_stock_02
group by  province_code) b on a.province_code=b.province_code
;
--计算部类数
drop table if exists temp.factory_cust_02;
CREATE temporary table temp.factory_cust_02
as 
select a.province_code,bd_id,bd_name,
shop_dept_cust,
big_dept_cust,
round(big_dept_cust/big_cust,4) sale_cust_ratio ,
big_cust from
(select province_code,bd_id,bd_name,
  count( distinct case when channel_name ='商超' then customer_no end) as shop_dept_cust,
  count( distinct case when channel_name ='大' then customer_no end) as big_dept_cust  from temp.factory_stock_02
group by  province_code,bd_id,bd_name)a
left join 
(select province_code,
  count( distinct case when channel_name ='大' then customer_no end) as big_cust  from temp.factory_stock_02
group by  province_code) b on a.province_code=b.province_code
;
 
 --计算数
drop table if exists temp.factory_cust_03;
CREATE temporary table temp.factory_cust_03
as 
select a.province_code,bd_id,bd_name,
shop_dept_cust,
big_dept_cust,
round(big_dept_cust/big_cust,4) sale_cust_ratio ,
big_cust from
(select province_code,channel,channel_name,
  count( distinct case when channel_name ='商超' and sale!=0 then customer_no end) as shop_dept_cust,
  count( distinct case when channel_name ='大'and sale!=0 then customer_no end) as big_dept_cust  from temp.factory_stock_02
group by  province_code,channel,channel_name)a
left join 
(select province_code,
  count( distinct case when channel_name ='大' then customer_no end) as big_cust  from temp.factory_stock_02
group by  province_code) b on a.province_code=b.province_code
;
