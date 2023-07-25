
set
mapreduce.job.queuename = caishixian;
SET sdate=regexp_replace(trunc(date_sub(current_date(),1),'MM'),'-','');
SET edate=regexp_replace(date_sub(current_date(),1),'-','');
set l_sdate=regexp_replace(trunc(add_months(date_sub(current_date(),1),-1),'MM'),'-','');
set l_edate=regexp_replace(add_months(date_sub(current_date(),1),-1),'-','');
set y_sdate=regexp_replace(trunc(add_months(date_sub(current_date(),1),-12),'MM'),'-','');
set y_edate=regexp_replace(add_months(date_sub(current_date(),1),-12),'-','');
--商品资料
-- DROP TABLE IF EXISTS temp.factory_stock_00;

-- --select * from temp.factory_stock_01;
-- CREATE
-- TEMPORARY TABLE temp.factory_stock_00 AS

-- select a.*,b.*,c.province_code,c.province_name from 
-- (select * from csx_dw.csx_product_info where sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','') )a 
-- left join 
-- (select distinct factory_location_code, goods_code from csx_dw.factory_bom 
--     where sdt=regexp_replace('${hiveconf:edate}','-','')  )b
-- on  a.product_code=b.goods_code and a.shop_code=b.factory_location_code
-- LEFT JOIN
-- (select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current') c 
-- on regexp_replace(a.shop_code,'(^E)','9')=c.shop_id
-- ;

-- 库存
DROP TABLE IF EXISTS temp.factory_stock_01;


CREATE
TEMPORARY TABLE temp.factory_stock_01 AS

SELECT dc_code,province_code,province_name,workshop_name,
      a.goods_code,
       bd_id,bd_name,
       final_qty,
       final_amt,
       period_qty,
       period_amt,
       receive_amt,
       if(c.goods_code is not null, '1', '0') as label
       from 
(
SELECT dc_code,
      a.goods_code,
      case when division_code  in ('12','13','14') then '12' 
	    when division_code  in ('10','11') then '11'
	    else division_code end bd_id,
	case when division_code in ('12','13','14') then '食品用品部' 
	    when division_code in ('10','11') then '生鲜部'
	    else a.division_name end bd_name,
       sum(CASE
               WHEN sdt=${hiveconf:edate} THEN qty
           END) AS final_qty,
       sum(CASE
               WHEN sdt=${hiveconf:edate} THEN amt
           END) AS final_amt,
       sum(qty) AS period_qty,
       sum(amt) AS period_amt
FROM csx_dw.wms_accounting_stock_m a 
WHERE sdt>=${hiveconf:sdate}
  AND sdt<=${hiveconf:edate}
  --and a.dc_code='W048'
  AND reservoir_area_code NOT IN ('B999',
                                  'B997',
                                  'PD01',
                                  'PD02',
                                  'TS01')
GROUP BY dc_code,
         a.goods_code,
         case when division_code  in ('12','13','14') then '12' 
	    when division_code  in ('10','11') then '11'
	    else division_code end ,
	case when division_code in ('12','13','14') then '食品用品部' 
	    when division_code in ('10','11') then '生鲜部'
	    else a.division_name end  )a
LEFT JOIN
(select shop_id,province_code,province_name from csx_dw.shop_m where sdt='current')b 
on regexp_replace(a.dc_code,'(^E)','9')=b.shop_id
LEFT OUTER JOIN
(select DISTINCT  factory_location_code, workshop_name, goods_code 
    from csx_dw.factory_bom where sdt= ${hiveconf:edate}
    )c 
    on a.goods_code=c.goods_code and a.dc_code=c.factory_location_code
LEFT JOIN
(select receive_location_code,goods_code,sum(receive_qty*price) as receive_amt 
from csx_dw.wms_entry_order_m where sdt= ${hiveconf:edate}
    GROUP BY receive_location_code,goods_code)d  on a.dc_code=d.receive_location_code and a.goods_code=d.goods_code
;

-- 销售渠道bd_id归属
drop table  if exists temp.factory_sale;
CREATE
TEMPORARY TABLE temp.factory_sale AS
select 
     a.dc_code,
     b.province_code,
     a.province_name,
     -- workshop_code ,
	  workshop_name ,
      channel,
      channel_name,
      customer_no,
      a.goods_code,
      bd_id,bd_name,
      label,
      sale,
      profit,
      mom_sale,
      mom_profit,
      yoy_sale,
      yoy_profit
from (
	SELECT
		a.dc_code,	
      a.province_code,
	  a.province_name,
	 -- workshop_code ,
	  workshop_name ,
    CASE WHEN channel IN ('1','7','3') THEN '1'	else a.channel END channel,
		CASE WHEN channel IN ('1','7','3') THEN '大'   WHEN channel IN ('2') then '商超' else a.channel_name END channel_name,
	customer_no,
	goods_code,
	case when division_code  in ('12','13','14') then '12' 
	    when division_code  in ('10','11') then '11'
	    else division_code end bd_id,
	case when division_code in ('12','13','14') then '食品用品部' 
	    when division_code in ('10','11') then '生鲜部'
	    else a.division_name end bd_name,
	  a.is_factory_goods_code as  label,
 		sum(CASE when sdt >=  ${hiveconf:sdate} and sdt <= ${hiveconf:edate} then sales_value end ) sale,
		sum(CASE when sdt >=  ${hiveconf:sdate} and sdt <= ${hiveconf:edate} then profit end) profit,
		sum(CASE when sdt >=  ${hiveconf:l_sdate}
            and sdt <= ${hiveconf:l_edate} then sales_value end ) mom_sale,
		sum(CASE when sdt >=  ${hiveconf:l_sdate}
           and sdt <= ${hiveconf:l_edate} then profit end)  mom_profit,
		sum(CASE when sdt >=  ${hiveconf:y_sdate} 
            and sdt <= ${hiveconf:y_edate}  then sales_value end ) yoy_sale,
		sum(CASE when sdt >=  ${hiveconf:y_sdate}  
            and sdt <= ${hiveconf:y_edate}  then profit end)  yoy_profit
		from
			csx_dw.customer_sale_m a 
		where
			sdt >= ${hiveconf:y_sdate} 
            and sdt <= ${hiveconf:edate} 
		group by 	a.dc_code,	
      a.province_code,
	  a.province_name,
     CASE WHEN channel IN ('1','7','3') THEN '1'	else a.channel END ,
		CASE WHEN channel IN ('1','7','3') THEN '大'   WHEN channel IN ('2') then '商超' else a.channel_name END ,
	customer_no,
	goods_code,
		case when division_code  in ('12','13','14') then '12' 
	    when division_code  in ('10','11') then '11'
	    else division_code end ,
	case when division_code in ('12','13','14') then '食品用品部' 
	    when division_code in ('10','11') then '生鲜部'
	    else a.division_name end,is_factory_goods_code,
		--workshop_code ,
	  workshop_name 
)a
LEFT JOIN 
 (select cast (`limit` as decimal (26,0)) as  province_code,province from csx_ods.sys_province_ods)b on a.province_name=b.province
;
--select * from temp.factory_sale_01;
-- 汇总SKU 动销
drop table if exists temp.factory_sale_01;
CREATE temporary table temp.factory_sale_01
as 
SELECT province_code,province_name,
       goods_code,
          workshop_name,
          bd_id,bd_name,label,channel,channel_name,
       sum(sale_sku)sale_sku,
     --  sum(all_sku) as all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit)as yoy_profit,
       sum(negative_sku) as negative_sku
FROM
  (
SELECT a.province_code,province_name,a.goods_code,
          workshop_name,A.bd_id,A.bd_name,label,channel,channel_name,
          count(DISTINCT CASE
                             WHEN coalesce(sale,0) !=0 THEN a.goods_code
                         END) AS sale_sku,
          0 AS negative_sku,
          sum(sale) AS sale,
          sum(profit) AS profit,
          sum(A.mom_sale) AS mom_sale,
          sum(A.mom_profit)as mom_profit,
          sum(A.yoy_sale) as yoy_sale,
          sum(A.yoy_profit)AS yoy_profit
   FROM temp.factory_sale A
   GROUP BY a.province_code,province_name,a.goods_code,
            workshop_name,A.bd_id,A.bd_name,label,channel,channel_name
	union all 
    SELECT province_code,province_name,goods_code,
          workshop_name,A.bd_id,A.bd_name,label,channel,channel_name,
          0 AS sale_sku,
           count(DISTINCT case when profit <0  then goods_code end ) AS negative_sku,
          0 AS sale,
          0 AS profit,
          0 AS mom_sale,
          0 as mom_profit,
          0 as yoy_sale,
          0 AS yoy_profit
   FROM (select a.province_code,province_name,a.goods_code,
          workshop_name,A.bd_id,A.bd_name,label,channel,channel_name,
           sum(profit) profit from temp.factory_sale a
   GROUP BY a.province_code,province_name,a.goods_code,
         A.bd_id,A.bd_name, workshop_name,label,channel,channel_name
            )a GROUP BY province_code,province_name,goods_code,A.bd_id,A.bd_name,
          workshop_name,label,channel,channel_name
	) a group by 
		province_code,province_name,
       goods_code,
          workshop_name,
          bd_id,bd_name,label,channel,channel_name;
-- 关联库存
drop table if exists temp.factory_stock_03;
CREATE temporary table temp.factory_stock_03
as 
SELECT province_code,province_name,
       goods_code,
          workshop_name,
          bd_id,bd_name,label,
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
  (SELECT a.province_code,province_name,a.goods_code,
          workshop_name,A.bd_id,A.bd_name,label,
          count(DISTINCT CASE
                             WHEN coalesce(sale,0) !=0 THEN a.goods_code
                         END) AS sale_sku,
          sum(negative_sku)as negative_sku,
          sum(sale) AS sale,
          sum(profit) AS profit,
          sum(A.mom_sale) AS mom_sale,
          sum(A.mom_profit)as mom_profit,
          sum(A.yoy_sale) as yoy_sale,
          sum(A.yoy_profit)AS yoy_profit,
          0 AS final_qty,
          0 AS final_amt,
          0 AS period_qty,
          0 AS period_amt,
          0 as receive_amt
   FROM temp.factory_sale_01 a
   GROUP BY a.province_code,province_name,a.goods_code,
            workshop_name,A.bd_id,A.bd_name,label
   UNION ALL 
   SELECT a.province_code,province_name ,a.goods_code,
          workshop_name,A.bd_id,A.bd_name,label,
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
    GROUP BY a.province_code,province_name,a.goods_code,
          workshop_name,A.bd_id,A.bd_name,label
    ) a
GROUP BY province_code,province_name,goods_code,A.bd_id,A.bd_name,
          workshop_name,label;
 
-- select * from  temp.factory_stock_04 where province_name like '重庆%';
 -- 计算工厂、车间销售额
-- select * from temp.factory_stock_04;
 drop table if exists temp.factory_stock_04;
CREATE temporary table temp.factory_stock_04
as 
 SELECT province_code,province_name,
       workshop_name,
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
temp.factory_stock_03 where  workshop_name is not null and label='1'
GROUP BY province_code,province_name,
       workshop_name
union all 
     SELECT province_code,province_name,
      '工厂'workshop_name,
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
temp.factory_stock_03 where label ='1' and workshop_name is not null 
GROUP BY province_code,province_name
       
;

--select * from temp.factory_stock_03 where  workshop_code is not null ;
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
temp.factory_stock_03 
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
temp.factory_stock_03 
GROUP BY province_code,province_name
       
;

--计算商超/大销售
--select * from temp.factory_stock_06;
drop table if exists temp.factory_stock_06;
CREATE temporary table temp.factory_stock_06
as 
 SELECT province_code,province_name,
       channel,channel_name,
       sum(sale_sku) AS sale_sku,
       0 all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       0 AS final_amt,
       0 AS period_qty,
       0 AS period_amt,
       0 as day_turnover,
       sum(negative_sku) as negative_sku,
       0 as receive_amt
FROM
temp.factory_sale_01
GROUP BY province_code,province_name,
        channel,channel_name
union all 
     SELECT province_code,province_name,
      '00' channel,'全渠道'channel_name,
       sum(sale_sku) AS sale_sku,
       0 all_sku,
       sum(sale) AS sale,
       sum(profit) AS profit,
       sum(mom_sale) AS mom_sale,
       sum(mom_profit) as mom_profit,
       sum(yoy_sale)AS yoy_sale,
       sum(yoy_profit) AS yoy_profit,
       0 AS final_amt,
       0  AS period_qty,
       0 AS period_amt,
       0 as day_turnover,
       sum(negative_sku) as negative_sku,
       0 as receive_amt
FROM
temp.factory_sale_01 
GROUP BY province_code,province_name
;

-- 计算车间数
drop table if exists temp.factory_cust_01;
CREATE temporary table temp.factory_cust_01
as 
select a.province_code,a.province_name,c.workshop_code,a.workshop_name,shop_dept_cust,big_dept_cust,round(big_dept_cust/big_cust,4) sale_cust_ratio ,big_cust,
mon_big_dept_cust from
(select province_code,province_name,workshop_name,
  count( distinct case when channel_name ='商超'and coalesce(sale,0)!=0 then customer_no end) as shop_dept_cust,
  count( distinct case when channel_name ='大'and coalesce(sale,0)!=0 then customer_no end) as big_dept_cust,
  count( distinct case when channel_name ='大'and coalesce(mom_sale,0)!=0 then customer_no end) as mon_big_dept_cust from temp.factory_sale 
  where label='1'
group by  province_code,workshop_name,province_name)a
left join 
(select province_code,province_name,
  count( distinct case when channel_name ='大'and coalesce(sale,0)!=0 then customer_no end) as big_cust  from temp.factory_sale
  where label='1'
group by  province_code,province_name) b on a.province_code=b.province_code and a.province_name=b.province_name
left join 
(select distinct workshop_code,workshop_name from csx_dw.factory_bom where sdt=regexp_replace(date_sub(current_date(),1),'-','')) c on a.workshop_name=c.workshop_name
;
--计算部类数
drop table if exists temp.factory_cust_02;
CREATE temporary table temp.factory_cust_02
as 
select a.province_code,a.province_name,bd_id,bd_name,
shop_dept_cust,
big_dept_cust,
round(big_dept_cust/big_cust,4) sale_cust_ratio ,
big_cust,mon_big_dept_cust from
(select province_code,province_name,bd_id,bd_name,
  count( distinct case when channel_name ='商超' and coalesce(sale,0)!=0 then customer_no end) as shop_dept_cust,
  count( distinct case when channel_name ='大'and coalesce(sale,0)!=0 then customer_no end) as big_dept_cust,
  count( distinct case when channel_name ='大'and coalesce(mom_sale,0)!=0 then customer_no end) as mon_big_dept_cust 
  from temp.factory_sale
group by  province_code,bd_id,bd_name,province_name)a
left join 
(select province_code,province_name,
  count( distinct case when channel_name ='大'and coalesce(sale,0)!=0 then customer_no end) as big_cust  from temp.factory_sale
group by  province_code,province_name) b on a.province_code=b.province_code and a.province_name=b.province_name
;
 
 --计算部类汇总数
drop table if exists temp.factory_cust_03;
CREATE temporary table temp.factory_cust_03
as  
select a.province_code,a.province_name,'00' bd_id,'物流'bd_name,
shop_dept_cust,
big_dept_cust,
round(big_dept_cust/big_cust,4) sale_cust_ratio ,
big_cust,mon_big_dept_cust from
(select nvl(province_code,'')province_code,province_name,channel,channel_name,
  count( distinct case when channel_name ='商超' and sale!=0 then customer_no end) as shop_dept_cust,
  count( distinct case when channel_name ='大'and sale!=0 then customer_no end) as big_dept_cust,
  count( distinct case when channel_name ='大'and coalesce(mom_sale,0)!=0 then customer_no end) as mon_big_dept_cust 
from temp.factory_sale
group by  province_code,province_name,channel,channel_name)a
left join 
(select nvl(province_code,'') province_code,province_name,
  count( distinct case when channel_name ='大' and coalesce(sale,0)!=0 then customer_no end) as big_cust  from temp.factory_sale
group by  province_code,province_name) b on a.province_name=b.province_name
;
 --计算渠道数
-- drop table if exists temp.factory_cust_04;
-- CREATE temporary table temp.factory_cust_04
-- as 
-- select a.province_code,a.province_name,'00' bd_id,'物流'bd_name,
-- shop_dept_cust,
-- big_dept_cust,
-- round(big_dept_cust/big_cust,4) sale_cust_ratio ,
-- big_cust from
-- (select province_code,province_name,channel,channel_name,
--   count( distinct case when channel_name ='商超' and sale!=0 then customer_no end) as shop_dept_cust,
--   count( distinct case when channel_name ='大'and sale!=0 then customer_no end) as big_dept_cust  from temp.factory_sale
-- group by  province_code,province_name,channel,channel_name)a
-- left join 
-- (select province_code,province_name,
--   count( distinct case when channel_name ='大' then customer_no end) as big_cust  from temp.factory_sale
-- group by  province_code,province_name) b on a.province_code=b.province_code and a.province_code=b.province_name
-- ;
-- 工厂销售
drop table if exists temp.fact_sale_all;
create temporary table if not exists temp.fact_sale_all as
select 
	province_code ,
	province_name ,
	workshop_code,
	a.workshop_name ,
	sum(sale_sku ) sale_sku ,
	sum(all_sku  )all_sku,	
	sum(sale     )sale,	
	sum(profit   )profit,	
	sum(mom_sale ) mom_sale,
	sum(mom_profit  )  mom_profit,
	sum(yoy_sale    )yoy_sale    ,
	sum(yoy_profit  )yoy_profit  ,
	sum(final_amt   )final_amt   ,
	sum(period_qty  )period_qty  ,
	sum(period_amt  )period_amt  ,
	coalesce(sum(period_amt)/sum(sale-profit),0) day_turnover ,
	sum(negative_sku)negative_sku  ,
	sum(receive_amt)receive_amt,
	sum(shop_dept_cust)shop_dept_cust,
    sum(big_dept_cust)big_dept_cust,
    round(coalesce(sum(big_dept_cust)/sum(big_cust),0),4) sale_cust_ratio,
    sum(big_cust)big_cust,
    sum(mon_big_dept_cust)mon_big_dept_cust
    from (
select
nvl(province_code,'')    	province_code ,
	province_name ,
	workshop_name ,
	coalesce(sale_sku      ,0)sale_sku,
	coalesce(all_sku       ,0)all_sku,
	coalesce(sale          ,0)sale,
	coalesce(profit        ,0)profit,
	coalesce(mom_sale      ,0)mom_sale,
	coalesce(mom_profit    ,0)mom_profit,
	coalesce(yoy_sale      ,0)yoy_sale,
	coalesce(yoy_profit    ,0)yoy_profit,
	coalesce(final_amt     ,0)final_amt,
	coalesce(period_qty    ,0)period_qty,
	coalesce(period_amt    ,0)period_amt,
	coalesce(day_turnover  ,0)day_turnover,
	coalesce(negative_sku  ,0)negative_sku,
	coalesce(receive_amt,0)receive_amt,
	0 shop_dept_cust,
    0   big_dept_cust,
    0   sale_cust_ratio,
    0   big_cust,
    0 mon_big_dept_cust
from
	temp.factory_stock_04
union all
SELECT nvl(province_code,'')     province_code,
province_name,
       workshop_name,
0 	sale_sku      ,
0 	all_sku       ,
0 	sale          ,
0 	profit        ,
0 	mom_sale      ,
0 	mom_profit    ,
0 	yoy_sale      ,
0 	yoy_profit    ,
0 	final_amt     ,
0 	period_qty    ,
0 	period_amt    ,
0 	day_turnover  ,
0 	negative_sku  ,
0 	receive_amt,
    shop_dept_cust,
    big_dept_cust,
    round(coalesce(big_dept_cust/big_cust,0),4) sale_cust_ratio,
    big_cust,
    mon_big_dept_cust
FROM temp.factory_cust_01 
)
a 
left join
(select distinct workshop_code,workshop_name from csx_dw.factory_bom where sdt=regexp_replace(date_sub(current_date(),1),'-','')) as b 
on a.workshop_name=b.workshop_name
	group by province_code,province_name,a.workshop_name,b.workshop_code;
;

-- 部类销售汇总
drop table if exists temp.dept_sale_all;
create temporary table if not exists temp.dept_sale_all as
select 
	province_code ,
	province_name ,
	workshop_code ,
	a.workshop_name ,
	sum(sale_sku ) sale_sku ,
	sum(all_sku  )all_sku,	
	sum(sale     )sale,	
	sum(profit   )profit,	
	sum(mom_sale ) mom_sale,
	sum(mom_profit  )  mom_profit,
	sum(yoy_sale    )yoy_sale    ,
	sum(yoy_profit  )yoy_profit  ,
	sum(final_amt   )final_amt   ,
	sum(period_qty  )period_qty  ,
	sum(period_amt  )period_amt  ,
	coalesce(sum(period_amt)/sum(sale-profit),0) day_turnover ,
	sum(negative_sku)negative_sku  ,
	sum(receive_amt)receive_amt,
	sum(shop_dept_cust)shop_dept_cust,
    sum(big_dept_cust)big_dept_cust,
    round(coalesce(sum(big_dept_cust)/sum(big_cust),0),4) sale_cust_ratio,
    sum(big_cust)big_cust,
    sum(mon_big_dept_cust) mon_big_dept_cust
    from (
SELECT
	nvl(province_code,'')    province_code           ,
	province_name,
	bd_id as workshop_code,
	bd_name    as workshop_name               ,
	coalesce(sale_sku      ,0)sale_sku,
	coalesce(all_sku       ,0)all_sku,
	coalesce(sale          ,0)sale,
	coalesce(profit        ,0)profit,
	coalesce(mom_sale      ,0)mom_sale,
	coalesce(mom_profit    ,0)mom_profit,
	coalesce(yoy_sale      ,0)yoy_sale,
	coalesce(yoy_profit    ,0)yoy_profit,
	coalesce(final_amt     ,0)final_amt,
	coalesce(period_qty    ,0)period_qty,
	coalesce(period_amt    ,0)period_amt,
	coalesce(day_turnover  ,0)day_turnover,
	coalesce(negative_sku  ,0)negative_sku,
	coalesce(receive_amt,0)receive_amt,
	0 shop_dept_cust,
     0   big_dept_cust,
       0 sale_cust_ratio,
      0 big_cust,
      0 mon_big_dept_cust
FROM
	temp.factory_stock_05
	union all
	SELECT nvl(province_code,'')     province_code,
province_name,
bd_id workshop_code,
  bd_name as      workshop_name,
0 	sale_sku      ,
0 	all_sku       ,
0 	sale          ,
0 	profit        ,
0 	mom_sale      ,
0 	mom_profit    ,
0 	yoy_sale      ,
0 	yoy_profit    ,
0 	final_amt     ,
0 	period_qty    ,
0 	period_amt    ,
0 	day_turnover  ,
0 	negative_sku  ,
0 	receive_amt,
       shop_dept_cust,
       big_dept_cust,
       round(coalesce(big_dept_cust/big_cust,0),4) sale_cust_ratio,
       big_cust,
       mon_big_dept_cust
FROM temp.factory_cust_02
	union all
	SELECT nvl(province_code,'')     province_code,
province_name,
bd_id workshop_code,
  bd_name as      workshop_name,
0 	sale_sku      ,
0 	all_sku       ,
0 	sale          ,
0 	profit        ,
0 	mom_sale      ,
0 	mom_profit    ,
0 	yoy_sale      ,
0 	yoy_profit    ,
0 	final_amt     ,
0 	period_qty    ,
0 	period_amt    ,
0 	day_turnover  ,
0 	negative_sku  ,
0 	receive_amt,
       shop_dept_cust,
       big_dept_cust,
       round(coalesce(big_dept_cust/big_cust,0),4) sale_cust_ratio,
       big_cust,
       mon_big_dept_cust
FROM temp.factory_cust_03
	) a
	group by province_code,province_name,a.workshop_name,a.workshop_code;


-- 渠道销售汇总
drop table if exists temp.channel_sale_all;
create temporary table if not exists temp.channel_sale_all as
select 
	province_code ,
	province_name ,
	workshop_code ,
	a.workshop_name ,
	sum(sale_sku ) sale_sku ,
	sum(all_sku  )all_sku,	
	sum(sale     )sale,	
	sum(profit   )profit,	
	sum(mom_sale ) mom_sale,
	sum(mom_profit  )  mom_profit,
	sum(yoy_sale    )yoy_sale    ,
	sum(yoy_profit  )yoy_profit  ,
	sum(final_amt   )final_amt   ,
	sum(period_qty  )period_qty  ,
	sum(period_amt  )period_amt  ,
	coalesce(sum(period_amt)/sum(sale-profit),0) day_turnover ,
	sum(negative_sku)negative_sku  ,
	sum(receive_amt)receive_amt,
	sum(shop_dept_cust)shop_dept_cust,
    sum(big_dept_cust)big_dept_cust,
    round(coalesce(sum(big_dept_cust)/sum(big_cust),0),4) sale_cust_ratio,
    sum(big_cust)big_cust,
    sum(mon_big_dept_cust)mon_big_dept_cust
    from (
SELECT
	nvl(province_code,'')    province_code           ,
	province_name,
	channel as workshop_code,
	channel_name    as workshop_name               ,
	coalesce(sale_sku      ,0)sale_sku,
	coalesce(all_sku       ,0)all_sku,
	coalesce(sale          ,0)sale,
	coalesce(profit        ,0)profit,
	coalesce(mom_sale      ,0)mom_sale,
	coalesce(mom_profit    ,0)mom_profit,
	coalesce(yoy_sale      ,0)yoy_sale,
	coalesce(yoy_profit    ,0)yoy_profit,
	coalesce(final_amt     ,0)final_amt,
	coalesce(period_qty    ,0)period_qty,
	coalesce(period_amt    ,0)period_amt,
	coalesce(day_turnover  ,0)day_turnover,
	coalesce(negative_sku  ,0)negative_sku,
	coalesce(receive_amt,0)receive_amt,
	0 shop_dept_cust,
     0   big_dept_cust,
       0 sale_cust_ratio,
      0 big_cust,
      0 mon_big_dept_cust
FROM
	temp.factory_stock_06
	union all
	SELECT nvl(province_code,'')     province_code,
 province_name,channel workshop_code,channel_name as workshop_name,
0 	sale_sku      ,
0 	all_sku       ,
0 	sale          ,
0 	profit        ,
0 	mom_sale      ,
0 	mom_profit    ,
0 	yoy_sale      ,
0 	yoy_profit    ,
0 	final_amt     ,
0 	period_qty    ,
0 	period_amt    ,
0 	day_turnover  ,
0 	negative_sku  ,
0 	receive_amt,
  count( distinct case when channel_name ='商超' and coalesce(sale,0)!=0 then customer_no end) as shop_dept_cust,
  count( distinct case when channel_name ='大'and coalesce(sale,0)!=0 then customer_no end) as big_dept_cust ,
   0 sale_cust_ratio,
  0 big_cust,
  0 mon_big_dept_cust
  from temp.factory_sale
group by  province_code,province_name,channel,channel_name
	) a
	group by province_code,province_name,a.workshop_name,a.workshop_code;

--select * from temp.dept_sale_all;	
-- 数据汇总
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.provinces_kanban partition(sdt)
select order_no type,
substr(${hiveconf:edate},1,6)months,
	province_code ,
	province_name ,
	workshop_code ,
	a.workshop_name ,
	sum(sale_sku ) sale_sku ,
	sum(all_sku  )all_sku,	
	sum(sale_sku )/sum(all_sku  ) as pin_rate,
	sum(sale     )sale,	
	sum(profit   )profit,	
	sum(profit   )/sum(sale) profit_rate,
	sum(mom_sale ) mom_sale,
	sum(mom_profit  )  mom_profit,
	sum(mom_profit  ) /	sum(mom_sale ) as mom_profit_rate,
	sum(yoy_sale    )yoy_sale    ,
	sum(yoy_profit  )yoy_profit  ,
	sum(yoy_profit  ) /	sum(yoy_sale ) as yoy_profit_rate,
	coalesce(sum(sale     )/sum(mom_sale )-1,0) sale_ring_ratio,
    coalesce((sum(profit)-sum(mom_profit ))/sum(mom_profit ),0) as profit_ring_ratio,
    coalesce(sum(profit   )/sum(sale)- sum(mom_profit  )/sum(mom_sale ),0)  as  mom_gross_rate_diff,
    coalesce(sum(sale     )/sum(yoy_sale )-1,0)  as sale_yoy_ratio,
    coalesce((sum(profit)-sum(yoy_profit ))/sum(yoy_profit ),0)  as profit_yoy_ratio,
    coalesce(sum(profit   )/sum(sale)- sum(yoy_profit  )/sum(yoy_sale ),0)   as yoy_gross_rate_diff,
	sum(final_amt   )final_amt   ,
	sum(period_qty  )period_qty  ,
	sum(period_amt  )period_amt  ,
	coalesce(sum(period_amt)/sum(sale-profit),0) day_turnover ,
	sum(negative_sku)negative_sku  ,
	sum(receive_amt)receive_amt,
	sum(shop_dept_cust)	shop_dept_cust,
    sum(big_dept_cust) big_dept_cust,
    coalesce(sum(big_dept_cust)/sum(big_cust),0)  sale_cust_ratio,
    sum( big_cust)big_cust,
    sum(mon_big_dept_cust)mon_big_dept_cust,
    ${hiveconf:edate}
    from (
select '1' order_no,
nvl(province_code,'')    	province_code ,
	province_name ,
    case when workshop_name='工厂' then '00' 
    else coalesce(workshop_code ,'H00') end workshop_code,
	workshop_name ,
	sale_sku,
	all_sku,
	sale,
	profit,
	mom_sale,
	mom_profit,
	yoy_sale,
	yoy_profit,
	final_amt,
	period_qty,
	period_amt,
	day_turnover,
	negative_sku,
	receive_amt,
	shop_dept_cust,
    big_dept_cust,
    sale_cust_ratio,
    big_cust,
    mon_big_dept_cust
from
	temp.fact_sale_all
union all
select
'2' order_no,
nvl(province_code,'')    	province_code ,
	province_name ,
	coalesce(workshop_code ,0) workshop_code ,
	workshop_name ,
	sale_sku,
	all_sku,
	sale,
	profit,
	mom_sale,
	mom_profit,
	yoy_sale,
	yoy_profit,
	final_amt,
	period_qty,
	period_amt,
	day_turnover,
	negative_sku,
	receive_amt,
	shop_dept_cust,
    big_dept_cust,
    sale_cust_ratio,
    big_cust,
    mon_big_dept_cust
from
	temp.dept_sale_all
union all 
select
'3' order_no,
nvl(province_code,'')    	province_code ,
	province_name ,
	coalesce(workshop_code ,0) workshop_code ,
	workshop_name ,
	sale_sku,
	all_sku,
	sale,
	profit,
	mom_sale,
	mom_profit,
	yoy_sale,
	yoy_profit,
	final_amt,
	period_qty,
	period_amt,
	day_turnover,
	negative_sku,
	receive_amt,
	shop_dept_cust,
    big_dept_cust,
    sale_cust_ratio,
    big_cust,
    mon_big_dept_cust
from
	temp.channel_sale_all
) a 
group by province_code ,
	province_name ,
	workshop_code ,
	a.workshop_name,order_no
	order by province_code,order_no,case when workshop_name ='工厂' then 1 when workshop_name='物流' then 3 when workshop_name='全渠道' then 4 end desc;