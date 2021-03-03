-- set hive.execution.engine=mr;
-- set mapreduce.job.queuename                 =caishixian;
set hive.execution.engine=spark;
set mapreduce.job.reduces                   =80;
set hive.map.aggr                           =true;
--set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
set edt ='${enddate}';
set yesterday     = regexp_replace(to_date(${hiveconf:edt}),'-',''); -- 昨日
set last_yesterday= regexp_replace(to_date(add_months(${hiveconf:edt},-1)),'-',''); -- 上月同天
--本月
set mon     = regexp_replace(trunc(to_date(${hiveconf:edt}),'MM'),'-','');  -- 本月月初
set last_mon= regexp_replace(trunc(to_date(add_months(${hiveconf:edt},-1)),'MM'),'-',''); -- 环比上月月初
--本年
set year     = regexp_replace(trunc(to_date(${hiveconf:edt}),'YY'),'-',''); -- 年初
set last_year= regexp_replace(trunc(to_date(add_months(${hiveconf:edt},-12)),'YY'),'-','');-- 同期年初
set last_year_ytd= regexp_replace(to_date(add_months(${hiveconf:edt},-12)),'-',''); -- 同期年至今
-- 本周
set weekay =  regexp_replace(date_add(${hiveconf:edt},1 - case when dayofweek(${hiveconf:edt}) =1 then 7 else dayofweek(${hiveconf:edt}) -1 end ),'-',''); --本周第一天
set last_weekay =  regexp_replace(date_add(date_add(${hiveconf:edt},1 - case when dayofweek(${hiveconf:edt}) =1 then 7 else dayofweek(${hiveconf:edt}) -1 end ),-7),'-',''); --上周第一天


--select ${hiveconf:edt},${hiveconf:yesterday},${hiveconf:last_yesterday},${hiveconf:mon},${hiveconf:last_year},${hiveconf:last_year_ytd},${hiveconf:weekay},${hiveconf:last_weekay};

-- 明细数据整理
drop table if exists  csx_tmp.temp_dept_sale_01;
create temporary table if not exists csx_tmp.temp_dept_sale_01 as 
select  sdt,
        province_code,
        province_name,
        case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end as division_code     ,
	    case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end as division_name ,
	    case when division_code='10' then 'U00' else b.department_code end 	department_code,
	    case when division_code='10' then '加工课' else b.department_name end department_name,
	    case when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
	         when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 	
	         else regexp_replace(channel_name,'\\s','') 
	    end channel_name,
	    a.customer_no,
	    a.goods_code,
	    sum(a.sales_value) sale,
	    sum(profit) profit,
	    sum(a.front_profit) front_profit
    from     
	csx_dw.dws_sale_r_d_customer_sale a 
	join 
	(select category_small_code,
	    purchase_group_name as department_name,
	    purchase_group_code as department_code 
	 from csx_dw.dws_basic_w_a_category_m 
	    where sdt='current') b on a.category_small_code=b.category_small_code
where
	 sdt    >= ${hiveconf:last_year}
	and sdt<= ${hiveconf:yesterday}
group by 
        sdt,
        province_code,
        province_name,
        case when division_code in ('11','10') then '11' when division_code in ('12','13','14')   then '12' else division_code end   ,
	    case when division_code in ('11','10') then '生鲜采购部' when division_code in ('12','13','14')   then '食百采购部' else division_name end  ,
	    case when division_code='10' then 'U00' else b.department_code end 	,
	    case when division_code='10' then '加工课' else b.department_name end ,
	    case when channel in ('5','6') and a.customer_no like 'S%' then '商超' 
	         when channel in ('5','6') and a.customer_no not like 'S%' then '大客户' 	
	         else regexp_replace(channel_name,'\\s','') 
	    end ,
	    a.customer_no,
	    a.goods_code
;


-- 月度销售汇总
drop table if exists csx_tmp.temp_dept_sale_02;
create temporary  table if not exists csx_tmp.temp_dept_sale_02 as 
select
    '本月' as date_m,
	coalesce(province_code,'00')province_code  ,
	coalesce(province_name,'全国')	province_name  ,
	coalesce(division_code,'00')division_code     ,
	coalesce(division_name,'合计')division_name ,
	case when division_name is null then '' else coalesce(department_code,'00')end department_code,
    case when division_name is null then '' else coalesce(department_name,'小计') end department_name,
	coalesce(channel_name,'全渠道') channel_name,
	sale_sku,
	sale,
    last_sale,
	profit,
	front_profit,
    sale_cust,
    last_sale_cust,
    rank_id
from
(
select
	province_code  ,
	province_name  ,
	division_code     ,
	division_name ,
	department_code,
    department_name,
	channel_name,
	count(distinct case	when sdt >= ${hiveconf:mon}	and sdt <= ${hiveconf:yesterday} then goods_code end) as sale_sku,
	sum	(case when sdt   >= ${hiveconf:mon}	and sdt<=${hiveconf:yesterday} then sale 	end	) sale,
	sum	(case when sdt   >= ${hiveconf:last_mon} and sdt<=${hiveconf:last_yesterday} then sale end ) as last_sale,
	sum	(case when sdt   >= ${hiveconf:mon}	and sdt<=${hiveconf:yesterday}	then profit	end	) profit,
	sum (case when sdt   >= ${hiveconf:mon}	and sdt<=${hiveconf:yesterday}	then front_profit end) front_profit,
	count(distinct case when (sdt   >= ${hiveconf:mon}	and sdt<=${hiveconf:yesterday} and sale>0)	then customer_no end) as sale_cust,
	count(distinct case when (sdt   >= ${hiveconf:last_mon}	and sdt<=${hiveconf:last_yesterday}  and sale>0) then customer_no end) as last_sale_cust,
	grouping__id as rank_id
from
    csx_tmp.temp_dept_sale_01	a 
where
	sdt    >= ${hiveconf:last_mon}
	and sdt<= ${hiveconf:yesterday}
--	and a.province_code='35'
group by
	province_code  ,
	province_name  ,
	division_code     ,
	division_name ,
	department_code,
    department_name,
	channel_name
grouping sets 
    
(   (),
    (division_code,division_name ),
    (channel_name),
    (division_code,division_name ,channel_name),
    (division_code,division_name ,department_code,  department_name),
    (division_code,division_name ,department_code,  department_name,channel_name),
    (province_code  ,province_name ),
    (province_code  ,province_name  ,division_code,division_name ),
    (province_code  ,province_name  ,channel_name),
	(province_code  ,province_name  ,division_code,division_name ,channel_name),
	(province_code  ,province_name  ,division_code,division_name ,department_code,  department_name),
	(province_code  ,province_name  ,division_code,division_name ,department_code,  department_name,channel_name)
)
) a 
order by rank_id asc ;

-- 本年数据汇总
drop table if exists csx_tmp.temp_dept_sale_03;
create temporary  table if not exists csx_tmp.temp_dept_sale_03 as 
select
    '本年' as date_m,
	coalesce(province_code,'00')province_code  ,
	coalesce(province_name,'全国')	province_name  ,
	coalesce(division_code,'00')division_code     ,
	coalesce(division_name,'合计')division_name ,
	case when division_name is null then '' else coalesce(department_code,'00')end department_code,
    case when division_name is null then '' else coalesce(department_name,'小计') end department_name,
	coalesce(channel_name,'全渠道') channel_name,
	sale_sku,
	sale,
    last_sale,
	profit,
	front_profit,
    sale_cust,
    last_sale_cust,
    rank_id
from
(
select
	province_code  ,
	province_name  ,
	division_code     ,
	division_name ,
	department_code,
    department_name,
	channel_name,
	count(distinct case	when sdt >= ${hiveconf:year}	and sdt <= ${hiveconf:yesterday} then goods_code end) as sale_sku,
	sum	(case when sdt   >= ${hiveconf:year}	and sdt<=${hiveconf:yesterday} then sale 	end	) sale,
	sum	(case when sdt   >= ${hiveconf:last_year} and sdt<=${hiveconf:last_year_ytd} then sale end ) as last_sale,
	sum	(case when sdt   >= ${hiveconf:year}	and sdt<=${hiveconf:yesterday}	then profit	end	) profit,
	sum (case when sdt   >= ${hiveconf:year}	and sdt<=${hiveconf:yesterday}	then front_profit end) front_profit,
	count(distinct case when (sdt   >= ${hiveconf:year}	and sdt<=${hiveconf:yesterday} and sale>0)	then customer_no end) as sale_cust,
	count(distinct case when (sdt   >= ${hiveconf:last_year}	and sdt<=${hiveconf:last_year_ytd}  and sale>0) then customer_no end) as last_sale_cust,
	grouping__id as rank_id
from
    csx_tmp.temp_dept_sale_01	a 
where
	sdt    >= ${hiveconf:last_year}
	and sdt<= ${hiveconf:yesterday}
--	and a.province_code='35'
group by
	province_code  ,
	province_name  ,
	division_code     ,
	division_name ,
	department_code,
    department_name,
	channel_name
grouping sets 
    
(   (),
    (division_code,division_name ),
    (channel_name),
    (division_code,division_name ,channel_name),
    (division_code,division_name ,department_code,  department_name),
    (division_code,division_name ,department_code,  department_name,channel_name),
    (province_code  ,province_name ),
    (province_code  ,province_name  ,division_code,division_name ),
    (province_code  ,province_name  ,channel_name),
	(province_code  ,province_name  ,division_code,division_name ,channel_name),
	(province_code  ,province_name  ,division_code,division_name ,department_code,  department_name),
	(province_code  ,province_name  ,division_code,division_name ,department_code,  department_name,channel_name)
)
) a 
order by rank_id asc ;


-- 昨日数据汇总
drop table if exists csx_tmp.temp_dept_sale_04;
create temporary  table if not exists csx_tmp.temp_dept_sale_04 as 
select
    '昨日' as date_m,
	coalesce(province_code,'00')province_code  ,
	coalesce(province_name,'全国')	province_name  ,
	coalesce(division_code,'00')division_code     ,
	coalesce(division_name,'合计')division_name ,
	case when division_name is null then '' else coalesce(department_code,'00')end department_code,
    case when division_name is null then '' else coalesce(department_name,'小计') end department_name,
	coalesce(channel_name,'全渠道') channel_name,
	sale_sku,
	sale,
    last_sale,
	profit,
	front_profit,
    sale_cust,
    last_sale_cust,
    rank_id
from
(
select
	province_code  ,
	province_name  ,
	division_code     ,
	division_name ,
	department_code,
    department_name,
	channel_name,
	count(distinct case	when sdt = ${hiveconf:yesterday} then goods_code end) as sale_sku,
	sum	(case when sdt  =${hiveconf:yesterday} then sale 	end	) sale,
	sum	(case when sdt  =${hiveconf:last_yesterday} then sale end ) as last_sale,
	sum	(case when sdt  =${hiveconf:yesterday}	then profit	end	) profit,
	sum (case when sdt  =${hiveconf:yesterday}	then front_profit end) front_profit,
	count(distinct case when (sdt =${hiveconf:yesterday} and sale>0)	then customer_no end) as sale_cust,
	count(distinct case when (sdt =${hiveconf:last_yesterday}  and sale>0) then customer_no end) as last_sale_cust,
	grouping__id as rank_id
from
    csx_tmp.temp_dept_sale_01	a 
where
	sdt    >= ${hiveconf:last_yesterday}
	and sdt<= ${hiveconf:yesterday}
--	and a.province_code='35'
group by
	province_code  ,
	province_name  ,
	division_code     ,
	division_name ,
	department_code,
    department_name,
	channel_name
grouping sets 
    
(   (),
    (division_code,division_name ),
    (channel_name),
    (division_code,division_name ,channel_name),
    (division_code,division_name ,department_code,  department_name),
    (division_code,division_name ,department_code,  department_name,channel_name),
    (province_code  ,province_name ),
    (province_code  ,province_name  ,division_code,division_name ),
    (province_code  ,province_name  ,channel_name),
	(province_code  ,province_name  ,division_code,division_name ,channel_name),
	(province_code  ,province_name  ,division_code,division_name ,department_code,  department_name),
	(province_code  ,province_name  ,division_code,division_name ,department_code,  department_name,channel_name)
)
) a 
order by rank_id asc ;


INSERT overwrite  table csx_dw.ads_sale_r_m_dept_sale_mon_report  partition(sdt)
 
SELECT
	date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
	channel_name   ,
	sum(coalesce(sale_sku,0)) as sale_sku       ,
	sum(coalesce(sale,0)) as sale           ,
    case when division_code='00' then  
            (sum(coalesce(sale,0))/sum(sum(coalesce(sale,0)))over(partition BY date_m, channel_name
                                                           ORDER BY date_m))*6.00
    else  (sum(coalesce(sale,0)) /sum(sum(coalesce(sale,0)) )over(partition by date_m,province_code, channel_name order by date_m ))*3.00 end  as sale_ratio,
	sum(coalesce(last_sale,0)) as last_sale      ,
	coalesce(sum(coalesce(sale,0))/sum(coalesce(last_sale,0))-1 ,0) as sale_rate,
	sum(coalesce(profit,0)) as profit         ,
	sum(coalesce(profit,0))/sum(coalesce(sale,0)) as profitrate,
	sum(coalesce(front_profit,0)) as front_profit   ,
	sum(coalesce(front_profit,0))/sum(coalesce(sale,0)) as front_profitrate,
	sum(coalesce(sale_cust,0))as sale_cust,
	sum(coalesce(last_sale_cust,0))as last_sale_cust,
	sum(coalesce(sale_cust,0))-sum(coalesce(last_sale_cust,0)) diff_cust,
	current_timestamp() as write_time,
	${hiveconf:yesterday}
from(
SELECT
	date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
    channel_name ,
	sale_sku       ,
	sale           ,
	last_sale      ,
	profit         ,
	front_profit   ,
	sale_cust,
	last_sale_cust
FROM
	csx_tmp.temp_dept_sale_02
UNION ALL
SELECT
	date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
    channel_name,
	sale_sku       ,
	sale           ,
	last_sale      ,
	profit         ,
	front_profit   ,
	sale_cust,
	last_sale_cust
FROM
	csx_tmp.temp_dept_sale_03
UNION ALL
SELECT
    date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
    channel_name,
	sale_sku       ,
	sale           ,
	last_sale      ,
	profit         ,
	front_profit   ,
	sale_cust,
	last_sale_cust
FROM
	csx_tmp.temp_dept_sale_04
) a 
group by date_m ,
	province_code  ,
	province_name  ,
	division_code  ,
	division_name  ,
	department_code,
	department_name,
	channel_name;