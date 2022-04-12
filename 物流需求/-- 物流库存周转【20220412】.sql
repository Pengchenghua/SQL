-- 物流库存周转【20220412】
-- 剔除仓位 'B999','B997','PD01','PD02','TS01','CY01'
--set hive.execution.engin=spark;
-- 20201026 增加客户配送销售，地采关联销售订单，仓配=销售-地采、
-- 20200825 增加DC用途及更改dctype 工厂 、仓库、门店
-- 20200825 调整销售业务类型，将客户直送、客户配送、一件代发 剔除，入库剔除客退入库、客户直送 03、货到即配 54
-- set mapreduce.job.reduces =80;

-- 20200923 更改销售表名 dws_sale_r_d_sale_item_simple_20200921
--set hive.execution.engine=mr;
-- set tez.queue.name=caishixian;
set hive.map.aggr         =true;
--set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错
SET edate                                   = '${enddate}';
SET sdate                                   = trunc(${hiveconf:edate},'MM');



-- 库存、期间库存、30天库存

DROP TABLE IF EXISTS csx_tmp.p_invt_1;

CREATE TEMPORARY TABLE  csx_tmp.p_invt_1 AS
	-- 库存查询
select
	dc_code           ,
	goods_code goods_id,
	sum	(case when sdt>=regexp_replace(${hiveconf:sdate},'-'  ,	'')	and sdt<=regexp_replace(${hiveconf:edate},'-','') then qty end)as	inv_qty,
	sum	(case when sdt>=regexp_replace(${hiveconf:sdate},'-'  ,	'')	and sdt<=regexp_replace(${hiveconf:edate},'-','') then amt end)as	inv_amt,
	sum	(case when sdt> regexp_replace(date_sub(${hiveconf:edate},30),'-' ,'') and sdt<=regexp_replace(${hiveconf:edate},'-','') then qty end)as inv_qty_30day,
	sum	(case when sdt> regexp_replace(date_sub(${hiveconf:edate},30),'-' ,'') and sdt<=regexp_replace(${hiveconf:edate},'-','') then amt end)as inv_amt_30day,
	sum	(case when sdt = regexp_replace(${hiveconf:edate},'-'  ,'') then qty	end )as qm_qty,
	sum	(case when sdt = regexp_replace(${hiveconf:edate},'-' ,	'')	then amt end)as	qm_amt 
from
	csx_dw.dws_wms_r_d_accounting_stock_m
where
	sdt>= regexp_replace(date_sub(${hiveconf:edate},31),'-','')
	and sdt<=regexp_replace(${hiveconf:edate},'-','')
	and reservoir_area_code not in ('B999',
									'B997',
									'PD01',
									'PD02',
									'TS01',
									'CY01')
group by
	dc_code,
	goods_code
;


-- 剔除 19 客户直送、73一件代发 据号
-- 关联地采单据号 销量-拣货量=0 剔除
-- 最近出库日期 20200807
-- 销售类型剔除返利
drop table if exists csx_tmp.p_sale_max	;	
create temporary table	csx_tmp.p_sale_max as
select
	dc_code as shop_id,
	goods_code as goods_id,
	coalesce(max(case when warehouse_sales_qty!=0   then  a.sdt end ),'') as max_send_sdt
FROM
	csx_tmp.ads_wms_r_d_warehouse_sales a 
where 1=1
	and business_type_code NOT IN ('19','73', 'R19', 'R73')
	and sdt>='20190101'
    and sdt<=regexp_replace(${hiveconf:edate},'-','')
	and sales_type !='fanli'  
GROUP BY
	dc_code,
	goods_code	;


		

--入库（不含仓间调拨）
drop table if exists csx_tmp.p_entry_max;


create temporary table if not exists csx_tmp.p_entry_max as
select
	a.receive_location_code                          ,
	a.goods_code                                     ,
	coalesce(sum(receive_qty) ,0)      as entry_qty  ,
	coalesce(sum(price*receive_qty),0) as entry_value,
	coalesce(regexp_replace(to_date(receive_time), '-', ''),'')                   as entry_sdt
from
	csx_dw.dws_wms_r_d_entry_detail a
join
(
  select
    dc_code receive_location_code,
    goods_code,
    max(receive_date) as max_sdt
  from
  (
    select 
      receive_location_code dc_code,
      goods_code,
      regexp_replace(to_date(receive_time), '-', '') receive_date
    from csx_dw.dws_wms_r_d_entry_detail
    where sys='new'
    and receive_qty>0
    and receive_status in (1,2)
    and business_type in ('01','02')
    union all--调拨入库只取10，11，12 且剔除收货dc所属区与发货dc所属区相同的数据
    select 
      dc_code,
      goods_code,
      receive_date
    from
    (
       select 
         receive_location_code dc_code,
         send_location_code,
         goods_code,
         regexp_replace(to_date(receive_time), '-', '') receive_date,
         business_type
       from csx_dw.dws_wms_r_d_entry_detail
       where sys='new'
       and receive_qty>0
       and receive_status in (1,2)
       and business_type in ('10','11','12')
    )a
    left join
    (
      select shop_id,town_code from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current'
    )b
    on a.dc_code=b.shop_id
    left join
    (
      select shop_id,town_code from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current'
    )c
    on a.send_location_code=c.shop_id
    where b.town_code<>c.town_code
  )t
  group by dc_code,goods_code
)	as b
on	a.receive_location_code=b.receive_location_code	and a.goods_code =b.goods_code	and regexp_replace(to_date(a.receive_time), '-', '') =b.max_sdt 
where  receive_status in (1,2) and return_flag <>'Y' and business_type in ('10','11','12','01','02')
group by
	a.receive_location_code ,
	a.goods_code            ,
coalesce(regexp_replace(to_date(receive_time), '-', ''),'')
;



--入库（含仓间调拨）
drop table if exists csx_tmp.p_contain_transfer_entry_max;


create temporary table if not exists csx_tmp.p_contain_transfer_entry_max as
select
	a.receive_location_code                          ,
	a.goods_code                                     ,
	coalesce(sum(receive_qty) ,0)      as contain_transfer_entry_qty  ,
	coalesce(sum(price*receive_qty),0) as contain_transfer_entry_value,
	coalesce(regexp_replace(to_date(receive_time), '-', ''),'')                   as contain_transfer_entry_sdt
from
	csx_dw.dws_wms_r_d_entry_detail a
join
(
  select
    dc_code receive_location_code,
    goods_code,
    max(receive_date) as max_sdt
  from
  (
    select 
      receive_location_code dc_code,
      goods_code,
      regexp_replace(to_date(receive_time), '-', '') receive_date
    from csx_dw.dws_wms_r_d_entry_detail
    where sys='new'
    and receive_qty>0
    and receive_status in (1,2)
    and business_type in ('01','02','10','11','12')
  )t
  group by dc_code,goods_code
)	as b
on	a.receive_location_code=b.receive_location_code	and a.goods_code =b.goods_code	and regexp_replace(to_date(a.receive_time), '-', '') =b.max_sdt 
where  receive_status in (1,2) and return_flag <>'Y' and business_type in ('10','11','12','01','02')
group by
	a.receive_location_code ,
	a.goods_code            ,
coalesce(regexp_replace(to_date(receive_time), '-', ''),'')
;



-- 计算销售数据剔除相关的单据号 单据类型:客户直送 19、 一件代发 73、地采 
drop TABLE if exists csx_tmp.p_sales_data ;
create TEMPORARY TABLE csx_tmp.p_sales_data as 
select dc_code,
	goods_id,
	SUM(qty) qty,
	sum(sales_cost) as sales_cost,
	SUM(sale) sale,
	SUM(profit) profit,
	sum(qty_30day) as qty_30day,
	sum(sales_30day) as sales_30day,
	sum(sales_cost30day) as sales_cost30day
from (
SELECT dc_code,
	goods_code goods_id,
	SUM(a.warehouse_sales_qty) qty,
	sum(a.warehouse_sales_cost) as sales_cost,
	SUM(a.warehouse_sales_value) sale,
	SUM(a.warehouse_sales_value-warehouse_sales_cost) profit,
	0 qty_30day,
	0 sales_30day,
	0 sales_cost30day
FROM 	csx_tmp.ads_wms_r_d_warehouse_sales a 
WHERE sdt >= regexp_replace(${hiveconf:sdate}, '-', '')
	AND sdt <= regexp_replace(${hiveconf:edate}, '-', '')
	and a.business_type_code not in  ('19','73', 'R19',  'R73')
	-- and ((warehouse_sales_qty > 0   or warehouse_sales_qty is  null) or  a.return_flag='X')
GROUP BY dc_code,
	goods_code,
	origin_order_no
UNION ALL
SELECT dc_code,
	goods_code goods_id,
	0 qty,
	0 sales_cost,
	0 sale,
	0 profit,
	sum(warehouse_sales_qty) as qty_30day,
	sum(a.warehouse_sales_value) as sales_30day,
	sum(a.warehouse_sales_cost) as sales_cost30day
FROM csx_tmp.ads_wms_r_d_warehouse_sales a 
WHERE sdt >  regexp_replace(date_sub(${hiveconf:edate}, 30), '-', '')
	AND sdt <= regexp_replace(${hiveconf:edate}, '-', '')
	and a.business_type_code NOT IN ('19','73', 'R19',  'R73')
    --	and ((warehouse_sales_qty > 0   or warehouse_sales_qty is  null) or  a.return_flag='X')	
GROUP BY dc_code,
	goods_code
) a 
group by 
	dc_code,
	goods_id;
	

-- 增加原料与领料数据
drop table if exists csx_tmp.temp_rece_01;
create temporary table csx_tmp.temp_rece_01 as 
SELECT location_code AS dc_code,
     product_code AS goods_id,
          coalesce(sum(CASE
                           WHEN move_type = '118A' THEN amt_no_tax*(1+tax_rate/100)
                           WHEN move_type = '118B' THEN amt_no_tax*(1+tax_rate/100)*-1
                           ELSE 0
                       END),0) AS receipt_amt,
          coalesce(sum(CASE
                           WHEN move_type = '118A' THEN txn_qty
                           WHEN move_type = '118B' THEN txn_qty*-1
                       END),0) AS receipt_qty,
          coalesce(sum(CASE
                           WHEN move_type = '119A' THEN amt_no_tax*(1+tax_rate/100)
                           WHEN move_type = '119B' THEN amt_no_tax*(1+tax_rate/100)*-1
                           ELSE 0
                       END),0) AS material_take_amt,
          coalesce(sum(CASE
                           WHEN move_type = '119A' THEN txn_qty
                           WHEN move_type = '119B' THEN txn_qty*-1
                           ELSE 0
                       END),0) AS material_take_qty
   FROM csx_dw.dwd_cas_r_d_accounting_stock_detail a
   WHERE sdt >  regexp_replace(date_sub(${hiveconf:edate},30),'-' ,'')  -- 历史开始时间前30天(20210601)
     AND sdt <= regexp_replace(${hiveconf:edate},'-' ,'')   
         and move_type in ('118A','118B','119A','119B')
GROUP BY location_code,
         product_code
    
;

	

--关联库存与销售

drop table if exists csx_tmp.p_invt_2	;
create temporary table	if not exists csx_tmp.p_invt_2 as
	select
		substr(regexp_replace(${hiveconf:edate},'-' ,''),1,4) as years,
		substr(regexp_replace(${hiveconf:edate},'-' ,''),1,6) as months,
		b.prov_code                              ,
		b.prov_name                              ,
		dist_code                                ,
		dist_name                                ,
		a.dc_code as shop_id                     ,
		b.shop_name                              ,
		a.goods_id                                ,
		sum(qty)            sales_qty             ,
		sum(a.sale)         sales_value           ,
		sum(profit)         profit                ,
		sum(sales_cost)    as sales_cost          ,
		sum(inv_qty)       as period_inv_qty      ,
		sum(inv_amt)       as period_inv_amt      ,
		sum(inv_qty_30day)  as period_inv_qty_30day,
		sum(inv_amt_30day)  as period_inv_amt_30day,
		sum(qm_qty)        as final_qty           ,
		sum(qm_amt)        as final_amt           ,
		coalesce(sum(sales_30day),0)  as sales_30day   , -- 30天销售额
		coalesce(sum(qty_30day),0)  as qty_30day    ,   -- 30天销量
		coalesce(sum(sales_cost30day),0) as cost_30day   ,   -- 30天成本
		sum(receipt_amt)receipt_amt,
        SUM(receipt_qty) receipt_qty,
        SUM(material_take_amt) material_take_amt,
        SUM(material_take_qty) material_take_qty,
		dc_type     ,
		dc_uses
from
	(select dc_code,
		goods_id,
		coalesce(qty,0) qty,
		coalesce(sales_cost,0) sales_cost,
		coalesce(sale,0) sale,
		coalesce(profit,0) profit,
		coalesce(qty_30day,0) qty_30day,
		coalesce(sales_30day,0) sales_30day,
		coalesce(sales_cost30day,0) sales_cost30day,
		0 as inv_qty,
		0 as inv_amt,
		0 as inv_qty_30day,
		0 as inv_amt_30day,
		0 as qm_qty,
		0 as qm_amt,
		0 receipt_amt,
        0 receipt_qty,
        0 material_take_amt,
        0 material_take_qty
	from csx_tmp.p_sales_data
	union all
	select
		a.dc_code        ,
		a.goods_id        ,
		0 qty            ,
		0 sales_cost     ,
		0 sale           ,
		0 profit         ,
		0 qty_30day      ,
		0 sales_30day    ,
		0 sales_cost30day,
		coalesce(a.inv_qty        ,0) inv_qty        ,
		coalesce(a.inv_amt        ,0) inv_amt        ,
		coalesce(a.inv_qty_30day  ,0) inv_qty_30day  ,
		coalesce(a.inv_amt_30day  ,0) inv_amt_30day  ,
		coalesce(a.qm_qty         ,0) qm_qty         ,
		coalesce(a.qm_amt,0) qm_amt,
		0 receipt_amt,
        0 receipt_qty,
        0 material_take_amt,
        0 material_take_qty
	from
		csx_tmp.p_invt_1 a
	union all 
	select
		a.dc_code        ,
		a.goods_id        ,
		0 qty            ,
		0 sales_cost     ,
		0 sale           ,
		0 profit         ,
		0 qty_30day      ,
		0 sales_30day    ,
		0 sales_cost30day,
		0 inv_qty        ,
		0 inv_amt        ,
		0 inv_qty_30day  ,
		0 inv_amt_30day  ,
		0 qm_qty         ,
		0 qm_amt,
		receipt_amt,
        receipt_qty,
        material_take_amt,
        material_take_qty
	from
	 csx_tmp.temp_rece_01 a

)	a
join
 (select location_code shop_id,
		shop_name            ,
		dist_code            ,
		dist_name            ,
		case when a.location_code = 'W0H4'
			then 'W0H4'
			else a.province_code
		end prov_code,
		case when a.location_code = 'W0H4'
			then '供应链平台'
			else a.province_name
		end prov_name,
		a.purpose       as dc_uses  ,
		a.location_type as dc_type
	from
		csx_dw.csx_shop a
	where
		sdt = 'current')b on dc_code= b.shop_id
	group by
		b.prov_code,
		b.prov_name,
		a.dc_code  ,
		b.shop_name,
		a.goods_id  ,
		dist_code  ,
		dist_name  ,
		dc_type    ,
		dc_uses
	;
	
	--select sum(sales_value) from   csx_dw.supply_turnover  where sdt='20200119';	

drop table if exists csx_tmp.temp_turnover_sum ;
create temporary table  csx_tmp.temp_turnover_sum as 
select
		substr(regexp_replace(${hiveconf:edate} ,'-' ,''),1,4) as years,
		substr(regexp_replace(${hiveconf:edate} ,'-'  ,''),1,6) as months  ,
		prov_code                                  ,
		prov_name                                  ,
		dist_code                                ,
		dist_name                                ,
		a.shop_id  as dc_code                                ,
		shop_name  as dc_name                                ,
		a.goods_id                                  ,
		goods_name                                 ,
		standard                                   ,
		c.unit_name                                ,
		brand_name                                 ,
		department_id     as dept_id                                ,
		department_name     as dept_name                             ,
		business_division_code,
		business_division_name,
		division_code        ,
        division_name        ,
        category_large_code  ,
        category_large_name  ,
        category_middle_code ,
        category_middle_name ,
        category_small_code  ,
        category_small_name  ,
        nvl(valid_tag,'')         valid_tag        ,
		nvl(valid_tag_name,'')    valid_tag_name   ,
		nvl(goods_status_id,'')   goods_status_id  ,
		nvl(goods_status_name,'') goods_status_name,
		sales_qty                                  ,
		sales_value                                ,
		profit                                     ,
		sales_cost                                 ,
		period_inv_qty                             ,
		period_inv_amt                             ,
		final_qty    ,
		final_amt    ,
		coalesce(case when sales_cost<=0 and period_inv_amt>0 then 9999 
		              when sales_cost<=0 and period_inv_amt<=0 then 0
					  when sales_cost>0 and period_inv_amt<0 then 0
		              else period_inv_amt/sales_cost 
		          end,0 ) as days_turnover,
		cost_30day ,
		sales_30day   ,
		qty_30day    ,
		if(qty_30day/30<=0,0.01,coalesce(qty_30day/30,0.01)) as dms,
		final_qty/if(qty_30day/30<=0,0.01,coalesce(qty_30day/30,0.01)) as inv_sales_days ,
		period_inv_qty_30day ,
		period_inv_amt_30day ,
        sum(receipt_amt)receipt_amt,
        SUM(receipt_qty) receipt_qty,
        SUM(material_take_amt) material_take_amt,
        SUM(material_take_qty) material_take_qty,
		coalesce(case when (cost_30day<=0 and period_inv_amt_30day>0) then 9999 
					 when cost_30day<=0 and period_inv_amt_30day<=0 then 0
					 when cost_30day>0 and period_inv_amt_30day<0 then 0
		             else period_inv_amt_30day/cost_30day 
		        end,0)	as days_turnover_30,
		nvl(max_send_sdt,'')   max_sale_sdt,
		coalesce(datediff(${hiveconf:edate},from_unixtime(unix_timestamp(max_send_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0) as no_sale_days,
		coalesce(dc_type, '') as dc_type     ,
		coalesce(entry_qty,0) as entry_qty   ,
		coalesce(entry_value,0)as entry_value ,
		nvl(entry_sdt,'')      as entry_sdt   ,
		coalesce(datediff(${hiveconf:edate},from_unixtime(unix_timestamp(entry_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0)    as entry_days  ,
		coalesce(contain_transfer_entry_qty,0) as contain_transfer_entry_qty   ,
		coalesce(contain_transfer_entry_value,0)as contain_transfer_entry_value ,
		nvl(contain_transfer_entry_sdt,'')      as contain_transfer_entry_sdt   ,
		coalesce(datediff(${hiveconf:edate},from_unixtime(unix_timestamp(contain_transfer_entry_sdt,'yyyyMMdd'),'yyyy-MM-dd')) ,0)    as contain_transfer_entry_days  ,
		nvl(dc_uses,'') as dc_uses     ,
		current_timestamp(),
		regexp_replace(${hiveconf:edate},'-' ,'')   sdt
	from csx_tmp.p_invt_2 a
	left join csx_tmp.p_sale_max b
	on a.shop_id    =b.shop_id and a.goods_id=b.goods_id
	left join csx_tmp.p_entry_max j
	on a.shop_id    =j.receive_location_code and a.goods_id=j.goods_code
	left join csx_tmp.p_contain_transfer_entry_max k
	on a.shop_id    =k.receive_location_code and a.goods_id=k.goods_code
	LEFT OUTER JOIN
	(
	  SELECT
	  	shop_code                   as shop_id          ,
	  	product_code                as goods_id          ,
	  	product_status_name         as goods_status_name,
	  	des_specific_product_status as goods_status_id  ,
	  	valid_tag                                       ,
	  	valid_tag_name
	  FROM
	  	csx_dw.dws_basic_w_a_csx_product_info
	  WHERE
	  	sdt = 'current'
	)d
	ON a.shop_id     = d.shop_id and a.goods_id = d.goods_id
    left join
    (
      select
      	goods_id                 ,
      	goods_name               ,
      	standard                 ,
      	unit_name                ,
      	brand_name               ,
      	department_id   ,
      	department_name ,
      	case when division_code in ('12', '13',  '14')	then '12'
      		 when division_code in ('10','11') then '11'
      		 else division_code
      	end business_division_code,
      	case
      		when division_code in ('12',
      							   '13',
      							   '14')
      			then '食品用品采购部'
      		when division_code in ('10',
      							   '11')
      			then '生鲜采购部'
      			else division_name
      	end                  as business_division_name    ,
      	division_code        ,
      	division_name        ,
      	category_large_code  ,
      	category_large_name  ,
      	category_middle_code ,
      	category_middle_name ,
      	category_small_code  ,
      	category_small_name  
      from
      	csx_dw.dws_basic_w_a_csx_product_m
      where
      	sdt='current'
    ) c
    ON a.goods_id = c.goods_id
		;



-- 插入数据


INSERT OVERWRITE TABLE csx_tmp.ads_wms_r_d_goods_turnover partition(sdt)
SELECT
    substr(a.sdt,1,4)years,
    substr(a.sdt,1,6)months,
    province_code     ,
    province_name     ,
    f.dist_code,
    f.dist_name,
    prefecture_city,
    prefecture_city_name,
    a.dc_code       ,
    dc_name     ,
    a.goods_id    ,
    goods_name    ,
    standard      ,
    unit_name     ,
    brand_name    ,
    dept_id       ,
    dept_name     ,
    business_division_code,
    business_division_name,
    division_code        ,
    division_name        ,
    category_large_code  ,
    category_large_name  ,
    category_middle_code ,
    category_middle_name ,
    a.category_small_code  ,
    category_small_name  ,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    joint_purchase_flag,
    valid_tag      ,
    valid_tag_name ,
    goods_status_id,
    goods_status_name,
    sales_qty      ,
    sales_value    ,
    profit         ,
    sales_cost     ,
    period_inv_qty ,
    period_inv_amt ,
    final_qty      ,
    final_amt      ,
    days_turnover  ,
    a.cost_30day,
    sales_30day     ,
    qty_30day      ,
    dms,
    inv_sales_days,
    a.period_inv_qty_30day,
    period_inv_amt_30day ,
    days_turnover_30 ,
     max_sale_sdt,
    no_sale_days,
    dc_type,
    entry_qty,
    entry_value,
    entry_sdt,
    entry_days,
    contain_transfer_entry_qty,
    contain_transfer_entry_value,
    contain_transfer_entry_sdt,
    contain_transfer_entry_days,
    coalesce(receipt_amt,0)  receipt_amt,          --领用金额
    coalesce(receipt_qty,0)receipt_qty,            --领用数量
    coalesce(material_take_amt, 0) material_take_amt,     --原料使用金额
    coalesce(material_take_qty, 0) material_take_qty,     --原料使用数量
    a.dc_uses,
    current_timestamp(),
    a.sdt
FROM
   csx_tmp.temp_turnover_sum a 
join 
(select
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	classify_small_code,
	classify_small_name,
	category_small_code
from
	csx_dw.dws_basic_w_a_manage_classify_m
where
	sdt = 'current' 
	) as m on a.category_small_code =m.category_small_code 
left join 
(select shop_code,shop_id,product_code,joint_purchase_flag from csx_dw.dws_basic_w_a_csx_product_info where sdt='current' ) d on a.dc_code=d.shop_code and a.goods_id=d.product_code
left join 
(select a.location_code,zone_id,zone_name,dist_code,dist_name,prefecture_city,prefecture_city_name,b.city_group_code,b.city_group_name,a.province_code,a.province_name 
from csx_dw.csx_shop a 
left join csx_dw.dws_sale_w_a_area_belong  b on a.county_city=b.city_code
    where sdt='current' and table_type=1
    ) f on a.dc_code=f.location_code
--and a.business_division_code='11'
WHERE   1=1
 ;
    