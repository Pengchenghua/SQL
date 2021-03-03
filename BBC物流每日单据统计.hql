--20200731 增加调拨入库字段、根据出库预计日期计算订单量
-- 20200724 插入表更改计算公式，将所有计算字段增加 coalesce
-- 1、涉及的表: csx_shop 门店表、csx_dw.dws_w_a_date_m 日期表、csx_dw.wms_entry_order 入库表、csx_dw.dws_wms_r_d_accounting_stock_m  库存表、 csx_dw.dwd_cas_r_d_accounting_credential_item 凭证表、csx_dw.wms_shipped_order 出库表、csx_ods.source_wms_w_d_wms_product_stock_log 库存日志表
-- 2、BBC每日单据统计
-- 3、BBC DC地点编码
--

set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式

set i_date='${s_date}';
set sdate =regexp_replace(add_months(trunc(${hiveconf:i_date},'MM'),-1),'-','') ;
set edate =regexp_replace(${hiveconf:i_date},'-','') ;
set plan_sdt =regexp_replace(add_months(trunc(${hiveconf:i_date},'MM'),-6),'-','') ; -- 计划发货日期
set shop=('W0P9', 'W0G8', 'W0N9', 'W0N8', 'W0S4', 'W0M9', 'W0K8', 'W0R2', 'W0B6', 'W0Q6', 'W0S6', 'W0H2');


-- DC编码及日期维度
drop table  if  exists csx_tmp.bbc_shop;
create temporary table if not exists csx_tmp.bbc_shop
as 
select 
  dist_code,dist_name,location_code,shop_name,calday 
from 
(
    select dist_code,dist_name,location_code,shop_name
    from csx_dw.csx_shop
    where sdt='current' 
        and location_code in ${hiveconf:shop}
) a
left join 
(
    select calday 
    from csx_dw.dws_w_a_date_m 
    where calday >=${hiveconf:sdate} 
        and calday<=${hiveconf:edate} 
) b on 1=1;


-- 入库数据 01 供应商配送 02 云超配送 12 调拨入库 R20 自提退货 R21 同城退货 R22 快递退货
drop table  if  exists csx_tmp.temp_wms_receive;
create temporary table if not exists csx_tmp.temp_wms_receive as 

select receive_location_code,
    sdt,
    count(distinct case when business_type_code in('01','02') then goods_code end ) as order_sku,
    count(distinct case when (receive_qty!=0 and business_type_code in('01','02')) then goods_code end ) as receive_sku,
    sum(case when business_type_code in('01','02') then plan_qty end )as plan_qty,
    sum(case when business_type_code in('01','02') then receive_qty end)as receive_qty ,
    count(distinct case when business_type_code in('12') then goods_code end ) as transfer_order_sku,
    count(distinct case when (receive_qty!=0 and business_type_code in('12')) then goods_code end ) as transfer_receive_sku,
    sum(case when business_type_code in('12') then plan_qty end )as transfer_plan_qty,
    sum(case when business_type_code in('12') then receive_qty end)as transfer_receive_qty 
from csx_dw.wms_entry_order 
where sdt>=${hiveconf:sdate} 
    and sdt<=${hiveconf:edate} 
    and business_type_code in ('01','02','12','R20','R21','R22')
    and receive_status=2
    and receive_location_code in ${hiveconf:shop}
group by 
receive_location_code,sdt
;  



-- 库存数据
drop table  if  exists csx_tmp.temp_wms_inventory;
create temporary table if not exists csx_tmp.temp_wms_inventory as 
select  dc_code ,sdt,
    count(distinct case when qty!=0 then  goods_code end )as inv_sku,
    sum(case when substr(reservoir_area_code,1,2)in ('BZ','TH')  then qty end )as qty,
    sum(case when substr(reservoir_area_code,1,2)in ('BZ','TH')  then amt end )as amt ,
    count(DISTINCT case when (reservoir_area_code like 'TH%' and division_code in ('12','13','14')and qty != 0)
                        then  goods_code end )as th_foods_sku,
    count(DISTINCT case when (reservoir_area_code like 'TH%' and division_code in ('10','11')and qty != 0)
                        then  goods_code end )as th_fresh_sku,
    sum( case when reservoir_area_code like 'TH%' and division_code in ('12','13','14')
                        then  qty end )as th_foods_qty,
    sum( case when reservoir_area_code like 'TH%' and division_code in ('10','11')
                        then  qty end )as th_fresh_qty, 
    sum( case when reservoir_area_code like 'TH%' and division_code in ('12','13','14')
                        then  amt end )as th_foods_amt,
    sum( case when reservoir_area_code like 'TH%' and division_code in ('10','11')
               then  amt end )as th_fresh_amt   
from csx_dw.dws_wms_r_d_accounting_stock_m 
where  sdt>=${hiveconf:sdate} 
    and sdt<=${hiveconf:edate}
-- and is_bz_reservoir=1
    and dc_code in ${hiveconf:shop}
group by dc_code,sdt
;

-- 报损/盘点
drop table  if  exists csx_tmp.temp_wms_loss;
create temporary table csx_tmp.temp_wms_loss as 

select   dc_code,
    sdt,
    sum(fresh_loss_amt)as fresh_loss_amt,
    sum(foods_loss_amt)as foods_loss_amt,
    sum(fresh_inventory_profit)as fresh_inventory_profit,
    sum(foods_inventory_profit)as foods_inventory_profit,
    sum(fresh_inventory_loss)as fresh_inventory_loss,
    sum(foods_inventory_loss)as foods_inventory_loss
from 
(
    select
       dc_code ,sdt,
       sum(case when substr(department_id,1,1) in ('H','U') then amt end ) as fresh_loss_amt, -- 生鲜报损金额
       sum(case when substr(department_id,1,1) in ('A','P') then amt end ) as foods_loss_amt, -- 食百报损金额
       0 as fresh_inventory_profit,
       0 as foods_inventory_profit,
       0 as fresh_inventory_loss,
       0 as foods_inventory_loss
    from csx_dw.ads_wms_r_d_bs_detail_days 
    where
        sdt>=${hiveconf:sdate} 
     and sdt<=${hiveconf:edate}
     group by dc_code,sdt
union all 
    select
       dc_code ,sdt, 
       0 as fresh_loss_amt,
       0 as foods_loss_amt,
       sum(case when move_type in('111A') and substr(department_id,1,1) in ('H','U') then amt end ) as fresh_inventory_profit, -- 生鲜盘盈（生鲜取未过帐）
       sum(case when move_type in('111A') and substr(department_id,1,1) in ('A','P') then amt end ) as foods_inventory_profit, -- 盘盈金额(食百取未过帐)
       sum(case when move_type in('110A') and substr(department_id,1,1) in ('H','U') then amt end ) as fresh_inventory_loss, -- 盘亏金额(生鲜取未过帐)
       sum(case when move_type in('110A') and substr(department_id,1,1) in ('A','P') then amt end ) as foods_inventory_loss -- 食百盘亏金额(生鲜取未过帐、食百取未过帐)
    from
        csx_dw.ads_wms_r_d_pd_detail_days
    where
        sdt>=${hiveconf:sdate} 
        and sdt<=${hiveconf:edate}
    group by dc_code,sdt
)a 
where dc_code in ${hiveconf:shop}
group by dc_code,sdt
;


-- 出库数据
drop table  if  exists csx_tmp.temp_wms_shipped;
create temporary table csx_tmp.temp_wms_shipped as 

select shipped_location_code,send_sdt as sdt,
    count(distinct case when business_type_code !='73' then  order_no end )order_num,
    count(DISTINCT case when (business_type_code !='73' and send_sdt <= regexp_replace(to_date(plan_date ),'-','')) then order_no end ) as shipped_plan_order_num,
    count(distinct case when business_type_code !='73' then goods_code end)as shipped_sku,
    sum(case when business_type_code !='73' then order_shipped_qty end)as shipped_qty ,
    sum(case when (business_type_code !='73' and plan_qty != order_shipped_qty) then (plan_qty-coalesce(order_shipped_qty,0)) end ) as diff_shipped_qty,  --  缺货数据
    count(distinct  case when (plan_qty != order_shipped_qty and business_type_code !='73') then goods_code  end )as stock_out_sku, -- 缺货SKU
    sum(case when (business_type_code !='73'and plan_qty != order_shipped_qty) then (coalesce(plan_qty*price,0) -coalesce(amount,0) )end ) as diff_shipped_amt, --缺货金额
    sum(case when business_type_code ='22' then amount end ) as bbc_express_amt, -- 快递配出库额
    sum(case when business_type_code ='21' then amount end ) as bbc_city_amt, -- 同城配
    sum(case when business_type_code ='20' then amount end ) as bbc_pick_amt, -- 自提
    sum(case when business_type_code ='73' then amount end ) as bbc_wholesale_amt -- 一件代发
from csx_dw.wms_shipped_order
where   send_sdt>=${hiveconf:sdate} 
    and send_sdt<=${hiveconf:edate}
and source_system ='BBC'
group by shipped_location_code,send_sdt
;



-- 退货入库
drop table  if  exists csx_tmp.temp_wms_return;
create temporary table csx_tmp.temp_wms_return as 

select receive_location_code,sdt,
       sum(case when business_type_code ='R21' then amount end )as return_city_amt ,
       sum(case when business_type_code ='R22' then amount end )as return_express_amt , 
       sum(case when business_type_code ='R20' then amount end )as return_pick_amt ,
       sum(case when business_type_code ='R73' then amount end )as return_wholesale_amt ,
       sum(case when business_type_code ='71' then amount end )as return_noorder_amt
from csx_dw.wms_entry_order 
where business_type_code in ('R21','R22','71','R20','R73') 
    and   sdt>=${hiveconf:sdate} 
    and sdt<=${hiveconf:edate}
    and receive_location_code in ${hiveconf:shop}
group by receive_location_code,sdt
;

-- 拣货打包数据
drop table  if  exists csx_tmp.temp_wms_pick;
create temporary table csx_tmp.temp_wms_pick as 
select warehouse_code ,
    -- create_by,
       regexp_replace(to_date(finish_time),'-','') sdt,
       count(distinct case when task_type='04' then product_code end) as pick_sku, -- 拣货SKU
       sum(case when task_type='04' then adjustment_qty end) as pick_qty, -- 拣货数量
       count(distinct case when task_type='08' then product_code end) as pack_sku, -- 打包SKU
       sum(case when task_type='08' then adjustment_qty end) as pack_qty -- 打包数
       count(distinct case when task_type='02' then product_code end) as shelf_sku,
       sum(case when task_type='02' then adjustment_qty end )as shelf_qty
 from csx_ods.source_wms_w_d_wms_product_stock_log  
 where sdt='19990101' 
    and regexp_replace(to_date(finish_time ),'-','')>= ${hiveconf:sdate} 
    and regexp_replace(to_date(finish_time ),'-','')<=${hiveconf:edate} 
    and warehouse_code in ${hiveconf:shop}
group by 
    warehouse_code ,
  --  create_by,
    regexp_replace(to_date(finish_time),'-','')
;


-- 插入表
INSERT overwrite TABLE csx_tmp.ads_wms_r_d_document_stat_bbc partition(sdt)
SELECT substr(calday,1,4) AS years,
       substr(calday,1,6) AS months,
       calday,
       j.dist_code,
       dist_name,
       j.location_code,
       shop_name,
       coalesce(a.order_sku,0) as order_sku,
       coalesce(a.receive_sku,0)as receive_sku,
       coalesce(a.plan_qty,0)as plan_qty,
       coalesce(a.receive_qty,0)as receive_qty,
       coalesce(a.transfer_order_sku,0)as transfer_order_sku,
       coalesce(transfer_receive_sku,0) as transfer_receive_sku,
       coalesce(transfer_plan_qty,0) as transfer_plan_qty,
       coalesce(transfer_receive_qty,0)as transfer_receive_qty,
       coalesce(h.shelf_qty,0)as shelf_qty,
       coalesce(h.shelf_sku,0) as shelf_sku,
       coalesce(b.inv_sku,0)as inv_sku,
       coalesce(b.qty,0)as qty,
       coalesce(b.amt,0)as amt,
       coalesce(b.th_foods_sku,0) as th_foods_sku, 
       coalesce(b.th_fresh_sku,0) as th_fresh_sku,
       coalesce(b.th_foods_qty,0) as th_foods_qty,
       coalesce(b.th_fresh_qty,0) as th_fresh_qty,
       coalesce(b.th_foods_amt,0) as th_foods_amt,
       coalesce(b.th_fresh_amt,0) as th_fresh_amt,
       coalesce(c.fresh_loss_amt,0) as fresh_loss_amt,
       coalesce(c.foods_loss_amt,0) as foods_loss_amt,
       coalesce(c.fresh_inventory_profit,0) as fresh_inventory_profit,
       coalesce(c.foods_inventory_profit,0) as foods_inventory_profit,
       coalesce(c.fresh_inventory_loss,0) as fresh_inventory_loss,
       coalesce(c.foods_inventory_loss,0) as foods_inventory_loss,
       coalesce(shipped_plan_order_num,0) as shipped_plan_order_num,
       coalesce(d.order_num,0) as order_num,
       coalesce(d.shipped_sku,0) as shipped_sku,
       coalesce(d.shipped_qty,0) as shipped_qty,
       coalesce(d.diff_shipped_qty,0) as diff_shipped_qty,
       coalesce(d.stock_out_sku,0) as  stock_out_sku,
       coalesce(d.diff_shipped_amt,0) AS diff_shipped_amt,
       coalesce(d.bbc_express_amt,0) AS bbc_express_amt,
       coalesce(d.bbc_city_amt,0)as bbc_city_amt,
       coalesce(d.bbc_pick_amt,0)as bbc_pick_amt,
       coalesce(d.bbc_wholesale_amt,0) as bbc_wholesale_amt,
       coalesce(f.return_city_amt,0) as return_city_amt,
       coalesce(f.return_express_amt,0) as return_express_amt,
       coalesce(f.return_pick_amt,0) as return_pick_amt,
       coalesce(f.return_wholesale_amt,0) as return_wholesale_amt,
       coalesce(f.return_noorder_amt,0) as return_noorder_amt,
       coalesce(bbc_express_amt,0)-coalesce(return_express_amt,0) AS bbc_express_sale,
       coalesce(bbc_city_amt,0)-coalesce(return_city_amt,0) AS bbc_city_sale,
       coalesce(bbc_pick_amt,0)-coalesce(return_pick_amt,0) AS bbc_pick_sale,
       coalesce(bbc_wholesale_amt,0)-coalesce(return_wholesale_amt,0) AS bbc_wholesale_sale,
       coalesce(h.pick_qty,0) as pick_qty,
       coalesce(h.pick_sku,0) as pick_sku,
       coalesce(h.pack_qty,0) as pack_qty,
       coalesce(h.pack_sku,0) as pack_sku,
       calday
FROM csx_tmp.bbc_shop j
LEFT JOIN csx_tmp.temp_wms_receive a ON j.location_code=a.receive_location_code
AND j.calday=a.sdt
LEFT JOIN csx_tmp.temp_wms_inventory AS b ON j.location_code=b.dc_code
AND j.calday=b.sdt
LEFT JOIN csx_tmp.temp_wms_loss AS c ON j.location_code=c.dc_code
AND j.calday=c.sdt
LEFT JOIN csx_tmp.temp_wms_shipped AS d ON j.location_code=d.shipped_location_code
AND j.calday=d.sdt
LEFT JOIN csx_tmp.temp_wms_return AS f ON j.location_code=f.receive_location_code
AND j.calday=f.sdt
LEFT JOIN csx_tmp.temp_wms_pick AS h ON j.location_code=h.warehouse_code
AND j.calday=h.sdt ;







-- 建表结构
create table csx_dw.ads_wms_r_d_document_stat_bbc
	(
		years string comment '年'                                  ,
		months string comment '月'                                 ,
		calday string comment '销售日期'                              ,
		dist_code string comment '省区编码'                           ,
		dist_name string comment '省区名称'                           ,
		location_code string comment '地点编码'                       ,
		shop_name string comment '地点编码'                           ,
		order_sku              Bigint comment '下单SKU'                     ,
		receive_sku            Bigint comment '收货SKU'                     ,
		plan_qty               decimal(26,6) comment '订单数量'               ,
		receive_qty            decimal(26,6) comment '收货数量'               ,
        transfer_order_sku Bigint comment '调拨订单SKU',
        transfer_receive_sku Bigint comment '调拨收货SKU',
        transfer_plan_qty decimal(26,6) comment '调拨订单数量',
        transfer_receive_qty decimal(26,6) comment '调拨收货数量',
		shelf_qty              decimal(26,6) comment '上架数量'               ,
		shelf_sku              Bigint comment '上架SKU'                     ,
		inv_sku                Bigint comment '库存SKU'                     ,
		qty                    decimal(26,6) comment '库存数量'               ,
		amt                    decimal(26,6) comment '库存金额'               ,
		th_foods_sku           Bigint comment '退货区食百SKU'                  ,
		th_fresh_sku           Bigint comment '退货区生鲜SKU'                  ,
		th_foods_qty           decimal(26,6) comment '退货区食百数量'            ,
		th_fresh_qty           decimal(26,6) comment '退货区生鲜数量'            ,
		th_foods_amt           decimal(26,6) comment '退货区食百金额'            ,
		th_fresh_amt           decimal(26,6) comment '退货区生鲜金额'            ,
		fresh_loss_amt         decimal(26,6) comment '生鲜报损额 117A  -117B冲销',
		foods_loss_amt         decimal(26,6) comment '食百报损额'              ,
		fresh_inventory_profit decimal(26,6) comment '生鲜盘盈额取过帐 115A'      ,
		foods_inventory_profit decimal(26,6) comment '省区编码'               ,
		fresh_inventory_loss   decimal(26,6) comment '生鲜盘亏额取过帐 116A'      ,
		foods_inventory_loss   decimal(26,6) comment '食百盘亏额'               ,
        shipped_plan_order_num bigint comment '出库计划订单数',
		order_num              bigint comment '订单数'                      ,
		shipped_sku            bigint comment '出库SKU'                      ,
		shipped_qty            decimal(26,6) comment '发货数量'               ,
		diff_shipped_qty       decimal(26,6) comment '差异数量，订单数量>发货数量'               ,
		stock_out_sku          bigint comment '缺货SKU'                      ,
		diff_shipped_amt       decimal(26,6) comment '缺货金额'               ,
		bbc_express_amt        decimal(26,6) comment '快递发货金额'               ,
		bbc_city_amt           decimal(26,6) comment '同城配发货金额'               ,
		bbc_pick_amt           decimal(26,6) comment '自提发货金额'               ,
		bbc_wholesale_amt      decimal(26,6) comment '一件代发发货金额'               ,
		return_city_amt        decimal(26,6) comment '同城退货金额'                ,
		return_express_amt     decimal(26,6) comment '快递退货金额'               ,
		return_pick_amt        decimal(26,6) comment '自提退货金额'               ,
		return_wholesale_amt   decimal(26,6) comment '一件代发退货金额'               ,
		return_noorder_amt     decimal(26,6) comment '无单退货额'               ,
		bbc_express_sale       decimal(26,6) comment '快递实际销售额：发货金额-退货金额'               ,
		bbc_city_sale          decimal(26,6) comment '同城实际销售额：同城发货金额-退货金额'               ,
		bbc_pick_sale          decimal(26,6) comment '自提实际销售额：自提发货金额-退货金额'               ,
		bbc_wholesale_sale     decimal(26,6) comment '一件代发实际金额：一件代发发货金额-退货金额'                ,
		pick_qty               decimal(26,6) comment '拣货数量'               ,
		pick_sku               bigint comment '拣货SKU'                      ,
		pack_qty               decimal(26,6) comment '打包数量'               ,
		pack_sku               bigint comment '打包SKU'
	)
	comment 'BBC每日单据统计' partitioned by(sdt string comment '分区日期')
	stored as parquet

;
