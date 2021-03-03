show create table  csx_tmp.temp_dept_turnover_01;
drop table  csx_tmp.ads_wms_r_d_goods_dept_turnover;
CREATE TABLE `csx_tmp.ads_wms_r_d_goods_dept_turnover`(
    `level` STRING COMMENT '层级 1 明细 2 部类 3 DC',
    `dc_type` string COMMENT '门店类型',
    `province_code` string COMMENT '省区编码',
    `province_name` string COMMENT '省区名称',
    `dist_code` string COMMENT '省区编码简称', 
    `dist_name` string COMMENT '省区编码简称', 
    `dc_code` string COMMENT '门店编码',
    `dc_name` string COMMENT '门店名称',
    `business_division_code` string COMMENT '事业部',
    `business_division_name` string COMMENT '事业部名称',
    `dept_id` string COMMENT '课组编码',
    `dept_name` string COMMENT '课组名称',
    `sales_qty` decimal(38, 6) COMMENT '销量',
    `sales_value` decimal(38, 6) COMMENT '销售额',
    `profit` decimal(38, 6) COMMENT '毛利额',
    `profit_rate` decimal(38, 6) COMMENT '毛利率',
    `sales_cost` decimal(38, 6) COMMENT '销售成本',
    `period_inv_qty` decimal(38, 6) COMMENT '月累计库存量',
    `period_inv_amt` decimal(38, 6) COMMENT '月累计库存额',
    `final_amt` decimal(38, 6) COMMENT '期末库存额',
    `final_qty` decimal(38, 6) COMMENT '期末库存量',
    `days_turnover` bigint COMMENT '月度周转天数',
    `goods_sku` bigint COMMENT 'SKU数',
    `sale_sku` bigint COMMENT '销售SKU',
    `pin_rate` decimal(38, 6) COMMENT '动销率',
    `cost_30day` decimal(38, 6) COMMENT '近30天成本',
    `sales_30day` decimal(38, 6) COMMENT '30天日均销售额',
    `qty_30day` decimal(38, 6) COMMENT '30天销售量',
    `dms` decimal(38, 6) COMMENT '30天日均销量',
    `inv_sales_days` decimal(38, 6) COMMENT '库存可销天数',
    `period_inv_qty_30day` decimal(38, 6) COMMENT '近30天累计库存量',
    `period_inv_amt_30day` decimal(38, 6) COMMENT '近30天累计库存额',
    `days_turnover_30` decimal(38, 6) COMMENT '近30天周转',
    `turnover_ratio_30day` decimal(38,6) comment '30天周转率',
    `negative_inventory` bigint COMMENT '负库存SKU',
    `negative_amt` decimal(38, 6) COMMENT '负库存额',
    `highet_sku` bigint COMMENT '高库存SKU',
    `highet_amt` decimal(38, 6) COMMENT '高库存额',
    `no_sale_sku` bigint COMMENT '未销售数',
    `no_sale_amt` decimal(38, 6) COMMENT '未销售额',
    `dc_uses` string COMMENT 'DC用途'
) COMMENT 'DC课组库存质量' 
PARTITIONED BY (`sdt` string COMMENT '日期分区') 
STORED AS parquet;



--- 20200415 增加dc_uses 用途，sku，逻辑调整，期间有销售或期末有库存sku

set  mapreduce.job.reduces = 80;
set  hive.map.aggr = true;
set  hive.groupby.skewindata = true;
set  hive.exec.parallel = true;
set  hive.exec.dynamic.partition = true;
--启动态分区
set  hive.exec.dynamic.partition.mode = nonstrict;
--设置为非严格模式
set  hive.exec.max.dynamic.partitions = 10000;
--在所有执行mr的节点上，最大一共可以创建多少个动态分区。
set  hive.exec.max.dynamic.partitions.pernode = 100000;
--源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

set edate='${edt}';

-- 课组统计
drop table  csx_tmp.temp_dept_turnover_01;
create temporary table csx_tmp.temp_dept_turnover_01
as
select '1' as level,
            dc_type,
            dc_uses,
            case when a.dc_code = 'W0H4' then 'W0H4' else a.province_code end province_code,
            case when a.dc_code = 'W0H4' then '供应链平台'  else a.province_name  end province_name,
            a.dist_code,
            a.dist_name,
            dc_code,
            dc_name,
            business_division_code,
            business_division_name,
            dept_id,
            dept_name,
            sum(a.sales_qty) sales_qty,
            sum(a.sales_value) sales_value,
            sum(a.profit) profit,
            sum(profit)/sum(sales_value) as profit_rate,
            sum(sales_cost)as sales_cost,
            sum(a.period_inv_qty) as period_inv_qty,
            sum(a.period_inv_amt) as period_inv_amt,
            sum(a.final_amt) as final_amt,
            sum(final_qty) final_qty,
            round(sum(period_inv_amt_30day) / sum(cost_30day), 2) as days_turnover,
            count( case  when ( a.sales_30day <> 0  or final_amt != 0  ) then goods_id  end ) as goods_sku,
            count (case  when a.sales_30day != 0 then goods_id  end ) as sale_sku,
            0 pin_rate,
            sum(a.cost_30day) as cost_30day,
            sum(a.sales_30day) as sales_30day,
            sum(a.qty_30day) as qty_30day,
            coalesce(sum(a.qty_30day)/30,0) dms,
            0 inv_sales_days,
            sum(period_inv_qty_30day) as period_inv_qty_30day,
            sum(a.period_inv_amt_30day) as period_inv_amt_30day,
            coalesce(sum(a.period_inv_amt_30day)/coalesce(sum(a.cost_30day),0),999) as days_turnover_30,
            count (case  when a.final_amt < 0 then goods_id  end ) as negative_inventory,
            sum ( case  when final_amt < 0 then final_amt end  ) as negative_amt,
            count (case when (days_turnover_30 > 15  and final_amt > 500  and entry_days > 3  and business_division_code = '11') then goods_id
                    when ( days_turnover_30 > 30 and final_amt > 2000 and entry_days > 7 and a.dept_id in ('A01','A02','A03','A04','A10')) then goods_id
                    when ( days_turnover_30 > 45 and final_amt > 2000 and entry_days > 7 and a.dept_id in ('A05','A06','A07','A08','A09','P01','P10')) then goods_id
                    end) as highet_sku,
            sum (case  when (days_turnover_30 > 15 and final_amt > 500 and business_division_code = '11' and entry_days > 3) then final_amt
                     when (days_turnover_30 > 30 and final_amt > 2000 and entry_days > 7 and a.dept_id in ('A01','A02','A03','A04','A10') ) then final_amt
                     when (days_turnover_30 > 45 and final_amt > 2000 and entry_days > 7 and a.dept_id in ('A05','A06','A07','A08','A09','P01','P10') ) then final_amt
                end )as  highet_amt,
            count(case when no_sale_days > 30 and entry_days > 3 and final_amt > 0 then goods_id end) as no_sale_sku,
            sum (case  when no_sale_days > 30  and entry_days > 3 and final_amt > 0 then final_amt end) as no_sale_amt
from csx_tmp.ads_wms_r_d_goods_turnover a
group by dc_type,
            dc_uses,
            case when a.dc_code = 'W0H4' then 'W0H4' else a.province_code end ,
            case when a.dc_code = 'W0H4' then '供应链平台'  else a.province_name  end ,
            a.dist_code,
            a.dist_name,
            dc_code,
            dc_name,
            business_division_code,
            business_division_name,
            dept_id,
            dept_name
 ;
 
 insert overwrite table csx_tmp.ads_wms_r_d_goods_dept_turnover partition(sdt)
 select level,
        dc_type,
        province_code,
        province_name,
        a.dist_code,
        a.dist_name,
        dc_code,
        dc_name,
        business_division_code,
        business_division_name,
        dept_id,
        dept_name,
        sales_qty,
        sales_value,
        profit,
        coalesce(profit/a.sales_value,0) as profit_rate,
        sales_cost,
        period_inv_qty,
        period_inv_amt,
        final_amt,
        final_qty,
        case when period_inv_amt!=0 and sales_cost=0 then 999 else coalesce(period_inv_amt/sales_cost,0) end as days_turnover,
        goods_sku,
        sale_sku,
        coalesce(sale_sku/a.goods_sku,0) as pin_rate,
        cost_30day,
        a.sales_30day,
        a.qty_30day,
        coalesce(a.qty_30day/30,0) as dms,
        coalesce(a.final_qty/coalesce(a.qty_30day/30,0),0) as inv_sales_days,
        a.period_inv_qty_30day,
        a.period_inv_amt_30day,
        case when period_inv_amt_30day!=0 and cost_30day=0 then 999 else coalesce(period_inv_amt_30day/cost_30day,0) end as days_turnover_30,
        coalesce(a.cost_30day/period_inv_amt_30day,0) as turnover_ratio_30day,
        negative_inventory,
        negative_amt,
        highet_sku,
        highet_amt,
        no_sale_sku,
        no_sale_amt,
        dc_uses,
        regexp_replace(${hiveconf:edate},'-','')
from csx_tmp.temp_dept_turnover_01 a;


