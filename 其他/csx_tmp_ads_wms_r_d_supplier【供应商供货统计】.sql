drop table  csx_tmp.ads_wms_r_d_supplier_in_out_report_fr;
CREATE TABLE  csx_tmp.ads_wms_r_d_supplier_in_out_report_fr
(
company_code string comment '公司代码',
company_name string comment '公司名称',
dist_code string comment '省区编码',
dist_name string comment '省区名称',
purchase_org string comment '采购组织',
purchase_name string comment '采购组织名称',
dc_code string comment 'DC编码',
dc_name string comment 'DC名称',
supplier_code string comment '供应商编码',
vendor_name string comment '供应商名称',
vat_regist_num string comment '税务代码',
vendor_pur_lvl string comment '供应商采购级别名称',
vendor_pur_lvl_name string comment '采购级别 02全国直采,03区域直采,04全国供应商,05大区供应商,06OEM供应商,07ODM供应商,08咏悦汇供应商,09精致供应商,10自采,11统采,12地采',
goods_code string comment '商品编码',
goods_name string comment '商品名称',
bar_code string comment '条码',
brand_name string comment '品牌',
standard string comment '规格',
division_code string comment '部类编码 ',
division_name string comment '部类名称',
category_large_code string comment '大类编码',
category_large_name string comment '大类名称',
category_middle_code string comment '中类编码',
category_middle_name string comment '中类名称',
category_small_code string comment '小类编码',
category_small_name string comment '小类名称',
classify_large_code string comment '管理一级分类',
classify_large_name string comment '管理一级分类名称',
classify_middle_code string comment '管理二级分类',
classify_middle_name string comment '管理二级分类名称',
classify_small_code string comment '管理三级分类',
classify_small_name string comment '管理三级分类名称',
purchase_goods_level string comment '商品等级',
purchase_goods_level_name string comment '商品等级名称',
department_id string comment '课组编码',
department_name string comment '课组名称',
tax_rate bigint comment '税率',
valuation_category_code string comment '评估类型',
valuation_category_name string comment '评估类型名称',
unit string comment '单位',
entry_qty decimal(30,2) comment '采购数量',
entry_amt decimal(32,6) comment '采购金额',
return_qty decimal(38,2) comment '退货数量',
return_amt decimal(38,6) comment '退货金额',
no_tax_entry_amt decimal(38,6) comment '未税入库金额',
no_tax_return_amt decimal(38,6) comment '未税退货额',
clear_flag string comment '结算标记 结算标记:0=统采 1=地采'
)COMMENT '供应商供货统计'
partitioned by (sdt string comment '日期分区')
stored as  parquet
;



set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;  --设置为非严格模式
set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set sdt=regexp_replace(date_sub(${hiveconf:edt},60),'-','');
-- 供应商配送 P01 类型
-- select regexp_replace(date_sub(${hiveconf:edt},60),'-','');
insert overwrite table csx_tmp.ads_wms_r_d_supplier_in_out_report_fr partition (sdt)

select company_code,
    company_name,
    dist_code,
    dist_name,
    purchase_org,
    purchase_name,
    dc_code,
    shop_name,
    supplier_code,
    vendor_name,
    vat_regist_num,
    vendor_pur_lvl,
    vendor_pur_lvl_name,
    goods_code,
    goods_name,
    bar_code,
    brand_name,
    standard,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    purchase_goods_level,
    purchase_goods_level_name,
    department_id,
    department_name,
    tax_rate,
    valuation_category_code,
    valuation_category_name,
    unit,
    sum(purchase_qty) as purchase_qty,
    sum(purchase_amt) as purchase_amt,
    sum(return_qty) as return_qty,
    sum(return_amt) as return_amt,
    sum(purchase_amt /(1 + tax_rate / 100)) no_tax_purchase_amt,
    sum(return_amt /(1 + tax_rate / 100)) no_tax_return_amt,
    clear_flag,
    sdt
from (
        select sdt,
            receive_location_code dc_code,
            order_code,
            supplier_code,
            goods_code,
            unit,
            sum(a.receive_qty) as purchase_qty,
            sum(price*a.receive_qty) as purchase_amt,
            0 return_qty,
            0 return_amt
        from csx_dw.dws_wms_r_d_entry_detail a
        where sdt >= ${hiveconf:sdt}
            and sdt <=${hiveconf:e_dt}
            --		and a.department_id='A02'
            --		and receive_location_code ='W0A7'
            and a.order_type_code like 'P%'
            and a.receive_status = 2
        group by sdt,
            receive_location_code,
            supplier_code,
            goods_code,
            unit,
            order_code
        union all
        select regexp_replace(to_date(send_time),'-','') as sdt,
            shipped_location_code as dc_code,
            order_no as order_code,
            supplier_code,
            goods_code,
            unit,
            0 purchase_qty,
            0 purchase_amt,
            sum(coalesce(shipped_qty, 0)) as return_qty,
            sum(price*shipped_qty) as return_amt
        from csx_dw.dws_wms_r_d_ship_detail
        where regexp_replace(to_date(send_time),'-','') >= ${hiveconf:sdt}
            and regexp_replace(to_date(send_time),'-','') <=  ${hiveconf:e_dt}
            and status in (6, 7, 8)
            and (
                order_type_code like 'P%'
                or order_type_code like 'RP%'
            )
        group by  regexp_replace(to_date(send_time),'-','') ,
            shipped_location_code,
            supplier_code,
            goods_code,
            unit,
            order_no
    ) a
    join (
        select location_code,
            shop_name,
            dist_code,
            dist_name,
            company_code,
            company_name,
            purchase_org,
            purchase_name
        from csx_dw.csx_shop
        where sdt = 'current'
    ) b on a.dc_code = b.location_code
    left join (
        select vendor_id,
            vendor_name,
            vat_regist_num,
            vendor_pur_lvl,
            vendor_pur_lvl_name,
            is_setl as clear_flag   --是否统采
        from csx_dw.dws_basic_w_a_csx_supplier_m
        where sdt = 'current'
    ) c on a.supplier_code = c.vendor_id
    join (
        select goods_id,
            goods_name,
            bar_code,
            brand_name,
            standard,
            division_code,
            division_name,
            category_large_code,
            category_large_name,
            category_middle_code,
            category_middle_name,
            category_small_code,
            category_small_name,
            classify_large_code,
            classify_large_name,
            classify_middle_code,
            classify_middle_name,
            classify_small_code,
            classify_small_name,
            purchase_goods_level,
            purchase_goods_level_name,
            department_id,
            department_name,
            tax_rate,
            valuation_category_code,
            valuation_category_name
        from csx_dw.dws_basic_w_a_csx_product_m
        where sdt = 'current'
    ) d on a.goods_code = d.goods_id
group by company_code,
    company_name,
    dist_code,
    dist_name,
    dc_code,
    shop_name,
    supplier_code,
    vendor_name,
    vat_regist_num,
    vendor_pur_lvl,
    vendor_pur_lvl_name,
    goods_code,
    goods_name,
    bar_code,
    brand_name,
    standard,
    division_code,
    division_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    purchase_goods_level,
    purchase_goods_level_name,
    department_id,
    department_name,
    tax_rate,
    valuation_category_code,
    valuation_category_name,
    unit,
    a.sdt,
    purchase_org,
    purchase_name,
    clear_flag;