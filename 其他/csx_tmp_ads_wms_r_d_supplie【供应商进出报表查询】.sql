
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;  --设置为非严格模式
set edt='${enddate}';
set e_dt =regexp_replace(${hiveconf:edt},'-','');
set sdt=regexp_replace(date_sub(${hiveconf:edt},90),'-','');
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
        select  sdt,
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
        where sdt >= ${hiveconf:sdt}
            and sdt <=  ${hiveconf:e_dt}
            and status in (6, 7, 8)
            and (
                order_type_code like 'P%'
                or order_type_code like 'RP%'
            )
        group by  sdt ,
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