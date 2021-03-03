SET hive.execution.engine=tez; 
set tez.queue.name=caishixian;
set edate='${edt}';

set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict; --设置为非严格模式

-- 移库类型剔除109A

-- 期初+期末数据
drop table if exists csx_tmp.temp_factory_goods_01;
create temporary table csx_tmp.temp_factory_goods_01
as 
select
        dc_code,
        goods_code,
        sum(end_inventory_qty)end_inventory_qty,
        sum(end_inventory_amt)end_inventory_amt,
        sum(begin_inventory_qty) begin_inventory_qty,
        sum(begin_inventory_amt) begin_inventory_amt
from
(
select
        dc_code,
        goods_code,
        sum(qty)end_inventory_qty,
        sum(amt)end_inventory_amt,
        0 begin_inventory_qty,
        0 begin_inventory_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
    where
        sdt = regexp_replace(to_date(${hiveconf:edate}),'-','')
       and reservoir_area_code not in ('PD02','PD01','TS01')
    group by
        dc_code,
        goods_code
union all 
    select
        dc_code,
        goods_code,
        0 end_inventory_qty,
        0 end_inventory_amt,
        sum(qty)begin_inventory_qty,
        sum(amt)begin_inventory_amt
    from
        csx_dw.dws_wms_r_d_accounting_stock_m a
    where
        sdt = regexp_replace(date_sub(trunc(${hiveconf:edate},'MM'),1),'-','')
         and reservoir_area_code not in ('PD02','PD01','TS01')
       --  and goods_code ='825121'
    group by
        dc_code,
        goods_code 
)a
group by 
        dc_code,
        goods_code 
;

-- 各业务类型数据
drop table if exists csx_tmp.temp_factory_goods_02;
create temporary table csx_tmp.temp_factory_goods_02
as 
select
         dc_code,
         goods_code ,
    coalesce(receive_amt, 0)            receive_amt,             --  `收货入库额`,
    coalesce(receive_qty,  0)           receive_qty,             -- `收货入库量`,   
    coalesce(transfer_in_amt,0)         transfer_in_amt,         -- `调拨入库额`
    coalesce(transfer_in_qty,  0)       transfer_in_qty,         -- `调拨退货入库量`,
    coalesce(transfer_return_in_amt,0) 	transfer_return_in_amt,  -- 调拨退货入库额
    coalesce(transfer_return_in_qty,0) 	transfer_return_in_qty,  -- 调拨退货入库量
    coalesce(receive_return_amt,0) 		receive_return_amt,      -- `退货入库额`,   
    coalesce(receive_return_qty,0) 		receive_return_qty,      -- `退货入库量`,
    coalesce(raw_material_amt,  0) 		raw_material_amt,        --   `原料成品入库额`,
    coalesce(raw_material_qty,  0) 		raw_material_qty,        -- `原料成品入库量`,
    coalesce(begin_import_amt,  0) 		begin_import_amt,        -- `期初导入额`,
    coalesce(begin_import_qty,  0) 		begin_import_qty,        -- `期初导入量`,
    coalesce(return_amt, 0) 		    return_amt,              --  `退货出库额`,
    coalesce(return_qty, 0) 		    return_qty,              -- `退货出库量`,
    coalesce(transfer_out_amt,  0)  	transfer_out_amt,        --`调拨出库额`,
    coalesce(transfer_out_qty,  0) 		transfer_out_qty,        --`调拨出库量`,
    coalesce(transfer_return_out_amt,0) transfer_return_out_amt, --`调拨退货出库额`,
    coalesce(transfer_return_out_qty,0) transfer_return_out_qty, --`调拨退货出库量`,
    coalesce(sale_amt, 0)               sale_amt,                --`销售出库额`,
    coalesce(sale_qty, 0)               sale_qty,                -- `销售出库量`,
    coalesce(sub_to_parent_amt,0)       sub_to_parent_amt,       --`子品转母品额`,
    coalesce(sub_to_parent_qty,0)       sub_to_parent_qty,       --`子品转母品量`,
    coalesce(parent_to_sub_amt,0)       parent_to_sub_amt,       --`母品转子品额`,
    coalesce(parent_to_sub_qty,0)       parent_to_sub_qty,       --`母品转子品量`,
    coalesce(post_amount_profit_amt,0) 	post_amount_profit_amt,  --`过帐盘盈额`,
    coalesce(post_amount_profit_qty,0) 	post_amount_profit_qty,  --`过帐盘盈量`,
    coalesce(post_amount_loss_amt,  0) 	post_amount_loss_amt,    --`过帐盘亏额`,
    coalesce(post_amount_loss_qty,  0) 	post_amount_loss_qty,    --`过帐盘亏量`,
    coalesce(report_loss_amt, 0)                report_loss_amt,                --`报损额`,
    coalesce(report_loss_qty, 0)                report_loss_qty,                --`报损量`,
    coalesce(requisition_amt,  0)        requisition_amt,         --`领用额`,
    coalesce(requisition_qty,  0)        requisition_qty,         --`领用量`,
    coalesce(material_use_amt, 0)        material_use_amt,        --`原料消耗额`,
    coalesce(material_use_qty, 0)        material_use_qty,        --`原料消耗量`,
    coalesce(retrun_material_diff_amt,0) retrun_material_diff_amt,--`退料差异额`,
    coalesce(retrun_material_diff_qty,0) retrun_material_diff_qty,--`退料差异量`,
    coalesce(product_transcode_amt,   0) product_transcode_amt,   --`商品转码额`,
    coalesce(product_transcode_qty,   0) product_transcode_qty,   --`商品转码量`,
    coalesce(move_stock_amt,          0) move_stock_amt,          --`移库额`,
    coalesce(move_stock_qty,          0) move_stock_qty,          --`移库量`,
    ((coalesce(post_amount_loss_qty,0)+coalesce(report_loss_qty,0))-coalesce(post_amount_profit_qty,0)+coalesce(retrun_material_diff_qty,0)) as loss_qty,
    ((coalesce(post_amount_loss_amt,0)+coalesce(report_loss_amt,0))-coalesce(post_amount_profit_amt,0)+coalesce(retrun_material_diff_amt,0)) AS loss_amt,
    (coalesce(material_use_qty,0)+coalesce(transfer_out_qty,0)+coalesce(transfer_return_out_qty,0)+coalesce(requisition_qty,0))  out_qty,
    (coalesce(material_use_amt,0)+coalesce(transfer_out_amt,0)+coalesce(transfer_return_out_amt,0)+coalesce(requisition_amt,0))  out_amt
 from (
select
        location_code as dc_code,
        a.product_code goods_code ,
        sum(case when move_type = '101A' then amt_no_tax*(1+tax_rate/100 )
                when move_type = '101B' then -amt_no_tax*(1+tax_rate/100 ) end) `receive_amt`,
        sum(case when move_type = '101A' then txn_qty
                when move_type = '101B' then -txn_qty end) `receive_qty`,
        sum(case when move_type = '102A' then amt_no_tax*(1+tax_rate/100 )  end) `transfer_in_amt`,
        sum(case when move_type = '102A' then txn_qty end) `transfer_in_qty`,
        sum(case when move_type = '105A' then amt_no_tax*(1+tax_rate/100 )  end) `transfer_return_in_amt`,
        sum(case when move_type = '105A' then txn_qty end) `transfer_return_in_qty`,
        sum(case when move_type = '108A' then amt_no_tax*(1+tax_rate/100 ) 
                when move_type = '108B' then -amt_no_tax*(1+tax_rate/100 )  end) `receive_return_amt`,
        sum(case when move_type = '108A' then txn_qty
                when move_type = '108B' then -txn_qty end) `receive_return_qty`,
        sum(case when move_type = '120A' then amt_no_tax*(1+tax_rate/100 )
                when move_type = '120B' then -amt_no_tax*(1+tax_rate/100 )  end) `raw_material_amt`,
        sum(case when move_type = '120A' then txn_qty
                when move_type = '120B' then -txn_qty end) `raw_material_qty`,
        sum(case when move_type = '201A' then amt_no_tax*(1+tax_rate/100 )  end) `begin_import_amt`,
        sum(case when move_type = '201A' then txn_qty end) `begin_import_qty`,
        sum(case when move_type = '103A' then amt_no_tax*(1+tax_rate/100 )  end) `return_amt`,
        sum(case when move_type = '103A' then txn_qty end) `return_qty`,
        sum(case when move_type = '104A' then amt_no_tax*(1+tax_rate/100 )  end) `transfer_out_amt`,
        sum(case when move_type = '104A' then txn_qty end) `transfer_out_qty`,
        sum(case when move_type = '106A' then amt_no_tax*(1+tax_rate/100 )  end) `transfer_return_out_amt`,
        sum(case when move_type = '106A' then txn_qty end) `transfer_return_out_qty`,
        sum(case when move_type = '107A' then amt_no_tax*(1+tax_rate/100 )
                when move_type = '107B' then -amt_no_tax*(1+tax_rate/100 )  end) `sale_amt`,
        sum(case when move_type = '107A' then txn_qty
                when move_type = '107B' then -txn_qty end) `sale_qty`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `sub_to_parent_amt`,
        sum(case when move_type = '112A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `sub_to_parent_qty`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `parent_to_sub_amt`,
        sum(case when move_type = '113A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `parent_to_sub_qty`,
        sum(case when move_type = '115A' then amt_no_tax*(1+tax_rate/100 )  end) `post_amount_profit_amt`,
        sum(case when move_type = '115A' then txn_qty end) `post_amount_profit_qty`,
        sum(case when move_type = '116A' then amt_no_tax*(1+tax_rate/100 )  end) `post_amount_loss_amt`,
        sum(case when move_type = '116A' then txn_qty end) `post_amount_loss_qty`,
        sum(case when move_type = '117A' then amt_no_tax*(1+tax_rate/100 )
                when move_type = '117B' then -amt_no_tax*(1+tax_rate/100 )  end) `report_loss_amt`,
        sum(case when move_type = '117A' then txn_qty
                when move_type = '117B' then -txn_qty end) `report_loss_qty`,
        sum(case when move_type = '118A' then amt_no_tax*(1+tax_rate/100 )
                when move_type='118B' then -amt_no_tax*(1+tax_rate/100)  end) `requisition_amt`,
        sum(case when move_type = '118A' then txn_qty
                when  move_type='118B' then -txn_qty end) `requisition_qty`,
        sum(case when move_type = '119A' then amt_no_tax*(1+tax_rate/100 )  end) `material_use_amt`,
        sum(case when move_type = '119A' then txn_qty end) `material_use_qty`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) )  End) `retrun_material_diff_amt`,
        sum(case when move_type = '121A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `retrun_material_diff_qty`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-amt_no_tax*(1+tax_rate/100 ) , amt_no_tax*(1+tax_rate/100 ) ) end) `product_transcode_amt`,
        sum(case when move_type = '202A' then if(in_or_out = 1,-txn_qty, txn_qty) end) `product_transcode_qty`,
        sum(case when move_type = '109A' then if(in_or_out = 0,amt_no_tax*(1+tax_rate/100 ) , 0) end) `move_stock_amt`,
        sum(case when move_type = '109A' then if(in_or_out = 0,txn_qty, 0) end) `move_stock_qty`
    from
       csx_dw.dwd_cas_r_d_accounting_stock_detail a 
    where
        sdt >= regexp_replace(trunc(to_date(${hiveconf:edate}),'MM'),'-','')
        and sdt <= regexp_replace(date_sub(to_date(${hiveconf:edate}),-5),'-','')
        and regexp_replace(to_date(posting_time),'-','') <= regexp_replace(to_date(${hiveconf:edate}),'-','')
        and regexp_replace(to_date(posting_time),'-','') >=regexp_replace(trunc(${hiveconf:edate},'MM'),'-','')
        --排除 114A出库在途、110A 盘亏转正常仓，111A盘盈转正常仓
        and move_type not in ('114A','110A','111A')
       -- and location_code ='W053'
        --排除盘点仓库存
        group by location_code,
        product_code 
) a         ;

insert overwrite table csx_tmp.ads_factory_r_d_inventory_loss_fr partition (months)
 select
    substr(regexp_replace(${hiveconf:edate},'-',''),1,4) as sale_yesrs,
    substr(regexp_replace(${hiveconf:edate},'-',''),1,6) as sale_month,
    province_code,
    province_name,
    a.dc_code,
    shop_name as dc_name,
    a.goods_code,
    goods_name,
    unit_name,
    division_code,
    division_name,
    department_id,
    department_name,
    category_large_code,
    category_large_name,
    category_middle_code,
    category_middle_name,
    category_small_code,
    category_small_name,
    tax_rate,
    mrp_prop_key ,
    mrp_prop_value,
    coalesce(end_inventory_qty,0)end_inventory_qty,
    coalesce(end_inventory_amt,0)end_inventory_amt,
    coalesce(begin_inventory_qty,0) begin_inventory_qty,
    coalesce(begin_inventory_amt,0) begin_inventory_amt,
    coalesce(receive_amt, 0)            receive_amt,             --  `收货入库额`,
    coalesce(receive_qty,  0)           receive_qty,             -- `收货入库量`,   
    coalesce(transfer_in_amt,0)         transfer_in_amt,         -- `调拨入库额`
    coalesce(transfer_in_qty,  0)       transfer_in_qty,         -- `调拨退货入库量`,
    coalesce(transfer_return_in_amt,0) 	transfer_return_in_amt,  -- 调拨退货入库额
    coalesce(transfer_return_in_qty,0) 	transfer_return_in_qty,  -- 调拨退货入库量
    coalesce(receive_return_amt,0) 		receive_return_amt,      -- `退货入库额`,   
    coalesce(receive_return_qty,0) 		receive_return_qty,      -- `退货入库量`,
    coalesce(raw_material_amt,  0) 		raw_material_amt,        --   `原料成品入库额`,
    coalesce(raw_material_qty,  0) 		raw_material_qty,        -- `原料成品入库量`,
    coalesce(begin_import_amt,  0) 		begin_import_amt,        -- `期初导入额`,
    coalesce(begin_import_qty,  0) 		begin_import_qty,        -- `期初导入量`,
    coalesce(return_amt, 0) 		    return_amt,              --  `退货出库额`,
    coalesce(return_qty, 0) 		    return_qty,              -- `退货出库量`,
    coalesce(transfer_out_amt,  0)  	transfer_out_amt,        --`调拨出库额`,
    coalesce(transfer_out_qty,  0) 		transfer_out_qty,        --`调拨出库量`,
    coalesce(transfer_return_out_amt,0) transfer_return_out_amt, --`调拨退货出库额`,
    coalesce(transfer_return_out_qty,0) transfer_return_out_qty, --`调拨退货出库量`,
    coalesce(sale_amt, 0)               sale_amt,                --`销售出库额`,
    coalesce(sale_qty, 0)               sale_qty,                -- `销售出库量`,
    coalesce(sub_to_parent_amt,0)       sub_to_parent_amt,       --`子品转母品额`,
    coalesce(sub_to_parent_qty,0)       sub_to_parent_qty,       --`子品转母品量`,
    coalesce(parent_to_sub_amt,0)       parent_to_sub_amt,       --`母品转子品额`,
    coalesce(parent_to_sub_qty,0)       parent_to_sub_qty,       --`母品转子品量`,
    coalesce(post_amount_profit_amt,0) 	post_amount_profit_amt,  --`过帐盘盈额`,
    coalesce(post_amount_profit_qty,0) 	post_amount_profit_qty,  --`过帐盘盈量`,
    coalesce(post_amount_loss_amt,  0) 	post_amount_loss_amt,    --`过帐盘亏额`,
    coalesce(post_amount_loss_qty,  0) 	post_amount_loss_qty,    --`过帐盘亏量`,
    coalesce(report_loss_amt, 0)                report_loss_amt,                --`报损额`,
    coalesce(report_loss_qty, 0)                report_loss_qty,                --`报损量`,
    coalesce(requisition_amt,  0)        requisition_amt,         --`领用额`,
    coalesce(requisition_qty,  0)        requisition_qty,         --`领用量`,
    coalesce(material_use_amt, 0)        material_use_amt,        --`原料消耗额`,
    coalesce(material_use_qty, 0)        material_use_qty,        --`原料消耗量`,
    coalesce(retrun_material_diff_amt,0) retrun_material_diff_amt,--`退料差异额`,
    coalesce(retrun_material_diff_qty,0) retrun_material_diff_qty,--`退料差异量`,
    coalesce(product_transcode_amt,   0) product_transcode_amt,   --`商品转码额`,
    coalesce(product_transcode_qty,   0) product_transcode_qty,   --`商品转码量`,
    coalesce(move_stock_amt,          0) move_stock_amt,          --`移库额`,
    coalesce(move_stock_qty,          0) move_stock_qty,          --`移库量`,
    loss_qty,
   loss_qty/coalesce(material_use_qty,0) AS loss_qty_rate,
   loss_amt,
   loss_amt/coalesce(material_use_amt,0) AS loss_amt_rate,
    out_qty,
    out_amt,
   loss_qty/out_qty AS out_loss_qty_rate,
   loss_amt/out_amt AS out_loss_amt_rate,
   substr(regexp_replace(${hiveconf:edate},'-',''),1,6)
from csx_tmp.temp_factory_goods_01 a
left join csx_tmp.temp_factory_goods_02 b on     a.dc_code = b.dc_code    and a.goods_code = b.goods_code
left join (
    select
        location_code,
        shop_name,
        province_code,
        province_name
    from
        csx_dw.csx_shop
    where
        sdt = 'current'
        and location_uses_code = '03' ) c on
    a.dc_code = c.location_code
    left join 
    (SELEct   factory_location_code ,product_code ,mrp_prop_key ,mrp_prop_value 
        from csx_dw.dws_mms_w_a_factory_bom_m where sdt=regexp_replace(to_date(${hiveconf:edate}),'-','')
        group by factory_location_code ,product_code ,mrp_prop_key ,mrp_prop_value ) e on 
    a.dc_code=e.factory_location_code and a.goods_code=e.product_code
     left join 
    (SELEct goods_id , 
        goods_name,
        unit_name,
        tax_rate,
        division_code,
        division_name,
        department_id,
        department_name,
        category_large_code,
        category_large_name,
        category_middle_code,
        category_middle_name,
        category_small_code,
        category_small_name
    from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' ) as k on 
    a.goods_code=k.goods_id
where
    shop_name is not null
order by province_code,dc_code,category_large_code

;




 create TABLE csx_tmp.ads_factory_r_d_inventory_loss_fr(
    sale_yesrs string comment '销售年份',
    sales_month string comment '销售月份',    
    province_code string comment '省区编码',
    province_name string comment '省区名称',
    dc_code string comment 'DC',
    dc_name string comment 'DC',
    goods_code string comment '商品编码',
    goods_name string comment '商品名称',
    unit_name string comment '单位',
    division_code string comment '部类编码',
    division_name string comment '部类名称',
    department_id string comment '课组编码',
    department_name string comment '课组名称',
    category_large_code string comment '大类编码',
    category_large_name string comment '大类名称',
    category_middle_code string comment '中类编码',
    category_middle_name string comment '中类名称',
    category_small_code string comment '小类编码',
    category_small_name string comment '小类名称',
    tax_rate decimal(26,6) comment '税率',
    mrp_prop_key string comment 'bom标识',
    mrp_prop_value string comment 'bom名称',
    end_inventory_qty  decimal(26,6) comment '期末库存量',
    end_inventory_amt  decimal(26,6) comment '期末库存额',
    begin_inventory_qty decimal(26,6) comment '期初库存量',
    begin_inventory_amt decimal(26,6) comment '期初库存额',    
    receive_amt decimal(26,6) comment '收货入库额' ,           
    receive_qty decimal(26,6) comment '收货入库量' ,           
    transfer_in_amt decimal(26,6) comment '调拨入库额',        
    transfer_in_qty decimal(26,6) comment '调拨退货入库量',    
    transfer_return_in_amt decimal(26,6) comment '调拨退货入库额',
    transfer_return_in_qty decimal(26,6) comment '调拨退货入库量',
    receive_return_amt decimal(26,6) comment '退货入库额',     
    receive_return_qty decimal(26,6) comment '退货入库量',     
    raw_material_amt decimal(26,6) comment '原料成品入库额',   
    raw_material_qty decimal(26,6) comment '原料成品入库量',   
    begin_import_amt decimal(26,6) comment '期初导入额',       
    begin_import_qty decimal(26,6) comment '期初导入量',       
    return_amt decimal(26,6) comment '退货出库额',             
    return_qty decimal(26,6) comment '退货出库量',             
    transfer_out_amt decimal(26,6) comment '调拨出库额',       
    transfer_out_qty decimal(26,6) comment '调拨出库量',       
    transfer_return_out_amt decimal(26,6) comment '调拨退货出库额',
    transfer_return_out_qty decimal(26,6) comment '调拨退货出库量',
    sale_amt decimal(26,6) comment '销售出库额',                  
    sale_qty decimal(26,6) comment '销售出库量',             
    sub_to_parent_amt decimal(26,6) comment '子品转母品额',   
    sub_to_parent_qty decimal(26,6) comment '子品转母品量',   
    parent_to_sub_amt decimal(26,6) comment '母品转子品额',   
    parent_to_sub_qty decimal(26,6) comment '母品转子品量',   
    post_amount_profit_amt decimal(26,6) comment '过帐盘盈额',
    post_amount_profit_qty decimal(26,6) comment '过帐盘盈量',
    post_amount_loss_amt decimal(26,6) comment '过帐盘亏额',  
    post_amount_loss_qty decimal(26,6) comment '过帐盘亏量',  
    report_loss_amt decimal(26,6) comment '报损额',          
    report_loss_qty decimal(26,6) comment '报损量',          
    requisition_amt decimal(26,6) comment '领用额',        
    requisition_qty decimal(26,6) comment '领用量',        
    material_use_amt decimal(26,6) comment '原料消耗额',     
    material_use_qty decimal(26,6) comment '原料消耗量',        
    retrun_material_diff_amt decimal(26,6) comment '退料差异额',
    retrun_material_diff_qty decimal(26,6) comment '退料差异量',
    product_transcode_amt decimal(26,6) comment '商品转码额',   
    product_transcode_qty decimal(26,6) comment '商品转码量',   
    move_stock_amt decimal(26,6) comment '移库额',          
    move_stock_qty decimal(26,6) comment '移库量',          
    loss_qty decimal(26,6) comment '损耗量:盈亏数量+报损数量',
    loss_qty_rate decimal(26,6) comment '量损耗率：损耗/原料消耗',
    loss_amt decimal(26,6) comment '损耗额:盈亏额+报损额',
    loss_amt_rate decimal(26,6) comment '额损耗率：损耗率：损耗/原料消耗',
    out_qty decimal(26,6) comment '出库量：调拨出库+调拨退货出库+领用额+原料消耗',
    all_out_amt decimal(26,6) comment '出库量：调拨出库+调拨退货出库+领用额+原料消耗',
    out_loss_qty_rate decimal(26,6) comment '量损耗率：损耗/出库',
    out_loss_amt_rate decimal(26,6) comment '额损耗率：损耗/出库额'
 )comment '工厂月度损耗表'
 partitioned by (months string comment'月份')
 stored as parquet
 ;
