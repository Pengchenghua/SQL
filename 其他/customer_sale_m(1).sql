set mapreduce.job.queuename=caishixian;
drop table b2b_tmp.customer_sale_m_1
;

create temporary table b2b_tmp.customer_sale_m_1 as
select
    aa.dc_code
  , aa.dc_name
  , aa.perform_dc_code
  , aa.perform_dc_name
  , aa.sap_dc_code
  , aa.sap_dc_name
  , aa.dc_company_code
  , aa.dc_company_name
  , aa.dc_province_code
  , aa.dc_province_name
  , aa.dc_city_code
  , aa.dc_city_name
  , aa.dc_address
  , aa.customer_no
  , aa.customer_name
  , aa.child_customer_no
  , aa.child_customer_name
  , aa.item_channel
  , aa.first_category
  , aa.first_category_code
  , aa.second_category
  , aa.second_category_code
  , aa.third_category
  , aa.third_category_code
  , aa.sales_province
  , aa.sales_province_code
  , aa.sales_city
  , aa.sales_city_code
  , aa.sign_time
  , aa.item_province_name
  , aa.item_province_code
  , aa.item_city_name
  , aa.item_city_code
  , aa.payment_terms
  , aa.payment_days
  , aa.phone
  , aa.sales_name
  , aa.sales_id
  , aa.work_no
  , aa.sales_phone
  , aa.supervisor_name
  , aa.supervisor_id
  , aa.supervisor_work_no
  , aa.city_manager_name
  , aa.city_manager_id
  , aa.city_manager_work_no
  , aa.item_province_manager_name
  , aa.item_province_manager_id
  , aa.item_province_manager_work_no
  , aa.org_name
  , aa.org_code
  , aa.order_time
  , aa.sales_date
  , aa.sales_channel
  , aa.sales_channel_name
  , aa.promotion_code
  , aa.goods_code
  , aa.goods_name
  , aa.self_product_name
  , aa.area_product_name
  , aa.bar_code
  , aa.unit
  , aa.category_name
  , aa.category_code
  , aa.category_large_name
  , aa.category_large_code
  , aa.category_middle_name
  , aa.category_middle_code
  , aa.category_small_name
  , aa.category_small_code
  , aa.brand
  , aa.brand_name
  , aa.department_name
  , aa.department_code
  , aa.origin_order_no
  , aa.order_no
  , aa.sap_doc_number
  , aa.report_price
  , aa.cost_price
  , aa.purchase_price
  , aa.middle_office_price
  , aa.sales_price
  , aa.promotion_cost_price
  , aa.promotion_price
  , aa.supplier_cost_rate
  , aa.tax_rate
  , aa.order_qty
  , aa.sales_qty
  , aa.sales_value
  , aa.sales_cost
  , aa.profit
  , aa.front_profit
  , aa.promotion_deduction
  , aa.excluding_tax_sales
  , aa.excluding_tax_cost
  , aa.excluding_tax_profit
  , aa.excluding_tax_deduction
  , aa.tax_value
  , aa.sales_tax_rate
  , aa.cost_tax_rate
  , aa.vendor_code
  , aa.vendor_name
  , aa.run_type
  , aa.storage_location
  , aa.bill_type
  , aa.bill_type_name
  , aa.return_flag
  , aa.customer_group
  , aa.origin_shop_id as sap_origin_dc_code
  , aa.sap_origin_dc_name
  , aa.order_mode
  , aa.order_kind
  , aa.sdt
  , aa.sales_type
  , aa.city_name
  , aa.region_city
  , aa.channel
  , case
        when channel   = '1'
            OR channel = ''
            then '大客户'
        when channel = '2'
            then '商超(对内)'
        when channel = '3'
            then '商超(对外)'
        when channel = '4'
            then '大宗'
        when channel = '5'
            then '供应链(食百)'
        when channel = '6'
            then '供应链(生鲜)'
        when channel = '7'
            then '企业购 '
        when channel = '8'
            then '其他'
    end as channel_name
  , case
        when channel = '5'
            then '平台-食百采购'
        when channel = '6'
            then '平台-生鲜采购'
        when channel = '4'
            then '平台-大宗'
            else aa.province_name
    end as province_name
from
    (
        select
            a.dc_code
          , a.dc_name
          , a.perform_dc_code
          , a.perform_dc_name
          , a.sap_dc_code
          , a.sap_dc_name
          , a.dc_company_code
          , a.dc_company_name
          , a.dc_province_code
          , a.dc_province_name
          , a.dc_city_code
          , a.dc_city_name
          , a.dc_address
          , a.customer_no
          , a.customer_name
          , a.child_customer_no
          , a.child_customer_name
          , a.item_channel
          , a.first_category
          , a.first_category_code
          , a.second_category
          , a.second_category_code
          , a.third_category
          , a.third_category_code
          , a.sales_province
          , a.sales_province_code
          , a.sales_city
          , a.sales_city_code
          , a.sign_time
          , a.item_province_name
          , a.item_province_code
          , a.item_city_name
          , a.item_city_code
          , a.payment_terms
          , a.payment_days
          , a.phone
          , a.sales_name
          , a.sales_id
          , a.work_no
          , a.sales_phone
          , a.supervisor_name
          , a.supervisor_id
          , a.supervisor_work_no
          , a.city_manager_name
          , a.city_manager_id
          , a.city_manager_work_no
          , a.item_province_manager_name
          , a.item_province_manager_id
          , a.item_province_manager_work_no
          , a.org_name
          , a.org_code
          , a.order_time
          , a.sales_date
          , a.sales_channel
          , a.sales_channel_name
          , a.promotion_code
          , a.goods_code
          , a.goods_name
          , a.self_product_name
          , a.area_product_name
          , a.bar_code
          , a.unit
          , a.category_name
          , a.category_code
          , a.category_large_name
          , a.category_large_code
          , a.category_middle_name
          , a.category_middle_code
          , a.category_small_name
          , a.category_small_code
          , a.brand
          , a.brand_name
          , a.department_name
          , a.department_code
          , a.origin_order_no
          , a.order_no
          , a.sap_doc_number
          , a.report_price
          , a.cost_price
          , a.purchase_price
          , a.middle_office_price
          , a.sales_price
          , a.promotion_cost_price
          , a.promotion_price
          , a.supplier_cost_rate
          , a.tax_rate
          , a.order_qty
          , a.sales_qty
          , a.sales_value
          , a.sales_cost
          , a.profit
          , a.front_profit
          , a.promotion_deduction
          , a.excluding_tax_sales
          , a.excluding_tax_cost
          , a.excluding_tax_profit
          , a.excluding_tax_deduction
          , a.tax_value
          , a.sales_tax_rate
          , a.cost_tax_rate
          , a.vendor_code
          , a.vendor_name
          , a.run_type
          , a.storage_location
          , a.bill_type
          , a.bill_type_name
          , a.return_flag
          , a.customer_group
          , a.origin_shop_id
          , a.sap_origin_dc_name
          , a.order_mode
          , a.order_kind
          , a.sdt
          , a.sales_type
          , c.city_name
          , b.sales_city region_city
          , case
                when a.origin_shop_id = 'W0B6'
                    or b.channel   like '%企业购%'
                    then '7'
                when (
                        a.shop_id            = 'W0H4'
                        and a.customer_no like 'S%'
                        and a.category_code in ('12'
                                              ,'13'
                                              ,'14')
                    )
                    or (
                        b.channel like '供应链%'
                        and a.category_code in ('12'
                                              ,'13'
                                              ,'14')
                    )
                    then '5'
                when (
                        a.shop_id            = 'W0H4'
                        and a.customer_no like 'S%'
                        and a.category_code in ('10'
                                              ,'11')
                    )
                    or (
                        b.channel like '供应链%'
                        and a.category_code in ('10'
                                              , '11')
                    )
                    then '6'
                when b.channel   = '大客户'
                    or b.channel = 'B端'
                    then '1'
                when b.channel      ='M端'
                    or b.channel like '%对内%'
                    then '2'
                when b.channel like '%对外%'
                    then '3'
                when b.channel = '大宗'
                    then '4'
                when b.channel='其他'
                    then '8'
                    else ''
            end as channel
          , case
                when a.shop_id in ('W0M1'
                                 ,'W0M4'
                                 ,'W0J6'
                                 ,'W0M6'
                                 ,'W0K5')
                    then '商超平台'
                when b.customer_no is not null
                    and b.sales_province     ='BBC'
                    then '福建省'
                when b.sales_province is not null
                    and b.channel              <> 'M端'
                    and b.channel        not like '商超%'
                    then b.sales_province
                when a.customer_no like 'S%'
                    and substr(c.province_name, 1, 2) in ('重庆'
                                                        ,'四川'
                                                        ,'北京'
                                                        ,'福建'
                                                        ,'上海'
                                                        ,'浙江'
                                                        ,'江苏'
                                                        ,'安徽'
                                                        ,'广东')
                    then c.province_name
                    else d.province_name
            end as province_name
        from
            (
                select
                    dc_code
                  , dc_code as shop_id
                  , dc_name
                  , perform_dc_code
                  , perform_dc_name
                  , sap_dc_code
                  , sap_dc_name
                  , dc_company_code
                  , dc_company_name
                  , dc_province_code
                  , dc_province_name
                  , dc_city_code
                  , dc_city_name
                  , dc_address
                  , customer_no
                  , customer_name
                  , child_customer_no
                  , child_customer_name
                  , channel as item_channel
                  , first_category
                  , first_category_code
                  , second_category
                  , second_category_code
                  , third_category
                  , third_category_code
                  , sales_province
                  , sales_province_code
                  , sales_city
                  , sales_city_code
                  , sign_time
                  , province_name as item_province_name
                  , province_code as item_province_code
                  , city_name     as item_city_name
                  , city_code     as item_city_code
                  , payment_terms
                  , payment_days
                  , phone
                  , sales_name
                  , sales_id
                  , work_no
                  , sales_phone
                  , supervisor_name
                  , supervisor_id
                  , supervisor_work_no
                  , city_manager_name
                  , city_manager_id
                  , city_manager_work_no
                  , province_manager_name    as item_province_manager_name
                  , province_manager_id      as item_province_manager_id
                  , province_manager_work_no as item_province_manager_work_no
                  , org_name
                  , org_code
                  , order_time
                  , sales_date
                  , sales_channel
                  , sales_channel_name
                  , promotion_code
                  , goods_code
                  , goods_name
                  , self_product_name
                  , area_product_name
                  , bar_code
                  , unit
                  , division_name as category_name
                  , division_code as category_code
                  , category_large_name
                  , category_large_code
                  , category_middle_name
                  , category_middle_code
                  , category_small_name
                  , category_small_code
                  , brand
                  , brand_name
                  , department_name
                  , department_code
                  , origin_order_no
                  , order_no
                  , sap_doc_number
                  , report_price
                  , cost_price
                  , purchase_price
                  , middle_office_price
                  , sales_price
                  , promotion_cost_price
                  , promotion_price
                  , supplier_cost_rate
                  , tax_rate
                  , order_qty
                  , sales_qty
                  , sales_value
                  , sales_cost
                  , profit
                  , front_profit
                  , promotion_deduction
                  , excluding_tax_sales
                  , excluding_tax_cost
                  , excluding_tax_profit
                  , excluding_tax_deduction
                  , tax_value
                  , sales_tax_rate
                  , cost_tax_rate
                  , vendor_code
                  , vendor_name
                  , run_type
                  , storage_location
                  , bill_type
                  , bill_type_name
                  , return_flag
                  , customer_group
                  , sap_origin_dc_code as origin_shop_id
                  , sap_origin_dc_name
                  , order_mode
                  , order_kind
                  , sdt
                  , sales_type
                from
                    csx_dw.sale_item_m
                where
                    sales_type in('qyg'
                                ,'gc'
                                ,'anhui'
                                ,'sc')
                    and sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
                    and sdt<=regexp_replace(date_sub(current_date,1),'-','')
            )
            a
            left outer join
                (
                    select
                        customer_no
                      , sales_province_code
                      , sales_province
                      , sales_city
                      , channel
                    from
                        csx_dw.customer_m
                    where
                        sdt             = regexp_replace(date_sub(current_date, 1), '-', '')
                        and customer_no<>''
                )
                b
                on
                    a.customer_no=b.customer_no
            left outer join
                (
                    select
                        shop_id
                      , case
                            when shop_id in ('W055'
                                           ,'W056')
                                then '上海市'
                                else province_name
                        end province_name
                      , case
                            when province_name like '%市'
                                then province_name
                                else city_name
                        end city_name
                    from
                        csx_dw.shop_m
                    where
                        sdt = 'current'
                )
                c
                on
                    a.customer_no = concat('S',c.shop_id)
            left outer join
                (
                    select
                        shop_id
                      , shop_name
                      , province_name
                    from
                        csx_dw.shop_m
                    where
                        sdt = 'current'
                )
                d
                on
                    a.shop_id = d.shop_id
    )
    aa
;

drop table b2b_tmp.customer_sale_m_2
;

create temporary table b2b_tmp.customer_sale_m_2 as
select
    a.dc_code
  , a.dc_name
  , a.perform_dc_code
  , a.perform_dc_name
  , a.sap_dc_code
  , a.sap_dc_name
  , a.dc_company_code
  , a.dc_company_name
  , a.dc_province_code
  , a.dc_province_name
  , a.dc_city_code
  , a.dc_city_name
  , a.dc_address
  , a.customer_no
  , a.customer_name
  , a.child_customer_no
  , a.child_customer_name
  , a.item_channel
  , a.first_category
  , a.first_category_code
  , a.second_category
  , a.second_category_code
  , a.third_category
  , a.third_category_code
  , a.sales_province
  , a.sales_province_code
  , a.sales_city
  , a.sales_city_code
  , a.sign_time
  , a.item_province_name
  , a.item_province_code
  , a.item_city_name
  , a.item_city_code
  , a.payment_terms
  , a.payment_days
  , a.phone
  , a.sales_name
  , a.sales_id
  , a.work_no
  , a.sales_phone
  , a.supervisor_name
  , a.supervisor_id
  , a.supervisor_work_no
  , a.city_manager_name
  , a.city_manager_id
  , a.city_manager_work_no
  , a.item_province_manager_name
  , a.item_province_manager_id
  , a.item_province_manager_work_no
  , a.org_name
  , a.org_code
  , a.order_time
  , a.sales_date
  , a.sales_channel
  , a.sales_channel_name
  , a.promotion_code
  , a.goods_code
  , a.goods_name
  , a.self_product_name
  , a.area_product_name
  , a.bar_code
  , a.unit
  , a.category_name as division_name
  , a.category_code as division_code
  , a.category_large_name
  , a.category_large_code
  , a.category_middle_name
  , a.category_middle_code
  , a.category_small_name
  , a.category_small_code
  , a.brand
  , a.brand_name
  , a.department_name
  , a.department_code
  , a.origin_order_no
  , a.order_no
  , a.sap_doc_number
  , a.report_price
  , a.cost_price
  , a.purchase_price
  , a.middle_office_price
  , a.sales_price
  , a.promotion_cost_price
  , a.promotion_price
  , a.supplier_cost_rate
  , a.tax_rate
  , a.order_qty
  , a.sales_qty
  , a.sales_value
  , a.sales_cost
  , a.profit
  , a.front_profit
  , a.promotion_deduction
  , a.excluding_tax_sales
  , a.excluding_tax_cost
  , a.excluding_tax_profit
  , a.excluding_tax_deduction
  , a.tax_value
  , a.sales_tax_rate
  , a.cost_tax_rate
  , a.vendor_code
  , a.vendor_name
  , a.run_type
  , a.storage_location
  , a.bill_type
  , a.bill_type_name
  , a.return_flag
  , a.customer_group
  , a.sap_origin_dc_code
  , a.sap_origin_dc_name
  , a.order_mode
  , a.order_kind
  , a.sdt
  , a.sales_type
  , a.channel
  , a.channel_name
  , case
        when a.province_name='商超平台'
            then '-100'
            else g.province_code
    end province_code
  , case
        when a.province_name='平台-B'
            then '大客户平台'
            else a.province_name
    end province_name
  , a.city_name
  , case
        when a.province_name = '福建省'
            then coalesce(b.city_real,'福州、宁德、三明')
            else '-'
    end city_real
  , case
        when a.province_name = '福建省'
            then coalesce(b.cityjob,'沈锋')
            else '-'
    end cityjob
  , h.province_manager_id
  , h.province_manager_work_no
  , h.province_manager_name
from
    (
        select
            dc_code
          , dc_name
          , perform_dc_code
          , perform_dc_name
          , sap_dc_code
          , sap_dc_name
          , dc_company_code
          , dc_company_name
          , dc_province_code
          , dc_province_name
          , dc_city_code
          , dc_city_name
          , dc_address
          , customer_no
          , customer_name
          , child_customer_no
          , child_customer_name
          , item_channel
          , first_category
          , first_category_code
          , second_category
          , second_category_code
          , third_category
          , third_category_code
          , sales_province
          , sales_province_code
          , sales_city
          , sales_city_code
          , sign_time
          , item_province_name
          , item_province_code
          , item_city_name
          , item_city_code
          , payment_terms
          , payment_days
          , phone
          , sales_name
          , sales_id
          , work_no
          , sales_phone
          , supervisor_name
          , supervisor_id
          , supervisor_work_no
          , city_manager_name
          , city_manager_id
          , city_manager_work_no
          , item_province_manager_name
          , item_province_manager_id
          , item_province_manager_work_no
          , org_name
          , org_code
          , order_time
          , sales_date
          , sales_channel
          , sales_channel_name
          , promotion_code
          , goods_code
          , goods_name
          , self_product_name
          , area_product_name
          , bar_code
          , unit
          , category_name
          , category_code
          , category_large_name
          , category_large_code
          , category_middle_name
          , category_middle_code
          , category_small_name
          , category_small_code
          , brand
          , brand_name
          , department_name
          , department_code
          , origin_order_no
          , order_no
          , sap_doc_number
          , report_price
          , cost_price
          , purchase_price
          , middle_office_price
          , sales_price
          , promotion_cost_price
          , promotion_price
          , supplier_cost_rate
          , tax_rate
          , order_qty
          , sales_qty
          , sales_value
          , sales_cost
          , profit
          , front_profit
          , promotion_deduction
          , excluding_tax_sales
          , excluding_tax_cost
          , excluding_tax_profit
          , excluding_tax_deduction
          , tax_value
          , sales_tax_rate
          , cost_tax_rate
          , vendor_code
          , vendor_name
          , run_type
          , storage_location
          , bill_type
          , bill_type_name
          , return_flag
          , customer_group
          , sap_origin_dc_code
          , sap_origin_dc_name
          , order_mode
          , order_kind
          , sdt
          , sales_type
          , case
                when channel is null
                    or channel     =''
                    then '1'
                when province_name='平台-B'
                    and channel   ='1'
                    then '1'
                    else channel
            end channel
          , case
                when channel is null
                    or channel     =''
                    then '大客户'
                when province_name='平台-B'
                    and channel   ='1'
                    then '大客户'
                    else channel_name
            end channel_name
          , case
                when province_name ='成都省'
                    then '四川省'
                when channel='7'
                    then '福建省'
                    else province_name
            end province_name
          , case
                when channel='2'
                    then city_name
                    --when channel='7' then '福州'
                when (
                        channel<>'2'
                    )
                    then region_city
                    else '-'
            end city_name
        from
            b2b_tmp.customer_sale_m_1
    )
    a
    left outer join
        (
            select
                '泉州'city
              ,'泉州' city_real
              ,'张铮' cityjob
            union all
            select
                '莆田'city
              ,'莆田' city_real
              ,'倪薇红'cityjob
            union all
            select
                '南平'city
              ,'南平' city_real
              ,'林挺' cityjob
            union all
            select
                '厦门'     city
              ,'厦门、龙岩、漳州'city_real
              ,'崔丽'      cityjob
            union all
            select
                '漳州'     city
              ,'厦门、龙岩、漳州'city_real
              ,'崔丽'      cityjob
            union all
            select
                '龙岩'     city
              ,'厦门、龙岩、漳州'city_real
              ,'崔丽'      cityjob
            union all
            select
                '福州'     city
              ,'福州、宁德、三明'city_real
              ,'沈锋'      cityjob
            union all
            select
                '宁德'     city
              ,'福州、宁德、三明'city_real
              ,'沈锋'      cityjob
            union all
            select
                '三明'     city
              ,'福州、宁德、三明'city_real
              ,'沈锋'cityjob
        )
        b
        on
            substr(a.city_name,1,2)=b.city
    left outer join
        (
            select
                province_code
              , province
            from
                csx_ods.sys_province_ods
        )
        g
        on
            a.province_name=g.province
    left outer join
        (
            -- 插入省区总信息
            select
                sales_id   as province_manager_id
              , sales_name as province_manager_name
              , work_no    as province_manager_work_no
              , province_name
            from
                csx_dw.region_data_permission
            where
                region_permission = 3
        )
        h
        on
            a.province_name = h.province_name
;

set hive.map.aggr                           = true;
set hive.groupby.skewindata                 =false;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;      -- 开启动态分析
set hive.exec.dynamic.partition.mode        =nonstrict; -- 动态分区模式
set hive.exec.max.dynamic.partitions.pernode=10000;
insert overwrite table csx_dw.customer_sale_m partition
    (sdt
      , sales_type
    )
select
    dc_code
  , dc_name
  , perform_dc_code
  , perform_dc_name
  , sap_dc_code
  , sap_dc_name
  , dc_company_code
  , dc_company_name
  , dc_province_code
  , dc_province_name
  , dc_city_code
  , dc_city_name
  , dc_address
  , customer_no
  , customer_name
  , child_customer_no
  , child_customer_name
  , item_channel
  , first_category
  , first_category_code
  , second_category
  , second_category_code
  , third_category
  , third_category_code
  , sales_province
  , sales_province_code
  , sales_city
  , sales_city_code
  , sign_time
  , item_province_name
  , item_province_code
  , item_city_name
  , item_city_code
  , payment_terms
  , payment_days
  , phone
  , sales_name
  , sales_id
  , work_no
  , sales_phone
  , supervisor_name
  , supervisor_id
  , supervisor_work_no
  , city_manager_name
  , city_manager_id
  , city_manager_work_no
  , item_province_manager_name
  , item_province_manager_id
  , item_province_manager_work_no
  , org_name
  , org_code
  , order_time
  , sales_date
  , sales_channel
  , sales_channel_name
  , promotion_code
  , goods_code
  , goods_name
  , self_product_name
  , area_product_name
  , bar_code
  , unit
  , division_name
  , division_code
  , category_large_name
  , category_large_code
  , category_middle_name
  , category_middle_code
  , category_small_name
  , category_small_code
  , brand
  , brand_name
  , department_name
  , department_code
  , origin_order_no
  , order_no
  , sap_doc_number
  , report_price
  , cost_price
  , purchase_price
  , middle_office_price
  , sales_price
  , promotion_cost_price
  , promotion_price
  , supplier_cost_rate
  , tax_rate
  , order_qty
  , sales_qty
  , sales_value
  , sales_cost
  , profit
  , front_profit
  , promotion_deduction
  , excluding_tax_sales
  , excluding_tax_cost
  , excluding_tax_profit
  , excluding_tax_deduction
  , tax_value
  , sales_tax_rate
  , cost_tax_rate
  , vendor_code
  , vendor_name
  , run_type
  , storage_location
  , bill_type
  , bill_type_name
  , return_flag
  , customer_group
  , sap_origin_dc_code
  , sap_origin_dc_name
  , order_mode
  , order_kind
  , channel
  , channel_name
  , province_code
  , province_name
  , city_name
  , city_real
  , cityjob
  , province_manager_id
  , province_manager_work_no
  , province_manager_name
  , sdt
  , sales_type
from
    b2b_tmp.customer_sale_m_2
;