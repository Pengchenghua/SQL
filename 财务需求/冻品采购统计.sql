select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,classify_middle_name,classify_middle_code,sum(a.receive_qty*price) as amt from csx_dw.dws_wms_r_d_entry_detail a 
join
(select location_code,zone_id,zone_name,purpose_code from csx_dw.csx_shop 
where sdt='current' 
    and table_type=1 
    and purchase_org !='P620' 
    and purpose_code in ('01','02','03','08','07') ) b on a.receive_location_code=b.location_code
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code='B0304') c on a.category_small_code=c.category_small_code
where  (sdt>='20200101'
       OR sdt='19990101')
    AND receive_status IN (1,2)
    and  business_type not in ('ZC01','02')
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')

group by  CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END ,classify_middle_name,classify_middle_code
;


--联采商品入库

select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,classify_middle_name,classify_middle_code,sum(a.receive_qty*price) as amt from csx_dw.dws_wms_r_d_entry_detail a 
join
(select location_code,zone_id,zone_name,purpose_code from csx_dw.csx_shop 
where sdt='current' 
    and table_type=1 
     and dist_name like '安徽%'
    -- and purchase_org !='P620' 
    and purpose_code in ('01','02','03','08','07') ) b on a.receive_location_code=b.location_code
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code='B0304') c on a.category_small_code=c.category_small_code
join 
(select shop_code,product_code,joint_purchase_flag 
    from csx_dw.dws_basic_w_a_csx_product_info
    where sdt='current' and joint_purchase_flag=1) d on a.receive_location_code=d.shop_code and a.goods_code=d.product_code
where  (sdt>='20200101'
       OR sdt='19990101')
    AND (receive_status IN (1,2)
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (business_type in ('ZN01','ZN02')
       OR ( order_type_code LIKE 'P%' and  business_type !='02')))
group by  CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END ,classify_middle_name,classify_middle_code
;




--四川蔬菜入库汇总
select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,sum(a.receive_qty*price) as amt from csx_dw.dws_wms_r_d_entry_detail a 
join
(select location_code,zone_id,zone_name,purpose_code from csx_dw.csx_shop 
where sdt='current' 
    and table_type=1 
     and dist_name like '四川%'
    -- and purchase_org !='P620' 
    and purpose_code in ('01','02','03','08','07') ) b on a.receive_location_code=b.location_code
join 
(select shop_code,product_code,joint_purchase_flag 
    from csx_dw.dws_basic_w_a_csx_product_info
    where sdt='current' 
    and purchase_group_code ='H03') d on a.receive_location_code=d.shop_code and a.goods_code=d.product_code
where  (sdt>='20200101'
       OR sdt='19990101')
    AND (receive_status IN (1,2)
    and a.category_large_code='1103'
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (business_type in ('ZN01','ZN02')
       OR ( order_type_code LIKE 'P%' and  business_type !='02')))
group by  CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END 
;





  -- 蔬菜商品销售毛利率  
select substr(sdt,1,6) mon,sum(sales_value)/10000 as sale ,sum(profit )/10000 as profit, sum(profit )/sum(sales_value) as profit_rate from csx_dw.dws_sale_r_d_detail a 

join 
(select shop_code,product_code,joint_purchase_flag 
    from csx_dw.dws_basic_w_a_csx_product_info
    where sdt='current'  and purchase_group_code ='H03'
    ) d on a.dc_code=d.shop_code and a.goods_code=d.product_code
where
 sdt>='20200101' 
  and province_name like '四川%'
 and a.business_type_code !='4'
 and a.channel_code in ('1','7','9')
 group by  substr(sdt,1,6)
 ;




  -- 蔬菜商品销售毛利率  
  
select substr(sdt,1,6) mon,goods_code,goods_name,sum(sales_value)/10000 as sale ,sum(profit )/10000 as profit, sum(profit )/sum(sales_value) as profit_rate from csx_dw.dws_sale_r_d_detail a 

join 
(select shop_code,product_code,joint_purchase_flag 
    from csx_dw.dws_basic_w_a_csx_product_info
    where sdt='current'  and purchase_group_code ='H03'
    ) d on a.dc_code=d.shop_code and a.goods_code=d.product_code
where
 sdt>='20200101' 
  and province_name like '四川%'
 and a.business_type_code !='4'
 and a.channel_code in ('1','7','9')
 group by  substr(sdt,1,6),goods_code,goods_name
 ;


   
  -- 联采商品销售毛利率  
select substr(sdt,1,6) mon,sum(sales_value)/10000 as sale ,sum(profit )/10000 as profit, sum(profit )/sum(sales_value) as profit_rate from csx_dw.dws_sale_r_d_detail a 
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code='B0304') c on a.category_small_code=c.category_small_code
join 
(select shop_code,product_code,joint_purchase_flag 
    from csx_dw.dws_basic_w_a_csx_product_info
    where sdt='current' and joint_purchase_flag=1) d on a.dc_code=d.shop_code and a.goods_code=d.product_code
where
 sdt>='20200101' 
 and a.business_type_code !='4'
 and a.channel_code in ('1','7','9')
 group by  substr(sdt,1,6)
 ;



     
-- 福建联采商品

 select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,classify_middle_name,classify_middle_code,sum(a.receive_qty*price)/10000  as amt from csx_dw.dws_wms_r_d_entry_detail a 
join
(select location_code,shop_name,zone_id,zone_name,purpose_code from csx_dw.csx_shop 
where sdt='current' 
     and table_type=1 
     and dist_name like '福建%'
     and location_code not in('W0J8','W0K4')
     and purchase_org !='P620' 
     and purpose_code in ('01','02','03','08','07') ) b on a.receive_location_code=b.location_code
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code='B0304') c on a.category_small_code=c.category_small_code
join 
(select  goods_id,spu_goods_code,spu_goods_status
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304','B0305')
    AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
where  (sdt>='20200101'
       OR sdt='19990101')
    AND (receive_status IN (1,2)
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (business_type in ('ZN01','ZN02')
       OR ( order_type_code LIKE 'P%' and  business_type !='02')))
group by  CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END ,classify_middle_name,classify_middle_code
;



   
  -- 联采商品销售毛利率  
select substr(sdt,1,6) mon,sum(sales_value)/10000 as sale ,sum(profit )/10000 as profit, sum(profit )/sum(sales_value) as profit_rate from csx_dw.dws_sale_r_d_detail a 
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code in ('B0304','B0305') ) c on a.category_small_code=c.category_small_code
join 
(select  goods_id,spu_goods_code,spu_goods_status
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304','B0305')
    AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
where
 sdt>='20200101' 
 and a.province_name like '安徽%'
 and a.business_type_code !='4'
 and a.channel_code in ('1','7','9')
 group by  substr(sdt,1,6)
 ;



    
  -- 联采商品销售毛利率  
select substr(sdt,1,6) mon,goods_code,goods_name,spu_goods_name,sum(sales_value)/10000 as sale ,sum(profit )/10000 as profit, sum(profit )/sum(sales_value) as profit_rate from csx_dw.dws_sale_r_d_detail a 
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code in ('B0304','B0305') ) c on a.category_small_code=c.category_small_code
join 
(select  goods_id,spu_goods_code,spu_goods_status,spu_goods_name
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304','B0305')
    AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
where
 sdt>='20200101' 
-- and ( a.province_name like '福建%'or  a.province_name like '安徽%')
 and a.business_type_code !='4'
 and a.channel_code in ('1','7','9')
 group by  goods_code,goods_name,spu_goods_name,substr(sdt,1,6) 
 ;



    
  -- 联采商品销售毛利率  
select substr(sdt,1,6) mon,a.province_code,a.province_name,goods_code,goods_name,spu_goods_name,sum(sales_value)/10000 as sale ,sum(profit )/10000 as profit, sum(profit )/sum(sales_value) as profit_rate from csx_dw.dws_sale_r_d_detail a 
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code in ('B0304','B0305') ) c on a.category_small_code=c.category_small_code
join 
(select  goods_id,spu_goods_code,spu_goods_status,spu_goods_name
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304','B0305')
    AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
where
 sdt>='20200101' 
 and ( a.province_name like '福建%'or  a.province_name like '安徽%')
 and a.business_type_code !='4'
 and a.channel_code in ('1','7','9')
 group by  goods_code,goods_name,spu_goods_name,substr(sdt,1,6),a.province_code,a.province_name
 ;





-- 福建联采商品select    mon, 
        
       sum(receive_amt)/sum(receive_qty) receive_cost,
       sum(receive_qty) as receive_qty,
       sum(receive_amt)/10000  as receive_amt ,
       sum(shipped_amt)/ sum(shipped_qty) as shipped_cost,
       sum(shipped_qty) as shipped_qty,
       sum(shipped_amt)/10000 as shipped_amt,
       sum(receive_amt-shipped_amt)/10000 as net_receive_amt,
       sum(receive_qty-shipped_qty) as net_receive_qty,
       sum(receive_amt-shipped_amt)/sum(receive_qty-shipped_qty) as net_receive_cost,
        sum(no_tax_receive_amt) as no_tax_receive_amt,
        sum(no_tax_shipped_amt) as no_tax_shipped_amt,
       sum(no_tax_receive_amt-no_tax_shipped_amt)/sum(receive_qty-shipped_qty) as no_tax_net_receive_cost,
       sum(no_tax_receive_amt-no_tax_shipped_amt)/10000 as no_tax_net_receive_amt
from 

( select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,
       a.receive_location_code as dc_code,
       a.goods_code,
       a.supplier_code,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN receive_qty*-1 ELSE receive_qty END) as receive_qty,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN (a.receive_qty*price)*-1 ELSE (a.receive_qty*price) END )  as receive_amt ,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN a.receive_qty*(price/(1+a.tax_rate))*-1 ELSE a.receive_qty*(price/(1+a.tax_rate)) END )  as no_tax_receive_amt ,
       0 shipped_qty,
       0 shipped_amt,
       0 no_tax_shipped_amt
from  csx_dw.dws_wms_r_d_entry_detail a 
where   ((sdt>='20200101' and sdt<'20210701')
       OR sdt='19990101')
    AND (receive_status IN (1,2)
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (business_type in ('ZN01','ZN02','ZNR1','ZNR2')
       OR ( order_type_code LIKE 'P%' and  business_type !='02')))
group by CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  ,
       a.receive_location_code ,
       a.goods_code,
       a.supplier_code       
       
union all 
select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,
       a.shipped_location_code as dc_code,
       a.goods_code,
       a.supplier_code,
       0 receive_qty,
       0 receive_amt,
       0 no_tax_receive_amt,
       sum(shipped_qty) as shipped_qty,
       sum(a.shipped_qty *price)  as shipped_amt ,
        sum(a.shipped_qty*(price/(1+a.tax_rate)))  as no_tax_shipped_amt 
from   csx_dw.dws_wms_r_d_ship_detail a 
where   ((sdt>='20200101' and sdt<'20210701')
       OR sdt='19990101')
    AND (a.status !=9 
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (a.business_type_code in ('ZNR1','ZNR2')
       OR ( order_type_code LIKE 'P%' and  a.business_type_code ='05')))
GROUP BY CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  ,
       a.shipped_location_code ,
       a.goods_code,
       a.supplier_code
)a

join
(select location_code,shop_name,zone_id,zone_name,purpose_code,dist_code,dist_name from csx_dw.csx_shop 
where sdt='current' 
     and table_type=1 
    -- and dist_name like '安徽%'
    --  and location_code not in('W0J8','W0K4')
    -- and purchase_org !='P620' 
     and purpose_code in ('01','02','03','08','07') ) b on a.dc_code=b.location_code

join 
(select  goods_id,spu_goods_code,spu_goods_status,classify_middle_code,classify_middle_name,goods_name,spu_goods_name
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304')
    -- 关联SPU 联采商品
    -- AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
group by   mon 
;




create temporary table csx_tmp.temp_sale_01 as 
select substr(sdt,1,6) mon,
    a.province_code,
    a.province_name,
    goods_code,
    goods_name,
    spu_goods_name,
    sum(sales_value)/10000 as sale ,
    sum(profit )/10000 as profit,
    sum(profit )/sum(sales_value) as profit_rate 
from csx_dw.dws_sale_r_d_detail a 
join
(select distinct classify_middle_code,classify_middle_name,category_small_code
from csx_dw.dws_basic_w_a_manage_classify_m
    where sdt='current' and classify_middle_code in ('B0304','B0305') ) c on a.category_small_code=c.category_small_code
join 
(select  goods_id,spu_goods_code,spu_goods_status,spu_goods_name
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304','B0305')
    AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
where
 sdt>='20200101' 
--  and ( a.province_name like '福建%'or  a.province_name like '安徽%') 
 and a.dc_code  not in('W0J8','W0K4')
 and a.business_type_code !='4'
 and a.channel_code in ('1','7','9')
 group by  goods_code,goods_name,spu_goods_name,substr(sdt,1,6),a.province_code,a.province_name
 ;
 
 
     select mon,
     a.province_name,
     sum(sale) sale, sum(profit) profit,sum(profit)/sum(sale) as profit 
    from csx_tmp.temp_sale_01 a 
    where  a.province_name like '福建%'or  a.province_name like '安徽%' 
    group by mon,a.province_name;
    
    
    select * from csx_tmp.temp_sale_01 a 
    where  a.province_name like '福建%'or  a.province_name like '安徽%' ;
 


 drop table  csx_tmp.tmp_entry_dc_aa;
create  table csx_tmp.tmp_entry_dc_aa as 
select    mon,dist_code,dist_name,
        dc_code,
        shop_name,
        goods_code,
        goods_name,
        spu_goods_code,
        spu_goods_name,
       classify_middle_code,classify_middle_name,
       sum(receive_amt)/sum(receive_qty) receive_cost,
       sum(receive_qty) as receive_qty,
       sum(receive_amt)/10000  as receive_amt ,
       sum(shipped_amt)/ sum(shipped_qty) as shipped_cost,
       sum(shipped_qty) as shipped_qty,
       sum(shipped_amt)/10000 as shipped_amt,
       sum(receive_amt-shipped_amt)/10000 as net_receive_amt,
       sum(receive_qty-shipped_qty) as net_receive_qty,
       sum(receive_amt-shipped_amt)/sum(receive_qty-shipped_qty) as net_receive_cost,
        sum(no_tax_receive_amt)/10000 as no_tax_receive_amt,
        sum(no_tax_shipped_amt)/10000 as no_tax_shipped_amt,
       sum(no_tax_receive_amt-no_tax_shipped_amt)/sum(receive_qty-shipped_qty) as no_tax_net_receive_cost,
        sum(no_tax_receive_amt-no_tax_shipped_amt)/10000 as no_tax_net_receive_amt
from 

( select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,
       a.receive_location_code as dc_code,
       a.goods_code,
       a.supplier_code,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN receive_qty*-1 ELSE receive_qty END) as receive_qty,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN (a.receive_qty*price)*-1 ELSE (a.receive_qty*price) END )  as receive_amt ,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN a.receive_qty*(price/(1+a.tax_rate))*-1 ELSE a.receive_qty*(price/(1+a.tax_rate)) END )  as no_tax_receive_amt ,
       0 shipped_qty,
       0 shipped_amt,
       0 no_tax_shipped_amt
from  csx_dw.dws_wms_r_d_entry_detail a 
where  (sdt>='20200101'
       OR sdt='19990101')
    AND (receive_status IN (1,2)
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (business_type in ('ZN01','ZN02','ZNR1','ZNR2')
       OR ( order_type_code LIKE 'P%' and  business_type !='02')))
group by CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  ,
       a.receive_location_code ,
       a.goods_code,
       a.supplier_code       
       
union all 
select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,
       a.shipped_location_code as dc_code,
       a.goods_code,
       a.supplier_code,
       0 receive_qty,
       0 receive_amt,
       0 no_tax_receive_amt,
       sum(shipped_qty) as shipped_qty,
       sum(a.shipped_qty *price)  as shipped_amt ,
        sum(a.shipped_qty*(price/(1+a.tax_rate)))  as no_tax_shipped_amt 
from   csx_dw.dws_wms_r_d_ship_detail a 
where  (sdt>='20200101'
       OR sdt='19990101')
    AND (a.status !=9 
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (a.business_type_code in ('ZNR1','ZNR2')
       OR ( order_type_code LIKE 'P%' and  a.business_type_code ='05')))
GROUP BY CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  ,
       a.shipped_location_code ,
       a.goods_code,
       a.supplier_code
)a

join
(select location_code,shop_name,zone_id,zone_name,purpose_code,dist_code,dist_name from csx_dw.csx_shop 
where sdt='current' 
     and table_type=1 
    -- and dist_name like '安徽%'
    --  and location_code not in('W0J8','W0K4')
    -- and purchase_org !='P620' 
     and purpose_code in ('01','02','03','08','07') ) b on a.dc_code=b.location_code

join 
(select  goods_id,spu_goods_code,spu_goods_status,classify_middle_code,classify_middle_name,goods_name,spu_goods_name
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304')
    -- 关联SPU 联采商品
     AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
group by   mon,dist_code,dist_name,
        dc_code,
        shop_name,
        goods_code,
        goods_name,
        spu_goods_code,
        spu_goods_name,
       classify_middle_code,classify_middle_name
;



-- 销售商品
drop table  csx_tmp.temp_spu_sale_01 ;
CREATE  table csx_tmp.temp_spu_sale_01 as 
SELECT substr(sdt, 1, 6) mon,
            a.dc_code,
             goods_code,
             goods_name,
             spu_goods_name,
             SUM(a.sales_qty) as qty,
             sum(sales_value)/10000 AS sale,
             SUM(SALES_VALUE)/SUM(a.sales_qty) price,
             sum(profit)/10000 AS profit,
             sum(profit)/sum(sales_value) AS profit_rate,
              sum(a.excluding_tax_sales)/10000 AS no_tax_sale,
             SUM(excluding_tax_sales)/SUM(a.sales_qty) no_tax_price,
             sum(a.excluding_tax_profit)/10000 AS no_tax_profit,
             sum(a.excluding_tax_profit)/sum(excluding_tax_sales) AS no_tax_profit_rate
FROM csx_dw.dws_sale_r_d_detail a
LEFT JOIN
  (SELECT goods_id,
          spu_goods_code,
          spu_goods_status,
          spu_goods_name,
          classify_middle_code
   FROM csx_dw.dws_basic_w_a_csx_product_M
   WHERE sdt='current'
     AND classify_middle_code IN ('B0304', 'B0305')
    -- AND spu_goods_code IS NOT NULL 
     ) d ON a.goods_code=d.goods_id
WHERE sdt>='20200101' 
    and sdt<'20210701'
-- and ( a.province_name like '福建%'or  a.province_name like '安徽%')
  AND a.business_type_code !='4'
  -- AND a.dc_code NOT IN('W0J8', 'W0K4')
  AND a.channel_code IN ('1','7','9')
  and d.classify_middle_code IN ('B0304', 'B0305')
GROUP BY goods_code,
         goods_name,a.dc_code,
         spu_goods_name,
         substr(sdt,1,6) ;
         
         
    
  select   
        mon,
        dc_code,shop_name,sales_province_code,sales_province_name,company_code,company_name,
        goods_code,
        goods_name,
        spu_goods_name,
        sum(receive_cost)receive_cost,
        sum(receive_qty)receive_qty,
        sum(receive_amt)receive_amt ,
        sum(shipped_cost)shipped_cost,
        sum(shipped_qty)shipped_qty,
        sum(shipped_amt)shipped_amt,
        sum(net_receive_amt)net_receive_amt,
        sum(net_receive_qty)net_receive_qty,
        sum(net_receive_cost)net_receive_cost,
        sum(no_tax_receive_amt)no_tax_receive_amt,
        sum(no_tax_shipped_amt)no_tax_shipped_amt,
        sum(no_tax_net_receive_cost)no_tax_net_receive_cost,
        sum(no_tax_net_receive_amt)no_tax_net_receive_amt,
        sum(qty ) as   qty,
        sum(sale) as   sale,
        sum(price) as   price,
        sum(profit) as   profit,
        sum(profit_rate) as   profit_rate,
        sum(no_tax_sale)as   no_tax_sale,
        sum(no_tax_price) as   no_tax_price,
        sum(no_tax_profit) as   no_tax_profit,
        sum(no_tax_profit_rate) as   no_tax_profit_rate
    from (
    select  mon,
        dc_code,
        goods_code,
        goods_name,
        spu_goods_name,
        receive_cost,
        receive_qty,
        receive_amt ,
        shipped_cost,
        shipped_qty,
        shipped_amt,
        net_receive_amt,
       net_receive_qty,
       net_receive_cost,
       no_tax_receive_amt,
       no_tax_shipped_amt,
       no_tax_net_receive_cost,
       no_tax_net_receive_amt,
        0 as  qty,
        0 as  sale,
        0 as   price,
        0 as   profit,
        0 as   profit_rate,
        0 as   no_tax_sale,
        0 as    no_tax_price,
        0 as   no_tax_profit,
        0 as   no_tax_profit_rate
            
    from  csx_tmp.tmp_entry_dc_aa a
    join 
    (select shop_id,shop_name,sales_province_code,sales_province_name,company_code,company_name 
        from csx_dw.dws_basic_w_a_csx_shop_m 
        where sdt='current' 
        and purpose in ('01','02','03','08','07') ) b on a.dc_code=b.shop_id
       union all 
         select mon,
            a.dc_code,
             goods_code,
             goods_name,
             spu_goods_name,
       0 as receive_cost,
     0 as  receive_qty,
     0 as  receive_amt ,
     0 as  shipped_cost,
     0 as  shipped_qty,
     0 as  shipped_amt,
     0 as  net_receive_amt,
     0 as net_receive_qty,
     0 as net_receive_cost,
     0 as no_tax_receive_amt,
     0 as no_tax_shipped_amt,
     0 as no_tax_net_receive_cost,
     0 no_tax_net_receive_amt,
               qty,
             sale,
              price,
              profit,
              profit_rate,
              no_tax_sale,
               no_tax_price,
              no_tax_profit,
              no_tax_profit_rate
    from csx_tmp.temp_spu_sale_01 a
    
    ) a 
    join 
    (select shop_id,shop_name,sales_province_code,sales_province_name,company_code,company_name 
        from csx_dw.dws_basic_w_a_csx_shop_m 
        where sdt='current' 
       -- and purpose in ('01','02','03','08','07') 
        ) b on a.dc_code=b.shop_id
    group by mon,
        dc_code,shop_name,sales_province_code,sales_province_name,company_code,company_name,
        goods_code,
        goods_name,
        spu_goods_name;



        select    mon,
       sum(receive_amt)/sum(receive_qty) receive_cost,
       sum(receive_qty) as receive_qty,
       sum(receive_amt)/10000  as receive_amt ,
       sum(shipped_amt)/ sum(shipped_qty) as shipped_cost,
       sum(shipped_qty) as shipped_qty,
       sum(shipped_amt)/10000 as shipped_amt,
       sum(receive_amt-shipped_amt)/10000 as net_receive_amt,
       sum(receive_qty-shipped_qty) as net_receive_qty,
       sum(receive_amt-shipped_amt)/sum(receive_qty-shipped_qty) as net_receive_cost,
        sum(no_tax_receive_amt)/10000 as no_tax_receive_amt,
        sum(no_tax_shipped_amt)/10000 as no_tax_shipped_amt,
       sum(no_tax_receive_amt-no_tax_shipped_amt)/sum(receive_qty-shipped_qty) as no_tax_net_receive_cost,
        sum(no_tax_receive_amt-no_tax_shipped_amt)/10000 as no_tax_net_receive_amt
from 

( select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,
       a.receive_location_code as dc_code,
       a.goods_code,
       a.supplier_code,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN receive_qty*-1 ELSE receive_qty END) as receive_qty,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN (a.receive_qty*price)*-1 ELSE (a.receive_qty*price) END )  as receive_amt ,
       sum(CASE WHEN a.business_type IN ('ZNR1','ZNR2') THEN a.receive_qty*(price/(1+a.tax_rate))*-1 ELSE a.receive_qty*(price/(1+a.tax_rate)) END )  as no_tax_receive_amt ,
       0 shipped_qty,
       0 shipped_amt,
       0 no_tax_shipped_amt
from  csx_dw.dws_wms_r_d_entry_detail a 
where  ((sdt>='20200101' and sdt<'20210701')
       OR sdt='19990101')
    AND (receive_status IN (1,2)
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (business_type in ('ZN01','ZN02','ZNR1','ZNR2')
       OR ( order_type_code LIKE 'P%' and  business_type !='02')))
group by CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  ,
       a.receive_location_code ,
       a.goods_code,
       a.supplier_code       
       
union all 
select   CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  mon,
       a.shipped_location_code as dc_code,
       a.goods_code,
       a.supplier_code,
       0 receive_qty,
       0 receive_amt,
       0 no_tax_receive_amt,
       sum(shipped_qty) as shipped_qty,
       sum(a.shipped_qty *price)  as shipped_amt ,
        sum(a.shipped_qty*(price/(1+a.tax_rate)))  as no_tax_shipped_amt 
from   csx_dw.dws_wms_r_d_ship_detail a 
where  ((sdt>='20200101' and sdt<'20210701')
       OR sdt='19990101')
    AND (a.status !=9 
    and  supplier_code not in
('20015439','20019761','20021783','20024437','20026794','75000002',
'75000016',
'75000022',
'75000031',
'75000047',
'75000052',
'75000079',
'75000082',
'75000086',
'75000087',
'75000089',
'75000097',
'75000104',
'75000105',
'75000124',
'75000143',
'75000157',
'75000174',
'75000182',
'75000192',
'75000199',
'75000203',
'75000207',
'75000217',
'75000223',
'75000226',
'75000247',
'75000251',
'G2115',
'G2116',
'G2126',
'G2127',
'G3506')
and  (a.business_type_code in ('ZNR1','ZNR2')
       OR ( order_type_code LIKE 'P%' and  a.business_type_code ='05')))
GROUP BY CASE
           WHEN sdt='19990101' THEN substr(regexp_replace(to_date(create_time),'-',''),1,6)
           ELSE substr(sdt,1,6)
       END  ,
       a.shipped_location_code ,
       a.goods_code,
       a.supplier_code
)a

join
(select location_code,shop_name,zone_id,zone_name,purpose_code,dist_code,dist_name from csx_dw.csx_shop 
where sdt='current' 
     and table_type=1 
    -- and dist_name like '安徽%'
     and location_code not in('W0K4')
    -- and purchase_org !='P620' 
     and purpose_code in ('01','02','03','08','07') ) b on a.dc_code=b.location_code

join 
(select  goods_id,spu_goods_code,spu_goods_status,classify_middle_code,classify_middle_name,goods_name,spu_goods_name
    from csx_dw.dws_basic_w_a_csx_product_M
    where sdt='current'
    and classify_middle_code in ('B0304')
    -- 关联SPU 联采商品
   --  AND spu_goods_code is not null 
) d on   a.goods_code=d.goods_id
group by   mon
;