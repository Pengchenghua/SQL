-- 财务集采、基地、现金采购对标永辉
set purpose = ('01','02','03','05','07','08');
set edate = regexp_replace('${enddate}','-','');
set sdate = regexp_replace(trunc('${enddate}','MM'),'-','');
set month = substr(regexp_replace('${enddate}','-',''),1,6);
set group_shop = ('W0A3','W0Q9','W0P8','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0R9','W0A5','W0N0','W0AT','W0T7','W0AS','W0A8','W0F4','W0L3','W0K1','WB11',
                'W0G9','WA96','W0AU','W0K6','W0F7','W0BK','W0A2','W0BR','W0BH','W048','W0Q8','W039','W0X1','W0Z8','W079','W0S9','W0R8','W088','W0P3',
                'W0AR','W053','W080','W0BT','WB04','W0AZ','WB00','W0BZ','WB01','WB03','WA93');
                
 
drop table csx_tmp.temp_dc_new ;
create TEMPORARY TABLE csx_tmp.temp_dc_new as 
select case when region_code!='10' then '大区'else '平台' end dept_name,
    region_code,
    region_name,
    sales_province_code,
    sales_province_name,
    purchase_org,
    purchase_org_name,
    case when performance_province_name like'平台%' then '00' else   sales_region_code end sales_region_code,
    case when performance_province_name like'平台%' then '平台' else  sales_region_name end sales_region_name,
    shop_id ,
    shop_name ,
    company_code ,
    company_name ,
    purpose,
    purpose_name,
    performance_city_code,
    performance_city_name,
    performance_province_code,
    performance_province_name,
    a.province_code,
    a.province_name
from csx_dw.dws_basic_w_a_csx_shop_m a 
left join 
(select a.code as province_code,a.name as province_name,b.code region_code,b.name region_name 
from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql a 
 left join 
(select code,name,parent_code from csx_tmp.dws_basic_w_a_performance_region_province_city_tomysql where level=1)  b on a.parent_code=b.code
 where level=2) b on a.performance_province_code=b.province_code
 where sdt='current'    
  --    and table_type=1 
    ;  

drop table csx_tmp.temp_group_goods;
CREATE temporary table csx_tmp.temp_group_goods as 
SELECT d.dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code sales_province_code,
       d.performance_province_name sales_province_name,
       a.province_code,
       a.province_name,
    --   d.performance_city_code city_code,
    --   d.performance_city_name city_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       b.short_name,
       goods_code,
       goods_name,
       a.classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when  b.classify_small_code IS NOT NULL and short_name is not NULL then '1' end group_purchase_tag,
       coalesce(sum(case when joint_purchase_flag=1 and b.classify_small_code IS NOT NULL then receive_amt  end ),0) as group_purchase_receive_amount,
       coalesce(sum(case when joint_purchase_flag=1 and b.classify_small_code IS NOT NULL then a.receive_qty  end ),0) as group_purchase_receive_qty,
       coalesce(sum(case when joint_purchase_flag=1 and b.classify_small_code IS NOT NULL then shipped_amt  end ),0) as group_purchase_shipped_amount,
       coalesce(sum(case when joint_purchase_flag=1 and b.classify_small_code IS NOT NULL then a.shipped_qty  end ),0) as group_purchase_shipped_qty,
       coalesce(sum(receive_amt  ),0) as receive_amt,
       coalesce(sum(a.receive_qty),0) as receive_qty,
       coalesce(sum(shipped_amt  ),0) as shipped_amt,
       coalesce(sum(shipped_qty),0) as   shipped_qty,
       months
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
 join 
  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE months <= '202206'
    and months>='202201'
   and source_type_name not in ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
   -- AND d.purpose IN ('01','03')
   and a.dc_code in ${hiveconf:group_shop}
  and a.classify_middle_code !='B0202'
  GROUP BY d.sales_region_code,
      d.sales_region_name,
    --   performance_city_code,
    --   performance_city_name,
      performance_province_code,
       performance_province_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end ,
       b.short_name,
       d.dept_name,
       d.region_code,
       d.region_name,
       months,
         a.province_code,
       a.province_name,
       a.classify_large_code,
       classify_large_name,
        case when  b.classify_small_code IS NOT NULL and short_name is not NULL then '1' end ,
        goods_code,
       goods_name
;


    -- 云超入库

drop table csx_tmp.temp_yc_entry;
create temporary table csx_tmp.temp_yc_entry as 
select substr(a.sdt,1,6) months,
    b.province_name,
    goodsid,
    sum(pur_qty_in) qty,
    sum(pur_val_in ) as amt,
    sum(pur_val_in )/sum(pur_qty_in) as yc_cost
from b2b.ord_orderflow_t a 
join 
(select * from  csx_dw.ads_sale_r_d_purprice_globaleye_shop where shop_channel='yc' and sdt='current') b on a.shop_id_in =b.shop_id
join 
(select distinct goods_code from  csx_tmp.temp_group_goods  where group_purchase_tag=1) c on a.goodsid=c.goods_code

where a.sdt>='20220101' and a.sdt<='20220630' 
    and  delivery_finish_flag='X'
    and  ordertype not in ('返配','退货') and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
group by substr(a.sdt,1,6) ,
    province_name,
    goodsid

;
       
select a.*,b.qty,amt,yc_cost from  csx_tmp.temp_group_goods a 
left join
csx_tmp.temp_yc_entry b on a.province_name=b.province_name and a.goods_code=b.goodsid and a.months=b.months
where group_purchase_tag=1;



-- 基地采购

-- 基地采购
drop table csx_tmp.temp_jd_goods;
CREATE temporary table csx_tmp.temp_jd_goods as 
SELECT d.dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code sales_province_code,
       d.performance_province_name sales_province_name,
       a.province_code,
       a.province_name,
    --   d.performance_city_code city_code,
    --   d.performance_city_name city_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       b.short_name,
       goods_code,
       goods_name,
       a.classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       coalesce(sum(case when  order_business_type=1 then receive_amt  end ),0) as group_purchase_receive_amount,
       coalesce(sum(case when  order_business_type=1 then a.receive_qty  end ),0) as group_purchase_receive_qty,
       coalesce(sum(case when  order_business_type=1 then shipped_amt  end ),0) as group_purchase_shipped_amount,
       coalesce(sum(case when  order_business_type=1 then a.shipped_qty  end ),0) as group_purchase_shipped_qty,
       coalesce(sum(receive_amt  ),0) as receive_amt,
       coalesce(sum(a.receive_qty),0) as receive_qty,
       coalesce(sum(shipped_amt  ),0) as shipped_amt,
       coalesce(sum(shipped_qty),0) as   shipped_qty,
       months
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
 join 
  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE months <= '202206'
    and months>='202201'
   and source_type_name not in ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
   -- AND d.purpose IN ('01','03')
   and a.dc_code in ${hiveconf:group_shop}
  and a.classify_large_code ='B02'
  GROUP BY d.sales_region_code,
      d.sales_region_name,
      performance_province_code,
       performance_province_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end ,
       b.short_name,
       d.dept_name,
       d.region_code,
       d.region_name,
       months,
         a.province_code,
       a.province_name,
       a.classify_large_code,
       classify_large_name,
        goods_code,
       goods_name
;
   -- 云超入库基地商品

drop table csx_tmp.temp_yc_jd_entry;
create temporary table csx_tmp.temp_yc_jd_entry as 
select substr(a.sdt,1,6) months,
    b.province_name,
    goodsid,
    sum(pur_qty_in) qty,
    sum(pur_val_in ) as amt,
    sum(pur_val_in )/sum(pur_qty_in) as yc_cost
from b2b.ord_orderflow_t a 
join 
(select * from  csx_dw.ads_sale_r_d_purprice_globaleye_shop where shop_channel='yc' and sdt='current') b on a.shop_id_in =b.shop_id
join 
(select distinct goods_code from  csx_tmp.temp_jd_goods where  group_purchase_receive_amount>0 or group_purchase_receive_amount<0  ) c on a.goodsid=c.goods_code

where a.sdt>='20220101' and a.sdt<='20220630' 
    and  delivery_finish_flag='X'
    and  ordertype not in ('返配','退货') and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
group by substr(a.sdt,1,6) ,
    province_name,
    goodsid

;


select dept_name,
       region_code,
       region_name,
       sales_province_code,
       sales_province_name,
       province_code,
       a.province_name,
       bd_id,
       bd_name,
       short_name,
       goods_code,
       goods_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_receive_amount,
       group_purchase_receive_qty,
       group_purchase_shipped_amount,
       group_purchase_shipped_qty, 
       receive_amt,
       receive_qty,
       shipped_amt,
       shipped_qty,
       a.months,
       b.qty,
       amt,
       yc_cost 
from  csx_tmp.temp_jd_goods a 
left join
csx_tmp.temp_yc_jd_entry b on a.province_name=b.province_name and a.goods_code=b.goodsid and a.months=b.months
where  group_purchase_receive_amount+ group_purchase_shipped_amount >0 or group_purchase_receive_amount+ group_purchase_shipped_amount<0


;

-- 现金采购明细对标永辉


drop table csx_tmp.temp_cash_goods ;
CREATE temporary table csx_tmp.temp_cash_goods as 
SELECT d.dept_name,
       d.region_code,
       d.region_name,
       d.performance_province_code sales_province_code,
       d.performance_province_name sales_province_name,
       a.province_code,
       a.province_name,
       case when a.division_code in ('10','11') then '11' else '12' end bd_id,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end bd_name,
       b.short_name,
       goods_code,
       goods_name,
       a.classify_large_code,
       classify_large_name,
       a.classify_middle_code,
       a.classify_middle_name,
       if(supplier_classify_code=2,'1','0') as tage,
       coalesce(sum(case when supplier_classify_code=2 then receive_amt  end ),0) as   group_purchase_receive_amount,
       coalesce(sum(case when supplier_classify_code=2 then a.receive_qty  end ),0) as group_purchase_receive_qty,
       coalesce(sum(case when supplier_classify_code=2 then shipped_amt  end ),0) as   group_purchase_shipped_amount,
       coalesce(sum(case when supplier_classify_code=2 then a.shipped_qty  end ),0) as group_purchase_shipped_qty,
       coalesce(sum(receive_amt  ),0) as receive_amt,
       coalesce(sum(a.receive_qty),0) as receive_qty,
       coalesce(sum(shipped_amt  ),0) as shipped_amt,
       coalesce(sum(shipped_qty),0) as   shipped_qty,
       months
FROM csx_tmp.report_fr_r_m_financial_purchase_detail a 
left join  csx_tmp.source_scm_w_a_group_purchase_classily b on a.classify_small_code=b.classify_small_code
 join 
  csx_tmp.temp_dc_new d on a.dc_code=d.shop_id 
WHERE months <= '202206'
    and months>='202201'
   and source_type_name not in ('城市服务商','联营直送','项目合伙人')
   and super_class_name in ('供应商订单','供应商退货订单')
   and a.dc_code in ${hiveconf:group_shop}
 -- and a.classify_large_code ='B02'
 --  and supplier_classify_code=2
  GROUP BY d.sales_region_code,
      d.sales_region_name,
      performance_province_code,
       performance_province_name,
       a.classify_middle_code,
       a.classify_middle_name,
       case when a.division_code in ('10','11') then '11' else '12' end ,
       case when a.division_code in ('10','11') then '生鲜' else '食百' end ,
       b.short_name,
       d.dept_name,
       d.region_code,
       d.region_name,
       months,
       a.province_code,
       a.province_name,
       a.classify_large_code,
       classify_large_name,
       goods_code,
       goods_name,
       if(supplier_classify_code=2,'1','0')
;

   -- 云超入库现金采买商品

drop table csx_tmp.temp_yc_cash_entry;
create temporary table csx_tmp.temp_yc_cash_entry as 
select substr(a.sdt,1,6) months,
    b.province_name,
    goodsid,
    sum(pur_qty_in) qty,
    sum(pur_val_in ) as amt,
    sum(pur_val_in )/sum(pur_qty_in) as yc_cost
from b2b.ord_orderflow_t a 
join 
(select * from  csx_dw.ads_sale_r_d_purprice_globaleye_shop where shop_channel='yc' and sdt='current') b on a.shop_id_in =b.shop_id
join 
(select distinct goods_code from  csx_tmp.temp_cash_goods  where tage='1' ) c on a.goodsid=c.goods_code

where a.sdt>='20220101' and a.sdt<='20220630' 
    and  delivery_finish_flag='X'
    and  ordertype not in ('返配','退货') and regexp_replace(vendor_id,'(0|^)([^0].*)',2) not like '75%'
group by substr(a.sdt,1,6) ,
    province_name,
    goodsid
;


select dept_name,
       region_code,
       region_name,
       sales_province_code,
       sales_province_name,
       province_code,
       a.province_name,
       bd_id,
       bd_name,
       short_name,
       goods_code,
       goods_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       group_purchase_receive_amount,
       group_purchase_receive_qty,
       group_purchase_shipped_amount,
       group_purchase_shipped_qty, 
       receive_amt,
       receive_qty,
       shipped_amt,
       shipped_qty,
       a.months,
       b.qty,
       amt,
       yc_cost 
from    
(select dept_name,
       region_code,
       region_name,
       sales_province_code,
       sales_province_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       short_name,
       goods_code,
       goods_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       coalesce(sum(group_purchase_receive_amount),0) as group_purchase_receive_amount,
       coalesce(sum(group_purchase_receive_qty),0) as group_purchase_receive_qty,
       coalesce(sum(group_purchase_shipped_amount),0) as group_purchase_shipped_amount,
       coalesce(sum(group_purchase_shipped_qty),0) as group_purchase_shipped_qty,
       coalesce(sum(receive_amt),0) as receive_amt,
       coalesce(sum(receive_qty),0) as receive_qty,
       coalesce(sum(shipped_amt),0) as shipped_amt,
       coalesce(sum(shipped_qty),0) as shipped_qty,
       months
from  csx_tmp.temp_cash_goods a 
 where 1=1
 group by  dept_name,
       region_code,
       region_name,
       sales_province_code,
       sales_province_name,
       province_code,
       province_name,
       bd_id,
       bd_name,
       short_name,
       goods_code,
       goods_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       months
 )a
left join
csx_tmp.temp_yc_cash_entry b on a.province_name=b.province_name and a.goods_code=b.goodsid and a.months=b.months
where group_purchase_receive_amount+ group_purchase_shipped_amount >0 or group_purchase_receive_amount+ group_purchase_shipped_amount<0
;
