select
    order_code,
    super_class ,
    receive_location_code ,
    receive_location_name ,
    settle_location_code ,
    settle_location_name ,
    shipped_location_code ,
    shipped_location_name,
    supplier_code ,
    supplier_name ,
    goods_code ,
    bar_code ,
    goods_name ,
    spec ,
    pack_qty ,
    unit ,
    category_code ,
    category_name ,
    purchase_group_code ,
    purchase_group_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    tax_code,
    tax_rate ,
    order_price ,
    order_qty ,
    order_amt ,
    receive_qty ,
    receive_amt ,
   receive_amt/(1+tax_rate/100) as no_tax_receive_amt,
    -- shipped_date ,
    shipped_qty ,
    shipped_amt ,
    shipped_amt/(1+tax_rate/100) as no_tax_shipped_amt,
    order_status ,
    source_type ,
    source_type_name ,
    local_purchase_flag ,
    receive_date ,
    shipped_date ,
   -- order_status ,
    to_date(order_create_time) order_create_date,
    to_date(order_update_time)order_update_date
from
    csx_dw.ads_supply_order_flow
where
    sdt = '20200617'
    and purchase_org_code ='P611';
    
-- 课组销售
select from_unixtime(unix_timestamp(calday,'yyyyMMdd'),'MM-dd')as sdt,division_code ,division_name,department_code,department_name ,sale ,profit,profit /sale as  profit_rate
from 
(select calday from csx_dw.dws_w_a_date_m where calday>='20200601'and calday <='20200630')a
left join 
(select sdt,division_code ,division_name,department_code,department_name ,sum(sales_value )/10000 sale ,sum(profit )/10000 profit,sum(profit )/sum(sales_value ) profit_rate
from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200601' and sdt<'20200701' and province_code ='2' and channel in ('1','7')
group by sdt,division_code ,division_name ,department_code ,department_name) b on a.calday=b.sdt 
order by calday desc ,division_code asc,division_name,department_code;

select * FROM csx_dw.dws_sale_r_d_customer_sale where sdt>='20200601' and province_name like '江苏%' and  channel_name like '商超%' and category_large_code ='1105';

select * from csx_dw.csx_shop where sdt='current' and location_code ='W055'and dist_name like '江苏%' and county_city_name like '昆山%';
 
select DISTINCT source_type,source_type_name from csx_dw.ads_supply_order_flow where sdt>='20200601';


select purchase_org_code,
purchase_org_name,
    order_code,
    link_order_code,
   case when super_class='1' then '供应商订单'when super_class='1' then '供应商退货订单' when super_class='1' then '配送订单' when super_class='1' then '返配订单' end super_class ,
    receive_location_code ,
    receive_location_name ,
    settle_location_code ,
    settle_location_name ,
    shipped_location_code ,
    shipped_location_name,
    supplier_code ,
    supplier_name ,
    goods_code ,
    bar_code ,
    goods_name ,
    spec ,
    pack_qty ,
    unit ,
    category_code ,
    category_name ,
    purchase_group_code ,
    purchase_group_name ,
    category_large_code ,
    category_large_name ,
    category_middle_code ,
    category_middle_name ,
    category_small_code ,
    category_small_name ,
    tax_code,
    tax_rate ,
    order_price ,
    order_qty ,
    order_amt ,
    receive_qty ,
    receive_amt ,
   receive_amt/(1+tax_rate/100) as no_tax_receive_amt,
    -- shipped_date ,
    shipped_qty ,
    shipped_amt ,
    shipped_amt/(1+tax_rate/100) as no_tax_shipped_amt,
   case when  order_status=1 then '已创建' when order_status=2 then '已发货' 
  when  order_status=3 then '部分入库' when order_status=4 then '已完成'
   when order_status=5 then '已取消'  end order_status,
    source_type ,
   concat(cast(source_type as string) ,' ', source_type_name)as source_type_name ,
    local_purchase_flag ,
    receive_date ,
    shipped_date ,
   -- order_status ,
    to_date(order_create_time) order_create_date,
    to_date(order_update_time)order_update_date
from
    csx_dw.ads_supply_order_flow
where
    sdt >='${ordersdate}'
    and sdt <='${orderedate}';

select category_large_code ,category_large_name ,category_middle_code ,category_middle_name ,category_small_code ,category_small_name ,
COUNT(DISTINCT  goods_code ) as sale_sku, 
sum(sales_value)sale,
sum(profit )profit
from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200601' and sdt<='20200618' and is_self_sale =1 and channel ='2'
and (category_large_code ='1241' or category_middle_code ='110406')
group by category_large_code ,category_large_name ,category_middle_code ,category_middle_name ,category_small_code ,category_small_name
union all 
select category_large_code ,category_large_name ,category_middle_code ,category_middle_name ,category_small_code ,category_small_name ,
COUNT(DISTINCT  goods_code ) as ring_sale_sku, 
sum(sales_value)ring_sale,
sum(profit )ring_profit
from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200501' and sdt<='20200518' and is_self_sale =1 and channel ='2'
and (category_large_code ='1241' or category_middle_code ='110406')
group by category_large_code ,category_large_name ,category_middle_code ,category_middle_name ,category_small_code ,category_small_name
;
select * from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current' and (category_large_name like '家禽%'or category_large_name  like '调味%');

SELECT distinct super_class,receive_location_code ,receive_location_name,shipped_location_code ,shipped_location_name,supplier_code ,supplier_name,
case when super_class in ('1','2') then settle_location_code when super_class ='3'then receive_location_code
when super_class ='4' then shipped_location_code end location_code,
case when super_class in ('1','2') then settle_location_name when super_class ='3'then receive_location_name
when super_class ='4' then shipped_location_name end location_name FROM csx_dw.ads_supply_order_flow where order_code ='RPW0J6200615000037';


select x.* from 
(select case when b.channel is null then '其他' else b.channel end sflag,
hkont,a.account_name,
comp_code,comp_name ,b.sales_province dist,b.sales_city, kunnr   as  kunnr,
b.customer_name name,b.first_category,b.second_category,b.third_category,
b.work_no,b.sales_name,b.first_supervisor_name,b.credit_limit,b.temp_credit_limit,
 zterm,diff,ac_all,
 case when ac_all<0 then ac_all else ac_wdq end ac_wdq,
 case when ac_all<0 then 0 else ac_15d end ac_15d,
 case when ac_all<0 then 0 else ac_30d end ac_30d,
 case when ac_all<0 then 0 else ac_60d end ac_60d,
 case when ac_all<0 then 0 else ac_90d end ac_90d,
 case when ac_all<0 then 0 else ac_120d end ac_120d,
 case when ac_all<0 then 0 else ac_180d end ac_180d,
 case when ac_all<0 then 0 else ac_365d end ac_365d,
 case when ac_all<0 then 0 else ac_2y end ac_2y,
 case when ac_all<0 then 0 else ac_3y end ac_3y,
 case when ac_all<0 then 0 else ac_over3y end ac_over3y 
from 
(select * from csx_dw.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0 and kunnr<>'0000910001'
and hkont not in ('1398030000','1398040000','1399020000','2202010000'))a 
 left join (select * from csx_dw.dws_crm_w_a_customer_m where sdt='20200621' and customer_no!='') b 
 on lpad(a.kunnr,10,'0')=lpad(b.customer_no,10,'0')
 union all 
 select a.sflag,
hkont,a.account_name,comp_code,comp_name,
case when substr(comp_name,1,2)in('上海','北京','重庆') then concat(substr(comp_name,1,2),'市')
    when substr(comp_name,1,2)='永辉' then substr(comp_name,1,2)
    else concat(substr(comp_name,1,2),'省') end dist,
substr(comp_name,1,2) sales_city,kunnr,name,
'个人及其他'first_category,'个人及其他'second_category,'个人及其他'third_category,
''work_no,''sales_name,''first_supervisor_name,0 as credit_limit,0 as temp_credit_limit,
 zterm,diff,ac_all,
 case when ac_all<0 then ac_all else ac_wdq end ac_wdq,
 case when ac_all<0 then 0 else ac_15d end ac_15d,
 case when ac_all<0 then 0 else ac_30d end ac_30d,
 case when ac_all<0 then 0 else ac_60d end ac_60d,
 case when ac_all<0 then 0 else ac_90d end ac_90d,
 case when ac_all<0 then 0 else ac_120d end ac_120d,
 case when ac_all<0 then 0 else ac_180d end ac_180d,
 case when ac_all<0 then 0 else ac_365d end ac_365d,
 case when ac_all<0 then 0 else ac_2y end ac_2y,
 case when ac_all<0 then 0 else ac_3y end ac_3y,
 case when ac_all<0 then 0 else ac_over3y end ac_over3y 
 from csx_dw.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0 and kunnr='0000910001')x
 where 1=1 and kunnr ='0000910001'
 -- (case when dist in ('平台-生鲜采购','平台-食百采购') then '平台-供应链' else dist end ) like '河北%'
order by sflag,comp_code,dist,kunnr;


select * from csx_dw.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0 and kunnr='0000910001'
and hkont not in ('1398030000','1398040000','1399020000','2202010000');

select COUNT(*) from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current';
SELECT * from   csx_dw.ads_sale_r_m_dept_sale_mon_report where sdt='20200622' and date_m ='本月';

select * from csx_dw.dws_sale_r_d_customer_sale  where sdt>'20200601' and department_code ='';


select
    date_m,
    province_code,
    province_name,
    division_code,
    division_name,
    department_code,
    department_name,
    channel_name,
    sale_sku,
    sale / 10000 sale ,
    sale / sum(sale)over(partition by channel_name)* 2.00 sale_ratio,
    last_sale / 10000 last_sale,
    sale_rate,
    profit / 10000 profit,
    profitrate,
    front_profit,
    front_profitrate,
    sale_cust,
    last_sale_cust,
    diff_cust,
    write_time,
    sdt
from
    csx_dw.ads_sale_r_m_dept_sale_mon_report
where
    sdt = '20200623'
   ${if(div =="00"," and department_code = '' "," and department_code = '00' ")} 
   ${if(div =="00","and division_code='00' "," and division_code = '"+div+"'")}
    and date_m = '本月' 
   ${if(len(channels)== 0,"and channel_name = '全渠道' "," and channel_name in ('"+channels+"') ")}
order by province_code;

select DISTINCT department_code ,category_large_code ,category_large_name from csx_dw.dws_sale_r_d_customer_sale  where sdt>'20200601' and department_code ='';
select category_small_code,purchase_group_code as department_code,purchase_group_name as department_name from csx_dw.dws_basic_w_a_category_m where sdt='current' and category_large_code in 
('1390',
'1388',
'1387',
'1389')
