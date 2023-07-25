

        SELECT
          CASE
            WHEN province_name LIKE '平台-B%' THEN '平台'
            WHEN channel IN ('1', '3') THEN '大'
            when channel in ('2') then '商超'
            ELSE a.channel_name
          END AS STYPE,
          CASE
            WHEN channel IN ('4', '5', '6') THEN '-'
            ELSE province_name
          END prov_name,
          substr(sdt, 1, 6) sdt,
          -- province_manager_name MANAGE,
          sum(a.sales_value) / 10000 * 1.00 sale,
          sum(a.profit) / 10000 * 1.00 profit,
          0 h_sale
        FROM csx_dw.dws_sale_r_d_customer_sale a
        WHERE
          sdt <= regexp_replace(to_date('${edate}'), '-', '')
          AND sdt >= regexp_replace(to_date(trunc('${edate}', 'YY')), '-', '')
        GROUP BY
          CASE
            WHEN province_name LIKE '平台-B%' THEN '平台'
            WHEN channel IN ('1', '3') THEN '大'
            when channel in ('2') then '商超'
            ELSE a.channel_name
          END,
          CASE
            WHEN channel IN ('4', '5', '6') THEN '-'
            ELSE province_name
          END,
          sdt ;
          
      select * from csx_dw.dws_sale_r_d_customer_sale where dc_code='9961' and sdt>='20200301';
      select * from csx_dw.wms_entry_order  where (origin_order_code =''or origin_order_code is null) ;
   select a.*,b.*,c.* from   
  (select  DISTINCT order_code from csx_dw.dws_scm_r_d_scm_order_trace_header_v1 where  sdt>='20190901') a  left join 
  (select DISTINCT order_code,e.origin_order_code from csx_dw.wms_entry_order as e where sys='new')b on a.order_code=b.origin_order_code
  left join 
   (select DISTINCT order_no,e.origin_order_no from csx_dw.wms_shipped_order as e where sys='new')c on a.order_code=c.origin_order_no
--  where a.order_code is null
  ;
      select * from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='20200419';
  select  * from csx_dw.wms_entry_order where  goods_code ='973432' and receive_location_code ='W080';
 refresh csx_dw.ads_sale_r_m_dept_sale_mon_report ;
      
  refresh csx_dw.supply_turnover_dc ;
   refresh csx_dw.supply_turnover_province ;
   
   select * from b2b.csx_customer  where cust_id ='0000102915';
   


--计算逾期率
select
    channel,
    customer_no,
    customer_name,
    zterm,
    payment_days,
    payment_terms,
    comp_code,
    comp_name ,
    sum(case when over_days>0 then ac_all end ) as over_amt,
    SUM(case when over_days>0 then ac_all*over_days end ) as over_amt_1,
    SUM(ac_all)* payment_days as diff_ac_all,
    round(case when SUM(ac_all)* payment_days <= 0 then 0  else coalesce(SUM(case when over_days>0 then ac_all*over_days end ), 0)/ coalesce(SUM(ac_all)* payment_days, 0) end, 4) rate, 
sum(ac_all) as ac_all 
from csx_dw.ads_fis_r_a_customer_days_overdue_dtl a 
where channel = '大' and sdt = '20200425' 
group by channel, customer_no, customer_name, zterm, payment_days, payment_terms, comp_code, comp_name;

   -- 逾期明细
select *  from csx_dw.ads_fis_r_a_customer_days_overdue_dtl  where sdt='20200425';
select * from ods_ecc.ecc_ytbcustomer  where sdt='20200428';

   select channel,subject_code,subject_name,comp_code,comp_name,customer_no,customer_name,zterm,payment_days,payment_terms,sdate,edate,over_days,sum(ac_all)amt
   from csx_dw.ads_fis_r_a_customer_days_overdue_dtl  where channel='大' and sdt='20200423'
   group by channel,subject_code,subject_name,comp_code,comp_name,customer_no,customer_name,zterm,payment_days,payment_terms,sdate,edate,over_days
  ;
  
  select * from csx_dw.ads_fis_r_a_customer_days_overdue_dtl  where sdt='20200428' and  customer_no='107182';
  
  select *
    from
        csx_dw.dws_wms_r_d_accounting_stock_operation_item_m
    where
        sdt >= '20200301'
        and sdt <= '20200331'
        and goods_code ='560'
        and dc_code ='W048';
   refresh csx_dw.ads_fis_r_a_customer_days_overdue_dtl ;
   select * from csx_dw.ads_fis_r_a_customer_days_overdue_dtl where sdt='20200429';
   
   select * from dim.dim_shop  where shop_id like 'G%';
        select * from csx_dw.account_age_dtl_fct_new  where kunnr ='0000107182' and sdt='20200422';
      --  SELECT * FROM  csx_dw.ads_sale_r_m_dept_sale_mon_report where sdt='20200425';
    
    
  select province_code,province_name,location_code,shop_name,full_shop from 
(select case when location_code in('W0H4') then location_code else  province_code end province_code,
case when location_code in('W0H4') then shop_name else  province_name end  province_name, location_code,shop_name,concat(location_code,'_',shop_name) as full_shop FROM csx_dw.csx_shop where sdt='current' and table_type=1 )a where 1=1
AND province_code in ('W0H4');


select x.* from 
(select case when b.channel is null then '其他' else b.channel end sflag,
hkont,a.account_name,
comp_code,comp_name,b.sales_province dist,b.sales_city,kunnr,

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
(select * from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0 and kunnr<>'0000910001'
and hkont not in ('1398030000','1398040000','1399020000','2202010000'))a 
 left join data_center_report.customer_m  b 
 on lpad(a.kunnr,10,'0')=lpad(b.customer_no,10,'0')
 union all 
 select a.sflag,
hkont,a.account_name,comp_code,comp_name,
case when substr(comp_name,1,2)in('上海','北京','重庆') then concat(substr(comp_name,1,2),'市')
    when substr(comp_name,1,2)='永辉' then substr(comp_name,1,2)
    else concat(substr(comp_name,1,2),'省') end dist,
substr(comp_name,1,2) sales_city,kunnr,name,
'个人及其他'first_category,'个人及其他'second_category,'个人及其他'third_category,
''work_no,''sales_name,''first_supervisor_name,''credit_limit,''temp_credit_limit,
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
 from data_center_report.account_age_dtl_fct_new a where sdt='${SDATE}' and a.ac_all<>0 and kunnr='0000910001')x
order by sflag,comp_code,dist,kunnr;


select
    a.customer_no,
    customer_name,
    payment_days,
    payment_terms,
    budat,
    receivable,
    prctr,
    abs(return_amt)return_amt,
    company_code,
     crm_company_code
    from (
    select
        customer_no,
        customer_name,
        payment_days,
        payment_terms,
        company_code as crm_company_code
    from
        csx_dw.dws_crm_w_a_customer_m
    where
        sdt = regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP(),
        1)),
        '-',
        '')
        and customer_no ='102998'
        ) as a
 join (
    SELECT
        regexp_replace(kunnr,'^0*','') as customer_no,
        budat,bukrs as company_code,prctr,
        sum(case when dmbtr>'0' then cast (dmbtr as decimal(26, 6))end ) as receivable,
        sum(case when dmbtr<'0' then cast (dmbtr as decimal(26, 6)) end ) as return_amt
    from
        ods_ecc.ecc_ytbcustomer a 
    where
        sdt = regexp_replace(to_date(CURRENT_TIMESTAMP()),'-','')
    AND kunnr='0000102998'
      and budat >='20200101'
    group by
        kunnr,
        budat,
        bukrs,prctr) as b on a.customer_no=b.customer_no

        order by budat
     ;
select * from csx_dw.account_age_dtl_fct_new where sdt>='20200425' and kunnr ='0000102998';
select * from csx_dw.ads_fis_r_a_customer_days_overdue_dtl where sdt>='20200425' and customer_no ='102998';

select *,ac_all-ac_wdq  from csx_dw.account_age_dtl_fct_new where sdt='20200512' and kunnr ='0000100326'and hkont like '1122%';
select * from
        ods_ecc.ecc_ytbcustomer a where sdt='20200517'  and kunnr ='G2121';
    '0000100326';
    
 select * from  ods_ecc.ecc_ytbcustomer where sdt='20200520' and kunnr like '%107164%';
 
    select
  *
from  b2b.csx_hepecc_bsid
where kunnr ='G2121';


select
    distinct belnr
from
    ods_ecc.ecc_ytbcustomer
where
    sdt = '20200515'
    and belnr in ( '0100004543',
                              '0100066952',
                              '0100154283',
                              '0100245041',
                              '0100339686',
                              '0100343558',
                              '0100343559',
                              '0100343582',
                              '0100384003',
                              '0100384014',
                              '0100384015',
                              '0100384016',
                              '0100387789',
                              '0100599788',
                              '0100698806',
                              '0100698807',
                              '0100698808',
                              '0100698810',
                              '0100698811',
                              '0100698814',
                              '0100698815',
                              '0100698826',
                              '0100698828',
                              '0100698829',
                              '0100794407',
                              '0100794408',
                              '0101042210',
                              '0090446436',
                              '0090446437',
                              '0090446438',
                              '0090446444',
                              '0090526357')
;
select * from csx_dw.dws_sale_r_d_customer_sale  where sdt>='20200401' and department_code is null ;
select * from csx_dw.goods_m  where sdt='20200430' and goods_id ='1241673';

INVALIDATE METADATA  csx_dw.peng_temp_account_data_02;
select a.*,sum(a.ac_all)over(PARTITION by a.comp_code,a.kunnr,a.hkont) as ac_amt,b.ac_all,a.ac_all-b.ac_all as diff from 
(select * from   csx_dw.peng_temp_account_data_02  --where kunnr='G2115'
)a 
left join 
(select comp_code ,kunnr , ac_all,hkont,ac_wdq from csx_dw.account_age_dtl_fct_new where sdt='20200517' ) b 
on a.comp_code=b.comp_code and regexp_replace(a.kunnr,'^0*','')=regexp_replace(b.kunnr,'^0*','') and a.hkont=b.hkont;

select * from csx_dw.peng_temp_account_data_01;
refresh csx_dw.peng_temp_account_data_01;
select * from  csx_dw.ads_fis_r_a_customer_days_overdue_dtl where sdt='20200506';

refresh csx_dw.wms_shipped_day_report;
select * from  csx_dw.wms_shipped_day_report where sdt>='20200507';

select * from csx_dw.wms_shipped_order where sdt>='20200508' and  shipped_location_code  not in ('W048','W0A3','W0R1','W0A2','W0Q2');

select region_code,province from csx_ods.source_crm_w_a_sys_province where region_code not in ('100000','100001') order by  region_code;

select province_code,province_name ,location_code,shop_name,concat(location_code,'_',shop_name)full_name from csx_dw.csx_shop where sdt='current';

select DISTINCT  province_code,province_name from csx_dw.csx_shop where sdt='current';


SELECT province_code,province_name,supplier_code ,supplier_name ,sku ,receive_qty ,receive_amt/10000 receive_amt,shipped_qty ,shipped_amt/10000 shipped_amt
select *
FROM csx_dw.ads_supply_kanban_supplier_entry 
where sdt='20200512' and supplier_name like '%和记黄埔（中国）商贸有限公司泉州分公司（彩食鲜）%' and date_m ='m'
order by receive_amt desc
limit 10;

select
    hkont ,
  --  kunnr ,
    sum(cast(dmbtr as decimal(26, 6) ) ),
    bukrs,
    belnr,
    lifnr
from
    ods_ecc.ecc_ytbcustomer
where
    sdt = '20200526'
   -- and budat < '20200517'
    and mandt = '800'
    -- and kunnr ='G2121'
    and ( substr(hkont,
    1,
    3)<> '139'
    or (substr(hkont,
    1,
    3) = '139'
    and budat >= '20190201' ))
    and concat_ws('-',
    hkont ,
    gjahr,
    belnr,
    bukrs)in ('1122010000-2020-0100183238-2700',
    '1122010000-2020-0100404461-2402',
    '1122010000-2020-0100467273-2400',
    '1122010000-2020-0100468834-2300',
    '1122010000-2020-0100755372-2202',
    '1122010000-2020-0100873656-2121',
    '1122010000-2020-0101263298-2200',
    '1122010000-2020-0090572072-1933')
GROUP BY
    hkont ,
  --  kunnr,
    bukrs,
    belnr,
    lifnr ;

select * from ods_ecc.ecc_ytbcustomer where sdt='20200517' and lifnr ='G1933' AND hkont like '%139902%' ;
select * from csx_dw.dws_sale_r_d_customer_sale where goods_code ='1250452';