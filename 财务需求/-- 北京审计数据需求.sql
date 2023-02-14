select basic_performance_province_name,net_value,dense_rank()over(partition by 1 order by net_value desc ) aa 
from (
select b.basic_performance_province_name ,
	a.company_code,
	a.company_name,
	sum(net_value)net_value 
from 
 csx_report.csx_report_gss_settle_bill_df  a 
 join 
 (select basic_performance_province_name,shop_code from csx_dim.csx_dim_shop where sdt='current' and basic_performance_province_name='北京')b on a.settle_place_code=b.shop_code
 where sdt>='20221001' 
   and cost_code='ZF01001'
   group by b.basic_performance_province_name ,
	a.company_code,
	a.company_name
	) a 
	;




	select b.basic_performance_province_name ,
	settle_no,
	agreement_no,
	settle_date,
	a.purchase_org_code,
	a.purchase_org_name,
	department_code,
	department_name,
	cost_code,
	cost_name,
	attribute_date,
	supplier_code,
	supplier_name,
	settle_place_code,
	settle_place_name,
	a.company_code,
	a.company_name,
	net_value,
	tax_amount,
	value_tax_total,
	bill_total_amount,
	invoice_code,
	invoice_name 
from 
 csx_report.csx_report_gss_settle_bill_df  a 
 join 
 (select basic_performance_province_name,shop_code from csx_dim.csx_dim_shop where sdt='current' and basic_performance_province_name='北京')b on a.settle_place_code=b.shop_code
 where sdt>='20221001' 
  -- and attribution_month >= '202210'
      and cost_code='ZF01001'
   ;


   select
  basic_performance_province_name,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  supplier_code,
  supplier_name,
  dense_rank()over(partition by 1 order by net_amt desc ) aa ,
  dense_rank()over(partition by division_name order by net_amt desc ) bb ,
  net_amt
from (
select
  basic_performance_province_name,
  case when division_code in ('10','11') then '11'
   else '12' end division_code,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end division_name,
  classify_large_code,
  classify_large_name,
  supplier_code,
  supplier_name,
  sum(no_tax_receive_amt-no_tax_shipped_amt) net_amt
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  join (
    select
      basic_performance_province_name,
      shop_code
    from
      csx_dim.csx_dim_shop
    where
      sdt = 'current'
      and basic_performance_province_name = '北京'
  ) b on a.dc_code = b.shop_code
  where sdt>='20221001'
  group by  basic_performance_province_name,
   case when division_code in ('10','11') then '11'
   else '12' end  ,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end,
  classify_large_code,
  supplier_code,
  supplier_name,
  classify_large_name
  ) a 
  ;


  with aa as (
 select
  basic_performance_province_name,
  case when division_code in ('10','11') then '11'
   else '12' end division_code,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end division_name,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  a.supplier_code,
  supplier_name,
  c.supplier_tax_code,
  sum(no_tax_receive_amt-no_tax_shipped_amt) net_amt
from
   csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  join (
    select
      basic_performance_province_name,
      shop_code
    from
      csx_dim.csx_dim_shop
    where
      sdt = 'current'
      and basic_performance_province_name = '北京'
  ) b on a.dc_code = b.shop_code
  join 
  (select supplier_code,supplier_tax_code from csx_dim.csx_dim_basic_supplier where sdt='current') c on a.supplier_code=c.supplier_code
  where sdt>='20221001'
  group by   basic_performance_province_name,
  case when division_code in ('10','11') then '11'
   else '12' end  ,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end  ,
  classify_large_code,
  classify_large_name,
  classify_middle_code,
  classify_middle_name,
  a.supplier_code,
  supplier_name,
  c.supplier_tax_code
  ) ,
  bb as 
  (select social_credit_code,a.customer_code,customer_name,classify_middle_code,classify_middle_name,sum(sale_amt_no_tax)sale_amt
  from  csx_dws.csx_dws_sale_detail_di a 
    join 
    (select customer_code,social_credit_code from   csx_dim.csx_dim_crm_customer_info where sdt='current') b on a.customer_code=b.customer_code
    where sdt>='20221001'
     --   and performance_province_name='北京市'
    group by social_credit_code,a.customer_code,customer_name,classify_middle_code,classify_middle_name)
    select  social_credit_code,customer_code,customer_name,bb.classify_middle_code,bb.classify_middle_name, sale_amt,
  aa.classify_middle_code a_classify_middle_code,
  aa.classify_middle_name a_classify_middle_name,
  aa.supplier_code,
  supplier_name,
  supplier_tax_code,
  net_amt from bb 
    join 
    aa on bb.social_credit_code=aa.supplier_tax_code 
    -- and aa.classify_middle_code=bb.classify_middle_code
	;



 with aa as (
 select
  basic_performance_province_name,
  case when division_code in ('10','11') then '11'
   else '12' end division_code,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end division_name,
  classify_large_code,
  classify_large_name,
  supplier_code,
  supplier_name,
  sum(no_tax_receive_amt-no_tax_shipped_amt) net_amt
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  join (
    select
      basic_performance_province_name,
      shop_code
    from
      csx_dim.csx_dim_shop
    where
      sdt = 'current'
      and basic_performance_province_name = '北京'
  ) b on a.dc_code = b.shop_code
  where sdt>='20221001'
  group by  basic_performance_province_name,
   case when division_code in ('10','11') then '11'
   else '12' end  ,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end,
  classify_large_code,
  supplier_code,
  supplier_name,
  classify_large_name
  ) ,  
   bb as (
 select basic_performance_province_name,supplier_code ,count(distinct division_code) cn 
 from (
 select
  basic_performance_province_name,
  case when division_code in ('10','11') then '11'
   else '12' end division_code,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end division_name,
  classify_large_code,
  classify_large_name,
  supplier_code,
  supplier_name,
  sum(no_tax_receive_amt-no_tax_shipped_amt) net_amt
from
  csx_analyse.csx_analyse_scm_purchase_order_flow_di a
  join (
    select
      basic_performance_province_name,
      shop_code
    from
      csx_dim.csx_dim_shop
    where
      sdt = 'current'
      and basic_performance_province_name = '北京'
  ) b on a.dc_code = b.shop_code
  where sdt>='20221001'
  group by  basic_performance_province_name,
   case when division_code in ('10','11') then '11'
   else '12' end  ,
  case when division_code in ('10','11') then '生鲜'
   else '食百' end,
  classify_large_code,
  supplier_code,
  supplier_name,
  classify_large_name
  ) a 
  group by basic_performance_province_name,supplier_code
  having cn>1
  ) 
  select
  aa.basic_performance_province_name,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  aa.supplier_code,
  supplier_name,
  dense_rank()over(partition by 1 order by net_amt desc ) aa ,
  dense_rank()over(partition by division_name order by net_amt desc ) bb ,
  net_amt
  from  aa 
  join bb on aa.supplier_code=bb.supplier_code