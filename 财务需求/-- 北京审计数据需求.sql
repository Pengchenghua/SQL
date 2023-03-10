-- 北京审计
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



--供应商结算
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
  case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end division_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
   case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end  ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
   else '食百' end,
  classify_large_code,
  supplier_code,
  supplier_name,
  classify_large_name
  ) a 
  ;

-- 客户是供应商&供应商是客户稽核
  with aa as (
  select
    basic_performance_province_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end division_code,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    c.supplier_tax_code,
    sum(no_tax_receive_amt - no_tax_shipped_amt) net_amt
  from
    csx_analyse.csx_analyse_scm_purchase_order_flow_di a
    join (
      select
        shop_code,
        company_code,
        basic_performance_province_name
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
       -- and company_code = '2304'
        and purpose not in('09', '04', '06')
        and basic_performance_province_name = '广东'
    ) b on a.dc_code = b.shop_code
    join (
      select
        supplier_code,
        supplier_tax_code
      from
        csx_dim.csx_dim_basic_supplier
      where
        sdt = 'current'
    ) c on a.supplier_code = c.supplier_code
  where
    sdt >= '20221001'
  group by
    basic_performance_province_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    c.supplier_tax_code
),
bb as (
  select
    performance_province_name,
    social_credit_code,
    a.customer_code,
    customer_name,
    classify_middle_code,
    classify_middle_name,
    sum(sale_amt_no_tax) sale_amt
  from
    csx_dws.csx_dws_sale_detail_di a
    join (
      select
        customer_code,
        social_credit_code
      from
        csx_dim.csx_dim_crm_customer_info
      where
        sdt = 'current'
    ) b on a.customer_code = b.customer_code
  where
    sdt >= '20221001' --   and performance_province_name='北京市'
    and performance_province_name='广东省'
  group by
    social_credit_code,
    a.customer_code,
    customer_name,
    classify_middle_code,
    classify_middle_name,
    performance_province_name
)
select
  performance_province_name,
  social_credit_code,
  customer_code,
  customer_name,
  bb.classify_middle_code,
  bb.classify_middle_name,
  sale_amt,
  basic_performance_province_name,
  aa.classify_middle_code a_classify_middle_code,
  aa.classify_middle_name a_classify_middle_name,
  aa.supplier_code,
  supplier_name,
  supplier_tax_code,
  net_amt
from
  bb
  join aa on bb.social_credit_code = aa.supplier_tax_code 
  -- and aa.classify_middle_code=bb.classify_middle_code
  where bb.social_credit_code !=''
;

-- 供应商信息无入库供应商
 with aa as (
  select
    basic_performance_province_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end division_code,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    c.supplier_tax_code,
    goods_code,
    goods_name,
    sum(no_tax_receive_amt - no_tax_shipped_amt) net_amt
  from
    csx_analyse.csx_analyse_scm_purchase_order_flow_di a
    join (
      select
        shop_code,
        company_code,
        basic_performance_province_name
      from
        csx_dim.csx_dim_shop
      where
        sdt = 'current'
       -- and company_code = '2304'
        and purpose not in('09', '04', '06')
        and basic_performance_province_name = '陕西'
    ) b on a.dc_code = b.shop_code
    join (
      select
        supplier_code,
        supplier_tax_code
      from
        csx_dim.csx_dim_basic_supplier
      where
        sdt = 'current'
    ) c on a.supplier_code = c.supplier_code
  where
    sdt >= '20221001'
  group by
    basic_performance_province_name,
    case
      when division_code in ('10', '11') then '11'
      else '12'
    end,
    case
      when division_code in ('10', '11') then '生鲜'
      else '食百'
    end,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    a.supplier_code,
    supplier_name,
    goods_code,
    goods_name,
    c.supplier_tax_code
),
bb as (select supplier_code,supplier_name,frozen_flag,supplier_classify_name,business_owner_name ,
    finance_frozen,
    purchase_group_code,
    purchase_group_name
    from    csx_dim.csx_dim_basic_supplier_purchase 
    where sdt='current' 
        and purchase_org_code='P621' 
        --and basic_performance_province_name='陕西'
)

  select bb.supplier_code,
    bb.supplier_name,
    bb.frozen_flag,
    bb.supplier_classify_name,
    bb.business_owner_name ,
    bb.finance_frozen,
    bb.purchase_group_code,
    bb.purchase_group_name
from
  bb
 left join aa on bb.supplier_code=aa.supplier_code
  where aa.supplier_code is null 
;


 with aa as (
 select
  basic_performance_province_name,
  case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end division_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
   case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end  ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
  case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end division_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
   case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end  ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
;

  with aa as (
 select
  basic_performance_province_name,
  case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end division_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
     -- and basic_performance_province_name = '北京'
  ) b on a.dc_code = b.shop_code
  join 
  (select supplier_code,supplier_tax_code from csx_dim.csx_dim_basic_supplier where sdt='current') c on a.supplier_code=c.supplier_code
  where sdt>='20221001'
  group by   basic_performance_province_name,
  case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end  ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
       and performance_province_name='北京市'
       and channel_code in ('1','7','9')
       and social_credit_code!=''
    group by social_credit_code,a.customer_code,customer_name,classify_middle_code,classify_middle_name)
    select  social_credit_code,customer_code,customer_name,bb.classify_middle_code,bb.classify_middle_name, sale_amt,
    basic_performance_province_name,
  aa.classify_middle_code a_classify_middle_code,
  aa.classify_middle_name a_classify_middle_name,
  aa.supplier_code,
  supplier_name,
  supplier_tax_code,
  net_amt from bb 
    join 
    aa on bb.social_credit_code=aa.supplier_tax_code 
    -- and aa.classify_middle_code=bb.classify_middle_code



--入库排名 跨品类
drop table csx_analyse_tmp.temp_entry_sj;
create table csx_analyse_tmp.temp_entry_sj as 
 select basic_performance_province_name,
 supplier_code ,
 cn , 
 net_amt,
 dense_rank()over(partition by 1 order by net_amt desc ) bb
 from (
 select basic_performance_province_name,supplier_code ,count(distinct division_code) cn ,sum(net_amt) net_amt
 from (
 select
 basic_performance_province_name,
  case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end division_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
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
      and basic_performance_province_name = '广东'
      and purpose not in ('09','06','')
  ) b on a.dc_code = b.shop_code
  where sdt>='20221001'
  group by  basic_performance_province_name,
   case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end  ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
   else '食百' end,
  classify_large_code,
  supplier_code,
  supplier_name,
  classify_large_name
  ) a 
  group by basic_performance_province_name,supplier_code
 ) a 
 where cn>1
 ;




 with aa as (
 select
  basic_performance_province_name,
  case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end division_code,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
   else '食百' end division_name,
  classify_large_code,
  classify_large_name,
  purchase_group_code,
  purchase_group_name,
  supplier_code,
  supplier_name,
  purchase_org_code,
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
      and basic_performance_province_name = '广东'
      and purpose not in('09', '04', '06')
  ) b on a.dc_code = b.shop_code
  where sdt>='20221001'
    and sdt<='20230213'
  group by  basic_performance_province_name,
   case when classify_large_code in ('B01','B02','B03') then '11'
   else '12' end  ,
  case when classify_large_code in ('B01','B02','B03') then '生鲜'
   else '食百' end,
  classify_large_code,
  supplier_code,
  supplier_name,
  classify_large_name,
  purchase_group_code,
  purchase_group_name,
  purchase_org_code
  )
  select
  aa.basic_performance_province_name,
  division_code,
  division_name,
  classify_large_code,
  classify_large_name,
  aa.supplier_code,
  supplier_name,
  business_owner_name,
  aa.net_amt
  from  aa 
  join  csx_analyse_tmp.temp_entry_sj bb on aa.supplier_code=bb.supplier_code
  join  
  (select supplier_code,purchase_group_code,
  purchase_group_name,
  business_owner_name,
  purchase_org_code
  from  csx_dim.csx_dim_basic_supplier_purchase where sdt='current') c on aa.supplier_code=c.supplier_code 
  and aa.purchase_group_code=c.purchase_group_code and aa.purchase_org_code=c.purchase_org_code
  where bb.bb<=20