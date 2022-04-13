 -- 供应商对帐修改统计
 drop table csx_tmp.temp_aa;
 create temporary table csx_tmp.temp_aa as 
select a.supplier_code,vendor_name,a.purchase_org,a.purchase_org_name,b.pay_condition as pay_condition_21,c.pay_condition as pay_condition_22,is_reconcile_value
from 
(select supplier_code,supplier_name,purchase_org,purchase_org_name
from csx_tmp.report_fr_r_m_financial_purchase_detail
where sdt>='20210101'
    and sdt<='20211231'
group by supplier_code,supplier_name,purchase_org,purchase_org_name
)  a 
left join 
( select distinct supplier_code,purchase_org_code,pay_condition 
    from csx_dw.dws_basic_w_a_supplier_purchase_info where sdt='20211231'  ) b on a.supplier_code=b.supplier_code and a.purchase_org=b.purchase_org_code
left join 
( select distinct supplier_code,purchase_org_code,pay_condition 
    from csx_dw.dws_basic_w_a_supplier_purchase_info where sdt='20220331'  ) c on a.supplier_code=c.supplier_code and a.purchase_org=c.purchase_org_code
left join
(select vendor_id,is_reconcile_value,vendor_name from  csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current') d on a.supplier_code=d.vendor_id
  
    
;


select * from  csx_tmp.temp_aa;