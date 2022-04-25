-- 供应商入库帐期信息

SELECT mon,
       sales_region_code,
       sales_region_name,
       a.purchase_org,
       province_code,
       province_name,
       city_code,
       city_name,
       source_type,
       source_type_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name,
       a.supplier_code,
       vendor_name,
       pay_condition,
       dic_value ,
       supplier_type,
       supplier_type_name,
       qty,
       amt,
       shipp_qty,
       shipp_amt,
       net_qty,
       net_amt
    from 
(SELECT mon,
       sales_region_code,
       sales_region_name,
       purchase_org,
       province_code,
       province_name,
       city_code,
       city_name,
       source_type,
       source_type_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name,
       supplier_code,
       vendor_name,
       supplier_type,
       supplier_type_name,
       sum(qty) qty,
       sum(amt) amt,
       sum(shipp_qty) shipp_qty,
       sum(shipp_amt) shipp_amt,
       sum(net_qty) net_qty,
       sum(net_amt) net_amt
FROM csx_tmp.temp_order_entry
WHERE MON BETWEEN '202101' AND '202109'
    and purpose !='04'
  GROUP BY mon,
       sales_region_code,
       sales_region_name,
       purchase_org,
       province_code,
       province_name,
       city_code,
       city_name,
       source_type,
       source_type_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name,
       supplier_code,
       vendor_name,
       supplier_type,
       supplier_type_name
      )a 
      left join 
(select a.supplier_code,a.purchase_org,a.pay_condition,dic_value from csx_ods.source_basic_w_a_md_purchasing_info a
 LEFT JOIN 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20211019' and dic_type='ACCOUNTCYCLE')b on a.pay_condition=b.dic_key
 where sdt='20211019') d on a.supplier_code=d.supplier_code  and a.purchase_org=d.purchase_org
;

-- 供应商入库帐期信息

SELECT 
       sales_region_code,
       sales_region_name,
       a.purchase_org,
       province_code,
       province_name,
       city_code,
       city_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name,
       a.supplier_code,
       vendor_name,
       purchase_frozen,
       frozen,
       industry_sector,
       create_date,
       pay_condition,
       dic_value ,
       supplier_type,
       supplier_type_name,
       acct_grp, 
       acct_grp_name,
       qty,
       amt,
       shipp_qty,
       shipp_amt,
       net_qty,
       net_amt
    from 
(SELECT  
       sales_region_code,
       sales_region_name,
       purchase_org,
       province_code,
       province_name,
       city_code,
       city_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name,
       supplier_code,
       vendor_name,
       supplier_type,
       supplier_type_name,
       sum(qty) qty,
       sum(amt) amt,
       sum(shipp_qty) shipp_qty,
       sum(shipp_amt) shipp_amt,
       sum(net_qty) net_qty,
       sum(net_amt) net_amt
FROM csx_tmp.temp_order_entry
WHERE MON BETWEEN '202101' AND '202109'
    and purpose !='04'
  GROUP BY  
       sales_region_code,
       sales_region_name,
       purchase_org,
       province_code,
       province_name,
       city_code,
       city_name,
       classify_large_code,
       classify_large_name,
       classify_middle_code,
       classify_middle_name,
       classify_small_code,
       classify_small_name,
       department_id,
       department_name,
       supplier_code,
       vendor_name,
       supplier_type,
       supplier_type_name
      )a 
      left join 
(select a.supplier_code,a.purchase_org,a.pay_condition,dic_value from csx_ods.source_basic_w_a_md_purchasing_info a
 LEFT JOIN 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20211019' and dic_type='ACCOUNTCYCLE')b on a.pay_condition=b.dic_key
 where sdt='20211019') d on a.supplier_code=d.supplier_code  and a.purchase_org=d.purchase_org
 LEFT JOIN 
 (select vendor_id,purchase_frozen,frozen,industry_sector,create_date,a.acct_grp,dic_value as acct_grp_name from   csx_dw.dws_basic_w_a_csx_supplier_m a 
 LEFT JOIN 
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt='20211019' and dic_type='VENDERAGROUP') b on a.acct_grp=b.dic_key
 where sdt='current')  c on a.supplier_code=c.vendor_id
;

show create table  csx_dw.dws_basic_w_a_csx_supplier_m ;
