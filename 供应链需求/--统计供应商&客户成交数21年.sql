--统计供应商&客户成交数21年

SELECT count(DISTINCT supplier_code)
FROM csx_dw.dws_wms_r_d_entry_detail
WHERE sdt>='20210101'
  AND sdt<='20211231'

  AND receive_status IN (1,
                         2)
  and business_type='01'
;

select count(distinct customer_no) from csx_dw.dws_sale_r_d_detail
WHERE sdt>='20210101'
  AND sdt<='20211231'
  and channel_code in ('1','7','9')
  and business_type_code !='4'