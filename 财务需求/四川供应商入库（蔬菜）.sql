CREATE temporary table csx_tmp.tmp_order_amt as 
SELECT order_code,source_type,
    supplier_code,
    supplier_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name ,
    c.department_id,
    c.department_name,
    target_location_code,
    a.target_location_name,
    a.goods_code,
    a.goods_name,
    sum(a.received_qty) as qty,
    sum(a.received_price1*a.received_qty) as amt,
    sdt,
    to_date(items_close_time) as items_close_date,
    to_date(received_update_time) as received_update_date,
    header_remark
FROM csx_dw.dws_scm_r_d_order_received a 
join 
(select shop_id,province_code,province_name,sales_province_code,sales_province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current' and sales_province_code='24' and table_type='1' ) b on a.target_location_code=b.shop_id
join
(select 
    a.classify_large_code,
    a.classify_large_name,
    a.classify_middle_code,
    a.classify_middle_name,
    a.classify_small_code,
    a.classify_small_name ,
    a.department_id,
    a.department_name,
    a.goods_id
    from csx_dw.dws_basic_w_a_csx_product_m a  where sdt='current') c on a.goods_code=c.goods_id
WHERE sdt>='20200101'
  AND super_class IN ('1',
                      '2')
  AND supplier_code IN ('20024574',
                        '20032180',
                        '20032247',
                        '20032873',
                        '20033027',
                        '20034005',
                        '20034256',
                        '20041849',
                        '20006377',
                        '20009825',
                        '20022074',
                        '20022481',
                        '20032205',
                        '20032287',
                        '20032559',
                        '20040329',
                        '20048520',
                        '20050418')
                        
    GROUP BY   order_code,
    source_type,
    supplier_code,
    supplier_name,
    c.classify_large_code,
    c.classify_large_name,
    c.classify_middle_code,
    c.classify_middle_name,
    c.classify_small_code,
    c.classify_small_name ,
    c.department_id,
    c.department_name,
    target_location_code,
    a.target_location_name,
    a.goods_code,
    a.goods_name,
    a.sdt,
    to_date(items_close_time) ,
    to_date(received_update_time)  ,
    header_remark                  ;
    
    
    select * from csx_tmp.tmp_order_amt ;