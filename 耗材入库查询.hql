
--  or (receive_time>='20190101'  and receive_time<'20210616' and order_type_code in ('ZN01','Z0N3'))
drop table csx_tmp.wms_entry_0611qu;
create table csx_tmp.wms_entry_0611qu
as
select * from
(
  select
    mon,
    sdt,
    province_code,
    province_name,
    receive_location_code,
    shop_name,
    bar_code,
    goods_code,
    goods_name,
    category_large_code,category_large_name,category_middle_code,category_middle_name,category_small_code,category_small_name,
    sum(receive_qty) receive_qty,
    sum(receive_qty*price) receive_amount,
    supplier_code,
    supplier_name,
    division_code
  from
  (
    select
        '新系统' as sys,
      case when sdt='19990101' then substr(regexp_replace(to_date(receive_time),'-',''),1,6) else sdt end  as mon,
      receive_location_code,
      goods_code,
      receive_qty,
      price,
      supplier_code,
      supplier_name,
      regexp_replace(to_date(receive_time),'-','') as sdt
    from
    csx_dw.dws_wms_r_d_entry_detail
    where (sdt>='20190101' or sdt='19990101')
    and sys='new'
    and receive_status in (1,2)
    and order_type_code like 'P%'  and to_date(receive_time)>='2019-01-01'  and to_date(receive_time)<'2021-06-16' 
    union all 
    select
        '旧系统' sys,
      case when sdt='19990101' then substr(receive_time,1,6) else sdt end  as mon,
      receive_location_code,
      goods_code,
      receive_qty,
      price,
      supplier_code,
      supplier_name,
      receive_time as sdt
    from
    csx_dw.dws_wms_r_d_entry_detail
    where (sdt>='20190101' or sdt='19990101')
    and sys='old'
    and receive_status in (1,2)
    and business_type in ('ZN01','ZN02','Z0N3','ZC01')  and receive_time>='20190101'  and receive_time<='20210616' 
  )a
  left join
  (
    select goods_id,goods_name,bar_code,division_code,category_large_code,category_large_name,category_middle_code,category_middle_name,category_small_code,category_small_name 
    from csx_dw.dws_basic_w_a_csx_product_m where sdt='current'
  )b
  on a.goods_code=b.goods_id
  left join
  (
    select shop_id,shop_name,province_code,province_name from csx_dw.dws_basic_w_a_csx_shop_m where sdt='current'
  )c 
  on a.receive_location_code = c.shop_id
  group by 
    province_code,
    province_name,
    receive_location_code,
    shop_name,
    bar_code,
    goods_code,
    goods_name,
    category_large_code,category_large_name,category_middle_code,category_middle_name,category_small_code,category_small_name,
    supplier_code,
    supplier_name,
    division_code,
    mon,
    sdt

)t
where t.division_code='15'
;

select * from  csx_tmp.wms_entry_0611qu where goods_code='1085702';
