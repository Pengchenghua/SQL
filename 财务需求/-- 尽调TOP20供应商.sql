-- 尽调TOP20供应商
select q,suppler_code,supplier_name,no_amt,dense_rank()over(partition by q order by no_amt desc) aa
from 
(select concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd')/3),2,0))) as q,
    supplier_code,
    supplier_name,
    sum(amount_no_tax) no_amt
from  csx_dw.dws_wms_r_d_entry_detail 
where sdt>='20210101' and sdt<='20220630'
and receive_status=2
and order_type_code like 'P%'
AND (business_type!='2' or supplier_name not like '%永辉%')
group by supplier_code,
    supplier_name,
    concat(substr(sdt,1,4),lpad(ceil(month(sdt)/3),2,0)) 
)a

;

select * from   csx_tmp.temp_supplier_fis_01 ;

drop table  csx_tmp.supplier_rankt;
create table csx_tmp.supplier_rankt as 
select concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0)) as q,
    supplier_code,
    supplier_name,
    sum(amount_no_tax) no_amt
from  csx_tmp.re 
where sdt>='20210101' and sdt<='20220630'
and receive_status=2
and order_type_code like 'P%'
AND (business_type!='02' and supplier_name not like '%永辉%')
group by supplier_code,
    supplier_name,
    concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0) )

;

select * from 
(select *, dense_rank()over(partition by q,supplier_type order by no_amt desc)  aa from csx_tmp.supplier_rankt a 
left join 
(select vendor_id,vendor_name,supplier_type 
    from csx_dw.dws_basic_w_a_csx_supplier_m 
    where sdt='current') b on a.supplier_code=b.vendor_id
) a 
where aa<21;

drop table  csx_tmp.supplier_rank;
create table csx_tmp.supplier_rank as 
select concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0)) as q,
    supplier_code,
    supplier_name,
    sum( no_tax_receive_amt ) no_amt,
    sum( no_tax_shipped_amt ) no_shipped_amt
from csx_tmp.report_fr_r_m_financial_purchase_detail 
where sdt>='20210101' and sdt<='20220630'
-- and status=2
and  source_type_code  not in ('8')
AND ( business_type_name not in ('云超配送退货','云超配送') and supplier_name not like '%永辉%')
group by supplier_code,
    supplier_name,
    concat(substr(sdt,1,4),lpad(ceil(month(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'),'yyyy-MM-dd'))/3),2,0) )

;
 
 select * from 
(select *, dense_rank()over(partition by q,supplier_type order by no_amt desc)  aa 
from csx_tmp.supplier_rank a 
left join 
(select vendor_id,vendor_name,supplier_type 
    from csx_dw.dws_basic_w_a_csx_supplier_m 
    where sdt='current') b on a.supplier_code=b.vendor_id
) a 
where aa<21;



select a.yesr as `年份`,
        a.mon as `月份`,
		a.purchase_order_code as `采购订单号`,
        a.supplier_code as `供应商号`,
        a.supplier_name as `供应商名称`,
        classify_large_code as `管理大类编码`,
        classify_large_name as `管理大类名称`,
        classify_middle_code as `管理中类编码`,
        classify_middle_name as `管理中类名称`,
        classify_small_code as `管理小类编码`,
        classify_small_name as `管理小类名称`,
        goods_code as `商品编码`,
        a.goods_name as `商品名称`,
        a.sales_region_name as `大区`,
        a.sales_province_name as `省区`,
        a.qty as `入库数量`,
        a.receive_amt as `入库额(含税)`,
        a.no_tax_receive_amt as `入库额(未税)`,
        a.shipped_qty as `出库数量`,
        a.shipped_amt as `出库额(含税)`,
        a.no_tax_shipped_amt as `出库额(未税)`
    from csx_tmp.report_fina_po_order_detail