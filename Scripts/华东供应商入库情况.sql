     
        
select sdt,
	to_date(posting_time) as posting_date,
	move_type,
	move_name,
    credential_no ,
    company_code ,
    company_name ,
    a.location_code ,
    location_name ,
    supplier_code ,
    supplier_name ,
    valuation_category_name ,
    purchase_group_code ,
    purchase_group_name ,
    product_code ,
    product_name ,
    unit ,
    price ,
    if(direction='+',qty,qty*-1) qty ,
    if(direction ='+', amt,amt*-1) amt,
    if(direction='+',amt_no_tax ,amt_no_tax*-1) amt_no_tax,
    if(direction='+',tax_amt ,tax_amt *-1) tax_amt,
    tax_rate ,
    vat_regist_num
from   csx_dw.dwd_cas_r_d_accounting_credential_item  a 
join 
(select location_code from csx_dw.csx_shop where sdt='current' and zone_id='1') c on a.location_code =c.location_code
join 
(select vat_regist_num,vendor_id from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current')b on a.supplier_code =b.vendor_id
where sdt>='20200901' and sdt<='20201230'
and posting_time>='2020-09-01 00:00:00' and posting_time<'2020-12-01 00:00:00'
--and purchase_org_code ='P616'
and move_type in('103A','103B','101A','101B')
--and move_type in(  '101A' )
;



refresh csx_dw.dwd_cas_r_d_accounting_credential_item ;
select * from csx_ods.source_wms_r_d_accounting_transfer_config    where sdt='20201003';
select * from csx_dw.dwd_cas_r_d_accounting_credential_item where credential_no ='PZ20200925035143';

SELECT customer_no ,customer_name 
from csx_dw.dws_sale_r_d_customer_sale  
where sdt>='20190101' and sdt<='20200630'
and channel!='2'
group by 
customer_no ,customer_name 
;

SELECT supplier_code ,
supplier_name ,
sum(receive_qty)qty,
sum(amount)amt
from csx_dw.wms_entry_order  
where sdt>='20201101' and sdt <='20201130'
and receive_location_code ='W0A7'
-- and business_type like '%供应商%'
--and supplier_code ='20019017'
and entry_type ='采购入库'
group by 
supplier_code ,supplier_name 
;

SELECT
	order_code ,wms_order_no,a.qty,b.qty
from
(
SELECT
	order_code ,
	sum(receive_qty)qty
from
	csx_dw.wms_entry_order
where sdt>='20201101' and sdt <='20201230'
and to_date(receive_time )>='2020-11-01' and to_date(receive_time)<='2020-11-30'
and receive_location_code ='W0A7'
-- and business_type like '%供应商%'
-- and supplier_code ='20045719'
-- and entry_type ='采购入库'
and receive_status =2
group by order_code
) a 
FULL  OUTER join 
(select   wms_order_no ,
			sum(qty)qty
from   csx_dw.dwd_cas_r_d_accounting_credential_item  a 
where sdt>='20201101' and sdt<='20201130'
--and posting_time>='2020-11-01 00:00:00' and posting_time<'2020-12-01 00:00:00'
and location_code ='W0A7'
-- and supplier_code ='20045719'
and move_type in(  '101A' )
group by wms_order_no
) b on a.order_code=b.wms_order_no
where  a.order_code is null 
;
select * from csx_dw.dwd_cas_r_d_accounting_credential_detail where sdt>='20201101' and wms_order_no ='IN201031000678';
 csx_dw.dws_sale_r_d_sale_item_simple ;

select * from  csx_dw.dwd_cas_r_d_accounting_credential_header where sdt>='20201101' and wms_order_no ='IN201125000460';


--供应商编码	纳税号	供应商名称	供应商名称简称	彩食鲜标签	惠商超等级标签	公司代码	区域	大区	账期	 采购金额（万元) 	 法人 	 联系电话 
select 
	dist_code,
	dist_name,
	zone_id,
	zone_name ,
    company_code ,
    company_name ,
    a.location_code ,
    location_name ,
    a.supplier_code ,
    short_call,
	b.supplier_name,
    sum(if(direction='+',qty,qty*-1)) qty ,
    sum(if(direction ='+', amt,amt*-1)) amt,
    sum(if(direction='+',amt_no_tax ,amt_no_tax*-1)) amt_no_tax,
    sum(if(direction='+',tax_amt ,tax_amt *-1)) tax_amt,
	phone,
	account_group,
	reconciliation_tag,
	represent_name,
    b.tax_code
from csx_dw.dwd_cas_r_d_accounting_credential_item  a 
join 
(select location_code,dist_code,dist_name,zone_id,zone_name from csx_dw.csx_shop where sdt='current' ) c on a.location_code =c.location_code
join 
(select tax_code,supplier_code,short_call,supplier_name,phone,account_group,reconciliation_tag,represent_name
from csx_ods.source_basic_w_a_md_supplier_info where sdt='20201104') b on a.supplier_code =b.supplier_code
where sdt>='20200101' and sdt<='20201105'
and posting_time>='2020-01-01 00:00:00' and posting_time<'2020-11-01 00:00:00'
and move_type in('103A','103B','101A','101B')
group by 
    dist_code,
	dist_name,
	zone_id,
	zone_name ,
    company_code ,
    company_name ,
    a.location_code ,
    location_name ,
    a.supplier_code ,
    short_call,
	b.supplier_name,
    short_call,
	supplier_name,
	phone,
	account_group,
	reconciliation_tag,
	represent_name,
    b.tax_code
	
;

-- SAP数据
select 
	dist_code,
	dist_name,
	zone_id,
	zone_name ,
    company_code ,
    company_name ,
    a.location_code ,
    location_name ,
    a.supplier_code ,
    short_call,
	b.supplier_name,
    sum(if(direction='+',qty,qty*-1)) qty ,
    sum(if(direction ='+', amt,amt*-1)) amt,
    sum(if(direction='+',amt_no_tax ,amt_no_tax*-1)) amt_no_tax,
    sum(if(direction='+',tax_amt ,tax_amt *-1)) tax_amt,
	phone,
	account_group,
	reconciliation_tag,
	represent_name,
    b.tax_code
from csx_dw.dwd_cas_r_d_accounting_credential_item  a 
join 
(select location_code,dist_code,dist_name,zone_id,zone_name from csx_dw.csx_shop where sdt='current' ) c on a.location_code =c.location_code
join 
(select tax_code,supplier_code,short_call,supplier_name,phone,account_group,reconciliation_tag,represent_name
from csx_ods.source_basic_w_a_md_supplier_info where sdt='20201104') b on a.supplier_code =b.supplier_code
where sdt>='20200101' and sdt<='20201105'
and posting_time>='2020-01-01 00:00:00' and posting_time<'2020-11-01 00:00:00'
and move_type in('103A','103B','101A','101B')
group by 
    dist_code,
	dist_name,
	zone_id,
	zone_name ,
    company_code ,
    company_name ,
    a.location_code ,
    location_name ,
    a.supplier_code ,
    short_call,
	b.supplier_name,
    short_call,
	supplier_name,
	phone,
	account_group,
	reconciliation_tag,
	represent_name,
    b.tax_code
	
;


--SAP 入库+出库 

 select
 	business_type,
 	dist_code,
	dist_name,
	zone_id,
	zone_name ,
    company_code ,
    company_name ,
    a.dc_code ,
    shop_name ,
    a.supplier_code ,
    short_call,
	b.supplier_name,
	SUM(qty)qty,
	sum(amount)amount ,
	sum(no_tax_amount)no_tax_amount,
	phone,
	account_group,
	reconciliation_tag,
	represent_name,
    b.tax_code
    from 
(select
 	business_type ,
	receive_location_code as dc_code ,
	supplier_code,
	SUM(receive_qty)qty,
	sum(amount)amount ,
	sum(amount/(1+tax_rate/100)) as no_tax_amount
from
	csx_dw.wms_entry_order a 
where
	sdt >= '20200101'
	and sdt <= '20200601'
	and sys = 'old'
	-- and business_type not like '采购%'
	group by business_type,
	receive_location_code,
	supplier_code
union all 
select
 	business_type ,
	shipped_location_code as dc_code,
	supplier_code,
	SUM(shipped_qty)*-1 qty,
	sum(amount)*-1 amount ,
	sum(amount/(1+tax_rate/100))*-1 as no_tax_amount
from
	csx_dw.wms_shipped_order 
where
	sdt >= '20200101'
	and sdt <= '20200901'
	and sys = 'old'
--	and business_type like '采购%'
	group by business_type,
	shipped_location_code,
	supplier_code
	) a 
join 
(select location_code,dist_code,dist_name,zone_id,zone_name,company_code ,company_name,shop_name  from csx_dw.csx_shop where sdt='current' ) c on a.dc_code =c.location_code
join 
(select tax_code,supplier_code,short_call,supplier_name,phone,account_group,reconciliation_tag,represent_name
from csx_ods.source_basic_w_a_md_supplier_info where sdt='20201104') b on a.supplier_code =b.supplier_code	
group by 
    dist_code,
	dist_name,
	zone_id,
	zone_name ,
    company_code ,
    company_name ,
    a.dc_code ,
    shop_name ,
    a.supplier_code ,
    short_call,
	b.supplier_name,
    short_call,
	supplier_name,
	phone,
	account_group,
	reconciliation_tag,
	represent_name,
    b.tax_code,
    business_type
	
;



SELECT
	business_type,
	sum(amount)
from
	csx_dw.wms_shipped_order 
where
	shipped_location_code = 'W0A3'
	and supplier_code = '20034005'
	and sdt >= '20200101'
	and sdt <= '20201031'
group by business_type
;

select * from csx_dw.dws_basic_w_a_csx_product_m where sdt='current' and goods_id ='33' and shop_code='W0A3';


 select
        * 
 from
        csx_dw.ads_wms_r_d_fineReport_city_purprice_globaleye_detail 
 where
        sdt='${SDATE}'  
 order by
        dept_id,
        goodsid,
        province_name;
SELECT
	business_type,
	sum(amount)
from
	csx_dw.wms_entry_order
where
	receive_location_code = 'W0A3'
	and supplier_code = '20034005'
	and sdt >= '20200101'
	and sdt <= '20201031'
group by business_type
;

