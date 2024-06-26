set hive.execution.engine=spark;
set sdate='20210101';
set edate='20210228';
set l_sdt= regexp_replace(add_months(from_unixtime(unix_timestamp(${hiveconf:edate},'yyyyMMdd'),'yyyy-MM-dd'),-12),'-','');

-- set tez.queue.name=caishixian;
drop table csx_tmp.temp_supplier_list;
create temporary table csx_tmp.temp_supplier_list
as 
select company_code,supplier_code from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select location_code,company_code,company_name from csx_dw.csx_shop where sdt='current') b on a.settlement_dc=b.location_code
where sdt>=${hiveconf:sdate} and sdt<=${hiveconf:edate}
and a.order_type_code like 'P%'
group by supplier_code,company_code
;
-- select count(*) from  csx_tmp.temp_supplier_list_01;
-- 付款条件
drop table csx_tmp.temp_supplier_list_01;
create temporary table csx_tmp.temp_supplier_list_01
as 
select  supplier_code,company_code ,pay_condition,dic_value
from 
(select  supplier_code,company_code ,pay_condition
    from csx_ods.source_basic_w_a_md_purchasing_info a 
 join 
(select location_code,company_code,purchase_org from csx_dw.csx_shop where sdt='current' and table_type=1) b on a.purchase_org= b.purchase_org 
where sdt=${hiveconf:edate} 
group by supplier_code,pay_condition,company_code
) a 
left join 
(select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='ACCOUNTCYCLE') c on trim(a.pay_condition)=c.dic_key
;

-- show create table csx_ods.source_basic_w_a_md_purchasing_info;

--供应商信息表码表
drop table csx_tmp.temp_supplier_list_02;
create temporary table csx_tmp.temp_supplier_list_02
as 
select a.tax_code,
    a.supplier_code,
    supplier_name,
    purchase_level,
    c.dic_value as purchase_name,
    industry_code,
    d.dic_value as industry,
    tax_type,
    b.dic_value as tax_type_name,
    reconciliation_tag ,
    f.dic_value as reconciliation,
    regexp_replace(to_date(create_time),'-','')create_date
from csx_ods.source_basic_w_a_md_supplier_info a 
left join 
-- 税务类型
    (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='TAXLEVEL')b on a.tax_type=b.dic_key
left join 
-- 采购级别
    (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='YPCGRDM') c on a.purchase_level=c.dic_key
left join
-- 行业
(select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='VENDERVARIETY') d on a.industry_code=d.dic_key
left join
-- 对帐日期
(select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='CONCILIATIONNFLAG') f on a.reconciliation_tag=f.dic_key
 where sdt=${hiveconf:edate}
;


-- 公司编码	供应商编码	供应商名称	纳税级别	付款条件	供应商采购级别	行业	对账日	创建时间
drop table  csx_tmp.temp_supplier_list_00;
create table csx_tmp.temp_supplier_list_00
as 
select 
    b.company_code,
    tax_code,
   regexp_replace(a.supplier_code,'^0*','')supplier_code,
    supplier_name,
    tax_type, -- 纳税级别
    d.tax_type_name,
    purchase_level,
    d.purchase_name,
    industry_code,
    d.industry,
    reconciliation_tag,
    d.reconciliation,
    pay_condition,
    dic_value,
    d.create_date
from 
csx_tmp.temp_supplier_list a
left join 
(select account,company_code from csx_ods.source_basic_w_a_md_supplier_company where sdt='19990101') b
 on regexp_replace(a.supplier_code,'^0*','')=regexp_replace(account,'^0*','') and a.company_code=b.company_code
left join csx_tmp.temp_supplier_list_01 c on regexp_replace(a.supplier_code,'^0*','')=regexp_replace(c.supplier_code,'^0*','') and a.company_code=c.company_code
left join 
csx_tmp.temp_supplier_list_02 d on regexp_replace(a.supplier_code,'^0*','')=regexp_replace(d.supplier_code,'^0*','')
;

-- 查询永辉入库供应商
drop table if exists csx_tmp.temp_supplier_yh;
create temporary table csx_tmp.temp_supplier_yh as 
select  regexp_replace(a.vendor_id,'^0*','')vendor_id,vat_regist_num from b2b.ord_orderflow_t a 
join 
(select location_code from csx_dw.csx_shop where sdt='current' and table_type=2 ) b on a.shop_id_in=b.location_code
left join 
(select vendor_id,vat_regist_num from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current' ) c on regexp_replace(a.vendor_id,'^0*','')=c.vendor_id
where sdt>=${hiveconf:l_sdt} and sdt<=${hiveconf:edate}
and a.pur_org not like 'P6%'
group by  regexp_replace(a.vendor_id,'^0*',''),vat_regist_num;


insert overwrite directory '/tmp/pengchenghua/data/ccc'row format delimited fields terminated by '\t'
select a.*,if(b.vat_regist_num is null ,'否','是') yh_note from csx_tmp.temp_supplier_list_00 a 
left join 
csx_tmp.temp_supplier_yh b on a.tax_code=b.vat_regist_num
group by 
a.company_code,
a.tax_code,
a.supplier_code,
a.supplier_name,
a.tax_type,
a.tax_type_name,
a.purchase_level,
a.purchase_name,
a.industry,
a.industry_code,
a.reconciliation,
a.reconciliation_tag,
a.pay_condition,
a.dic_value,
a.create_date,
if(b.vat_regist_num is null ,'否','是') 
;





---- 根据彩食鲜供应商信息查询
--------------------------------------------------
set edate='20210328';
set l_sdt= regexp_replace(add_months(from_unixtime(unix_timestamp(${hiveconf:edate},'yyyyMMdd'),'yyyy-MM-dd'),-12),'-','');

--供应商信息基础表
drop table if exists csx_tmp.temp_supplier_csx;
create temporary table csx_tmp.temp_supplier_csx as 
select tax_code as vat_regist_num,
    a.supplier_code,
    a.supplier_name,
    a.tax_type, 
    c.dic_value as tax_type_name,
    purchase_level , 
    g.dic_value as vendor_pur_lvl_name, 
    account_group,
    j.dic_value as acct_grp_name, 
    industry_code  , 
    d.dic_value as industry_name,
    reconciliation_tag ,
    f.dic_value as reconciliation_name,
    to_date(a.create_time) as create_date    
from csx_ods.source_basic_w_a_md_supplier_info a  
join 
(select account from csx_ods.source_basic_w_a_md_supplier_company 
where sdt='19990101' 
and company_code in ('2115','2116','2126','2127','2128','2129','2130','2207','2210','2211','2216','2304','2408','2814','2815','3505','3506','3750','3751','3752','3753','2132','2133','2131')
group by account) b on a.supplier_code=b.account
left join 
-- 税务类型
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='TAXLEVEL')c on a.tax_type=c.dic_key
left join 
-- 采购级别
    (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='YPCGRDM') g on a.purchase_level=g.dic_key
left join
-- 行业
(select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='VENDERVARIETY') d on a.industry_code=d.dic_key
left join
-- 对帐标志
 (select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='CONCILIATIONNFLAG') f on a.reconciliation_tag=f.dic_key
-- 帐期 --供应商帐户组 dic_type='VENDERAGROUP'
left join
(select dic_type,dic_key,dic_value from csx_ods.source_basic_w_a_md_dic where sdt=${hiveconf:edate} and dic_type='ZTERM') j on a.account_group=j.dic_key
where sdt=${hiveconf:edate};


-- 查询永辉入库供应商
drop table if exists csx_tmp.temp_supplier_yh;
create temporary table csx_tmp.temp_supplier_yh as 
select  regexp_replace(a.vendor_id,'^0*','')vendor_id,vat_regist_num from b2b.ord_orderflow_t a 
join 
(select location_code from csx_dw.csx_shop where sdt='current' and table_type=2 ) b on a.shop_id_in=b.location_code
left join 
(select vendor_id,vat_regist_num from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current' ) c on regexp_replace(a.vendor_id,'^0*','')=c.vendor_id
where sdt>=${hiveconf:l_sdt} and sdt<=${hiveconf:edate}
and a.pur_org not like 'P6%'
group by  regexp_replace(a.vendor_id,'^0*',''),vat_regist_num;



insert overwrite directory '/tmp/pengchenghua/data/ccc'row format delimited fields terminated by '\t'
select a.vat_regist_num,
    a.supplier_code,
    a.supplier_name,
    a.tax_type, 
    tax_type_name ,
    purchase_level , 
    vendor_pur_lvl_name, 
    account_group,
    acct_grp_name, 
    industry_code , 
    industry_name,
    a.create_date,
    if(b.vat_regist_num is null ,'否','是') yh_note 
from csx_tmp.temp_supplier_csx a 
left join 
csx_tmp.temp_supplier_yh b on a.vat_regist_num=b.vat_regist_num
group by 
    a.vat_regist_num,
    a.supplier_code,
    a.supplier_name,
    a.tax_type, 
    a.tax_type_name ,
    purchase_level , 
    vendor_pur_lvl_name, 
    account_group,
    acct_grp_name, 
    industry_code , 
    industry_name,
    a.create_date,
if(b.vat_regist_num is null ,'否','是') 
;

select * from csx_tmp.temp_supplier_csx a ;