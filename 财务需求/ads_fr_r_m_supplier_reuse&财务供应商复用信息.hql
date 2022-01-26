--更改门店信息表
-- set mapreduce.job.reduces =80;
-- set hive.execution.engine=tez;
-- set tez.queue.name=caishixian;
-- set hive.map.aggr         =true;
-- set hive.groupby.skewindata                 =true;
set hive.exec.parallel                      =true;
set hive.exec.dynamic.partition             =true;     --开启动态分区
set hive.exec.dynamic.partition.mode        =nonstrict;--设置为非严格模式
set hive.exec.max.dynamic.partitions        =10000;    --在所有执行MR的节点上，最大一共可以创建多少个动态分区。
set hive.exec.max.dynamic.partitions.pernode=100000;   --源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错

SET edate  = regexp_replace('${enddate}','-','');
set w_edt=regexp_replace(date_sub(current_date,1),'-',''); --维度最新日期
set l_sdt= regexp_replace(add_months(from_unixtime(unix_timestamp(${hiveconf:edate},'yyyyMMdd'),'yyyy-MM-dd'),-12),'-',''); --倒推12个月

SET edate  = regexp_replace('${enddate}','-','');
set w_edt=regexp_replace(date_sub(current_date,1),'-',''); --维度最新日期
set l_sdt= regexp_replace(add_months(from_unixtime(unix_timestamp(${hiveconf:edate},'yyyyMMdd'),'yyyy-MM-dd'),-12),'-',''); --倒推12个月

-- set tez.queue.name=caishixian;
-- 入库关联采购组织
drop table csx_tmp.temp_supplier_list;
create temporary table csx_tmp.temp_supplier_list
as 
select company_code,
    supplier_code ,
    purchase_org
from csx_dw.dws_wms_r_d_entry_detail a 
join 
(select shop_id,
    company_code,
    company_name ,
    purchase_org
from csx_dw.dws_basic_w_a_csx_shop_m 
where sdt='current') b on a.receive_location_code=b.shop_id  --收货DC关联
where sdt>=${hiveconf:l_sdt} 
    and sdt<=${hiveconf:edate}
    and a.order_type_code like 'P%'
group by supplier_code,
    company_code,
    purchase_org
;
-- select * from csx_tmp.temp_supplier_list where supplier_code='20033782';

-- select count(*) from  csx_tmp.temp_supplier_list_01;
-- 付款条件
drop table csx_tmp.temp_supplier_list_01;
create temporary table csx_tmp.temp_supplier_list_01
as 
select  supplier_code,
    purchase_org,
    company_code ,
    pay_condition,
    dic_value
from 
(select  supplier_code,
        company_code ,
        pay_condition,
        a.purchase_org
    from csx_ods.source_basic_w_a_md_purchasing_info a 
 join 
(select shop_id ,
        company_code,
        purchase_org 
from csx_dw.dws_basic_w_a_csx_shop_m 
where sdt='current') b on a.purchase_org= b.purchase_org 
where sdt=${hiveconf:w_edt} 
) a 
left join 
(select dic_type,
        dic_key,
        dic_value
 from csx_ods.source_basic_w_a_md_dic
  where sdt=${hiveconf:w_edt} 
    and dic_type='ACCOUNTCYCLE') c on trim(a.pay_condition)=c.dic_key
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
    (select dic_type,
            dic_key,
            dic_value 
    from csx_ods.source_basic_w_a_md_dic 
        where sdt=${hiveconf:w_edt} 
            and dic_type='TAXLEVEL')b on a.tax_type=b.dic_key
left join 
-- 采购级别
    (select dic_type,
            dic_key,
            dic_value 
        from csx_ods.source_basic_w_a_md_dic 
    where sdt=${hiveconf:w_edt} 
        and dic_type='YPCGRDM') c on a.purchase_level=c.dic_key
left join
-- 行业
(select dic_type,
        dic_key,
        dic_value
    from csx_ods.source_basic_w_a_md_dic 
    where sdt=${hiveconf:w_edt} 
    and dic_type='VENDERVARIETY') d on a.industry_code=d.dic_key
left join
-- 对账日分组
(select dic_type,
        dic_key,
        dic_value 
    from csx_ods.source_basic_w_a_md_dic 
    where sdt=${hiveconf:w_edt} 
        and dic_type='CONCILIATIONNFLAG') f on a.reconciliation_tag=f.dic_key
 where sdt=${hiveconf:w_edt}
;


-- 公司编码	供应商编码	供应商名称	纳税级别	付款条件	供应商采购级别	行业	对账日	创建时间
drop table  csx_tmp.temp_supplier_list_00;
create table csx_tmp.temp_supplier_list_00
as 
select 
    a.company_code,
    company_name,
    a.purchase_org,
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
(select distinct 
    company_code,
    company_name 
from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt='current') b
 on a.company_code=b.company_code
left join csx_tmp.temp_supplier_list_01 c on regexp_replace(a.supplier_code,'^0*','')=regexp_replace(c.supplier_code,'^0*','')
                                    and a.company_code=c.company_code 
                                    and a.purchase_org=c.purchase_org
left join 
csx_tmp.temp_supplier_list_02 d on regexp_replace(a.supplier_code,'^0*','')=regexp_replace(d.supplier_code,'^0*','')
;

-- 查询永辉入库供应商
drop table if exists csx_tmp.temp_supplier_yh;
create temporary table csx_tmp.temp_supplier_yh as 
select  regexp_replace(a.vendor_id,'^0*','')vendor_id,
        vat_regist_num 
    from b2b.ord_orderflow_t a 
join 
(select shop_id 
    from csx_dw.dws_basic_w_a_csx_shop_m 
    where sdt='current' 
        and table_type=2 ) b on a.shop_id_in=b.shop_id
left join 
(select vendor_id,
    vat_regist_num 
from csx_dw.dws_basic_w_a_csx_supplier_m where sdt='current' ) c on regexp_replace(a.vendor_id,'^0*','')=c.vendor_id
where sdt>=${hiveconf:l_sdt} 
    and sdt<=${hiveconf:edate}
and a.pur_org not like 'P6%'
group by  regexp_replace(a.vendor_id,'^0*',''),vat_regist_num;


-- insert overwrite directory '/tmp/pengchenghua/data/ccc'row format delimited fields terminated by '\t'
insert overwrite table csx_tmp.ads_fr_r_m_supplier_reuse partition (months)
select a.company_code,
a.company_name,
a.tax_code as vat_regist_num,
a.supplier_code,
a.supplier_name,
a.tax_type,
a.tax_type_name,
a.purchase_level,
a.purchase_name as purchase_level_name,
a.industry_code  as industry_code,
a.industry as industry_name,
a.reconciliation as account_date, 
a.reconciliation_tag as account_date_name,
a.pay_condition as payment_clause_code,
a.dic_value as panment_clause_name, 
a.create_date ,
if(b.vat_regist_num is null ,'否','是') yh_note,
purchase_org,
'',
current_timestamp(),
substr(${hiveconf:edate},1,6) 
from csx_tmp.temp_supplier_list_00 a 
left join 
csx_tmp.temp_supplier_yh b on a.tax_code=b.vat_regist_num
group by 
a.company_code,
a.company_name,
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
if(b.vat_regist_num is null ,'否','是') ,
a.purchase_org
;

CSX_TMP_ADS_FR_R_M_SUPPLIER_REUSE


drop table  csx_tmp.ads_fr_r_m_supplier_reuse;
create table csx_tmp.ads_fr_r_m_supplier_reuse
(company_code string comment '公司编码',
 company_code_name string comment '公司名称',
 vat_regist_num string comment '供应商税号',
 supplier_code string comment '供应商编码',
 supplier_num string comment '供应商名称',
 tax_type string comment '纳税级别编码',
 tax_type_name string comment '纳税级别名称',
 purchase_level string comment '供应商采购级别',
 purchase_level_name string comment '供应商采购级别名称',
 industry_code string comment '行业编码',
 industry_name string comment '行业名称',
 account_date string comment '对帐组',
 account_date_name string comment '对帐组名称',
 payment_clause_code string comment '付款条件编码',
 panment_clause_name string comment '付款条件名称',
 create_date string comment '供应商创建日期',
 yh_reuse_tag string comment '永辉复用标识',
 notes1 string comment '预留字段',
 notes2 string comment '预留字段2',
 update_time timestamp comment '更新日期'
)comment '彩食鲜复用永辉供应商报表'
partitioned by (months string comment '月分区')
stored as parquet
;