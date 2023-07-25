-- CONNECTION: name=Hadoop - HIVE
  `qdflag` string COMMENT '渠道', 
  `dist` string COMMENT '省区', 
  `diro` int COMMENT '省区序号', 
  `manage` string COMMENT '区总', 
  `city_real` string COMMENT '城市调整', 
  `cityjob` string COMMENT '城市负责人', 
  `cust_id` string COMMENT '编码', 
  `cust_name` string COMMENT '名称', 
  `xse` decimal(26,4) COMMENT '销额', 
  `mle` decimal(26,4) COMMENT '毛利额')
COMMENT '每日销售，使用发票表数据'
PARTITIONED BY ( 
  `sdt` string COMMENT '运行日期')
  ;*/
-- CONNECTION: name=Hadoop - default
 SET hive.exec.parallel = TRUE;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
drop table b2b_tmp.temp_sale;
CREATE temporary table b2b_tmp.temp_sale
as
select sdt,case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M端' else c.sflag end qdflag,
case when c.dist is not null and c.sflag<>'M端' then substr(dist,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end dist,region_no,region_manage,b.city_name,region_city,
--manage,manage_no,暂时先不添加，直接按照表格给的福州沈锋，泉州胡永水
a.cust_id,c.cust_name,
sum(xse)xse,
sum(mle)mle
from 
(select shop_id,customer_no cust_id,origin_shop_id shopid_orig,sdt
       ,sum(sales_value ) xse
       ,sum(profit ) mle
from  csx_dw.sale_b2b_item   
where 
-- sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,4),'0101')
sdt >=regexp_replace(to_date(trunc(date_sub(current_date,1),'MM')),'-','')

and shop_id<>'W098'
and sales_type in('qyg','qyg_c','gc') 
group by shop_id,customer_no,origin_shop_id,sdt)a
left join csx_ods.b2b_customer_new c on a.cust_id=c.cust_id
left join (select shop_id,prov_name from dim.dim_shop where edate='9999-12-31')d on a.shop_id=d.shop_id
left join
(select shop_id,case when shop_id in ('W055','W056') then '上海市' else prov_name end prov_name,
    case when prov_name like '%市' then prov_name else city_name end city_name from dim.dim_shop where edate='9999-12-31' )b 
on a.cust_id=concat('S',b.shop_id)
group by sdt,case when shopid_orig='W0B6' then 'BBC' when source_id_new='22' then 'M端' else c.sflag end,
case when c.dist is not null and c.sflag<>'M端' then substr(dist,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end,region_no,region_manage,b.city_name,region_city,a.cust_id,c.cust_name;

/*

 select sdt,qdflag,  dist,cust_id,cust_name,sum(xse) from b2b_tmp.temp_sale where sdt>='20190701'AND dist like'江苏%'
 group by  sdt,qdflag, cust_id,cust_name, dist;
*/

-- SELECT sdt,sum(xse) from b2b_tmp.temp_sale where sdt>='20190101'AND cust_id ='SW0A7'group by sdt;

--  福建省的要划分到市，其余省份无该需求
drop table b2b_tmp.temp_sale01;
CREATE temporary table b2b_tmp.temp_sale01
as
select x.qdflag,x.dist,b.diro,b.manage,
case when x.dist='福建' then coalesce(c.city_real,'福州')else '-' end city_real,
case when x.dist='福建' then coalesce(c.cityjob,'沈锋')else '-' end cityjob,
cust_id,cust_name,sum(xse)xse,sum(mle)mle,sdt
from 
(select case when qdflag is null or qdflag='' then 'B端' when dist='平台' and qdflag='B端' then '平台'else qdflag end qdflag,
case when dist ='成都' then '四川' when qdflag='BBC' then '福建'else dist end dist,
case when qdflag='M端' and dist ='福建' then city_name 
when qdflag='BBC' then '福州' 
when (qdflag<>'M端' and dist ='福建')  then region_city else '-' end region_city,
cust_id,cust_name,
sum(xse) xse,
sum(mle) mle,
sdt
from b2b_tmp.temp_sale
group by case when qdflag is null or qdflag='' then 'B端' when dist='平台' and qdflag='B端' then '平台'else qdflag end,
case when dist ='成都' then '四川' when qdflag='BBC' then '福建'else dist end,
case when qdflag='M端' and dist ='福建' then city_name 
when qdflag='BBC' then '福州' 
when (qdflag<>'M端' and dist ='福建')  then region_city else '-' end,cust_id,cust_name,sdt)x 
left join 
(select '平台'dist,'陈晓'manage,1 diro
union all 
select '福建'dist,'沈锋'manage,2 diro
union all 
select '北京'dist,'解飞'manage,3 diro
union all 
select '重庆'dist,'赵致伟 'manage,4 diro
union all 
select '四川'dist,'陈建斌'manage,5 diro
union all 
select '上海'dist,'俞小平'manage,6 diro
union all 
select '江苏'dist,'胥浩'manage,7 diro
union all 
select '安徽'dist,'潘兆贤'manage,8 diro
union all 
select '浙江'dist,'潘兆贤'manage,9 diro
union all 
select '广东'dist,''manage,10 diro

)b on x.dist=b.dist
left join 
(select '泉州'city,'泉州'city_real,'胡永水'cityjob
union all 
select '莆田'city,'泉州'city_real,'胡永水'cityjob
union all 
select '厦门'city,'厦门'city_real,'崔丽'cityjob
union all 
select '漳州'city,'厦门'city_real,'崔丽'cityjob
union all 
select '龙岩'city,'厦门'city_real,'崔丽'cityjob
union all 
select '福州'city,'福州'city_real,'沈锋'cityjob
)c on substr(x.region_city,1,2)=c.city
group by x.qdflag,x.dist,b.diro,b.manage,
case when x.dist='福建' then coalesce(c.city_real,'福州')else '-' end,
case when x.dist='福建' then coalesce(c.cityjob,'沈锋')else '-' end,cust_id,cust_name,sdt;


insert overwrite table csx_dw.sale_customer_day partition (sdt)
select qdflag,dist,diro,manage,city_real,cityjob,cust_id,cust_name,(xse)xse,(mle)mel,sdt from b2b_tmp.temp_sale01 a

-- 
select * from  csx_dw.sale_customer_day where sdt>='20190801' and sdt<='20190806'
;
select * from csx_dw.sale_warzone02_detail_dtl where sdt>='20190801' and sdt<='20190806'
;
and dist like'江苏%';

--SELECT min(sdt)from csx_dw.sale_customer_day;

-- insert overwrite table csx_dw.display_warzone02_res_dtl partition (sdt)
/*
select  qdrno,diro,
qdflag,dist,manage,
city_real,
cityjob,
cust_id,xse/10000 xse,mle/10000 mle,sdt
from 
(select case when qdflag='B端' then 1 when qdflag='M端' then 2 when qdflag='BBC' then 3 end qdrno,
qdflag,dist,diro,manage,
city_real,
cityjob,
cust_id,xse,mle,sdt 
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','平台','供应链(S端)')
union all 
select 1 qdrno,'B端'qdflag,'平台'dist,1 diro,''manage,
''city_real,
''cityjob,
cust_id,xse,mle,sdt
from b2b_tmp.temp_sale01 a where qdflag in ('平台')
union all 
select 
case when qdflag='B端' then 1 when qdflag='M端' then 2 when qdflag='BBC' then 3 end qdrno,
qdflag,dist,diro,manage,
'Z-小计' city_real,
'-'cityjob,
cust_id,sum(xse)xse,sum(mle)mle,sdt
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','平台','供应链(S端)') and dist='福建'
group by case when qdflag='B端' then 1 when qdflag='M端' then 2 when qdflag='BBC' then 3 end,
qdflag,dist,diro,manage,cust_id,sdt
union all 
select 
case when qdflag in ('B端','平台') then 1 when qdflag='M端' then 2 when qdflag='BBC' then 3 end qdrno,
case when qdflag='平台' then 'B端' else qdflag end qdflag,'总计'dist,10 diro,''manage,
'' city_real,
'-'cityjob,
cust_id,sum(xse)xse,sum(mle)mle,sdt 
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','供应链(S端)') 
group by case when qdflag in ('B端','平台') then 1 when qdflag='M端' then 2 when qdflag='BBC' then 3 end,
case when qdflag='平台' then 'B端' else qdflag end,cust_id,sdt
)x
order by diro,qdrno,city_real desc;



*/
