/*20190807更改战报数据源b2b销售表为发票表汇总出来的csx_dw.sale_b2b_item表
qyg  gc
W0H4卖给彩食鲜外部的这部分数据算入供应链S端*/
战报里：泉州改成：沈峰   上海：改成徐学亮  
--20190814S端拆分食百生鲜部门，且报表呈现种S端销售至门店部门拆分成S端M端，其余为S端B端
-- 福建省城市门店重新划分
-- , fourth_supervisor_name
-- sflag='M端' or sflag like '商超%'

drop table b2b_tmp.temp_sale;
CREATE temporary table b2b_tmp.temp_sale
as
select a.sdt,case when shopid_orig='W0B6' then 'BBC' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
when c.channel='M端' then '商超（对内）'
when c.channel like '供应链%' then '供应链(S端)'
when c.channel='大'then 'B端' else c.channel end qdflag,
case when sales_province is not null and c.channel<>'M端' then substr(sales_province,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end dist,fourth_supervisor_work_no region_no,fourth_supervisor_name region_manage,b.city_name,sales_city region_city,
--manage,manage_no,暂时先不添加，直接按照表格给的福州沈锋，泉州胡永水
a.cust_id,c.customer_name cust_name,bd_name,
sum(xse)xse,
sum(mle)mle
from 
(select shop_id,customer_no cust_id,origin_shop_id shopid_orig,sdt,
       case when category_code in ('10','11') then '生鲜' when category_code in ('12','13') then '食百' else '其他' end bd_name
       ,sum(sales_value) xse
       ,sum(profit) mle
from  csx_dw.sale_b2b_item 
where sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
and sdt<=regexp_replace(date_sub(current_date,1),'-','')
and shop_id<>'W098'
and sales_type in('qyg','gc','anhui') 
group by shop_id,customer_no,origin_shop_id,sdt,
case when category_code in ('10','11') then '生鲜' when category_code in ('12','13') then '食百' else '其他' end)a
left join (select * from csx_dw.customer_m where sdt=regexp_replace(date_sub(current_date,1),'-','')) c 
on lpad(a.cust_id,10,'0')=lpad(c.customer_no,10,'0')
left join (select shop_id,prov_name from dim.dim_shop where edate='9999-12-31')d on a.shop_id=d.shop_id
left join
(select shop_id,case when shop_id in ('W055','W056') then '上海市' else prov_name end prov_name,case when prov_name like '%市' then prov_name else city_name end city_name from dim.dim_shop where edate='9999-12-31' )b 
on a.cust_id=concat('S',b.shop_id)
group by a.sdt,case when shopid_orig='W0B6' then 'BBC' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
when c.channel='M端' then '商超（对内）'
when c.channel like '供应链%' then '供应链(S端)'
when c.channel='大'then 'B端' else c.channel end,
case when sales_province is not null and c.channel<>'M端' then substr(sales_province,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end ,fourth_supervisor_work_no,fourth_supervisor_name,b.city_name,sales_city,a.cust_id,c.customer_name,bd_name;



--福建省的要划分到市，其余省份无该需求
drop table b2b_tmp.temp_sale01;
CREATE temporary table b2b_tmp.temp_sale01
as
select x.qdflag,x.dist,b.diro,b.manage,
case when x.dist='福建' then coalesce(c.city_real,'福州、宁德、三明')else '-' end city_real,
case when x.dist='福建' then coalesce(c.cityjob,'沈锋')else '-' end cityjob,
cust_id,cust_name,bd_name,sum(xse)xse,sum(mle)mle,sdt
from 
(select case when qdflag is null or qdflag='' then 'B端' when dist='平台' and qdflag='B端' then '平台'else qdflag end qdflag,
case when dist ='成都' then '四川' when qdflag='BBC' then '福建'else dist end dist,

case when qdflag='商超（对内）' then city_name 
when qdflag='BBC' then '福州' 
when (qdflag<>'商超（对内）')  then region_city else '-' end region_city,

cust_id,cust_name,bd_name,
sum(xse) xse,
sum(mle) mle,
sdt
from b2b_tmp.temp_sale
group by case when qdflag is null or qdflag='' then 'B端' when dist='平台' and qdflag='B端' then '平台'else qdflag end,
case when dist ='成都' then '四川' when qdflag='BBC' then '福建'else dist end,
case when qdflag='商超（对内）' and dist ='福建' then city_name 
when qdflag='BBC' then '福州' 
when (qdflag<>'商超（对内）' and dist ='福建')  then region_city else '-' end,cust_id,cust_name,bd_name,sdt)x 
left join 
(select '平台'dist,'陈晓'manage,1 diro
union all 
select '福建'dist,'沈锋'manage,2 diro
union all 
select '北京'dist,'解飞'manage,3 diro
union all 
select '重庆'dist,'赵致伟'manage,4 diro
union all 
select '四川'dist,'陈建斌'manage,5 diro
union all 
select '上海'dist,'徐学亮'manage,6 diro
union all 
select '江苏'dist,'胥浩'manage,7 diro
union all 
select '安徽'dist,'潘兆贤'manage,8 diro
union all 
select '浙江'dist,'潘兆贤'manage,9 diro
union all
select '广东'dist,'谢小军'manage,10 diro
)b on x.dist=b.dist
left join 
(select '泉州'city,'泉州'city_real,'张铮'cityjob
union all 
select '莆田'city,'莆田'city_real,'倪薇红'cityjob
union all 
select '南平'city,'南平'city_real,'林挺'cityjob
union all 
select '厦门'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
union all 
select '漳州'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
union all 
select '龙岩'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
union all 
select '福州'city,'福州、宁德、三明'city_real,'沈锋'cityjob
union all 
select '宁德'city,'福州、宁德、三明'city_real,'沈锋'cityjob
union all 
select '三明'city,'福州、宁德、三明'city_real,'沈锋'cityjob
)c on substr(x.region_city,1,2)=c.city
group by x.qdflag,x.dist,b.diro,b.manage,
case when x.dist='福建' then coalesce(c.city_real,'福州、宁德、三明')else '-' end,
case when x.dist='福建' then coalesce(c.cityjob,'沈锋')else '-' end,cust_id,cust_name,bd_name,sdt;


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=1000;
insert overwrite table csx_dw.sale_warzone01_detail_dtl partition (sdt)
select qdflag,dist,diro,manage,city_real,cityjob,
cust_id,cust_name,bd_name,xse,mle,sdt 
from b2b_tmp.temp_sale01 a;





insert overwrite table csx_dw.display_warzone02_res_dtl partition (sdt)
select  qdrno,diro,
qdflag,dist,manage,
city_real,
cityjob,
cust_id,sum(xse/10000) xse,sum(mle/10000) mle,sdt
from 
(select case when qdflag='B端' then 1 when qdflag='商超（对内）' then 2 
when qdflag='商超（对外）' then 3
when qdflag='BBC' then 4 end qdrno,
qdflag,dist,diro,manage,
city_real,
cityjob,
cust_id,bd_name,xse,mle,sdt 
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','平台','供应链(S端)')
union all 
select 1 qdrno,'B端'qdflag,'平台'dist,1 diro,''manage,
''city_real,
''cityjob,
cust_id,bd_name,xse,mle,sdt
from b2b_tmp.temp_sale01 a where qdflag in ('平台')
union all 
select 
case when qdflag='B端' then 1 when qdflag='商超（对内）' then 2 
when qdflag='商超（对外）' then 3
when qdflag='BBC' then 4 end qdrno,
qdflag,dist,diro,manage,
'Z-小计' city_real,
'-'cityjob,
cust_id,bd_name,sum(xse)xse,sum(mle)mle,sdt
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','平台','供应链(S端)') and dist='福建'
group by case when qdflag='B端' then 1 when qdflag='商超（对内）' then 2 
when qdflag='商超（对外）' then 3
when qdflag='BBC' then 4 end,
qdflag,dist,diro,manage,cust_id,bd_name,sdt
union all 
select 
case when qdflag in ('B端','平台') then 1 when qdflag='商超（对内）' then 2 
when qdflag='商超（对外）' then 3
when qdflag='BBC' then 4 end qdrno,
case when qdflag='平台' then 'B端' else qdflag end qdflag,'总计'dist,100 diro,''manage,
'' city_real,
'-'cityjob,
cust_id,bd_name,sum(xse)xse,sum(mle)mle,sdt 
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','供应链(S端)') 
group by case when qdflag in ('B端','平台') then 1 when qdflag='商超（对内）' then 2 
when qdflag='商超（对外）' then 3
when qdflag='BBC' then 4 end,
case when qdflag='平台' then 'B端' else qdflag end,cust_id,bd_name,sdt
)x
group by qdrno,diro,
qdflag,dist,manage,city_real,cityjob,cust_id,sdt
order by diro,qdrno,city_real desc;


/*


create table csx_dw.sale_warzone01_detail_dtl
(
qdflag string comment '渠道',
dist string comment '省区',
diro int comment '省区序号',
manage string comment '区总',
city_real string comment '城市调整',
cityjob string comment '城市负责人',
cust_id string comment '编码',
cust_name string comment '名称',
bd_name string comment '部门名称',
xse decimal(26,4) comment '销额',
mle decimal(26,4) comment '毛利额'
)
comment '战区报表日明细'
partitioned by (sdt string comment '运行日期')
row format delimited
stored as parquet;

select qdrno,diro,qdflag,dist,manage,city_real,cityjob,
sum(cust_num)cust_num,sum(xse) xse,sum(mle) mle,
case when sum(xse)=0 then null else sum(mle)/sum(xse) end prorate,
sum(xse_lm)xse_lm,
case when sum(xse_lm)=0 then null else sum(xse)/sum(xse_lm)-1 end hb_sale 
from 
(select qdrno,coalesce(diro,9.1)diro,qdflag,dist,manage,city_real,cityjob,
count(distinct cust_id)cust_num,sum(xse) xse,sum(mle) mle,0 xse_lm 
 from csx_dw.display_warzone02_res_dtl 
where sdt>='${SDATE}' and sdt<='${EDATE}'
group by qdrno,diro,qdflag,dist,manage,city_real,cityjob
union all 
select qdrno,coalesce(diro,9.1)diro,qdflag,dist,manage,city_real,cityjob,
0 cust_num,0 xse,0 mle,sum(xse) xse_lm 
from csx_dw.display_warzone02_res_dtl 
where sdt>='${SBJ}' and sdt<='${EBJ}'
group by qdrno,diro,qdflag,dist,manage,city_real,cityjob)x
group by qdrno,diro,qdflag,dist,manage,city_real,cityjob
order by diro,qdrno,city_real desc;


select qdflag,case when qdflag='大宗' then '大宗' else 'S端' end dist,
case when qdflag='大宗' then'方佑智' else '-' end manage,
'' city_real,
'-'cityjob,
sum(xse)/10000 xse,sum(mle)/10000 mle,
case when sum(xse)=0 then null else sum(mle)/sum(xse) end prorate,
sum(xse_lm)/10000 xse_lm,case when sum(xse_lm)=0 then null else sum(xse)/sum(xse_lm)-1 end hb_sale 
from csx_dw.sale_warzone02_detail_dtl a where sdt='${SDATE}' and qdflag in ('大宗','供应链(S端)') 
group by qdflag

select 
qdflag,case when qdflag='大宗' then '大宗' else 'S端' end dist,
case when qdflag='大宗' then'方佑智' else '-' end manage,
'' city_real,
'-'cityjob,
sum(xse)/10000 xse,sum(mle)/10000 mle,
case when sum(xse)=0 then null else sum(mle)/sum(xse) end prorate,
sum(xse_lm)/10000 xse_lm,
case when sum(xse_lm)=0 then null else sum(xse)/sum(xse_lm)-1 end hb_sale
from 
(select qdflag,sum(xse) xse,sum(mle) mle,0 xse_lm 
from csx_dw.sale_warzone02_detail_dtl 
where sdt>='${SDATE}' and sdt<='${EDATE}' 
and qdflag in ('大宗','供应链(S端)')
group by qdflag
union all 
select qdflag,0 xse,0 mle,sum(xse) xse_lm 
from csx_dw.sale_warzone02_detail_dtl 
where sdt>='${SBJ}' and sdt<='${EBJ}'
and qdflag in ('大宗','供应链(S端)') 
group by qdflag)x
group by qdflag;



select qdflag,dist,diro,case when qdflag='大宗'then '方佑智' when qdflag='供应链(S端)' then '-'else manage end manage,city_real,cityjob,
cust_id,cust_name,sum(xse)xse,sum(mle) mle,
case when sum(xse)=0 then null else sum(mle)/sum(xse) end prorate,
sum(xse_lm) xse_lm,
case when sum(xse_lm)=0 then null else sum(xse)/sum(xse_lm)-1 end hb_sale
 from 
(select qdflag,dist,diro,cust_id,cust_name,manage,city_real,cityjob,sum(xse)xse,sum(mle)mle,0 xse_lm 
from csx_dw.sale_warzone02_detail_dtl
where sdt>='${SDATE}' and sdt<='${EDATE}' 
group by qdflag,dist,diro,cust_id,cust_name,manage,city_real,cityjob
union all 
select qdflag,dist,diro,cust_id,cust_name,manage,city_real,cityjob,0 xse,0 mle,sum(xse) xse_lm 
from csx_dw.sale_warzone02_detail_dtl
where sdt>='${SBJ}' and sdt<='${EBJ}' 
group by qdflag,dist,diro,cust_id,cust_name,manage,city_real,cityjob)x 
group by qdflag,dist,diro,
case when qdflag='大宗'then '方佑智' when qdflag='供应链(S端)' then '-'else manage end,city_real,cityjob,
cust_id,cust_name
order by diro,city_real desc





/*
DROP TABLE IF EXISTS csx_dw.sale_warzone02_detail_dtl;
create table csx_dw.sale_warzone02_detail_dtl
(
qdflag string comment '渠道',
dist string comment '省区',
diro int comment '省区序号',
manage string comment '区总',
city_real string comment '城市调整',
cityjob string comment '城市负责人',
cust_id string comment '编码',
cust_name string comment '名称',
xse decimal(26,4) comment '销额',
mle decimal(26,4) comment '毛利额'
)
comment '战区报表日明细'
partitioned by (sdt string comment '运行日期')
row format delimited
stored as parquet;

DROP TABLE IF EXISTS csx_dw.display_warzone02_res_dtl;
create table csx_dw.display_warzone02_res_dtl
(
qdrno int comment '渠道序号',
diro int comment '省区序号',
qdflag string comment '渠道',
dist string comment '省区',
manage string comment '区总',
city_real string comment '城市调整',
cityjob string comment '城市负责人',
cust_id string comment '编码',
xse decimal(26,4) comment '销额',
mle decimal(26,4) comment '毛利额'
)
comment '战区报表日展示'
partitioned by (sdt string comment '运行日期')
row format delimited
stored as parquet;

