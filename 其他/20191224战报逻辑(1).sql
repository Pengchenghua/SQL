---20191224增加BBC小程序数据
set mapreduce.job.queuename=caishixian;

drop table b2b_tmp.temp_sale;
CREATE temporary table b2b_tmp.temp_sale
as
select a.sdt,case when a.shop_id like 'E%' then '商超(对内)'
when shopid_orig='W0B6' or sales_type='bbc'then '企业购' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
when c.channel='M端' then '商超(对内)'
when c.channel like '供应链%' then '供应链(S端)' else c.channel end qdflag,

case when a.shop_id in ('W0M1','W0M4','W0J6','W0M6','W0K5')then '商超平台' 
when sales_province is not null and c.channel<>'M端' and c.channel not like '商超%' then substr(sales_province,1,2)
when a.cust_id like 'S%' and 
substr(b.prov_name,1,2) in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东') then substr(b.prov_name,1,2)
else substr(d.prov_name,1,2) end dist,fourth_supervisor_work_no region_no,fourth_supervisor_name region_manage,b.city_name,sales_city region_city,
--manage,manage_no,暂时先不添加，直接按照表格给的福州沈锋，泉州胡永水
a.cust_id,c.customer_name cust_name,bd_name,
sum(xse)xse,
sum(mle)mle
from 
(select shop_id,sales_type,case when shop_id like 'E%' then concat('9',substr(shop_id,2,3)) else shop_id end shop_no,
customer_no cust_id,origin_shop_id shopid_orig,sdt,
       case when category_code in ('10','11') then '生鲜' when category_code in ('12','13') then '食百' else '其他' end bd_name
       ,sum(sales_value) xse
       ,sum(profit) mle
from  csx_dw.sale_b2b_item 
where sdt>=concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
and sdt<=regexp_replace(date_sub(current_date,1),'-','')
and shop_id<>'W098'
and sales_type in('qyg','gc','anhui','sc','bbc') 
group by shop_id,sales_type,
customer_no,origin_shop_id,sdt,
case when category_code in ('10','11') then '生鲜' when category_code in ('12','13') then '食百' else '其他' end)a
left join (select * from csx_dw.customer_m where sdt=regexp_replace(date_sub(current_date,1),'-','')) c 
on lpad(a.cust_id,10,'0')=lpad(c.customer_no,10,'0')
left join (select shop_id,prov_name from dim.dim_shop where edate='9999-12-31')d on a.shop_no=d.shop_id
left join
(select shop_id,case when shop_id in ('W055','W056') then '上海市' else prov_name end prov_name,
case when prov_name like '%市' then prov_name else city_name end city_name from dim.dim_shop where edate='9999-12-31' )b 
on a.cust_id=concat('S',b.shop_id)
group by a.sdt,case when a.shop_id like 'E%' then '商超(对内)'
when shopid_orig='W0B6' or sales_type='bbc' then '企业购' 
when a.shop_id='W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
when c.channel='M端' then '商超(对内)'
when c.channel like '供应链%' then '供应链(S端)'else c.channel end,
case when a.shop_id in ('W0M1','W0M4','W0J6','W0M6','W0K5')then '商超平台' 
when sales_province is not null and c.channel<>'M端' and c.channel not like '商超%' then substr(sales_province,1,2)
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
(select case when qdflag is null or qdflag='' then '大' when dist='平台' and qdflag='大' then '平台'else qdflag end qdflag,
case when dist='BB' then '福建'else dist end dist,
case when qdflag='商超(对内)' and dist ='福建' then city_name 
when dist='BB' then '福州' 
when (qdflag<>'商超(对内)' and dist ='福建')  then region_city else '-' end region_city,
cust_id,cust_name,bd_name,
sum(xse) xse,
sum(mle) mle,
sdt
from b2b_tmp.temp_sale
group by case when qdflag is null or qdflag='' then '大' when dist='平台' and qdflag='大' then '平台'else qdflag end,
case when dist='BB' then '福建'else dist end,
case when qdflag='商超(对内)' and dist ='福建' then city_name 
when dist='BB' then '福州' 
when (qdflag<>'商超(对内)' and dist ='福建')  then region_city else '-' end,cust_id,cust_name,bd_name,sdt)x 
left join 
(select '平台'dist,'陈晓'manage,1 diro
union all 
select '福建'dist,'沈锋'manage,2 diro
union all 
select '北京'dist,'方爱锦'manage,3 diro
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
(select case when qdflag='大' then 1 when qdflag='商超(对内)' then 2 
when qdflag='商超(对外)' then 3
when qdflag='企业购' then 4 end qdrno,
qdflag,dist,diro,manage,
city_real,
cityjob,
cust_id,bd_name,xse,mle,sdt 
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','平台','供应链(S端)') and dist not in ('商超平台')
union all 
select 1 qdrno,'大'qdflag,'大平台'dist,102 diro,''manage,
''city_real,
''cityjob,
cust_id,bd_name,xse,mle,sdt
from b2b_tmp.temp_sale01 a where qdflag in ('平台')

union all 
select 
case when qdflag='大' then 1 when qdflag='商超(对内)' then 2 
when qdflag='商超(对外)' then 3
when qdflag='企业购' then 4 end qdrno,qdflag,'商超平台'dist,101 diro,''manage,
''city_real,
''cityjob,
cust_id,bd_name,xse,mle,sdt
from b2b_tmp.temp_sale01 a where dist in ('商超平台')
union all 
select 
case when qdflag='大' then 1 when qdflag='商超(对内)' then 2 
when qdflag='商超(对外)' then 3
when qdflag='企业购' then 4 end qdrno,
qdflag,dist,diro,manage,
'Z-小计' city_real,
'-'cityjob,
cust_id,bd_name,sum(xse)xse,sum(mle)mle,sdt
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','平台','供应链(S端)') and dist='福建'
group by case when qdflag='大' then 1 when qdflag='商超(对内)' then 2 
when qdflag='商超(对外)' then 3
when qdflag='企业购' then 4 end,
qdflag,dist,diro,manage,cust_id,bd_name,sdt
union all 
select 
case when qdflag in ('大','平台') then 1 when qdflag='商超(对内)' then 2 
when qdflag='商超(对外)' then 3
when qdflag='企业购' then 4 end qdrno,
case when qdflag='平台' then '大' else qdflag end qdflag,'总计'dist,100 diro,''manage,
'' city_real,
'-'cityjob,
cust_id,bd_name,sum(xse)xse,sum(mle)mle,sdt 
from b2b_tmp.temp_sale01 a where qdflag not in ('大宗','供应链(S端)','平台')  and dist not in ('商超平台')
group by case when qdflag in ('大','平台') then 1 when qdflag='商超(对内)' then 2 
when qdflag='商超(对外)' then 3
when qdflag='企业购' then 4 end,
case when qdflag='平台' then '大' else qdflag end,cust_id,bd_name,sdt
)x
group by qdrno,diro,
qdflag,dist,manage,city_real,cityjob,cust_id,sdt
order by diro,qdrno,city_real desc;
