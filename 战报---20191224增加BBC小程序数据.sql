---20191224增加BBC小程序数据

-- 设置动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=2000;
set hive.optimize.sort.dynamic.partition=true;


drop table csx_tmp.tmp_temp_sale_tmp;
create temporary table csx_tmp.tmp_temp_sale_tmp
as
select 
  a.sdt,
  case when a.shopid_orig = 'W0B6' or a.sales_type = 'bbc' then '企业购' 
    when a.shop_id = 'W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
    when c.channel like '供应链%' then '供应链(S端)' 
	else c.channel end as qdflag,
  case --when a.shop_id in ('W0M1','W0M4','W0J6','W0M6') then '商超平台' 
    when c.sales_province is not null and c.channel <> '商超' then substr(c.sales_province,1,2)
    else substr(d.prov_name,1,2) end as dist,
  d.city_name,
  c.sales_city as region_city,
  a.cust_id,
  c.customer_name as cust_name, 
  a.bd_name, 
  c.third_supervisor_code,
  sum(xse) as xse,
  sum(mle) as mle
from 
(
  select 
    shop_id, sales_type, customer_no as cust_id, origin_shop_id as shopid_orig, sdt,
    case when category_code in ('10','11') then '生鲜' 
      when category_code in ('12','13') then '食百' 
	  else '其他' end as bd_name, -- 生鲜、食百划分条件
    sales_value as xse, profit as mle
  from csx_dw.dws_sale_r_d_sale_b2b_item 
  where sdt >= concat(substr(regexp_replace(add_months(date_sub(current_date,1),-1),'-','') ,1,6),'01')
    and sdt <= regexp_replace(date_sub(current_date,1),'-','') 
    and sales_type in ('sapqyg', 'sapgc', 'qyg', 'sc', 'bbc') and shop_id <> 'W098'
) a left join 
(
  select
    customer_no,
    customer_name,
    channel,
    sales_province,
    sales_city,
    third_supervisor_code
  from csx_dw.dws_crm_w_a_customer_m 
  where sdt = regexp_replace(date_sub(current_date,1),'-','')
) c on a.cust_id = c.customer_no
left join 
(
  select
    shop_id,
    shop_name, 
    case when province_name = '上海市' then '江苏省' else province_name end as prov_name,
    case when province_name like '%市' then province_name else city_name end as city_name
  from csx_dw.dws_basic_w_a_csx_shop_m
  where sdt = 'current'
) d on a.shop_id = d.shop_id
group by a.sdt, case when a.shopid_orig = 'W0B6' or a.sales_type = 'bbc' then '企业购' 
    when a.shop_id = 'W0H4' and a.cust_id like 'S%' then '供应链(S端)' 
    when c.channel like '供应链%' then '供应链(S端)' else c.channel end,
  case when c.sales_province is not null and c.channel <> '商超' then substr(c.sales_province,1,2)
    else substr(d.prov_name,1,2) end,
  d.city_name, c.sales_city, a.cust_id, c.customer_name, a.bd_name, c.third_supervisor_code;


--福建、江苏、浙江的要划分到市，其余省份无该需求
drop table csx_tmp.tmp_temp_sale01_tmp;
create temporary table csx_tmp.tmp_temp_sale01_tmp
as
select 
  x.qdflag,
  x.dist,
  b.diro,
  b.manage,
  -- 划分城市组
  case when x.dist = '福建' then coalesce(c.city_real,'福州、宁德、三明')
    when x.dist = '江苏' then coalesce(c.city_real,'苏州')
    when x.dist = '浙江' and third_supervisor_code = '1000000211087' then '宁波'
    when x.dist = '浙江' and third_supervisor_code <> '1000000211087' then '杭州'
    else '-' end as city_real,
  -- 添加城市经理
  case when x.dist = '福建' then coalesce(c.cityjob,'郭若秀')
    when x.dist = '江苏' then coalesce(c.cityjob,'部桦')
    when x.dist = '浙江' and third_supervisor_code = '1000000211087' then '林艳'
    when x.dist = '浙江' and third_supervisor_code <> '1000000211087' then '王海燕'
    else '-' end as cityjob,
  cust_id,
  cust_name,
  bd_name,
  sum(xse)xse,
  sum(mle)mle,
  sdt
from 
(
  select 
    case when qdflag is null or qdflag = '' then '大客户' 
      when dist = '平台' and qdflag = '大客户' then '平台' 
	  else qdflag end as qdflag,
    case when dist = 'BB' then '福建' 
	  else dist end as dist,
    case when qdflag = '商超' and dist in ('福建','江苏','浙江') then city_name -- M端取DC所属城市
      when dist = 'BB' then '福州' 
      when (qdflag <> '商超' and dist in ('福建','江苏','浙江')) then region_city -- 非M端取销售城市
      else '-' end as region_city,
    cust_id, cust_name, bd_name, third_supervisor_code, xse, mle, sdt
  from csx_tmp.tmp_temp_sale_tmp
) x left join 
(
  select '平台'dist,'陈晓'manage,1 diro
  union all 
  select '福建'dist,'罗达英'manage,2 diro
  union all 
  select '北京'dist,'方爱锦'manage,3 diro
  union all 
  select '重庆'dist,'赵致伟'manage,4 diro
  union all 
  select '四川'dist,'林鼎意'manage,5 diro
  union all 
  select '上海'dist,'徐学亮'manage,6 diro
  union all 
  select '江苏'dist,'胥浩'manage,7 diro
  union all 
  select '安徽'dist,'袁礼广'manage,8 diro
  union all 
  select '浙江'dist,'沈达'manage,9 diro
  union all
  select '广东'dist,''manage,10 diro
  union all
  select '河北'dist,'张晨'manage,11 diro
  union all
  select '贵州'dist,'李秋屏'manage,12 diro
) b on x.dist = b.dist
left join 
(
  select '泉州'city,'泉州'city_real,'张铮'cityjob
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
  select '福州'city,'福州、宁德、三明'city_real,'郭若秀'cityjob
  union all 
  select '宁德'city,'福州、宁德、三明'city_real,'郭若秀'cityjob
  union all 
  select '三明'city,'福州、宁德、三明'city_real,'郭若秀'cityjob
  union all 
  select '南京'city,'南京'city_real,'黄巍'cityjob
) c on substr(x.region_city,1,2) = c.city
group by x.qdflag, x.dist, b.diro, b.manage, case when x.dist = '福建' then coalesce(c.city_real,'福州、宁德、三明')
    when x.dist = '江苏' then coalesce(c.city_real,'苏州') when x.dist = '浙江' and third_supervisor_code = '1000000211087' then '宁波'
    when x.dist = '浙江' and third_supervisor_code <> '1000000211087' then '杭州' else '-' end,
  case when x.dist = '福建' then coalesce(c.cityjob,'郭若秀') when x.dist = '江苏' then coalesce(c.cityjob,'部桦')
    when x.dist = '浙江' and third_supervisor_code = '1000000211087' then '林艳'
    when x.dist = '浙江' and third_supervisor_code <> '1000000211087' then '王海燕' else '-' end,
  cust_id, cust_name, bd_name, sdt;


-- 插入客户单日销售业绩
insert overwrite table csx_dw.ads_sale_s_d_sale_warzone01_detail_dtl partition (sdt)
select 
  qdflag, dist, diro, manage, city_real, cityjob, cust_id, cust_name, bd_name, xse, mle, sdt 
from csx_tmp.tmp_temp_sale01_tmp;


-- 插入各省区单日销售业绩
insert overwrite table csx_dw.ads_sale_s_d_display_warzone02_res_dtl partition (sdt)
select
  qdrno,
  diro,
  qdflag,
  dist,
  manage,
  city_real,
  cityjob,
  cust_id,
  sum(xse)/10000 as xse,
  sum(mle)/10000 as mle,
  sdt
from 
(
  select 
    case when qdflag = '大客户' then 1 
	  when qdflag = '商超' then 2 -- when qdflag = '商超(对外)' then 3
      when qdflag = '企业购' then 4 end as qdrno,
    qdflag,
	dist,
	diro,
	manage,
    city_real,
    cityjob,
    cust_id,
	bd_name,
	xse,
	mle,
	sdt 
  from csx_tmp.tmp_temp_sale01_tmp
  where qdflag not in ('大宗','平台','供应链(S端)') -- and dist not in ('商超平台')
  union all 
  select 
    case when qdflag = '大客户' then 1 
	  when qdflag = '商超' then 2
      when qdflag = '企业购' then 4 end as qdrno,
    qdflag,
    dist,
    diro,
    manage,
    'Z-小计' as city_real,
    '-' as cityjob,
    cust_id,
    bd_name,
    xse,
    mle,
    sdt
  from csx_tmp.tmp_temp_sale01_tmp
  where qdflag not in ('大宗','平台','供应链(S端)') and dist in ('福建','江苏','浙江')
  union all 
  select 
    case when qdflag in ('大客户','平台') then 1 
      when qdflag = '商超' then 2
      when qdflag = '企业购' then 4 end as qdrno,
    case when qdflag = '平台' then '大客户' 
      else qdflag end as qdflag,
    '总计' as dist,
    100 as diro,
    '' as manage,
    '' as city_real,
    '-' as cityjob,
    cust_id,
    bd_name,
    xse,
    mle,
    sdt 
  from csx_tmp.tmp_temp_sale01_tmp
  where qdflag not in ('大宗','供应链(S端)','平台') -- and dist not in ('商超平台')
) x group by qdrno, diro, qdflag, dist, manage, city_real, cityjob, cust_id, sdt
order by diro, qdrno, city_real desc;

  -- union all 
  -- select
  --   1 as qdrno,
  --   '大客户' as qdflag,
  --   '大客户平台' as dist,
  --   102 as diro,
  --   '' as manage,
  --   '' as city_real,
  --   '' as cityjob,
  --   cust_id,
  --   bd_name,
  --   xse,
  --   mle,
  --   sdt
  -- from csx_tmp.tmp_temp_sale01_tmp
  -- where qdflag = '平台'
  -- union all 
  -- select 
  --   case when qdflag = '大客户' then 1 
  --     when qdflag = '商超' then 2 -- when qdflag = '商超(对外)' then 3
  --     when qdflag = '企业购' then 4 end as qdrno,
  --   qdflag,
  --   '商超平台' as dist,
  --   101 as diro,
  --   '' as manage,
  --   '' as city_real,
  --   '' as cityjob,
  --   cust_id,
  --   bd_name,
  --   xse,
  --   mle,
  --   sdt
  -- from csx_tmp.tmp_temp_sale01_tmp
  -- where dist in ('商超平台')
