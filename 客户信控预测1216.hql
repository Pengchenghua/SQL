-- 逻辑规则于20191112 更改
-- 增加大宗、供应链 万四舍五入 round(new_cr,-4), 大客户、商超 千四舍五入round(a,-3)
-- 只针对大宗与供应链，将帐期 转换改30 ，签约日<=90或者首单<=90 则不调整信控(20191215)

SET sdate='2019-12-24';
SET mapreduce.job.queuename =caishixian;
set hive.groupby.skewindata = true;
SET hive.exec.parallel      =TRUE;
set hive.exec.parallel.thread.number = 12;
drop table if exists temp.p_sale_01
;
create temporary table if not exists temp.p_sale_01
as 
select 
'票到0'  diff,0   index_data
union all
select 
'票到15' diff,15  index_data
union all
select 
'票到30' diff,30  index_data
union all
select 
'票到45' diff,45  index_data
union all
select 
'票到60' diff,60  index_data
union all
select 
'票到7'  diff,7  index_data
union all
select 
'月结10' diff,25  index_data
union all
select 
'月结15' diff,30  index_data
union all
select 
'月结30' diff,45  index_data
union all
select 
'月结45' diff,60  index_data
union all
select 
'月结60' diff,75  index_data
union all
select 
'月结90' diff,105 index_data
;


-- 客户资料
drop table if exists temp.p_sale_02;
CREATE temporary table if NOT EXISTS temp.p_sale_02
as 
SELECT channel,
          sales_province_code,
          sales_province,
          sales_city_code,
          sales_city,
          province_code,
          province_name,
          attribute ,
          city_code,
          city_name,
          sales_name,
          work_no,
          customer_no,
          customer_name,
          credit_limit,
          temp_credit_limit,
          first_category,
          second_category,
          third_category,
         to_date(sign_time)sign_date,
         company_code
   FROM csx_dw.customer_m
   WHERE sdt=regexp_replace(${hiveconf:sdate},'-','') and customer_no<>''
;
   
 drop table if exists temp.p_sale_03;
 CREATE temporary table if not EXISTS temp.p_sale_03
 as 
-- 帐龄数据
SELECT sflag,
          hkont,
          account_name,
          comp_code,
          comp_name,
          regexp_replace(kunnr,'(^0*)','')kunnr,
          name,
          zterm,
          diff,
         -- regexp_extract(diff,'([0-9]+)',0) AS period,
          ac_all,
          ac_wdq,
          ac_15d,
          ac_30d,
          ac_60d,
          ac_90d,
          ac_120d,
          ac_180d,
          ac_365d,
          ac_2y,
          ac_3y,
          ac_over3y
   FROM csx_dw.account_age_dtl_fct_new
   WHERE sdt=regexp_replace(${hiveconf:sdate},'-','') 
   ;
-- regexp_replace(${hiveconf:sdate},'-','') 
--select * from temp.p_sale_03 where kunnr='106903';
DROP table if exists temp.p_sale_04;
CREATE temporary table if NOT EXISTS temp.p_sale_04
as 
-- 销售数据
SELECT --channel,
       --channel_name,
       customer_no,
       a.company_name,
       a.company_code,
      coalesce(sale_30,sale_60)sale,
      coalesce(sale_30/30,sale_60/60) avg_sale,
       min_sdt
FROM
  (SELECT customer_no,
          a.dc_company_code company_code,
          a.dc_company_name company_name,
          coalesce(sum(CASE when(channel IN ('4','5','6')
                        AND sdt>=regexp_replace(date_sub(${hiveconf:sdate},60),'-','')) THEN a.sales_value end ),0) sale_60,
           coalesce( sum(case    WHEN (channel IN ('1','2','3','7')
                        AND sdt>=regexp_replace(date_sub(${hiveconf:sdate},30),'-','')) THEN a.sales_value
              END),0) sale_30,
          min(sdt)min_sdt
   FROM csx_dw.customer_sales a
   WHERE a.sdt>=regexp_replace(trunc(date_sub(${hiveconf:sdate},30),'YY'),'-','')
   GROUP BY customer_no,
            dc_company_code,
            dc_company_name) a;
   

--  SELECT sflag, hkont,account_name,comp_code,comp_name,kunnr,name,zterm,diff,ac_all,ac_wdq,ac_15d,ac_30d,ac_60d,ac_90d,ac_120d,ac_180d,ac_365d,ac_2y,ac_3y,ac_over3y,
--channel,channel_name,customer_no,company_name,company_code,sale,avg_sale,min_sdt FROM temp.p_sale_03 AS b LEFT OUTER JOIN temp.p_sale_04 AS c ON b.kunnr=c.customer_no
-- AND b.comp_code=c.company_code ;

-- select * from temp.p_sale_04 where customer_no='103097';
-- 1、如果首单日期是空白的，且过去30个自然日日均销售也是空白的，那么信控降为0。
-- 2、如果首单日期不是空白的，但是过去30个自然日日均销售是空白的（代表销售未满30天），则信控和原固定额度保持一致。
-- 3、其他情况下都是对比（已使用信控额度+未来信控额度）和原固定额度，取较小值作为新的信控额度。如果对比下来较小值是个负数，则新信控额度为0。
-- 预测客户信控 20191022
SELECT sflag,
       hkont,
       account_name,
       comp_code,
       comp_name,
       company_code,
       sale_company_code,
       sales_province_code,
       sales_province,
       sales_city_code,
       sales_city,
       province_code,
       province_name,
       city_code,
       city_name,
       sales_name,
       work_no,
       customer_no,
       customer_name,
       attribute,
       credit_limit,
       temp_credit_limit,
       channel,
       first_category,
       second_category,
       third_category,
       sign_date,
       zterm,
       diff,  -- 帐期时长
       ac_wdq,
       ac_15d,
       ac_30d,
       ac_60d,
       ac_90d,
       ac_120d,
       ac_180d,
       ac_365d,
       ac_2y,
       ac_3y,
       ac_over3y,
       ac_all, -- 应收金额=已使用额度
       overdue_account, -- 逾期金额
       ac_all AS use_ac_all, -- 应收金额=已使用额度
       avg_sale, -- 30日均销售额
       diff, --帐期
      index_data, -- 转换
       unuse_ac, -- 未使用额度
       new_credit_limit, -- 调整后额度
       future_amount, -- 未来额度,
       surplus_credit,-- 剩余信控
 case when regexp_replace(sign_date,'-','')>=${hiveconf:s_date_30} 
            And channel in ('大客户','企业购','商超(对外)','商超(对内)')  then credit_limit
        when  regexp_replace(sign_date,'-','')>=${hiveconf:s_date_90} 
            And channel in ('供应链(食百)','供应链(生鲜)','大宗')   then credit_limit
        when channel in ('供应链(食百)','供应链(生鲜)','大宗') and  min_sdt>${hiveconf:s_date_90} then round(credit_limit,-4)
        when channel in ('大客户','企业购','商超(对外)','商超(对内)') and  min_sdt>${hiveconf:s_date_30} then round(credit_limit,-3)
  -- when new_credit_limit=0 then 0
   when channel in ('大客户','企业购','商超(对外)','商超(对内)') and (min_sdt<=${hiveconf:s_date_30} or min_sdt='')    then 
   --least(a.credit_limit,a.new_credit_limit) ELSE 0
    round(sort_array(array(credit_limit,new_credit_limit))[0],-3) 
  when channel in ('供应链(食百)','供应链(生鲜)','大宗') and (min_sdt<=${hiveconf:s_date_90} or min_sdt='')   then 
   --least(a.credit_limit,a.new_credit_limit) ELSE 0
   round(sort_array(array(credit_limit,new_credit_limit))[0] ,-4)
  end  credit_limit1,
 min_sdt,
 if(  case when regexp_replace(sign_date,'-','')>=${hiveconf:s_date_30} 
            And channel in ('大客户','企业购','商超(对外)','商超(对内)')  then credit_limit
        when  regexp_replace(sign_date,'-','')>=${hiveconf:s_date_90}
            And channel in ('供应链(食百)','供应链(生鲜)','大宗')   then credit_limit
        when channel in ('供应链(食百)','供应链(生鲜)','大宗') and  min_sdt>${hiveconf:s_date_90} then round(credit_limit,-4)
        when channel in ('大客户','企业购','商超(对外)','商超(对内)') and  min_sdt>${hiveconf:s_date_30} then round(credit_limit,-3)
  -- when new_credit_limit=0 then 0
   when channel in ('大客户','企业购','商超(对外)','商超(对内)') and (min_sdt<=${hiveconf:s_date_30} or min_sdt='')    then 
   --least(a.credit_limit,a.new_credit_limit) ELSE 0
    round(sort_array(array(credit_limit,new_credit_limit))[0],-3) 
  when channel in ('供应链(食百)','供应链(生鲜)','大宗') and (min_sdt<=${hiveconf:s_date_90} or min_sdt='')   then 
   --least(a.credit_limit,a.new_credit_limit) ELSE 0
   round(sort_array(array(credit_limit,new_credit_limit))[0] ,-4)
  end =credit_limit,'否','是') as change_type,
  ${hiveconf:s_date} sdt
 FROM
  (SELECT sflag,
          hkont,
          account_name,
          comp_code,
          comp_name,
          a.company_code,
          c.company_code as sale_company_code,
          sales_province_code,
          sales_province,
          sales_city_code,
          sales_city,
          province_code,
          province_name,
          city_code,
          city_name,
          sales_name,
          work_no,
          a.customer_no,
          customer_name,
          attribute,
          credit_limit,
          temp_credit_limit,
          a.channel,
          --c.channel as channel_id,
         -- c.channel_name ,
          first_category,
          second_category,
          third_category,
          sign_date,
          zterm,
          ac_wdq,
          ac_15d,
          ac_30d,
          ac_60d,
          ac_90d,
          ac_120d,
          ac_180d,
          ac_365d,
          ac_2y,
          ac_3y,
          ac_over3y,
          ac_all, -- 应收金额=已使用额度
          ac_all-ac_wdq AS overdue_account, -- 逾期金额
          ac_all AS use_ac_all, -- 应收金额=已使用额度
          round(coalesce(avg_sale,0),2) avg_sale, -- 大宗与供应链60日均销售额其他30日均
          d.diff, --帐期
         case when channel in ('供应链(食百)','供应链(生鲜)','大宗') then 30 else  d.index_data end index_data, -- 转换
          round(case when channel in ('供应链(食百)','供应链(生鲜)','大宗') then coalesce(if(ac_all*30<0,0,ac_all*30),0)
                else coalesce(b.ac_all*index_data,0)  end ,2)    AS unuse_ac, -- 未使用额度
               
          round( case 
               when channel in ('供应链(食百)','供应链(生鲜)','大宗') then if( (ac_all+coalesce(avg_sale,0)*30)<0,0, ac_all+coalesce(avg_sale,0)*30)
                when (ac_all+coalesce(avg_sale*index_data,0))<0 then 0
			    else ac_all+coalesce(avg_sale*index_data,0)	 end ,2) new_credit_limit, -- 新信控=ac_all+未来信控
			    
          round( case 
               when channel in ('供应链(食百)','供应链(生鲜)','大宗') then coalesce(avg_sale*30,0)
               --when coalesce(avg_sale*index_data,0)<0 then 0
               else  coalesce(avg_sale*index_data,0) END ,2) AS future_amount , -- 未来信控=sale/30*index_data
          
          case when coalesce(a.credit_limit-b.ac_all,0)<= 0 then 0 else round(coalesce(a.credit_limit-b.ac_all,0),2) end  AS surplus_credit, -- 剩余信控
          coalesce(c.min_sdt,'')min_sdt
 FROM temp.p_sale_02 AS a
   LEFT JOIN temp.p_sale_03 AS b ON a.customer_no=b.kunnr
   LEFT OUTER JOIN temp.p_sale_04 AS c ON b.kunnr=c.customer_no
   AND b.comp_code=c.company_code
   LEFT OUTER JOIN temp.p_sale_01 AS d ON b.diff=d.diff
--   where A.customer_no IN ('105514',
--                           '105693',
--                           '106439',
--                           '106684',
--                           '103058',
--                           '104172',
--                           '104664',
--                           '104791',
--                           '105575')
   )a
WHERE customer_no NOT LIKE 'S%' ;

-- select * from  temp.p_sale_04 where min_sdt=''  limit 100;

-- 问题点：无销售情况下 sale is null 取ac_all，当ac_all<0 取固定信控 credit_limit 
-- 异常：1、CRM出现固定信控等于0 2、 应收款为负
-- 当future_amount<surplus_credit then 变更 ,未来信控=sale/30*index_data  ，新信控=ac_all+未来信控,剩余信控=固定信控credit_limit-应收帐款ac_all
 -- 查询公司代码不一致
-- select a.customer_no,customer_name,a.company_code,b.comp_code,b.comp_name,c.company_code,c.company_name from
-- (select customer_no,customer_name,company_code from csx_dw.customer_m where sdt='20191022' and customer_no!=''  and customer_no not like 'S%')a
-- left outer join
-- (select regexp_replace(kunnr,'(^0*)','')kunnr,comp_code,comp_name from csx_dw.account_age_dtl_fct_new where sdt='20191022') b on a.customer_no=b.kunnr
-- left outer join
-- (SELECT customer_no,company_code,company_name,
--           sum(sales_value) sale,
--           min(sdt)min_sdt
--   FROM csx_dw.sale_goods_m a
--   join
--   (select shop_id,company_code,company_name from csx_dw.shop_m where sdt='current')b
--   on a.shop_id=b.shop_id
--   and  a.sdt>=regexp_replace(to_date(date_sub(current_timestamp(),31)),'-','')
--   GROUP BY customer_no,company_code,company_name)c
--   on a.customer_no=c.customer_no
-- --       CASE
--           WHEN min_sdt='' AND avg_sale=0 THEN 0  -- 日期为空且销售为0 信控为0 
-- 		   WHEN min_sdt>regexp_replace(to_date(date_sub(current_date(),31)),'-','') then credit_limit  -- 首单日期小于30天，
-- 		   -- 或者首单日期不为空且销售=0 或 应收帐<0 等于原信控
-- 		   	--when (min_sdt !='' AND avg_sale=0) then credit_limit
-- 			when sort_array(array(credit_limit,new_credit_limit))[0]<0 then 0
-- 		else sort_array(array(credit_limit,new_credit_limit))[0]  -- 以上条件不满足取最小的信控额
--  END new_credit,
-- =MAX(IF(AY2>20190926,S2,MIN(AU2+AO2,S2)),0)
-- 说明 取最大值（当首单日期大于=30天取固定信控，（新信控与固定信控对比取最小值））
