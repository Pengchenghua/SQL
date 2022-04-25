--组：CSX_ADS_REPORT_SALE_PLAT_SALE_GATHER；
--组：CSX_MYSQL_REPORT_SALE_PLAT_SALE_GATHER；

--csx_dw.customer_m 用户表  fixation_report_sale_data_center_plat_sale_gather --mysql
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table csx_dw.report_data_center_plat_sale_gather  partition(sdt)
select
  aa.channel_code  ---'渠道编码'
  ,aa.channel_name 
 --- ,if(d.customer_no is null,aa.channel_name,d.channel)  channel_name ---'渠道名称'
  ,aa.region_code ---大区code
  ,aa.region_name  ---'大区名称'
  ,'' region_manager_id  ---'大区主管id'
  ,'' region_manager_name ---'大区主管'
  ,aa.province_code  ---'省区编码'
  ,aa.province_name  ---'省区'
  ,b.province_manager_id ---'省区主管id'
  ,b.province_manager_name  ---'省区主管'
  ,aa.city_group_code ---'城市组编码'
  ,aa.city_group_name ---'城市组名称'
  ,c.city_group_manager_id city_manager_id   ---'城市主管id'
  ,c.city_group_manager_name  city_manager_name ---'城市主管名称'
  ,''strategy_code 
  ,a.nature_code
  ,a.nature_name
  ---,if(d.customer_no is null,'否','是')   strategy_code ---'是否战略客户',
  ---,if(d.customer_type is null,a.nature_code,if(d.customer_type='日配客户',1,0))  nature_code  ----'自营,联营(非自营)标识: 1自营,0联营(非自营)'
  ---,if(d.customer_type is null,a.nature_name,if(d.customer_type='日配客户','自营','非自营'))  ----'性质名称'
  ,a.sales_month                                      ---'销售所有月'
  ,sum(if(a.sdt=regexp_replace(date_sub(current_date,1),'-',''),a.sales_value,0))/10000 sales_value ---'昨日业绩'
  ,sum(a.sales_value)/10000  sales_value_m                                                    --- '月至今业绩'
  ,sum(if(a.sdt=regexp_replace(date_sub(current_date,1),'-',''),a.profit,0))/10000 profit ---'昨日毛利'
  ,sum(a.profit)/10000 profit_m                                                      --- '月至今毛利'  
  ,sum(if(a.sdt=regexp_replace(date_sub(current_date,1),'-',''),a.round_sales_value,0))/10000 round_sales_value---'环比过机额'
  ,sum(a.round_sales_value)/10000 round_sales_value_m                                               ----'月至今环比过机额'
  ,max(f.sales_value) sales_value_mb                          --业绩月目标值
  ,regexp_replace(date_sub(current_date,1),'-','') sdt 
from 
(select 
	region_code
    ,region_name
	,case when region_name in ('大宗','供应链') then substr(region_name,1,2)
	      when province_code not in (10,13,15) then substr(province_name,1,2)
		  when province_code in (10,13,15)  then substr(city_group_name,1,2)
		  end as qs
    ,channel_code  --商超需要修改5条
    ,channel_name
    ,province_code
    ,province_name
    ,city_group_code
    ,city_group_name
from csx_dw.sale_m_target_code) aa
left join 
(select 
    region_code
    ,region_name
---,if(channel_code=7,1,channel_code) channel_code
---,if(channel_name='企业购','大客户',channel_name) channel_name
	,case when channel_code in (1,7,8,9) then '1' else channel_code end channel_code
    ,case when channel_code in (1,7,8,9) then '大客户' else channel_name end channel_name
    ,province_code
    ,province_name
    ,city_group_code
    ,city_group_name
    ,nature_code
    ,nature_name
    ,sales_month
	,customer_no  --客户号
    ,sales_value
    ,profit
    ,round_sales_value
	,sdt
from csx_dw.report_data_center_plat_sale_head_bigtable 
where sdt>=regexp_replace(trunc(current_date,'MM'),'-','')
) a on a.channel_name=aa.channel_name and a.province_code=aa.province_code and a.city_group_code=aa.city_group_code
left join (SELECT *
			from csx_dw.dim_area
			where area_rank=13) b on b.province_code=aa.province_code
left join (SELECT *
			from csx_dw.dim_area
			where area_rank=12 ) c on c.city_group_code=aa.city_group_code
---left join csx_dw.sale_customer_name d on d.customer_no=a.customer_no ----战略客户表
left join csx_dw.sale_m_target f on substr(f.province_name,1,2)=aa.qs  -----目标销售表
										and f.channel=aa.channel_name
										and f.sdt=substr(regexp_replace(date_sub(current_date,1),'-',''),1,6)
								
group by
    aa.channel_code
  ,aa.channel_name 
---  ,if(d.customer_no is null,aa.channel_name,d.channel)
  ,aa.region_code
  ,aa.region_name
  ,aa.province_code
  ,aa.province_name
  ,b.province_manager_id 
  ,b.province_manager_name
  ,aa.city_group_code
  ,aa.city_group_name
  ,c.city_group_manager_id
  ,c.city_group_manager_name 
  ,a.nature_code
  ,a.nature_name 
---  ,if(d.customer_no is null,'否','是') 
---  ,if(d.customer_type is null,a.nature_code,if(d.customer_type='日配客户',1,0)) 
---  ,if(d.customer_type is null,a.nature_name,if(d.customer_type='日配客户','自营','非自营'))
  ,a.sales_month;




/*
create table csx_tmp.sale_m_target (
  `province_name` string COMMENT '所属省区',
  `channel` string COMMENT '渠道',
  `sales_value`  decimal(26,6) COMMENT '目标销售收入（含税）万元',
  `sdt` string COMMENT '日期分区'
  ) COMMENT '目标销售表 '
STORED AS TEXTFILE;

create table csx_tmp.sale_customer_name (
  `customer_no` string COMMENT '客户号',
  `customer_type` string COMMENT '客户属性',
  `channel` string COMMENT '渠道',
  `province_name` string COMMENT '所属省区',
  `sdt` string COMMENT '日期分区'
  ) COMMENT '战略客户表 '
STORED AS TEXTFILE;
insert into 
csx_tmp.sale_m_target(province_name,channel,sales_value,sdt)
 VALUES 
('广东','大客户',53,'202006'),

insert into 
csx_tmp.sale_customer_name(customer_no,customer_type,channel,province_name,sdt)
 VALUES 
('107125','日配客户','大客户','北京','202005'),
('107113','日配客户','大客户','北京','202005');



alter table csx_dw.report_data_center_plat_sale_gather ADD COLUMNS 
(sales_value_mb decimal(26,6) COMMENT '业绩月目标值');

create table csx_dw.report_data_center_plat_sale_gather (
`channel_code`  string COMMENT '渠道编码',
`channel_name`  string COMMENT '渠道名称',
`region_code`  string COMMENT '大区code',
`region_name`  string COMMENT '大区名称',
`region_manager_id`  string COMMENT '大区主管id',
`region_manager_name`  string COMMENT '大区主管名称',
`province_code`  string COMMENT '省区编码',
`province_name`  string COMMENT '省区名称',
`province_manager_id`  string COMMENT '省区主管id',
`province_manager_name`  string COMMENT '省区主管名称',
`city_group_code`  string COMMENT '城市组编码',
`city_group_name`  string COMMENT '城市组名称',
`city_manager_id`  string COMMENT '城市主管id',
`city_manager_name`  string COMMENT '城市主管名称',
`strategy_code`  string COMMENT '是否战略客户',
`nature_code`  int COMMENT '自营,联营(非自营)标识: 1自营,0联营(非自营)',
`nature_name`  string COMMENT '性质名称',
`sales_month`  string COMMENT '销售所有月',
`sales_value`  decimal(26,6) COMMENT '昨日业绩',
`sales_value_m`  decimal(26,6) COMMENT '月至今业绩',
`profit`  decimal(26,6) COMMENT '昨日毛利',
`profit_m`  decimal(26,6) COMMENT '月至今毛利',
`round_sales_value`  decimal(26,6) COMMENT '环比过机额',
`round_sales_value_m`  decimal(26,6) COMMENT '月至今环比过机额'
) COMMENT '业绩战报汇总表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;*/






