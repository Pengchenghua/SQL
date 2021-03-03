        drop table csx_tmp.ads_sss_r_d_customer_accounts_fr;
        create table csx_tmp.ads_sss_r_d_customer_accounts_fr  
          ( customer_no string comment '客户编码',
            customer_name       string   comment '客户名称'           ,
            channel_code string comment '渠道编码',
	        channel_name string comment '渠道名称',
            attribute_code      string   comment '客户属性编码 '           ,
            attribute_name      string   comment '客户属性名称'           ,
            first_category_code string   comment '企业一级分类名称'           ,
            first_category_name string   comment '企业一级分类名称'           ,
            second_category_code  string comment '企业二级分类编码'             ,
            second_category_name  string comment '企业二级分类名称'             ,
            third_category_code string   comment '企业三级分类编码'           ,
            third_category_name string   comment '企业三级分类名称'           ,
            sales_id      string         comment '销售员ID'   ,
            work_no       string         comment '销售员OA工号'   ,
            sales_name     string        comment '销售员名称'  ,
            sales_supervisor_id      string         comment '主管ID'     ,
            sales_supervisor_work_no string         comment '主管OA工号'        ,
            sales_supervisor_name     string        comment '主管名称'     ,
            province_code string         comment '省区编码'                    ,
            province_name string         comment '省区名称'                    ,
            city_code     string         comment '城市编码'                    ,
            city_name     string         comment '城市名称'                    ,
            company_code  string         comment '公司代码'                    ,
            company_name  string         comment '公司代码名称'                    ,
            payment_terms string         comment '付款条件'                    ,
            payment_name  string         comment '付款条件名称'                    ,
            payment_days  int            comment '账期值'                  ,
            customer_level   string      comment '客户等级'                     ,
            credit_limit       decimal(26,2)    comment '信控额度'             ,
            temp_credit_limit  decimal(26,2)    comment '临时额度'                 ,
            temp_begin_time    timestamp        comment '临时额度起始时间'             ,
            temp_end_time     timestamp         comment '临时额度截止时间'                 ,
            overdue_amount      decimal(15,2)   comment '逾期金额'                    ,
            overdue_amount1     decimal(15,2)   comment '逾期1-15天'                ,
            overdue_amount15    decimal(15,2)   comment '逾期15-30天'                    ,
            overdue_amount30    decimal(15,2)   comment '逾期30-60天'                    ,
            overdue_amount60    decimal(15,2)   comment '逾期60-90天'                    ,
            overdue_amount90    decimal(15,2)   comment '逾期90-120天'                    ,
            overdue_amount120   decimal(15,2)   comment '逾期120-180天'                        ,
            overdue_amount180   decimal(15,2)   comment '逾期180-365天'                        ,
            overdue_amount365   decimal(15,2)   comment '逾期1-2年'                        ,
            overdue_amount730   decimal(15,2)   comment '逾期2-3年'                        ,
            overdue_amount1095  decimal(15,2)   comment '逾期3年以上'                        ,
            non_overdue_amount  decimal(15,2)   comment '未逾期金额'                        ,
            receivable_amount   decimal(15,2)   comment '应收账款'                        ,
            bad_debt_amount     decimal(15,2)   comment '坏账金额'                        ,
            max_overdue_day      int     comment '最大逾期天数'                         ,
            paid_amount                      decimal(15,2)  comment '年至今回款金额'                    ,
            overdue_coefficient_numerator    decimal(15,2)  comment '逾期金额*逾期天数 计算因子，用于计算逾期系数分子'                        ,
            overdue_coefficient_denominator  decimal(15,2)  comment '应收金额*账期天数 计算因子，用于计算逾期系数分母'                        ,
            overdue_coefficient              decimal(15,2)  comment '逾期系数'                        ,
            last_sales_date     string   comment '最后销售日期'     ,
	        last_to_now_days     int     comment '未销售天数'  ,
	        customer_active_status_code      string  comment '客户标识名称'  ,
	        customer_active_status   string comment '客户标识'     
            
           ) comment '新系统客户帐龄表--帆软' 
           partitioned by (sdt string comment '日分区')
           stored as parquet
           
           ;
           
          set e_date='${enddate}';


set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table  csx_tmp.ads_sss_r_d_customer_accounts_fr partition(sdt)
select     
             x.customer_no,
            customer_name,
            channel_code,
            channel_name,
            attribute_code,
            attribute_name,
            first_category_code,
            first_category_name,
            second_category_code,
            second_category_name,
            third_category_code,
            third_category_name,
            x.sales_id,
            x.work_no,
            x.sales_name,
            sales_supervisor_id ,
            sales_supervisor_work_no ,
            sales_supervisor_name ,
            x.province_code,
            x.province_name,
            city_code,
            city_name,
            company_code,
            company_name,
            payment_terms,
            payment_name,
            payment_days,
            customer_level,
            credit_limit,
            temp_credit_limit,
            temp_begin_time,
            temp_end_time,
            overdue_amount,
            overdue_amount1,
            overdue_amount15,
            overdue_amount30,
            overdue_amount60,
            overdue_amount90,
            overdue_amount120,
            overdue_amount180,
            overdue_amount365,
            overdue_amount730,
            overdue_amount1095,
            non_overdue_amount,
            receivable_amount,
            bad_debt_amount,
            max_overdue_day,
            paid_amount,
            overdue_coefficient_numerator,
            overdue_coefficient_denominator,
            overdue_coefficient,
            last_sales_date,
            last_to_now_days,
            customer_active_status_code,
            customer_active_status,
            x.sdt
from    (
        SELECT 
            a.customer_no,
            customer_name,
            channel_code,
            channel_name,
            attribute_code,
            attribute_name,
            sales_id,
            work_no,
            sales_name,
            province_code,
            province_name,
            city_code,
            city_name,
            company_code,
            company_name,
            payment_terms,
            payment_name,
            payment_days,
            customer_level,
            credit_limit,
            temp_credit_limit,
            temp_begin_time,
            temp_end_time,
            overdue_amount,
            overdue_amount1,
            overdue_amount15,
            overdue_amount30,
            overdue_amount60,
            overdue_amount90,
            overdue_amount120,
            overdue_amount180,
            overdue_amount365,
            overdue_amount730,
            overdue_amount1095,
            non_overdue_amount,
            receivable_amount,
            bad_debt_amount,
            max_overdue_day,
            paid_amount,
            overdue_coefficient_numerator,
            overdue_coefficient_denominator,
            overdue_coefficient,
            last_sales_date,
            last_to_now_days,
              customer_active_status_code,
            case when  customer_active_status_code = 1 then '活跃客户'
	        		when customer_active_status_code = 2 then '沉默客户'
	        		when customer_active_status_code = 3 then '预流失客户'
	        		when customer_active_status_code = 4 then '流失客户'
	        		else '其他'
	        		end  as  customer_active_status,
            a.sdt
        FROM csx_dw.dws_sss_r_a_customer_accounts a 
        left OUTER JOIN
        (
          select * from csx_dw.dws_sale_w_a_customer_company_active
          where sdt =regexp_replace(${hiveconf:e_date},'-','')
        ) e on a.customer_no= e.customer_no and a.company_code = e.sign_company_code and a.sdt=e.sdt
        WHERE a.sdt = regexp_replace(${hiveconf:e_date},'-','')
	
	) x
	left outer join 
	(select customer_no,
	    first_category_code,
	    first_category_name,
	    second_category_code,
	    second_category_name,
	    third_category_code,
	    third_category_name
	from csx_dw.dws_crm_w_a_customer where sdt='current' ) d on x.customer_no=d.customer_no
	left join 
	(select * from csx_dw.dws_uc_w_a_sale_org_m where sdt='current' ) g on x.sales_id=g.sales_id and x.province_code=g.province_code
where
    1 = 1 
;


