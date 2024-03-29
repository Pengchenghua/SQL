-- ads_fr_r_d_forecast_collection_report_20220304
	CREATE TABLE `csx_tmp.ads_fr_r_d_forecast_collection_report_20220304`(
	  `channel_name` string COMMENT '类型', 
	  `sales_channel_name` string COMMENT '销售渠道,增加城市服务商,其他根据CRM渠道', 
	  `hkont` string COMMENT '科目代码', 
	  `account_name` string COMMENT '科目名称', 
	  `comp_code` string COMMENT '公司代码', 
	  `comp_name` string COMMENT '公司名称', 
	  `region_code` string COMMENT '大区编码', 
	  `region_name` string COMMENT '大区名称', 
	  `province_code` string COMMENT '销售省区编码', 
	  `province_name` string COMMENT '销售省区名称', 
	  `city_group_code` string COMMENT '城市组编码', 
	  `city_group_name` string COMMENT '城市组名称', 
	  `sales_city` string COMMENT '销售城市名称', 
	  `prctr` string COMMENT '利润中心', 
	  `shop_name` string COMMENT '利润中心名称', 
	  `customer_no` string COMMENT '编码', 
	  `customer_name` string COMMENT '名称', 
	  `first_category_code` string COMMENT '行业一级分类编码', 
	  `first_category` string COMMENT '行业一级分类名称', 
	  `second_category_code` string COMMENT '行业二级分类编码', 
	  `second_category` string COMMENT '行业二级分类名称', 
	  `third_category_code` string COMMENT '行业三级分类编码', 
	  `third_category` string COMMENT '行业三级分类名称', 
	  `work_no` string COMMENT '销售员工号', 
	  `sales_name` string COMMENT '销售员姓名', 
	  `first_supervisor_work_no` string COMMENT '销售主管工号', 
	  `first_supervisor_name` string COMMENT '销售主管', 
	  `credit_limit` decimal(26,4) COMMENT '信控额度', 
	  `temp_credit_limit` decimal(26,4) COMMENT '临时信控额度', 
	  `payment_terms` string COMMENT '付款条件', 
	  `payment_name` string COMMENT '付款条件名称', 
	  `payment_days` string COMMENT '帐期', 
	  `zterm` string COMMENT '账期类型', 
	  `diff` string COMMENT '账期', 
	  `ac_all` decimal(26,4) COMMENT '应收金额', 
	  `ac_wdq` decimal(26,4) COMMENT '未到期金额', 
	  `ac_all_month_last_day` decimal(26,4) COMMENT '月底预测应收账款', 
	  `ac_wdq_month_last_day` decimal(26,4) COMMENT '月底预测未到期账款', 
	  `ac_overdue_month_last_day` decimal(26,4) COMMENT '月底预测逾期金额', 
	  `ac_overdue_month_last_day_rate` decimal(26,4) COMMENT '月底预测逾期率', 
	  `target_sale_value` decimal(26,4) COMMENT '预测收入', 
	  `receivable_amount_target` decimal(26,6) COMMENT '回款目标:取1号预测回款金额', 
	  `unreceivable_amount` decimal(26,4) COMMENT '无法回款金额', 
	  `current_receivable_amount` decimal(26,4) COMMENT '当期回款金额', 
	  `need_receivable_amount` decimal(26,6) COMMENT '可回款金额:回款目标-当期回款金额', 
	  `temp_1` decimal(26,6) COMMENT '预留', 
	  `temp_2` decimal(26,6) COMMENT '预留', 
	  `temp_3` decimal(26,6) COMMENT '预留', 
	  `law_is_flag` int COMMENT '法务介入标识', 
	  `update_time` timestamp COMMENT '更新时间')
	COMMENT '预测回款金额-帆软'
	PARTITIONED BY ( 
	  `sdt` string COMMENT '日期分区')