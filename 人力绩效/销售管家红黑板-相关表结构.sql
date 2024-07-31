 csx_dws.csx_dws_sale_detail_di 

col_name	data_type	comment
id	string	主键id(credential_no成本核算凭证号&goods_code商品编码)
sale_time	timestamp	销售时间(表示与sdt同一天的时分秒时间格式)
order_time	timestamp	下单时间
require_delivery_date	string	要求送货日期(b端专用,yyyymmdd)
delivery_time	timestamp	出库时间
receive_time	timestamp	签收时间
original_order_code	string	原销售单号(逆向单对应的正向单单号)
order_code	string	订单编号
business_type_code	int	业务类型编码(1.日配业务,2.福利业务,3.批发内购,4.城市服务商,5.省区大宗,6.bbc,7.大宗一部,8.大宗二部,9.商超)
business_type_name	string	业务类型名称
delivery_type_code	int	配送类型编码(1.配送,2.直送,3.自提,4.直通,11.同城配送,12.快递配送,13.一件代发)
delivery_type_name	string	配送类型名称
order_business_type_code	int	订单业务类型编码(1.日配,2.福利,3.大宗贸易,4.内购,11.云超,12.云创,13.bbc,14.永辉生活)
order_business_type_name	string	订单业务类型名称
order_channel_detail_code	int	下单渠道细分编码(11.中台,12.小程序,13.红旗,21.云超,22.云创,23.bbc,24.永辉生活,25.返利,26.价格补救,27.调价)
order_channel_detail_name	string	下单渠道名称细分
operation_mode_code	int	经营方式编码(0.自营,1.联营)
operation_mode_name	string	经营方式名称
customer_code	string	客户编码
customer_name	string	客户名称
sub_customer_code	string	子客户编码
sub_customer_name	string	子客户名称
channel_code	string	渠道编码(1.大客户,2.商超,4.大宗,5.供应链(食百),6.供应链(生鲜),8.其他,9.业务代理)
channel_name	string	渠道名称
business_attribute	string	商机属性(1:日配,2:福利,3:大宗贸易,4:m端,5:bbc,6:内购)
business_attribute_desc	string	商机属性描述
first_category_code	string	一级客户分类编码
first_category_name	string	一级客户分类名称
second_category_code	string	二级客户分类编码
second_category_name	string	二级客户分类名称
third_category_code	string	三级客户分类编码
third_category_name	string	三级客户分类名称
shop_format_code	string	(永辉门店业态)销售归属标识,1_云超,2_云创会员店,3_云创超级物种,4_企业购,5_彩食鲜,6_云创到家,7_上蔬托管联华,8_云超mini,-1_其他(m端专用)
shop_format_name	string	永辉门店业态描述
shop_company_code	string	门店所属公司编码(m端专用)
shop_company_name	string	门店所属公司名称
performance_region_code	string	业绩大区编码
performance_region_name	string	业绩大区名称
performance_province_code	string	业绩省区编码
performance_province_name	string	业绩省区名称
performance_city_code	string	业绩城市编码
performance_city_name	string	业绩城市名称
sign_time	timestamp	客户签约时间
sales_user_id	bigint	业务员id
sales_user_number	string	业务员工号
sales_user_name	string	业务员名称
sales_user_position	string	业务员职务
supervisor_user_id	bigint	销售主管id
supervisor_user_number	string	销售主管工号
supervisor_user_name	string	销售主管名称
sign_company_code	string	签约公司编码
sign_company_name	string	签约公司名称
agreement_dc_code	string	履约地点编码
agreement_dc_name	string	履约地点名称
company_code	string	销售出库主体公司编码
company_name	string	销售出库主体公司名称
inventory_dc_code	string	库存地点编码
inventory_dc_name	string	库存地点名称
inventory_dc_province_code	string	库存地点所在的省区编码
inventory_dc_province_name	string	库存地点所在的省区名称
inventory_dc_city_code	string	库存地点所在的城市编码
inventory_dc_city_name	string	库存地点所在的城市名称
goods_code	string	商品编码
goods_name	string	商品名称
goods_bar_code	string	商品条码
spec	string	规格
unit_name	string	计量单位描述
purchase_group_code	string	采购组(课组)编码
purchase_group_name	string	采购组(课组)名称
business_division_code	string	业务部编码(11.生鲜,12.食百)
business_division_name	string	业务部名称(生鲜,食百)
division_code	string	部类编号
division_name	string	部类描述
classify_large_code	string	管理大类编号
classify_large_name	string	管理大类名称
classify_middle_code	string	管理中类编号
classify_middle_name	string	管理中类名称
classify_small_code	string	管理小类编号
classify_small_name	string	管理小类名称
brand_code	string	品牌编码
brand_name	string	品牌名称
goods_tax_code	string	商品税务编码
goods_tax_name	string	商品税务名称
is_factory_goods_flag	int	是否工厂商品(0.否,1.是)(字段来源主数据商品表)
supplier_code	string	供应商编码
supplier_name	string	供应商名称
purchase_price	decimal(20,6)	采购报价
cost_price	decimal(20,6)	含税成本单价
sale_price	decimal(20,6)	含税销售单价
purchase_qty	decimal(20,6)	购买数量
sale_qty	decimal(20,6)	销售数量
sale_amt	decimal(20,6)	含税销售金额
sale_cost	decimal(20,6)	含税销售成本
profit	decimal(20,6)	含税定价毛利额
sale_amt_no_tax	decimal(20,6)	未税销售金额
sale_cost_no_tax	decimal(20,6)	未税销售成本
profit_no_tax	decimal(20,6)	未税定价毛利额
tax_code	string	税码
tax_rate	decimal(5,3)	税率
tax_amt	decimal(20,6)	税金
refund_order_flag	int	退货订单标识(0.正向单,1.逆向单)
wms_biz_type_code	string	wms业务类型编码
wms_biz_type_name	string	wms业务类型名称
daoza_qty	decimal(15,6)	倒杂数量
daoza_price_no_tax	decimal(15,6)	倒杂单价
daoza_amt_no_tax	decimal(20,6)	倒杂金额
task_sync_time	timestamp	数据最后一次同步时间
adjustment_amt	decimal(20,6)	含税调整金额
adjustment_amt_no_tax	decimal(20,6)	不含税调整金额
adjustment_codes	string	调整单号列表
credit_code	string	信控编码
order_channel_code	int	订单来源渠道(1.b端,2.m端,3.bbc,4.返利,5.价格补救,6.调价,-1.sap)
source_order_code	string	来源单号
wms_order_code	string	wms订单编码
direct_delivery_type	int	直送类型: 0-p(普通) 1-r(融单)、2-z(过账)、11-(临时加单)、12-(紧急补货)
sdt	string	销售日期{\"FORMAT\":\"yyyymmdd\"}
	NULL	NULL
# Partition Information	NULL	NULL
# col_name	data_type	comment
sdt	string	销售日期{\"FORMAT\":\"yyyymmdd\"}



csx_analyse.csx_analyse_tc_person_profit_target_rate
col_name	data_type	comment
biz_id	string	业务主键
region_code	string	大区编码
region_name	string	大区名称
province_code	string	省区编码
province_name	string	省区名称
city_group_code	string	城市编码
city_group_name	string	城市名称
user_position	string	岗位类别
sales_id	string	销售员id
work_no	string	销售员工号
sales_name	string	销售员
begin_date	string	入职日期
begin_less_1year_flag	string	入职是否小于1年
sale_amt	decimal(20,6)	销售额
profit	decimal(20,6)	毛利额
profit_basic	decimal(20,6)	毛利目标
profit_target_rate	decimal(20,6)	毛利目标达成系数
update_time	timestamp	更新时间
smt_ct	string	日期分区复制
smt	string	日期分区{\"FORMAT\":\"yyyymm\"}
	NULL	NULL
# Partition Information	NULL	NULL
# col_name	data_type	comment
smt	string	日期分区{\"FORMAT\":\"yyyymm\"}


desc csx_dws.csx_dws_crm_customer_business_active_di  客户动态信息
customer_id	bigint	客户id
customer_code	string	客户编码
customer_name	string	客户名称
business_type_code	int	业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
business_type_name	string	业务类型名称
channel_code	string	渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
channel_name	string	渠道名称
sales_user_id	bigint	业务员id
sales_user_number	string	业务员工号
sales_user_name	string	业务员名称
sales_user_position	string	业务员职务
sales_province_id	string	销售归属省区id
sales_province_name	string	销售归属省区名称
sales_city_code	string	销售归属城市编码
sales_city_name	string	销售归属城市名称
performance_region_code	string	业绩大区编码
performance_region_name	string	业绩大区名称
performance_province_code	string	业绩省区编码
performance_province_name	string	业绩省区名称
performance_city_code	string	业绩城市编码
performance_city_name	string	业绩城市名称
business_attribute_code	int	商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购
business_attribute_name	string	商机属性名称
business_sign_date	string	最近业务类型签约日期
first_business_sign_date	string	首次业务类型签约日期
first_business_sale_date	string	首次业务类型销售日期
last_business_sale_date	string	末次业务类型销售日期
sale_business_active_days	int	销售业务类型活跃天数(即历史至今有销售的日期)
sub_sale_business_active_days	int	减数销售业务类型活跃天数(只用作计算客户总活跃天数使用)
sale_business_total_amt	decimal(20,6)	销售业务类型总金额
sub_business_sale_amt	decimal(20,6)	减数业务类型金额(只用作计算销售总金额使用)
sdt	string	导入日期{\"FORMAT\":\"yyyymmdd\"}
	NULL	NULL
# Partition Information	NULL	NULL
# col_name	data_type	comment
sdt	string	导入日期{\"FORMAT\":\"yyyymmdd\"}


desc csx_dws.csx_dws_crm_customer_active_di
col_name	data_type	comment
customer_id	bigint	客户id
customer_code	string	客户编码
customer_name	string	客户名称
channel_code	string	渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
channel_name	string	渠道名称
sales_user_id	bigint	业务员id
sales_user_number	string	业务员工号
sales_user_name	string	业务员名称
sales_user_position	string	业务员职务
supervisor_user_id	bigint	销售主管id
supervisor_user_number	string	销售主管工号
supervisor_user_name	string	销售主管名称
sales_province_id	string	销售归属省区id
sales_province_name	string	销售归属省区名称
sales_city_code	string	销售归属城市编码
sales_city_name	string	销售归属城市名称
performance_region_code	string	业绩大区编码
performance_region_name	string	业绩大区名称
performance_province_code	string	业绩省区编码
performance_province_name	string	业绩省区名称
performance_city_code	string	业绩城市编码
performance_city_name	string	业绩城市名称
business_attribute	string	商机属性(1：日配 2：福利 3：大宗贸易 4：m端 5：bbc 6：内购)
business_attribute_desc	string	商机属性描述
customer_acquisition_type_code	int	获客方式编码(1:投标,2:非投标)
customer_acquisition_type_name	string	获客方式名称
sign_date	string	最新签约日期
first_sign_date	string	首次签约日期
first_sale_date	string	首次销售日期
last_sale_date	string	末次销售日期
sale_active_days	int	销售活跃天数(即历史至今有销售的日期)
sub_sale_active_days	int	减数销售活跃天数(只用作计算客户总活跃天数使用)
sale_total_amt	decimal(20,6)	销售总金额
sub_sale_amt	decimal(20,6)	减数金额(只用作计算销售总金额使用)
2b_first_order_date	string	b端首次下单日期
2b_last_order_date	string	b端末次下单日期
2b_order_active_days	int	b端下单客户活跃天数
sub_2b_order_active_days	int	减数b端下单活跃天数(只用作计算b端系统活跃天数使用)
sdt	string	导入日期{\"FORMAT\":\"yyyymmdd\"}
	NULL	NULL
# Partition Information	NULL	NULL
# col_name	data_type	comment
sdt	string	导入日期{\"FORMAT\":\"yyyymmdd\"}



createtab_stmt
CREATE EXTERNAL TABLE `csx_analyse`.`csx_analyse_report_crm_customer_sale_service_manager_info_df`(
  `biz_id` string COMMENT '唯一id', 
  `region_code` string COMMENT '客户大区编码', 
  `region_name` string COMMENT '客户大区名称', 
  `province_code` string COMMENT '客户省区编码', 
  `province_name` string COMMENT '客户省区名称', 
  `city_group_code` string COMMENT '客户城市编码', 
  `city_group_name` string COMMENT '客户城市名称', 
  `region_code_sales` string COMMENT '销售员大区编码', 
  `region_name_sales` string COMMENT '销售员大区名称', 
  `province_code_sales` string COMMENT '销售员省份编码', 
  `province_name_sales` string COMMENT '销售员省份名称', 
  `city_group_code_sales` string COMMENT '销售员城市组编码', 
  `city_group_name_sales` string COMMENT '销售员城市组名称', 
  `region_code_rp_service` string COMMENT '日配服务管家大区编码', 
  `region_name_rp_service` string COMMENT '日配服务管家大区名称', 
  `province_code_rp_service` string COMMENT '日配服务管家省份编码', 
  `province_name_rp_service` string COMMENT '日配服务管家省份名称', 
  `city_group_code_rp_service` string COMMENT '日配服务管家城市组编码', 
  `city_group_name_rp_service` string COMMENT '日配服务管家城市组名称', 
  `region_code_fl_service` string COMMENT '福利服务管家大区编码', 
  `region_name_fl_service` string COMMENT '福利服务管家大区名称', 
  `province_code_fl_service` string COMMENT '福利服务管家省份编码', 
  `province_name_fl_service` string COMMENT '福利服务管家省份名称', 
  `city_group_code_fl_service` string COMMENT '福利服务管家城市组编码', 
  `city_group_name_fl_service` string COMMENT '福利服务管家城市组名称', 
  `region_code_bbc_service` string COMMENT 'bbc服务管家大区编码', 
  `region_name_bbc_service` string COMMENT 'bbc服务管家大区名称', 
  `province_code_bbc_service` string COMMENT 'bbc服务管家省份编码', 
  `province_name_bbc_service` string COMMENT 'bbc服务管家省份名称', 
  `city_group_code_bbc_service` string COMMENT 'bbc服务管家城市组编码', 
  `city_group_name_bbc_service` string COMMENT 'bbc服务管家城市组名称', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道名称', 
  `customer_id` string COMMENT '客户id', 
  `customer_no` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `sales_id` string COMMENT '主销售员id', 
  `work_no` string COMMENT '销售员工号', 
  `sales_name` string COMMENT '销售员', 
  `user_position` string COMMENT '销售员岗位', 
  `sales_id_new` string COMMENT '主销售员id_new', 
  `work_no_new` string COMMENT '销售员工号_new', 
  `sales_name_new` string COMMENT '销售员_new', 
  `user_position_new` string COMMENT '销售员岗位_new', 
  `first_supervisor_code` string COMMENT '一级主管编码', 
  `first_supervisor_work_no` string COMMENT '一级主管工号', 
  `first_supervisor_name` string COMMENT '一级主管姓名', 
  `third_supervisor_code` string COMMENT '三级主管编码', 
  `third_supervisor_work_no` string COMMENT '三级主管工号', 
  `third_supervisor_name` string COMMENT '三级主管姓名', 
  `fourth_supervisor_code` string COMMENT '四级主管编码', 
  `fourth_supervisor_work_no` string COMMENT '四级主管工号', 
  `fourth_supervisor_name` string COMMENT '四级主管姓名', 
  `rp_service_user_work_no` string COMMENT '日配_服务管家工号', 
  `rp_service_user_name` string COMMENT '日配_服务管家', 
  `rp_service_user_id` string COMMENT '日配_服务管家id', 
  `rp_service_user_position` string COMMENT '日配_服务管家岗位', 
  `rp_service_user_work_no_new` string COMMENT '日配_服务管家工号_new', 
  `rp_service_user_name_new` string COMMENT '日配_服务管家_new', 
  `rp_service_user_id_new` string COMMENT '日配_服务管家id_new', 
  `rp_service_user_position_new` string COMMENT '日配_服务管家岗位_new', 
  `fl_service_user_work_no` string COMMENT '福利_服务管家工号', 
  `fl_service_user_name` string COMMENT '福利_服务管家', 
  `fl_service_user_id` string COMMENT '福利_服务管家id', 
  `fl_service_user_position` string COMMENT '福利_服务管家岗位', 
  `dz_service_user_work_no` string COMMENT '省区大宗_服务管家工号', 
  `dz_service_user_name` string COMMENT '省区大宗_服务管家', 
  `dz_service_user_id` string COMMENT '省区大宗_服务管家id', 
  `dz_service_user_position` string COMMENT '省区大宗_服务管家岗位', 
  `bbc_service_user_work_no` string COMMENT 'bbc_服务管家工号', 
  `bbc_service_user_name` string COMMENT 'bbc_服务管家', 
  `bbc_service_user_id` string COMMENT 'bbc_服务管家id', 
  `bbc_service_user_position` string COMMENT 'bbc_服务管家岗位', 
  `ng_service_user_work_no` string COMMENT '内购_服务管家工号', 
  `ng_service_user_name` string COMMENT '内购_服务管家', 
  `ng_service_user_id` string COMMENT '内购_服务管家id', 
  `ng_service_user_position` string COMMENT '内购_服务管家岗位', 
  `fl_service_user_work_no_new` string COMMENT '福利_服务管家工号_new', 
  `fl_service_user_name_new` string COMMENT '福利_服务管家_new', 
  `fl_service_user_id_new` string COMMENT '福利_服务管家id_new', 
  `fl_service_user_position_new` string COMMENT '福利_服务管家岗位_new', 
  `bbc_service_user_work_no_new` string COMMENT 'bbc_服务管家工号_new', 
  `bbc_service_user_name_new` string COMMENT 'bbc_服务管家_new', 
  `bbc_service_user_id_new` string COMMENT 'bbc_服务管家id_new', 
  `bbc_service_user_position_new` string COMMENT 'bbc_服务管家岗位_new', 
  `is_sale` string COMMENT '是否有销售', 
  `is_overdue` string COMMENT '是否有逾期', 
  `is_strategy_department` string COMMENT '销售员是否属于战略部', 
  `rp_sales_sale_rate` decimal(20,6) COMMENT '日配销售员_销售额提成比例', 
  `rp_sales_profit_rate` decimal(20,6) COMMENT '日配销售员_毛利提成比例', 
  `rp_service_user_sale_rate` decimal(20,6) COMMENT '日配服务管家_销售额提成比例', 
  `rp_service_user_profit_rate` decimal(20,6) COMMENT '日配服务管家_毛利提成比例', 
  `fl_sales_sale_rate` decimal(20,6) COMMENT '福利销售员_销售额提成比例', 
  `fl_sales_profit_rate` decimal(20,6) COMMENT '福利销售员_毛利提成比例', 
  `fl_service_user_sale_rate` decimal(20,6) COMMENT '福利服务管家_销售额提成比例', 
  `fl_service_user_profit_rate` decimal(20,6) COMMENT '福利服务管家_毛利提成比例', 
  `bbc_sales_sale_rate` decimal(20,6) COMMENT 'bbc销售员_销售额提成比例', 
  `bbc_sales_profit_rate` decimal(20,6) COMMENT 'bbc销售员_毛利提成比例', 
  `bbc_service_user_sale_rate` decimal(20,6) COMMENT 'bbc服务管家_销售额提成比例', 
  `bbc_service_user_profit_rate` decimal(20,6) COMMENT 'bbc服务管家_毛利提成比例', 
  `is_sales_xuni` string COMMENT '是否虚拟销售员', 
  `update_time` timestamp COMMENT '更新时间', 
  `employee_status_sales` string COMMENT '销售员雇佣状态', 
  `employee_status_supervisor` string COMMENT '一级主管雇佣状态', 
  `employee_status_rp_service` string COMMENT '日配_服务管家雇佣状态', 
  `employee_status_fl_service` string COMMENT '福利_服务管家雇佣状态', 
  `employee_status_bbc_service` string COMMENT 'BBC_服务管家雇佣状态', 
  `second_supervisor_code` string COMMENT '销售经理编码', 
  `second_supervisor_work_no` string COMMENT '销售经理工号', 
  `second_supervisor_name` string COMMENT '销售经理姓名', 
  `employee_status_sales_manager` string COMMENT '销售经理雇佣状态')
COMMENT '客户销售员与服务管家对应表'
PARTITIONED BY ( 
  `sdt` string COMMENT '日期分区{"FORMAT":"yyyymmdd"}')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_analyse/csx_analyse_report_crm_customer_sale_service_manager_info_df'

createtab_stmt
CREATE EXTERNAL TABLE `csx_dws`.`csx_dws_sss_customer_credit_invoice_bill_settle_stat_di`(
  `group_code` string COMMENT '集团号', 
  `group_name` string COMMENT '集团名', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `credit_code` string COMMENT '信控号', 
  `company_code` string COMMENT '公司编码', 
  `company_name` string COMMENT '公司名称', 
  `region_code` string COMMENT '大区编码', 
  `region_name` string COMMENT '大区名称', 
  `province_code` string COMMENT '省区编码', 
  `province_name` string COMMENT '省区名称', 
  `city_group_code` string COMMENT '城市编码', 
  `city_group_name` string COMMENT '城市名称', 
  `channel_code` string COMMENT '渠道编码', 
  `channel_name` string COMMENT '渠道名称', 
  `customer_attribute_code` string COMMENT '客户属性编码', 
  `customer_attribute_name` string COMMENT '客户属性名称', 
  `sales_id` string COMMENT '销售员id', 
  `sales_employee_code` string COMMENT '销售员工号', 
  `sales_employee_name` string COMMENT '销售员名称', 
  `supervisor_user_id` bigint COMMENT '主管编码, b端客户：销售主管, s端：采购总监 大宗：主管', 
  `supervisor_user_number` string COMMENT '销售主管工号', 
  `supervisor_user_name` string COMMENT '销售主管名称', 
  `account_period_code` string COMMENT '账期编码', 
  `account_period_name` string COMMENT '账期名称', 
  `account_period_value` int COMMENT '账期值', 
  `customer_level` string COMMENT '客户等级', 
  `credit_limit` string COMMENT '信控额度', 
  `temp_credit_limit` string COMMENT '临时额度', 
  `temp_begin_time` timestamp COMMENT '临时额度起始时间', 
  `temp_end_time` timestamp COMMENT '临时额度截止时间', 
  `back_money_amount_month` decimal(36,2) COMMENT '回款金额_本月', 
  `pay_on_line_amount_month` decimal(36,2) COMMENT '线上支付金额_微信支付_对账表中字段_本月', 
  `bill_amt` decimal(36,2) COMMENT '上一结算周期排除期初的对账金额', 
  `unbill_amt` decimal(36,2) COMMENT '上一结算周期排除期初的未对账金额', 
  `invoice_amount` decimal(36,2) COMMENT '上一结算周期排除期初的开票金额', 
  `sale_amt` decimal(36,2) COMMENT '上一结算周期排除期初的财务含税销售额_财务业务确认以财务对账来源单为销售金额计算(张正孝)', 
  `tax_amt` decimal(36,2) COMMENT '上一结算周期排除期初的财务含税额', 
  `bill_amt_all` decimal(36,2) COMMENT '对账金额', 
  `unbill_amt_all` decimal(36,2) COMMENT '未对账金额', 
  `invoice_amount_all` decimal(36,2) COMMENT '开票金额', 
  `sale_amt_all` decimal(36,2) COMMENT '财务含税销售额_财务业务确认以财务对账来源单为销售金额计算', 
  `tax_amt_all` decimal(36,2) COMMENT '税额', 
  `overdue_amount` decimal(36,2) COMMENT '逾期金额', 
  `overdue_amount_15_day` decimal(36,2) COMMENT '逾期1-15天金额', 
  `overdue_amount_30_day` decimal(36,2) COMMENT '逾期15-30天金额', 
  `overdue_amount_60_day` decimal(36,2) COMMENT '逾期30-60天金额', 
  `overdue_amount_90_day` decimal(36,2) COMMENT '逾期60-90天金额', 
  `overdue_amount_120_day` decimal(36,2) COMMENT '逾期90-120天金额', 
  `overdue_amount_180_day` decimal(36,2) COMMENT '逾期120-180天金额', 
  `overdue_amount_1_year` decimal(36,2) COMMENT '逾期180天-1年金额', 
  `overdue_amount_2_year` decimal(36,2) COMMENT '逾期1年-2年金额', 
  `overdue_amount_3_year` decimal(36,2) COMMENT '逾期2年-3年金额', 
  `overdue_amount_more_3_year` decimal(36,2) COMMENT '逾期3年以上金额', 
  `no_overdue_amount` decimal(36,2) COMMENT '未逾期金额', 
  `receivable_amount` decimal(36,2) COMMENT '应收账款', 
  `bad_debt_amount` decimal(36,2) COMMENT '坏账金额', 
  `residue_amt` decimal(36,2) COMMENT '剩余预付款金额_预付款客户抵消订单金额后', 
  `residue_amt_sss` decimal(36,2) COMMENT '剩余预付款金额_原销售结算', 
  `max_overdue_day` int COMMENT '最大逾期天数', 
  `bill_rate` decimal(36,2) COMMENT '对账率', 
  `invoice_rate` decimal(36,2) COMMENT '开票率', 
  `overdue_rate` decimal(36,2) COMMENT '逾期率', 
  `overdue_coefficient_numerator` decimal(36,2) COMMENT '逾期金额*逾期天数 计算因子，用于计算逾期系数分子', 
  `overdue_coefficient_denominator` decimal(36,2) COMMENT '应收金额*账期天数 计算因子，用于计算逾期系数分母', 
  `claim_amt` decimal(36,2) COMMENT '认领金额_本月', 
  `claim_close_bill_amount` decimal(36,2) COMMENT '认领已核销金额_本月', 
  `claim_unclose_bill_amount` decimal(36,2) COMMENT '认领未核销金额_本月', 
  `claim_amt_all` decimal(36,2) COMMENT '总认领金额', 
  `claim_close_bill_amount_all` decimal(36,2) COMMENT '总认领已核销金额', 
  `claim_unclose_bill_amount_all` decimal(36,2) COMMENT '总认领未核销金额', 
  `unbill_amount_history` decimal(36,2) COMMENT '历史未对账金额', 
  `sales_manager_user_id` bigint COMMENT '销售总监id', 
  `sales_manager_user_number` string COMMENT '销售总监工号', 
  `sales_manager_user_name` string COMMENT '销售总监名称', 
  `city_manager_user_id` bigint COMMENT '城市经理id', 
  `city_manager_user_number` string COMMENT '城市经理工号', 
  `city_manager_user_name` string COMMENT '城市经理名称', 
  `province_manager_user_id` bigint COMMENT '省区总id', 
  `province_manager_user_number` string COMMENT '省区总工号', 
  `province_manager_user_name` string COMMENT '省区总名称', 
  `dev_source_code` int COMMENT '开发来源编码(1:自营,2:业务代理人,3:城市服务商,4:内购)', 
  `dev_source_name` string COMMENT '开发来源名称', 
  `credit_business_attribute_code` string COMMENT '信控业务属性编码', 
  `credit_business_attribute_name` string COMMENT '信控业务属性名称', 
  `no_invoice_amt_all` decimal(36,4) COMMENT '未开票金额', 
  `no_invoice_amt` decimal(36,4) COMMENT '未开票金额(上周期)', 
  `invoice_amt_history` decimal(36,4) COMMENT '历史开票金额', 
  `no_invoice_amt_history` decimal(36,4) COMMENT '历史未开票金额', 
  `bill_amt_history` decimal(36,4) COMMENT '历史对账金额')
COMMENT '客户开票对账回款逾期信息表_信控号维度'
PARTITIONED BY ( 
  `sdt` string COMMENT '统计日期{"FORMAT":"yyyymmdd"}')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_dws/csx_dws_sss_customer_credit_invoice_bill_settle_stat_di'
TBLPROPERTIES (
  'TRANSLATED_TO_EXTERNAL'='TRUE', 
  'bucketing_version'='2', 
  'external.table.purge'='TRUE', 
  'last_modified_by'='hivemeta', 
  'last_modified_time'='1698041306', 
  'transient_lastDdlTime'='1698041306')



createtab_stmt
CREATE EXTERNAL TABLE `csx_dim`.`csx_dim_crm_business_info`(
  `business_number` string COMMENT '商机编号', 
  `customer_id` bigint COMMENT '客户id', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `owner_user_id` bigint COMMENT '归属人id', 
  `owner_user_number` string COMMENT '归属人工号', 
  `owner_user_name` string COMMENT '归属人姓名', 
  `owner_user_position` string COMMENT '归属人职位', 
  `supervisor_user_id` bigint COMMENT '销售主管id', 
  `supervisor_user_number` string COMMENT '销售主管工号', 
  `supervisor_user_name` string COMMENT '销售主管名称', 
  `owner_province_id` string COMMENT '所属省区id', 
  `owner_province_name` string COMMENT '所属省区名称', 
  `owner_city_code` string COMMENT '所属城市编码', 
  `owner_city_name` string COMMENT '所属城市名称', 
  `performance_region_code` string COMMENT '业绩大区编码', 
  `performance_region_name` string COMMENT '业绩大区名称', 
  `performance_province_code` string COMMENT '业绩省区编码', 
  `performance_province_name` string COMMENT '业绩省区名称', 
  `performance_city_code` string COMMENT '业绩城市编码', 
  `performance_city_name` string COMMENT '业绩城市名称', 
  `business_attribute_code` int COMMENT '商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购', 
  `business_attribute_name` string COMMENT '商机属性名称', 
  `sign_type_code` int COMMENT '审批流类型 1:新客 2：商机', 
  `status` int COMMENT '是否有效 0无效 1有效', 
  `approval_id` bigint COMMENT '审批id', 
  `approval_status_code` int COMMENT '审批状态编码 0:待发起 1：审批中 2：审批完成 3：审批拒绝', 
  `approval_status_name` string COMMENT '审批状态名称', 
  `business_stage` int COMMENT '阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5', 
  `business_sign_time` timestamp COMMENT '业务类型签约时间', 
  `first_business_sign_time` timestamp COMMENT '首次业务类型签约时间', 
  `channel_code` string COMMENT '渠道编号-1.大客户 2.商超(对内) 3.商超(对外) 4.大宗 5.供应链(食百) 6.供应链(食百) 7.企业购 8.其他', 
  `channel_name` string COMMENT '渠道名称', 
  `first_category_code` string COMMENT '一级客户分类编码', 
  `first_category_name` string COMMENT '一级客户分类名称', 
  `second_category_code` string COMMENT '二级客户分类编码', 
  `second_category_name` string COMMENT '二级客户分类名称', 
  `third_category_code` string COMMENT '三级客户分类编码', 
  `third_category_name` string COMMENT '三级客户分类名称', 
  `customer_address_details` string COMMENT '客户详细地址json', 
  `customer_address_full` string COMMENT '客户详细地址', 
  `longitude` string COMMENT '经度', 
  `latitude` string COMMENT '纬度', 
  `contact_person` string COMMENT '联系人姓名', 
  `contact_phone` string COMMENT '联系电话', 
  `contract_number` string COMMENT '合同编号', 
  `contract_type` int COMMENT '合同类型 1临时合同 2正式合同', 
  `contract_must` int COMMENT '是否需签订合同 0否 1是', 
  `contract_begin_date` timestamp COMMENT '合同起始日期', 
  `contract_end_date` timestamp COMMENT '合同终止日期', 
  `last_visit_time` timestamp COMMENT '最后拜访时间', 
  `guide_user_id` bigint COMMENT '指导人id', 
  `guide_user_name` string COMMENT '指导人名称', 
  `estimate_contract_amount` string COMMENT '预估合同签约金额', 
  `other_needs_code` string COMMENT '其他需求编码 1:餐卡、2:福利、3:商城对接、4:微信支付、5:无', 
  `other_needs_name` string COMMENT '其他需求名称', 
  `types_cooperation` string COMMENT '合作品类', 
  `price_type_code` int COMMENT '报价类型编码 1:下单前报价  2:发货前报价  3:发货后报价', 
  `price_type_name` string COMMENT '报价类型名称', 
  `price_period_code` int COMMENT '报价周期编码 1:每天 2:每周 3:每半月4：每月', 
  `price_period_name` string COMMENT '报价周期名称', 
  `price_date_code` int COMMENT '报价日期编码', 
  `price_date_name` string COMMENT '报价日期名称', 
  `contract_cycle` string COMMENT '合同周期', 
  `expect_sign_time` string COMMENT '预计签约时间', 
  `expect_execute_time` string COMMENT '预计履约时间', 
  `gross_profit_rate` string COMMENT '预计毛利率', 
  `business_introduction` string COMMENT '业务模式介绍', 
  `estimate_once_amount` string COMMENT '预估一次性配送金额', 
  `estimate_month_amount` string COMMENT '预估月度配送金额', 
  `estimate_delivery_times` string COMMENT '预估配送次数', 
  `invoice_requirement_code` int COMMENT '发票要求编码 1.专用 2.普通 3.皆可', 
  `invoice_requirement_name` string COMMENT '发票要求名称', 
  `meals_person_count` int COMMENT '用餐人数', 
  `meals_avg_amount` string COMMENT '人均餐标', 
  `site_name` string COMMENT '站点名称', 
  `bbc_credit_type_code` string COMMENT '授信类型编码', 
  `bbc_credit_type_name` string COMMENT '授信类型名称', 
  `employees` int COMMENT '企业人数', 
  `expect_project_begin_time` string COMMENT '预计项目开始时间', 
  `expect_project_end_time` string COMMENT '预计项目结束时间', 
  `certificate_photos` string COMMENT '企业证照资料', 
  `legal_person_photos` string COMMENT '个人证明资料（法定代表人或实际控制人）', 
  `customer_site_photos` string COMMENT '企业经营现场拍照', 
  `attachment_list` string COMMENT '附件列表', 
  `create_time` timestamp COMMENT '创建时间', 
  `create_by` string COMMENT '创建人', 
  `update_time` timestamp COMMENT '更新时间', 
  `update_by` string COMMENT '更新人', 
  `customer_address_province_code` string COMMENT '客户地址省区编码', 
  `customer_address_province_name` string COMMENT '客户地址省区名称', 
  `customer_address_city_code` string COMMENT '客户地址城市编码', 
  `customer_address_city_name` string COMMENT '客户地址城市名称', 
  `first_sign_time` string COMMENT '客户第一次签约时间', 
  `business_type_code` int COMMENT '业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)', 
  `business_type_name` string COMMENT '业务类型名称', 
  `customer_acquisition_type_code` int COMMENT '获客方式编码', 
  `customer_acquisition_type_name` string COMMENT '获客方式名称', 
  `price_set_type` string COMMENT '定价方式 编号,编号', 
  `contract_cycle_int` int COMMENT '新合同周期 0小于1个月', 
  `invoice_time` timestamp COMMENT '开票时间', 
  `reconciliation_time` int COMMENT '对账日期 1:每月1-5日 2:每月6-15日 3:其它', 
  `receive_time` timestamp COMMENT '销售单整单签收时间', 
  `union_bid_status` int COMMENT '是否联合招标', 
  `only_supplier_status` int COMMENT '是否唯一供应商', 
  `expect_bid_win_share` string COMMENT '预计可履约中标份额 100.00', 
  `company_code` string COMMENT '公司代码', 
  `account_period_code` string COMMENT '账期编码', 
  `cooperation_intention_photos` string COMMENT '客户确定合作意向截图', 
  `contract_cycle_desc` string COMMENT '新合同周期描述', 
  `credit_code` string COMMENT '信控编号', 
  `credit_create_time` timestamp COMMENT '信控创建时间')
COMMENT '商机信息表'
PARTITIONED BY ( 
  `sdt` string COMMENT '导入日期{format:yyyymmdd}{"FORMAT":"yyyymmdd"}')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_dim/csx_dim_crm_business_info'
TBLPROPERTIES (
  'DO_NOT_UPDATE_STATS'='true', 
  'STATS_GENERATED'='TASK', 
  'bucketing_version'='2', 
  'discover.partitions'='true', 
  'impala.events.catalogServiceId'='d623cb5a2d274646:a1de0f25f0c58bd7', 
  'impala.events.catalogVersion'='353226', 
  'impala.lastComputeStatsTime'='1684556722', 
  'last_modified_by'='hivemeta', 
  'last_modified_time'='1682056037', 
  'spark.sql.partitionProvider'='catalog', 
  'transient_lastDdlTime'='1713427426')


;


createtab_stmt
CREATE EXTERNAL TABLE `csx_analyse`.`csx_analyse_fr_ts_return_order_detail_di`(
  `biz_id` string COMMENT '业务主键', 
  `performance_region_name` string COMMENT '大区', 
  `performance_province_name` string COMMENT '省区', 
  `performance_city_name` string COMMENT '城市', 
  `inventory_dc_code` string COMMENT '库存dc编码', 
  `inventory_dc_name` string COMMENT '库存dc名称', 
  `source_type_name` string COMMENT '订单来源', 
  `sdt_refund` string COMMENT '退货申请日期', 
  `week` string COMMENT '彩食鲜周', 
  `refund_code` string COMMENT '退货单号', 
  `sale_order_code` string COMMENT '销售单号', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `sub_customer_code` string COMMENT '子客户编码', 
  `sub_customer_name` string COMMENT '子客户名称', 
  `goods_code` string COMMENT '商品编码', 
  `goods_name` string COMMENT '商品名称', 
  `source_biz_type_name` string COMMENT '订单来源', 
  `refund_operation_type_name` string COMMENT '退货处理方式', 
  `has_goods_name` string COMMENT '是否有实物退回', 
  `responsibility_reason` string COMMENT '定责原因', 
  `reason_detail` string COMMENT '原因说明', 
  `business_type_name` string COMMENT '业务类型', 
  `delivery_type_name` string COMMENT '物流模式', 
  `refund_order_type_name` string COMMENT '退货单类型', 
  `refund_qty` decimal(20,6) COMMENT '退货数量', 
  `refund_total_amt` decimal(20,6) COMMENT '退货总金额', 
  `refund_scale_total_amt` decimal(20,6) COMMENT '处理后退货金额', 
  `delivery_date` string COMMENT '出库日期', 
  `sale_price` decimal(20,6) COMMENT '团购价', 
  `send_qty` decimal(20,6) COMMENT '出库数量', 
  `sale_amt` decimal(20,6) COMMENT '销售金额', 
  `first_level_reason_name` string COMMENT '客退原因-一级', 
  `second_level_reason_name` string COMMENT '客退原因-二级', 
  `stock_process_type` string COMMENT '库存处理方式', 
  `stock_process_confirm` string COMMENT '是否确认', 
  `responsible_department_name` string COMMENT '责任部门名称', 
  `status` string COMMENT '状态', 
  `is_appeal` string COMMENT '是否申诉', 
  `appeal_reason` string COMMENT '申诉理由', 
  `child_return_type_name` string COMMENT '子退货单类型', 
  `update_time` string COMMENT '报表更新时间', 
  `order_status_name` string COMMENT '订单状态', 
  `refund_amt` decimal(20,6) COMMENT '退货金额', 
  `refund_order_status_name` string COMMENT '退货单状态', 
  `classify_large_code` string COMMENT '管理大类编号', 
  `classify_large_name` string COMMENT '管理大类名称', 
  `classify_middle_code` string COMMENT '管理中类编号', 
  `classify_middle_name` string COMMENT '管理中类名称', 
  `classify_small_code` string COMMENT '管理小类编号', 
  `classify_small_name` string COMMENT '管理小类名称', 
  `images` string COMMENT '图片', 
  `refund_reason` string COMMENT '退货原因', 
  `create_by` string COMMENT '创建人', 
  `product_type_name` string COMMENT '商品归属部门', 
  `refund_responsible_version` int COMMENT '客退版本号', 
  `frmloss_amt` decimal(20,6) COMMENT '报损商品金额', 
  `frmloss_amt_no_tax` decimal(20,6) COMMENT '不含税报损商品金额', 
  `customer_large_level` string COMMENT '客户等级', 
  `business_type_code` string COMMENT '业务类型编码(1.日配业务,2.福利业务,3.批发内购,4.城市服务商,5.省区大宗,6.bbc,7.大宗一部,8.大宗二部,9.商超)', 
  `business_type_name_1` string COMMENT '业务类型名称', 
  `direct_delivery_type` string COMMENT '直送类型')
COMMENT '客服效率-退货复盘数据推送'
PARTITIONED BY ( 
  `sdt` string COMMENT '日期分区：退货申请日期{"FORMAT":"yyyymmdd"}')
;



createtab_stmt
CREATE EXTERNAL TABLE `csx_dws`.`csx_dws_crm_customer_visit_record_di`(
  `id` bigint COMMENT 'id', 
  `customer_id` bigint COMMENT '客户id', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `business_attribute_code` int COMMENT '商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购', 
  `business_attribute_name` string COMMENT '商机属性名称', 
  `sales_user_id` string COMMENT '业务员id', 
  `sales_user_name` string COMMENT '业务员名称', 
  `first_category_code` string COMMENT '一级客户分类编码', 
  `first_category_name` string COMMENT '一级客户分类名称', 
  `second_category_code` string COMMENT '二级客户分类编码', 
  `second_category_name` string COMMENT '二级客户分类名称', 
  `third_category_code` string COMMENT '三级客户分类编码', 
  `third_category_name` string COMMENT '三级客户分类名称', 
  `sign_time` timestamp COMMENT '签约时间', 
  `visit_user_id` bigint COMMENT '拜访用户id', 
  `visit_user_number` string COMMENT '拜访人工号', 
  `visit_user_name` string COMMENT '拜访用户名称', 
  `visit_user_position` string COMMENT '拜访人职务', 
  `visit_type_code` int COMMENT '拜访类型编码:1:上门拜访,2:电话拜访,3:陪访', 
  `visit_type_name` string COMMENT '拜访类型名称', 
  `visit_target_code` string COMMENT '拜访目的编码', 
  `visit_target_name` string COMMENT '拜访目的名称', 
  `visit_imgs_url` string COMMENT '拍照图片列表', 
  `visit_time` timestamp COMMENT '拜访时间', 
  `visit_summary` string COMMENT '拜访纪要', 
  `visit_location` string COMMENT '拜访定位', 
  `contact_person` string COMMENT '联系人姓名', 
  `contact_phone` string COMMENT '联系人电话', 
  `address` string COMMENT '地址', 
  `create_by` string COMMENT '创建人', 
  `create_time` timestamp COMMENT '创建时间', 
  `update_by` string COMMENT '更新人', 
  `update_time` timestamp COMMENT '更新时间', 
  `sdt` string COMMENT '拜访日期', 
  `task_sync_time` timestamp COMMENT '任务同步时间')
COMMENT 'crm拜访记录事实表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_dws/csx_dws_crm_customer_visit_record_di'
TBLPROPERTIES (
  'TRANSLATED_TO_EXTERNAL'='TRUE', 
  'bucketing_version'='2', 
  'external.table.purge'='TRUE', 
  'transient_lastDdlTime'='1718732366')
;


createtab_stmt
CREATE EXTERNAL TABLE `csx_dim`.`csx_dim_crm_customer_business_ownership`(
  `biz_id` string COMMENT '业务主键(sdt分区日期&customer_id客户id&business_attribute_code商机属性编码&service_manager_user_id服务管家id)', 
  `customer_id` bigint COMMENT '客户id', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `channel_code` string COMMENT '渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)', 
  `channel_name` string COMMENT '渠道名称', 
  `business_attribute_code` int COMMENT '商机属性编码(1：日配 2：福利 3：大宗贸易 4：m端 5：bbc 6：内购)', 
  `business_attribute_name` string COMMENT '商机属性名称', 
  `sales_user_id` bigint COMMENT '业务员id', 
  `sales_user_number` string COMMENT '业务员工号', 
  `sales_user_name` string COMMENT '业务员名称', 
  `sales_user_position` string COMMENT '业务员职务', 
  `service_manager_user_id` bigint COMMENT '服务管家id', 
  `service_manager_user_number` string COMMENT '服务管家工号', 
  `service_manager_user_name` string COMMENT '服务管家名称', 
  `service_manager_user_position` string COMMENT '服务管家职务', 
  `is_exist_service_manager` int COMMENT '是否存在客户服务管家', 
  `is_customer_business_valid` int COMMENT '是否客户业务类型在客户信息表中有效', 
  `sales_province_id` string COMMENT '销售归属省区id', 
  `sales_province_name` string COMMENT '销售归属省区名称', 
  `sales_city_code` string COMMENT '销售归属城市编码', 
  `sales_city_name` string COMMENT '销售归属城市名称', 
  `performance_region_code` string COMMENT '业绩大区编码', 
  `performance_region_name` string COMMENT '业绩大区名称', 
  `performance_province_code` string COMMENT '业绩省区编码', 
  `performance_province_name` string COMMENT '业绩省区名称', 
  `performance_city_code` string COMMENT '业绩城市编码', 
  `performance_city_name` string COMMENT '业绩城市名称', 
  `first_category_code` string COMMENT '一级客户分类编码', 
  `first_category_name` string COMMENT '一级客户分类名称', 
  `second_category_code` string COMMENT '二级客户分类编码', 
  `second_category_name` string COMMENT '二级客户分类名称', 
  `third_category_code` string COMMENT '三级客户分类编码', 
  `third_category_name` string COMMENT '三级客户分类名称', 
  `archive_category_code` string COMMENT '档案分类编码(1:国有企业,2:私营企业,3:上市企业,4:外资/合资企业,5:个体工商户,6:永辉供应商,7:政府机关、事业单位、部队、监狱)', 
  `archive_category_name` string COMMENT '档案分类名称', 
  `cooperation_mode_code` string COMMENT '合作模式编码(01长期客户,02一次性客户)', 
  `cooperation_mode_name` string COMMENT '合作模式名称', 
  `customer_nature_code` string COMMENT '客户性质编码', 
  `customer_nature_name` string COMMENT '客户性质名称', 
  `social_credit_code` string COMMENT '统一社会信用代码', 
  `customer_acquisition_type_code` int COMMENT '获客方式编码(1:投标,2:非投标)', 
  `customer_acquisition_type_name` string COMMENT '获客方式名称', 
  `source_code` int COMMENT '来源编码(1.品牌推荐，2.客服400，3.搜索引擎，4.招投标网)', 
  `source_name` string COMMENT '来源名称', 
  `dev_source_code` int COMMENT '开发来源编码(1:自营,2:业务代理人,3:城市服务商,4:内购)', 
  `dev_source_name` string COMMENT '开发来源名称', 
  `contact_person` string COMMENT '联系人姓名', 
  `contact_phone` string COMMENT '联系电话', 
  `sign_time` timestamp COMMENT '签约时间')
COMMENT 'crm客户业务类型归属人员信息表'
PARTITIONED BY ( 
  `sdt` string COMMENT '分区日期{"FORMAT":"yyyymmdd"}')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_dim/csx_dim_crm_customer_business_ownership'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'discover.partitions'='true', 
  'transient_lastDdlTime'='1655272622')
;
-- 慧共享保证金
createtab_stmt
CREATE EXTERNAL TABLE `csx_analyse`.`csx_analyse_fr_sss_incidental_write_off_info_di`(
  `biz_id` string COMMENT '业务主键', 
  `belong_region_code` string COMMENT '大区编码', 
  `belong_region_name` string COMMENT '大区名称', 
  `performance_province_code` string COMMENT '省份编码', 
  `performance_province_name` string COMMENT '省份名称', 
  `incidental_expenses_no` string COMMENT '杂项用款单号', 
  `payment_unit_name` string COMMENT '签约主体', 
  `payment_company_code` string COMMENT '实际付款公司编码', 
  `payment_company_name` string COMMENT '实际付款公司名称', 
  `receiving_customer_code` string COMMENT '收款客户编码', 
  `receiving_customer_name` string COMMENT '收款客户名称', 
  `business_scene` string COMMENT '业务场景名称', 
  `business_scene_code` string COMMENT '业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约', 
  `payment_amount` decimal(15,6) COMMENT '付款金额', 
  `write_off_amount` decimal(15,6) COMMENT '核销金额', 
  `lave_write_off_amount` decimal(15,6) COMMENT '剩余待核销金额', 
  `payment_status` string COMMENT '付款状态  s:付款成功 t:已退汇', 
  `payment_status_name` string COMMENT '付款状态名称', 
  `payment_method` string COMMENT '支付方式  1:外付网银 2:线下', 
  `payment_method_name` string COMMENT '支付方式名称', 
  `apply_user` string COMMENT '申请人', 
  `responsible_person` string COMMENT '负责人', 
  `write_off_status` string COMMENT '核销状态  0:未核销  1:已核销 2:部分核销', 
  `write_off_status_name` string COMMENT '核销状态名称', 
  `write_off_type` string COMMENT '核销类型  1:慧共享核销  2:手工核销', 
  `write_off_type_name` string COMMENT '核销类型名称', 
  `apply_date` string COMMENT '单据申请日期', 
  `apply_reason` string COMMENT '申请事由', 
  `entry_company_code` string COMMENT '入账单位编码', 
  `entry_company_name` string COMMENT '入账单位名称', 
  `assignment_number` string COMMENT '分配号', 
  `audit_status` string COMMENT '审批状态', 
  `approved_date` string COMMENT '单据审批通过日期', 
  `voucher_code` string COMMENT '凭证编码', 
  `receiving_account` string COMMENT '客户收款账号', 
  `unpaid_progress` string COMMENT '未回款进度 1:合同未签署 2:退款流程中 3:合同已丢失 4:诉讼中 5:收据已丢失 6:其他', 
  `unpaid_progress_name` string COMMENT '未回款进度名称', 
  `form_todo_progress` string COMMENT '表单待办进度  0:未填写  1:已完成 2:待办中', 
  `form_todo_progress_name` string COMMENT '表单待办进度名称', 
  `finance_form_todo_progress` string COMMENT '财务表单待办进度  0:未填写  1:已完成', 
  `finance_form_todo_progress_name` string COMMENT '财务表单待办进度名称', 
  `sale_form_todo_progress` string COMMENT '销售表单待办进度  0:未填写  1:已完成', 
  `sale_form_todo_progress_name` string COMMENT '销售表单待办进度名称', 
  `tender_form_todo_progress` string COMMENT '投标表单待办进度  0:未填写  1:已完成', 
  `tender_form_todo_progress_name` string COMMENT '投标表单待办进度名称', 
  `attachment_todo_progress` string COMMENT '附件待办进度  0:未上传  1:已完成 2:待办中', 
  `attachment_todo_progress_name` string COMMENT '附件待办进度名称', 
  `finance_attachment_todo_progress` string COMMENT '财务附件待办进度  0:未上传  1:已完成', 
  `finance_attachment_todo_progress_name` string COMMENT '财务附件待办进度名称', 
  `tender_attachment_todo_progress` string COMMENT '投标附件待办进度  0:未上传  1:已完成', 
  `tender_attachment_todo_progress_name` string COMMENT '投标附件待办进名称', 
  `update_by` string COMMENT '中台更新人', 
  `update_time` string COMMENT '中台更新时间', 
  `is_deleted` string COMMENT '状态：0:正常、1:删除', 
  `self_employed` string COMMENT '是否自营  0:否  1:是', 
  `self_employed_name` string COMMENT '是否自营', 
  `cooperation_deposit_recovery` string COMMENT '合作保证金是否已收回  0:否  1:是', 
  `cooperation_deposit_recovery_name` string COMMENT '合作保证金是否已收回', 
  `money_back_no_write_off` string COMMENT '是否已回款未核销  0:否  1:是', 
  `money_back_no_write_off_name` string COMMENT '是否已回款未核销', 
  `change_business_scene` string COMMENT '转其他业务场景', 
  `change_business_scene_code` string COMMENT '转其他业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约', 
  `change_performance_offline_voucher` string COMMENT '转履约线下凭证号', 
  `receipt_recover` string COMMENT '收据是否回收  0:否  1:是', 
  `receipt_recover_name` string COMMENT '收据是否回收', 
  `contract_recover` string COMMENT '合同是否回收  0:否  1:是', 
  `contract_recover_name` string COMMENT '合同是否回收', 
  `finance_remark` string COMMENT '财务备注', 
  `sign_off_attachment_url` string COMMENT '签呈文件附件', 
  `receipt_attachment_url` string COMMENT '收据文件附件', 
  `sales_contract_attachment_url` string COMMENT '销售合同文件附件', 
  `break_contract` string COMMENT '是否已经断约  0:否  1:是', 
  `break_contract_name` string COMMENT '是否已经断约', 
  `sale_remark` string COMMENT '销售备注', 
  `won_bid` string COMMENT '是否中标', 
  `won_bid_date` string COMMENT '中标日期', 
  `target_payment_time` string COMMENT '目标回款时间', 
  `tender_attachment_url` string COMMENT '投标文件附件', 
  `tender_remark` string COMMENT '投标备注', 
  `operate_no` string COMMENT '核销单号', 
  `voucher_no` string COMMENT 'sap凭证单号', 
  `operate_amount` string COMMENT '操作金额', 
  `trade_time` string COMMENT '交易时间', 
  `write_off_source` string COMMENT '核销来源：1:认领、2:冲抵', 
  `write_off_source_name` string COMMENT '核销来源名称', 
  `all_write_off` string COMMENT '是否全部核销：0:否、1:是', 
  `all_write_off_name` string COMMENT '是否全部核销', 
  `operate_type` string COMMENT '操作类型 0:冲抵中 1:取消占用 2:已执行 3:已释放', 
  `operate_type_name` string COMMENT '操作类型名称', 
  `break_contract_date` string COMMENT '断约时间', 
  `receiving_account_name` string COMMENT '收款账号名称', 
  `account_diff` string COMMENT '账期天数差值', 
  `account_type` string COMMENT '账期类型', 
  `is_borrow_zizhi` string COMMENT '是否借资质', 
  `create_by` string COMMENT '数据创建人', 
  `customer_status_name` string COMMENT '客户活跃状态', 
  `follow_up_user_code` string COMMENT '跟进人工号', 
  `follow_up_user_name` string COMMENT '跟进人姓名', 
  `credit_customer_code` string COMMENT '信控号', 
  `no_payback_reason` string COMMENT '未回款原因', 
  `payback_time` timestamp COMMENT '目标回款时间', 
  `task_remark` string COMMENT '任务备注', 
  `job_status` int COMMENT '催办状态', 
  `job_status_name` string COMMENT '催办状态名称', 
  `process_time` timestamp COMMENT '最新处理时间', 
  `real_perform_customer_code` string COMMENT '实际履约客户', 
  `sdt` string COMMENT '日期分区', 
  `is_problem_account` string COMMENT '是否问题账款 0-否 1-是', 
  `is_review` string COMMENT '是否复核 0-未复核 1-已复核')
COMMENT '慧共享保证金'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_analyse/csx_analyse_fr_sss_incidental_write_off_info_di'
TBLPROPERTIES (
  'STATS_GENERATED'='TASK', 
  'bucketing_version'='2', 
  'impala.events.catalogServiceId'='ecb09831d14b4405:a2d2ec8904161c14', 
  'impala.events.catalogVersion'='2235493', 
  'impala.lastComputeStatsTime'='1714019643', 
  'last_modified_by'='hivemeta', 
  'last_modified_time'='1713948130', 
  'transient_lastDdlTime'='1721847074')
;



show create table csx_dws.csx_dws_crm_customer_business_active_di

createtab_stmt
CREATE EXTERNAL TABLE `csx_dws`.`csx_dws_crm_customer_business_active_di`(
  `customer_id` bigint COMMENT '客户id', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `business_type_code` int COMMENT '业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)', 
  `business_type_name` string COMMENT '业务类型名称', 
  `channel_code` string COMMENT '渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)', 
  `channel_name` string COMMENT '渠道名称', 
  `sales_user_id` bigint COMMENT '业务员id', 
  `sales_user_number` string COMMENT '业务员工号', 
  `sales_user_name` string COMMENT '业务员名称', 
  `sales_user_position` string COMMENT '业务员职务', 
  `sales_province_id` string COMMENT '销售归属省区id', 
  `sales_province_name` string COMMENT '销售归属省区名称', 
  `sales_city_code` string COMMENT '销售归属城市编码', 
  `sales_city_name` string COMMENT '销售归属城市名称', 
  `performance_region_code` string COMMENT '业绩大区编码', 
  `performance_region_name` string COMMENT '业绩大区名称', 
  `performance_province_code` string COMMENT '业绩省区编码', 
  `performance_province_name` string COMMENT '业绩省区名称', 
  `performance_city_code` string COMMENT '业绩城市编码', 
  `performance_city_name` string COMMENT '业绩城市名称', 
  `business_attribute_code` int COMMENT '商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购', 
  `business_attribute_name` string COMMENT '商机属性名称', 
  `business_sign_date` string COMMENT '最近业务类型签约日期', 
  `first_business_sign_date` string COMMENT '首次业务类型签约日期', 
  `first_business_sale_date` string COMMENT '首次业务类型销售日期', 
  `last_business_sale_date` string COMMENT '末次业务类型销售日期', 
  `sale_business_active_days` int COMMENT '销售业务类型活跃天数(即历史至今有销售的日期)', 
  `sub_sale_business_active_days` int COMMENT '减数销售业务类型活跃天数(只用作计算客户总活跃天数使用)', 
  `sale_business_total_amt` decimal(20,6) COMMENT '销售业务类型总金额', 
  `sub_business_sale_amt` decimal(20,6) COMMENT '减数业务类型金额(只用作计算销售总金额使用)')
COMMENT '客户业务类型动态信息表'
PARTITIONED BY ( 
  `sdt` string COMMENT '导入日期{"FORMAT":"yyyymmdd"}')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_dws/csx_dws_crm_customer_business_active_di'
TBLPROPERTIES (
  'DO_NOT_UPDATE_STATS'='true', 
  'STATS_GENERATED'='TASK', 
  'bucketing_version'='2', 
  'discover.partitions'='true', 
  'impala.events.catalogServiceId'='752405fcbe594125:9636d2e6c0bb932d', 
  'impala.events.catalogVersion'='402215', 
  'impala.lastComputeStatsTime'='1686802987', 
  'spark.sql.partitionProvider'='catalog', 
  'transient_lastDdlTime'='1720427680')
;


-- 商机 csx_dim.csx_dim_crm_business_info
createtab_stmt
CREATE EXTERNAL TABLE `csx_dim`.`csx_dim_crm_business_info`(
  `business_number` string COMMENT '商机编号', 
  `customer_id` bigint COMMENT '客户id', 
  `customer_code` string COMMENT '客户编码', 
  `customer_name` string COMMENT '客户名称', 
  `owner_user_id` bigint COMMENT '归属人id', 
  `owner_user_number` string COMMENT '归属人工号', 
  `owner_user_name` string COMMENT '归属人姓名', 
  `owner_user_position` string COMMENT '归属人职位', 
  `supervisor_user_id` bigint COMMENT '销售主管id', 
  `supervisor_user_number` string COMMENT '销售主管工号', 
  `supervisor_user_name` string COMMENT '销售主管名称', 
  `owner_province_id` string COMMENT '所属省区id', 
  `owner_province_name` string COMMENT '所属省区名称', 
  `owner_city_code` string COMMENT '所属城市编码', 
  `owner_city_name` string COMMENT '所属城市名称', 
  `performance_region_code` string COMMENT '业绩大区编码', 
  `performance_region_name` string COMMENT '业绩大区名称', 
  `performance_province_code` string COMMENT '业绩省区编码', 
  `performance_province_name` string COMMENT '业绩省区名称', 
  `performance_city_code` string COMMENT '业绩城市编码', 
  `performance_city_name` string COMMENT '业绩城市名称', 
  `business_attribute_code` int COMMENT '商机属性编码 1：日配客户 2：福利客户 3：大宗贸易 4：m端 5：bbc 6：内购', 
  `business_attribute_name` string COMMENT '商机属性名称', 
  `sign_type_code` int COMMENT '审批流类型 1:新客 2：商机', 
  `status` int COMMENT '是否有效 0无效 1有效', 
  `approval_id` bigint COMMENT '审批id', 
  `approval_status_code` int COMMENT '审批状态编码 0:待发起 1：审批中 2：审批完成 3：审批拒绝', 
  `approval_status_name` string COMMENT '审批状态名称', 
  `business_stage` int COMMENT '阶段状态 1.阶段1 2.阶段2 3.阶段3 4.阶段4 5.阶段5', 
  `business_sign_time` timestamp COMMENT '业务类型签约时间', 
  `first_business_sign_time` timestamp COMMENT '首次业务类型签约时间', 
  `channel_code` string COMMENT '渠道编号-1.大客户 2.商超(对内) 3.商超(对外) 4.大宗 5.供应链(食百) 6.供应链(食百) 7.企业购 8.其他', 
  `channel_name` string COMMENT '渠道名称', 
  `first_category_code` string COMMENT '一级客户分类编码', 
  `first_category_name` string COMMENT '一级客户分类名称', 
  `second_category_code` string COMMENT '二级客户分类编码', 
  `second_category_name` string COMMENT '二级客户分类名称', 
  `third_category_code` string COMMENT '三级客户分类编码', 
  `third_category_name` string COMMENT '三级客户分类名称', 
  `customer_address_details` string COMMENT '客户详细地址json', 
  `customer_address_full` string COMMENT '客户详细地址', 
  `longitude` string COMMENT '经度', 
  `latitude` string COMMENT '纬度', 
  `contact_person` string COMMENT '联系人姓名', 
  `contact_phone` string COMMENT '联系电话', 
  `contract_number` string COMMENT '合同编号', 
  `contract_type` int COMMENT '合同类型 1临时合同 2正式合同', 
  `contract_must` int COMMENT '是否需签订合同 0否 1是', 
  `contract_begin_date` timestamp COMMENT '合同起始日期', 
  `contract_end_date` timestamp COMMENT '合同终止日期', 
  `last_visit_time` timestamp COMMENT '最后拜访时间', 
  `guide_user_id` bigint COMMENT '指导人id', 
  `guide_user_name` string COMMENT '指导人名称', 
  `estimate_contract_amount` string COMMENT '预估合同签约金额', 
  `other_needs_code` string COMMENT '其他需求编码 1:餐卡、2:福利、3:商城对接、4:微信支付、5:无', 
  `other_needs_name` string COMMENT '其他需求名称', 
  `types_cooperation` string COMMENT '合作品类', 
  `price_type_code` int COMMENT '报价类型编码 1:下单前报价  2:发货前报价  3:发货后报价', 
  `price_type_name` string COMMENT '报价类型名称', 
  `price_period_code` int COMMENT '报价周期编码 1:每天 2:每周 3:每半月4：每月', 
  `price_period_name` string COMMENT '报价周期名称', 
  `price_date_code` int COMMENT '报价日期编码', 
  `price_date_name` string COMMENT '报价日期名称', 
  `contract_cycle` string COMMENT '合同周期', 
  `expect_sign_time` string COMMENT '预计签约时间', 
  `expect_execute_time` string COMMENT '预计履约时间', 
  `gross_profit_rate` string COMMENT '预计毛利率', 
  `business_introduction` string COMMENT '业务模式介绍', 
  `estimate_once_amount` string COMMENT '预估一次性配送金额', 
  `estimate_month_amount` string COMMENT '预估月度配送金额', 
  `estimate_delivery_times` string COMMENT '预估配送次数', 
  `invoice_requirement_code` int COMMENT '发票要求编码 1.专用 2.普通 3.皆可', 
  `invoice_requirement_name` string COMMENT '发票要求名称', 
  `meals_person_count` int COMMENT '用餐人数', 
  `meals_avg_amount` string COMMENT '人均餐标', 
  `site_name` string COMMENT '站点名称', 
  `bbc_credit_type_code` string COMMENT '授信类型编码', 
  `bbc_credit_type_name` string COMMENT '授信类型名称', 
  `employees` int COMMENT '企业人数', 
  `expect_project_begin_time` string COMMENT '预计项目开始时间', 
  `expect_project_end_time` string COMMENT '预计项目结束时间', 
  `certificate_photos` string COMMENT '企业证照资料', 
  `legal_person_photos` string COMMENT '个人证明资料（法定代表人或实际控制人）', 
  `customer_site_photos` string COMMENT '企业经营现场拍照', 
  `attachment_list` string COMMENT '附件列表', 
  `create_time` timestamp COMMENT '创建时间', 
  `create_by` string COMMENT '创建人', 
  `update_time` timestamp COMMENT '更新时间', 
  `update_by` string COMMENT '更新人', 
  `customer_address_province_code` string COMMENT '客户地址省区编码', 
  `customer_address_province_name` string COMMENT '客户地址省区名称', 
  `customer_address_city_code` string COMMENT '客户地址城市编码', 
  `customer_address_city_name` string COMMENT '客户地址城市名称', 
  `first_sign_time` string COMMENT '客户第一次签约时间', 
  `business_type_code` int COMMENT '业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)', 
  `business_type_name` string COMMENT '业务类型名称', 
  `customer_acquisition_type_code` int COMMENT '获客方式编码', 
  `customer_acquisition_type_name` string COMMENT '获客方式名称', 
  `price_set_type` string COMMENT '定价方式 编号,编号', 
  `contract_cycle_int` int COMMENT '新合同周期 0小于1个月', 
  `invoice_time` timestamp COMMENT '开票时间', 
  `reconciliation_time` int COMMENT '对账日期 1:每月1-5日 2:每月6-15日 3:其它', 
  `receive_time` timestamp COMMENT '销售单整单签收时间', 
  `union_bid_status` int COMMENT '是否联合招标', 
  `only_supplier_status` int COMMENT '是否唯一供应商', 
  `expect_bid_win_share` string COMMENT '预计可履约中标份额 100.00', 
  `company_code` string COMMENT '公司代码', 
  `account_period_code` string COMMENT '账期编码', 
  `cooperation_intention_photos` string COMMENT '客户确定合作意向截图', 
  `contract_cycle_desc` string COMMENT '新合同周期描述', 
  `credit_code` string COMMENT '信控编号', 
  `credit_create_time` timestamp COMMENT '信控创建时间')
COMMENT '商机信息表'
PARTITIONED BY ( 
  `sdt` string COMMENT '导入日期{format:yyyymmdd}{"FORMAT":"yyyymmdd"}')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://yhbigdata/yhcsx/warehouse/csx_dim/csx_dim_crm_business_info'
TBLPROPERTIES (
  'DO_NOT_UPDATE_STATS'='true', 
  'STATS_GENERATED'='TASK', 
  'bucketing_version'='2', 
  'discover.partitions'='true', 
  'impala.events.catalogServiceId'='d623cb5a2d274646:a1de0f25f0c58bd7', 
  'impala.events.catalogVersion'='353226', 
  'impala.lastComputeStatsTime'='1684556722', 
  'last_modified_by'='hivemeta', 
  'last_modified_time'='1682056037', 
  'spark.sql.partitionProvider'='catalog', 
  'transient_lastDdlTime'='1721215433')
