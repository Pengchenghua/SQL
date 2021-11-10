--产量能耗建表语句
--工程部
-- truncate table dws_wms_r_m_output ;
drop table dws_wms_r_m_output;
CREATE TABLE `dws_wms_r_m_output` (
   `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID', 
  `months` varchar(8) NOT NULL COMMENT '月份',
  `province_code` varchar(64) NOT NULL COMMENT '省区编码',
  `province_name` varchar(64) NOT NULL COMMENT '省区名称',
    `area_code` varchar(64) NOT NULL COMMENT '区域',
  `area_name` varchar(64) NOT NULL COMMENT '区域',
   workshop_code varchar(64)  comment'车间编码',
   workshop_name varchar(64) comment'车间名称',
  `output_qty` decimal(18,2) DEFAULT NULL COMMENT '产量',
  `man_hour` decimal(18,2) DEFAULT NULL COMMENT '工时',
  `b_output_qty` decimal(18,2) DEFAULT NULL COMMENT 'B端产量',
  `bbc_output_qty` decimal(18,2) DEFAULT NULL COMMENT 'M端产量',
 PRIMARY KEY (`id`,months)
) ENGINE=InnoDB AUTO_INCREMENT=67 DEFAULT CHARSET=utf8mb4 COMMENT='产量工时表';


CREATE TABLE `dws_wms_r_m_output_fill_rate` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `months` varchar(8) NOT NULL COMMENT '月份',
  `province_code` varchar(64) NOT NULL COMMENT '省区编码',
  `province_name` varchar(64) NOT NULL COMMENT '省区名称',
  `area_code` varchar(64) NOT NULL COMMENT '区域',
  `area_name` varchar(64) NOT NULL COMMENT '区域',
  employees_num DECIMAL(18,2) NOT NULL COMMENT '工人数',
  man_hour DECIMAL(18,2) NOT NULL COMMENT '工时',
  order_qty DECIMAL(18,2) not NULL COMMENT '需求订单量',
  real_order_qty DECIMAL(18,2) not NULL COMMENT '实际产量',
  order_fill_rate DECIMAL(18,6) NOT NULL COMMENT '订单满足率',
  transfer_qty DECIMAL(18,2) not NULL COMMENT '转配量',
  PRIMARY KEY (`id`,`months`)
) ENGINE=InnoDB AUTO_INCREMENT=239 DEFAULT CHARSET=utf8mb4 COMMENT='产量——订单满足率'
;


SELECT *,case when workshop_code not in ('1101','1102') then output_qty+b_output_qty+bbc_output_qty end total_qty FROM dws_wms_r_m_output WHERE MONTHS='202109'
ORDER BY cast(AREA_CODE as SIGNED),case when workshop_code='1101' then '110109'
	when  workshop_code='1102' then '110209' else workshop_code end  ;


--  
DROP TABLE data_analysis_prd.dws_wms_r_m_output_tce;

CREATE TABLE data_analysis_prd.dws_wms_r_m_output_tce (
	id int(11) auto_increment NOT NULL COMMENT 'ID',
	months varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '月份',
	region_name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '大区',
	province_name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '省区名称',
		city_name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '城市名称',
	area_code varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '区域编码',
	area_name varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '区域名称',
	output_qty decimal(18,2) NULL COMMENT '产量',
	tce_amount decimal(18,2) NULL COMMENT '能耗',
	piece_yardage decimal(18,2) NULL COMMENT '单耗'	,
 PRIMARY KEY (id,months)
	)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_general_ci
COMMENT='产量能耗';
