-- 更新注释
ALTER TABLE csx_dw.csx_shop CHANGE location_status location_status int COMMENT '地点启用 1启用 0禁用';

create table csx_dw.csx_shop
	(
		rt_shop_code STRING comment '融通门店编码'                                   ,
		org_short_name STRING comment '机构简称'                                   ,
		org_full_name STRING comment '机构全称'                                    ,
		shop_status_code string comment '门店状态编码'                               ,
		shop_status STRING comment '门店状态'                                      ,
		post_code STRING comment '城市邮编'                                        ,
		purchase_org STRING comment '采购组织'                                     ,
		purchase_name string comment '采购组织名称'                                  ,
		company_code STRING comment '公司代码'                                     ,
		company_name string comment '公司名称'                                     ,
		street STRING comment '街道'                                             ,
		distribution_area STRING comment '配送费区域（包含编码，名称）dic_type =RATIONAEWAID',
		shop_group_code STRING comment '进价店群'                                  ,
		location_code STRING comment '地点编码(新系统)'                               ,
		ec_flag STRING comment '是否电商'                                          ,
		province_code STRING comment '省份编码'                                    ,
		province_name string comment '省份名称'                                    ,
		prefecture_city_code STRING comment '地级市'                              ,
		prefecture_city_name string comment '地级市名称'                            ,
		county_city STRING comment '县级市'                                       ,
		county_city_name string comment '县级市名称'                                ,
		delivery_cent_type STRING comment '配送中心类型'                             ,
		delivery_cent_name string comment '配送中心类型'                             ,
		jda_tity STRING comment 'JDA城市'                                        ,
		ascription_type STRING comment '区域类型 dic_type= YTSATTRI '              ,
		region_code STRING comment '区域代码'                                      ,
		region_name string comment '区域代码名称'                                    ,
		logistics_mode STRING comment 'DC物流模式'                                 ,
		logistics_name STRING comment 'DC物流模式名称'                               ,
		open_date STRING comment '开业日期'                                        ,
		-- close_date string comment '闭店日期',
		shop_type STRING comment '店类型 比如 11:A大卖场 12:B卖场'                                              ,
		market_net_area STRING comment '超市净面积'                                                        ,
		operation_floor STRING comment '经营楼层 比如:00:未知 01:单一首层 02:单层多层 03:楼上单层 04:楼上多层 05:地下单层 06:地下多层',
		operation_floor_count STRING comment '经营楼层总数'                                                 ,
		business_classify STRING comment '商圈分类 比如 01:1.5公里以内 02:3公里以内 03:3公里以外'                       ,
		food_area STRING comment '食品用品营业面积'                                                           ,
		clothes_area STRING comment '服装营业面积'                                                          ,
		fresh_area STRING comment '生鲜营业面积'                                                            ,
		process_area STRING comment '加工营业面积'                                                          ,
		fresh_flag STRING comment '是否经营生鲜 比如:X:是 空:否'                                                 ,
		table_type string comment '公司类型 1 彩食鲜 、2 永辉'                                                  ,
		location_uses string comment '仓库用途(01大客户物流、02	商超物流、03工厂、04	寄售门店、05	彩食鲜小店)'                    ,
		location_type string comment '仓库类型 1仓库、2工厂、3	门店)'                                             ,
		location_status comment '地点启用 1开启 2禁用'                                                        ,
		zone_id string comment '战区编码'                                                                 ,
		zone_name string comment '战区名称'
	)
	comment '彩食鲜门店资料' partitioned by
	(
		sdt string comment '日期分区'
	)
	stored as parquet
;

-- 仓库用途
md_shop_configuration 仓库类型 location_type 仓库用途 location_uses
md_dic
仓库用途 dic_type                        ='FUNCTION'
仓库类型 dic_type                        ='WAREHOUSETYPE'
归属维护 dic_type                        = 'YTSATTRI'
配送费区域（包含编码，名称） dic_type              ='RATIONAEWAID'
配送中心类型 dic_type                      ='YDCTYP'
物流模式 dic_type                        ='LOGISTICSMODE'
csx_basic_data.base_address_info code= region_code;
CREATE TABLE `csx_dw.csx_shop`
	(
		`rt_shop_code` string comment '融通编码'           ,
		`org_short_name` string comment '机构简称'         ,
		`org_full_name` string comment '机构全称'          ,
		`shop_status_code` string comment '门店状态'       ,
		`shop_status` string comment '门店状态名称'          ,
		`post_code` string comment '邮编'                ,
		`purchase_org` string comment '采购组织'           ,
		`purchase_name` string comment '采购组织名称'        ,
		`company_code` string comment '公司代码'           ,
		`company_name` string comment '公司代码名称'         ,
		`province_code` string comment '省区编码'          ,
		`province_name` string comment '省区名称'          ,
		`prefecture_city` string comment '地级市'         ,
		`prefecture_city_name` string comment '地级市名称'  ,
		`county_city` string comment '县级市'             ,
		`county_city_name` string comment '县级市名称'      ,
		`street` string comment '街道'                   ,
		`distribution_area` string comment '配送费区域'     ,
		distribution_area_name string comment '配送费区域名称',
		`shop_group_code` string comment '进价店群'        ,
		`shop_group_name` string comment '进价群名称'       ,
		`location_code` string comment '地点编码(新) '      ,
		`ec_flag` string comment '是否电商'                ,
		`delivery_cent_type` string comment '配送中心类型'   ,
		`delivery_cent_name` string comment '配送中心类型名称' ,
		`jda_tity` string comment 'JDA城市'              ,
		`ascription_type` string comment '区域类型'        ,
		`ascription_type_name` string comment '区域类型名称' ,
		`sales_dist_code` string comment '销售区域代码'      ,
		`sales_dist` string comment '销售区域名称'           ,
		`logistics_mode` string comment '物流模式'         ,
		`logistics_name` string comment '物流名称'         ,
		`open_date` string comment '开业日期'              ,
		`shop_type` string comment '店类型'               ,
		`market_net_area` string comment '超市净面积'       ,
		`operation_floor` string comment '经营楼层'        ,
		`operation_floor_count` string comment '经营楼层总数',
		`business_classify` string comment '商圈分类'      ,
		`food_area` string comment '食品用品营业面积'          ,
		`clothes_area` string comment '服装营业面积'         ,
		`fresh_area` string comment '生鲜营业面积'           ,
		`process_area` string comment '加工营业面积'         ,
		`fresh_flag` string comment '是否经营生鲜'           ,
		`table_type` int comment '公司类型'                ,
		`location_uses_code` string comment '仓库用途编码'   ,
		`location_uses` string comment '仓库用途'          ,
		`location_type_code` string comment '仓库类型编码'   ,
		`location_type` string comment '仓库类型'          ,
		`location_status` int comment '地点启用'           ,
		`zone_id` string comment '战区编码'                ,
		`zone_name` string comment '战区名称'
	)
	comment '彩食鲜门店资料' partitioned by
	(
		sdt string comment '日期分区'
	)
	stored as parquet
;

CREATE TABLE csx_dw.csx_shop AS
SELECT
	rt_shop_code                      , --融通编码
	org_short_name                    , --机构简称
	org_full_name                     , --机构全称
	shop_status_code                  , --门店状态
	shop_status                       , --门店状态名称
	post_code                         , --邮编
	purchase_org                      , --采购组织
	purchase_org_name AS purchase_name, --采购组织名称
	company_code                      , --公司代码
	company_name                      , --公司代码名称
	street                            , --街道
	distribution_area                 , --配送费区域（包含编码，名称）
	shop_group_code                   , --进价店群
	shop_group_name                   , -- 进价群名称
	location_code                     , --地点编码(新)
	ec_flag                           , --是否电商
	province_code                     , --省区编码
	province_name                     , --省区名称
	prefecture_city                   , --地级市编码
	prefecture_city_name              , --地级市名称
	county_city                       , --县级市编码
	county_city_name                  , --县级市名称
	delivery_cent_type                , -- 配送中心类型
	delivery_cent_name                , --配送中心类型名称
	jda_tity                          , --JDA城市
	ascription_type                   , --区域类型
	ascription_type_name              , --区域类型名称
	sales_dist_code                   , -- 销售区域代码
	sales_dist                        , -- 销售区域名称
	logistics_mode                    , --物流模式
	logistics_name                    , --物流名称
	open_date                         , --开业日期
	shop_type                         , --店类型
	market_net_area                   , --超市净面积
	operation_floor                   , -- 经营楼层
	operation_floor_count             , -- 经营楼层总数
	business_classify                 , -- 商圈分类
	food_area                         , -- 食品用品营业面积
	clothes_area                      , -- 服装营业面积
	fresh_area                        , -- 生鲜营业面积
	process_area                      , --加工营业面积
	fresh_flag                        , -- 是否经营生鲜
	table_type                        , -- 公司类型
	location_uses                     , -- 仓库用途
	location_type                     , -- 仓库类型
	location_status                   , --地点启用
	''zone_id                         , -- 战区编码
	''zone_name                         --战区名称
FROM
	csx_dw.temp_csx_shop
;

set mapreduce.job.queuename                 =caishixian;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode        =nonstrict;
set hive.exec.dynamic.partition             =true;
drop table if exists csx_dw.temp_shop
;

create temporary table if not exists csx_dw.temp_shop as
select
	id                            ,
	rt_shop_code                  ,
	org_short_name                ,
	org_full_name                 ,
	shop_status                   ,
	post_code                     ,
	purchase_org                  ,
	company_code                  ,
	street                        ,
	distribution_area             ,
	a.shop_group_code             ,
	f.shop_group_name             ,
	location_code                 ,
	ec_flag                       ,
	province as province_code     ,
	b.name   as province_name     ,
	prefecture_city               ,
	c.name as prefecture_city_name,
	county_city                   ,
	d.name as county_city_name    ,
	delivery_cent_type            ,
	jda_tity                      ,
	ascription_type               ,
	region_code                   ,
	logistics_mode                ,
	open_date                     ,
	shop_type                     ,
	market_net_area               ,
	operation_floor               ,
	operation_floor_count         ,
	business_classify             ,
	food_area                     ,
	clothes_area                  ,
	fresh_area                    ,
	process_area                  ,
	fresh_flag                    ,
	working_flag                  ,
	clothes_flag                  ,
	table_type                    ,
	food_shelf_count              ,
	shop_business_type            ,
	shop_consume_grade            ,
	business_primary_demand       ,
	business_sec_demand           ,
	create_time                   ,
	update_time                   ,
	location_type                 ,
	location_status               ,
	created_by                    ,
	updated_by                    ,
	sdt
from
	(
		select
			id                      ,
			rt_shop_code            ,
			org_short_name          ,
			org_full_name           ,
			shop_status             ,
			post_code               ,
			purchase_org            ,
			company_code            ,
			street                  ,
			distribution_area       ,
			shop_group_code         ,
			location_code           ,
			ec_flag                 ,
			province                ,
			prefecture_city         ,
			county_city             ,
			delivery_cent_type      ,
			jda_tity                ,
			ascription_type         ,
			region_code             ,
			logistics_mode          ,
			open_date               ,
			shop_type               ,
			market_net_area         ,
			operation_floor         ,
			operation_floor_count   ,
			business_classify       ,
			food_area               ,
			clothes_area            ,
			fresh_area              ,
			process_area            ,
			fresh_flag              ,
			working_flag            ,
			clothes_flag            ,
			table_type              ,
			food_shelf_count        ,
			shop_business_type      ,
			shop_consume_grade      ,
			business_primary_demand ,
			business_sec_demand     ,
			create_time             ,
			update_time             ,
			location_type           ,
			location_status         ,
			created_by              ,
			updated_by              ,
			sdt
		from
			csx_ods.source_basic_w_a_md_all_shop_info
		where
			sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
	)
	a
	left outer join
		(
			SELECT
				code,
				name
			FROM
				csx_ods.source_basic_w_a_base_address_info
			where
				sdt     =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and type='2'
		)
		b
		on
			a.province=b.code
	left outer join
		(
			SELECT
				code,
				name
			FROM
				csx_ods.source_basic_w_a_base_address_info
			where
				sdt     =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and type='3'
		)
		c
		on
			a.prefecture_city=c.code
	left outer join
		(
			SELECT
				code,
				name
			FROM
				csx_ods.source_basic_w_a_base_address_info
			where
				sdt     =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and type='4'
		)
		d
		on
			a.county_city=d.code
	left outer join
		(
			SELECT
				shop_group_code     ,
				shop_group_name     ,
				shop_group_type_code,
				shop_group_type_name
			FROM
				csx_ods.source_basic_w_a_md_shop_group
			where
				sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
		)
		f
		on
			a.shop_group_code=f.shop_group_code
;

-- 仓库类型
drop table if exists csx_dw.temp_shop_type
;

create temporary table if not exists csx_dw.temp_shop_type as
select
	a.shop_code                                ,
	a.warehouse_uses_code    as location_uses_code,
	b.dic_value              as location_uses     ,
	a.warehouse_channel_code as location_type_code,
	c.dic_value              as location_type
from
	(
		SELECT
			shop_code            ,
			a.warehouse_uses_code,
			a.warehouse_channel_code
		FROM
			csx_ods.source_basic_w_a_md_shop_configuration a
		WHERE
			sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
	)
	a
	left outer join
		(
			SELECT
				dic_key,
				dic_value
			from
				csx_ods.source_basic_w_a_md_dic
			where
				sdt          =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and dic_type ='FUNCTION'
		)
		b
		on
			a.warehouse_uses_code=b.dic_key
	left outer join
		(
			SELECT
				dic_key,
				dic_value
			from
				csx_ods.source_basic_w_a_md_dic
			where
				sdt          =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and dic_type ='WAREHOUSETYPE'
		)
		c
		on
			a.warehouse_channel_code=c.dic_key
;

-- 大区省市县主数据
drop table if exists csx_dw.temp_csx_shop
;

create temporary table if not exists csx_dw.temp_csx_shop as
SELECT
	id                                 ,
	a.rt_shop_code                     ,
	a.org_short_name                   ,
	a.org_full_name                    ,
	a.shop_status as shop_status_code  ,
	a.post_code                        ,
	a.purchase_org                     ,
	a.company_code                     ,
	a.street                           ,
	a.distribution_area                ,
	a.shop_group_code                  ,
	a.shop_group_name                  ,
	a.location_code                    ,
	a.ec_flag                          ,
	a.province_code                    ,
	a.province_name                    ,
	a.prefecture_city                  ,
	a.prefecture_city_name             ,
	a.county_city                      ,
	a.county_city_name                 ,
	a.delivery_cent_type               ,
	k.dic_value as delivery_cent_name  ,
	a.jda_tity                         ,
	a.ascription_type                  ,
	h.dic_value as ascription_type_name,
	--  a.region_code as sales_dist_code,
	a.logistics_mode          ,
	a.open_date               ,
	a.shop_type               ,
	a.market_net_area         ,
	a.operation_floor         ,
	a.operation_floor_count   ,
	a.business_classify       ,
	a.food_area               ,
	a.clothes_area            ,
	a.fresh_area              ,
	a.process_area            ,
	a.fresh_flag              ,
	a.working_flag            ,
	a.clothes_flag            ,
	a.table_type              ,
	a.food_shelf_count        ,
	a.shop_business_type      ,
	a.shop_consume_grade      ,
	a.business_primary_demand ,
	a.business_sec_demand     ,
	a.create_time             ,
	a.update_time             ,
	--a.location_type ,
	a.location_status                     ,
	a.created_by                          ,
	a.updated_by                          ,
	sdt                                   ,
	c.name              AS company_name   ,
	d.purchase_org_name as purchase_name  ,
	d.region_code       as sales_dist_code,
	d.region_name       as sales_dist     ,
	e.location_uses_code                  ,
	e.location_uses                       ,
	e.location_type_code                  ,
	e.location_type                       ,
	f.dic_value as distribution_area_name ,
	j.dic_value as shop_status            ,
	p.dic_value as logistics_name
FROM
	csx_dw.temp_shop a
	LEFT JOIN
		(
			SELECT
				code,
				name
			FROM
				csx_ods.source_basic_w_a_md_company_code
			WHERE
				sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
		)
		c
		ON
			a.company_code=c.code
	LEFT JOIN
		(
			SELECT
				purchase_org_code,
				purchase_org_name,
				region_code      ,
				region_name
			FROM
				csx_ods.source_basic_w_a_base_purchase_org_info
			WHERE
				sdt=regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
		)
		d
		ON
			a.purchase_org=d.purchase_org_code
	LEFT OUTER JOIN
		csx_dw.temp_shop_type e
		on
			a.location_code=e.shop_code
	left OUTER JOIN
		(
			SELECT
				dic_key,
				dic_value
			from
				csx_ods.source_basic_w_a_md_dic
			where
				sdt          =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and dic_type ='RATIONAEWAID'
		)
		f
		on
			a.distribution_area=f.dic_key
	left OUTER JOIN
		(
			SELECT
				dic_key,
				dic_value
			from
				csx_ods.source_basic_w_a_md_dic
			where
				sdt          =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and dic_type ='YTSATTRI'
		)
		h
		on
			a.ascription_type=h.dic_key
	LEFT OUTER JOIN
		(
			SELECT
				dic_key,
				dic_value
			from
				csx_ods.source_basic_w_a_md_dic
			where
				sdt          =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and dic_type ='YSTATS'
		)
		j
		on
			a.shop_status=j.dic_key
	left outer join
		(
			SELECT
				dic_key,
				dic_value
			from
				csx_ods.source_basic_w_a_md_dic
			where
				sdt          =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and dic_type ='YDCTYP'
		)
		k
		on
			lpad(a.delivery_cent_type,2,'0')=k.dic_key
	left outer join
		(
			SELECT
				dic_key,
				dic_value
			from
				csx_ods.source_basic_w_a_md_dic
			where
				sdt          =regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
				and dic_type ='LOGISTICSMODE'
		)
		p
		on
			a.logistics_mode=p.dic_key
;

--select * from  csx_dw.temp_csx_shop;
--set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.csx_shop partition
	(sdt
	)
select
	nvl( `rt_shop_code` ,'')          as `rt_shop_code`          , --融通编码
	nvl( `org_short_name` ,'')        as `org_short_name`        , --机构简称
	nvl( `org_full_name` ,'')         as `shop_name`             , --机构全称
	nvl( `shop_status_code` ,'')      as `shop_status_code`      , --门店状态
	nvl( `shop_status` ,'')           as `shop_status`           , --门店状态名称
	nvl( `post_code` ,'')             as `post_code`             , --邮编
	nvl( `purchase_org` ,'')          as `purchase_org`          , --采购组织
	nvl( `purchase_name` ,'')         as `purchase_name`         , --采购组织名称
	nvl( `company_code` ,'')          as `company_code`          , --公司代码
	nvl( `company_name` ,'')          as `company_name`          , --公司代码名称
	nvl( `province_code` ,'')         as `province_code`         , --省区编码
	nvl( `province_name` ,'')         as `province_name`         , --省区名称
	nvl( `prefecture_city` ,'')       as `prefecture_city`       , --地级市编码
	nvl( `prefecture_city_name` ,'')  as `prefecture_city_name`  , --地级市名称
	nvl( `county_city` ,'')           as `county_city`           , --县级市编码
	nvl( `county_city_name` ,'')      as `county_city_name`      , --县级市名称
	nvl( `street` ,'')                as `street`                , --街道
	nvl( `distribution_area` ,'')     as `distribution_area`     , --配送费区域（包含编码，名称）
	nvl(distribution_area_name ,'')   as distribution_area_name  , --配送费区域名称
	nvl( `shop_group_code` ,'')       as `shop_group_code`       , -- 进价群名称
	nvl( `shop_group_name` ,'')       as `shop_group_name`       , --进价群名称
	nvl( `location_code` ,'')         as `location_code`         , --地点编码(新)
	nvl( `ec_flag` ,'')               as `ec_flag`               , -- 是否电商
	nvl( `delivery_cent_type` ,'')    as `delivery_cent_type`    , --配送中心类型名称
	nvl( `delivery_cent_name` ,'')    as `delivery_cent_name`    , --配送中心类型名称
	nvl( `jda_tity` ,'')              as `jda_tity`              , --JDA城市
	nvl( `ascription_type` ,'')       as `ascription_type`       , --区域类型
	nvl( `ascription_type_name` ,'')  as `ascription_type_name`  , --区域类型名称
	nvl( `sales_dist_code` ,'')       as `sales_dist_code`       , -- 销售区域代码
	nvl( `sales_dist` ,'')            as `sales_dist`            , -- 销售区域名称
	nvl( `logistics_mode` ,'')        as `logistics_mode`        , --物流模式
	nvl( `logistics_name` ,'')        as `logistics_name`        , --物流名称
	nvl( `open_date` ,'')             as `open_date`             , --开业日期
	nvl( `shop_type` ,'')             as `shop_type`             , --店类型
	nvl( `market_net_area` ,'')       as `market_net_area`       , --超市净面积
	nvl( `operation_floor` ,'')       as `operation_floor`       , -- 经营楼层
	nvl( `operation_floor_count` ,'') as `operation_floor_count` , -- 经营楼层总数
	nvl( `business_classify` ,'')     as `business_classify`     , -- 商圈分类
	nvl( `food_area` ,'')             as `food_area`             , -- 食品用品营业面积
	nvl( `clothes_area` ,'')          as `clothes_area`          , -- 服装营业面积
	nvl( `fresh_area` ,'')            as `fresh_area`            , -- 生鲜营业面积
	nvl( `process_area` ,'')          as `process_area`          , --加工营业面积
	nvl( `fresh_flag` ,'')            as `fresh_flag`            , -- 是否经营生鲜
	nvl( `table_type` ,'')            as `table_type`            , -- 公司类型
	nvl( `location_uses_code` ,'')    as `location_uses_code`    , -- 仓库用途编码
	nvl( `location_uses` ,'')         as `location_uses`         , -- 仓库用途
	nvl( `location_type_code` ,'')    as `location_type_code`    , -- 仓库类型编码
	nvl( `location_type` ,'')         as `location_type`         , -- 仓库类型
	nvl( `location_status` ,'')       as `location_status`       , --地点启用
	'' `zone_id`                                                 , -- 战区编码
	'' `zone_name`                                               , --战区名称
	regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
from
	csx_dw.temp_csx_shop
;

insert overwrite table csx_dw.csx_shop partition
	(sdt
	)
select
	nvl( `rt_shop_code` ,'')          as `rt_shop_code`          , --融通编码
	nvl( `org_short_name` ,'')        as `org_short_name`        , --机构简称
	nvl( `org_full_name` ,'')         as `shop_name`             , --机构全称
	nvl( `shop_status_code` ,'')      as `shop_status_code`      , --门店状态
	nvl( `shop_status` ,'')           as `shop_status`           , --门店状态名称
	nvl( `post_code` ,'')             as `post_code`             , --邮编
	nvl( `purchase_org` ,'')          as `purchase_org`          , --采购组织
	nvl( `purchase_name` ,'')         as `purchase_name`         , --采购组织名称
	nvl( `company_code` ,'')          as `company_code`          , --公司代码
	nvl( `company_name` ,'')          as `company_name`          , --公司代码名称
	nvl( `province_code` ,'')         as `province_code`         , --省区编码
	nvl( `province_name` ,'')         as `province_name`         , --省区名称
	nvl( `prefecture_city` ,'')       as `prefecture_city`       , --地级市编码
	nvl( `prefecture_city_name` ,'')  as `prefecture_city_name`  , --地级市名称
	nvl( `county_city` ,'')           as `county_city`           , --县级市编码
	nvl( `county_city_name` ,'')      as `county_city_name`      , --县级市名称
	nvl( `street` ,'')                as `street`                , --街道
	nvl( `distribution_area` ,'')     as `distribution_area`     , --配送费区域（包含编码，名称）
	nvl(distribution_area_name ,'')   as distribution_area_name  , --配送费区域名称
	nvl( `shop_group_code` ,'')       as `shop_group_code`       , -- 进价群名称
	nvl( `shop_group_name` ,'')       as `shop_group_name`       , --进价群名称
	nvl( `location_code` ,'')         as `location_code`         , --地点编码(新)
	nvl( `ec_flag` ,'')               as `ec_flag`               , -- 是否电商
	nvl( `delivery_cent_type` ,'')    as `delivery_cent_type`    , --配送中心类型名称
	nvl( `delivery_cent_name` ,'')    as `delivery_cent_name`    , --配送中心类型名称
	nvl( `jda_tity` ,'')              as `jda_tity`              , --JDA城市
	nvl( `ascription_type` ,'')       as `ascription_type`       , --区域类型
	nvl( `ascription_type_name` ,'')  as `ascription_type_name`  , --区域类型名称
	nvl( `sales_dist_code` ,'')       as `sales_dist_code`       , -- 销售区域代码
	nvl( `sales_dist` ,'')            as `sales_dist`            , -- 销售区域名称
	nvl( `logistics_mode` ,'')        as `logistics_mode`        , --物流模式
	nvl( `logistics_name` ,'')        as `logistics_name`        , --物流名称
	nvl( `open_date` ,'')             as `open_date`             , --开业日期
	nvl( `shop_type` ,'')             as `shop_type`             , --店类型
	nvl( `market_net_area` ,'')       as `market_net_area`       , --超市净面积
	nvl( `operation_floor` ,'')       as `operation_floor`       , -- 经营楼层
	nvl( `operation_floor_count` ,'') as `operation_floor_count` , -- 经营楼层总数
	nvl( `business_classify` ,'')     as `business_classify`     , -- 商圈分类
	nvl( `food_area` ,'')             as `food_area`             , -- 食品用品营业面积
	nvl( `clothes_area` ,'')          as `clothes_area`          , -- 服装营业面积
	nvl( `fresh_area` ,'')            as `fresh_area`            , -- 生鲜营业面积
	nvl( `process_area` ,'')          as `process_area`          , --加工营业面积
	nvl( `fresh_flag` ,'')            as `fresh_flag`            , -- 是否经营生鲜
	nvl( `table_type` ,'')            as `table_type`            , -- 公司类型
	nvl( `location_uses_code` ,'')    as `location_uses_code`    , -- 仓库用途编码
	nvl( `location_uses` ,'')         as `location_uses`         , -- 仓库用途
	nvl( `location_type_code` ,'')    as `location_type_code`    , -- 仓库类型编码
	nvl( `location_type` ,'')         as `location_type`         , -- 仓库类型
	nvl( `location_status` ,'')       as `location_status`       , --地点启用
	'' `zone_id`                                                 , -- 战区编码
	'' `zone_name`                                               , --战区名称
	'current'
from
	csx_dw.temp_csx_shop
;