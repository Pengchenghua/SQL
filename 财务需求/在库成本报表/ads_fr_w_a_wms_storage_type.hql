-- create table csx_tmp.ads_fr_w_a_wms_storage_type
-- as 
 set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table  csx_tmp.ads_fr_w_a_wms_storage_type partition(sdt)
select distinct classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    case when classify_large_code between 'B04' and 'B09' then '2' else '1' end as pur_class_code, --采购分类 B04-B09 2、食百 1、生鲜
    case when classify_large_code between 'B04' and 'B09' then '食百' else '生鲜' end as pur_class_name,
    case when ( classify_middle_code='B0304' or classify_small_code in ('B070202','B070203')) then '3'
    when  classify_large_code between 'B04' and 'B09' then '2' else '1' end as wms_storage_type_code,  --仓库存储类型：3、冻品、1、生鲜、2、食百
    case when ( classify_middle_code='B0304' or classify_small_code in ('B070202','B070203')) then '冻品库'
    when  classify_large_code between 'B04' and 'B09' then '食百库' else '生鲜库' end as wms_storage_type_name,
    'current'
from csx_dw.dws_basic_w_a_manage_classify_m where sdt='current';



drop table `csx_tmp.ads_fr_w_a_wms_storage_type`;
CREATE TABLE `csx_tmp.ads_fr_w_a_wms_storage_type`(
  `classify_large_code` string comment '一级分类编码', 
  `classify_large_name` string comment '一级分类名称', 
  `classify_middle_code` string comment '二级分类', 
  `classify_middle_name` string comment '二级分类', 
  `classify_small_code` string comment '三级分类', 
  `classify_small_name` string comment '三级分类',
  `pur_class_code` string comment '采购分类 B04-B09 2、食百 1、其他生鲜', 
  `pur_class_name` string comment '采购分类 B04-B09 食百 其他生鲜', 
  `wms_storage_type_code `string comment '仓库存储类型：3、冻品、1、生鲜、2、食百',
  `wms_storage_type_name` string comment '仓库存储类型：3、冻品、1、生鲜、2、食百'
  )comment 'WMS管理分类仓库仓储类型'
  PARTITIONED BY (sdt string COMMENT '日期当前current')
  ;


