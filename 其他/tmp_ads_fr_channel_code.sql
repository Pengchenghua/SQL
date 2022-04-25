drop table csx_tmp.ads_fr_channel_code;
create table csx_tmp.ads_fr_channel_code
( id string comment '原channel_code 编码 ',
 channel_code string comment '组合编码',
 channel_name string comment '组合名称'
)comment '渠道聚合'
;

drop table csx_tmp.ads_fr_channel_code_01 ;
create temporary table  csx_tmp.ads_fr_channel_code_01 as 
select '1' id,'1' channel_code,'大客户' channel_name
union all 
select '2' id,'2' channel_code,'商超' channel_name
union all 
select '3' id,'1' channel_code,'大客户' channel_name
union all 
select '4' id,'4' channel_code,'大宗(一部)' channel_name
union all 
select '5' id,'5' channel_code,'大宗(二部)' channel_name
union all 
select '6' id,'6' channel_code,'大宗(二部)' channel_name
union all 
select '9' id,'1' channel_code,'大客户' channel_name
union all 
select '7' id,'1' channel_code,'大客户' channel_name
union all 
select '8' id,'8' channel_code,'其他' channel_name
;

insert overwrite table csx_tmp.ads_fr_channel_code
select * from  csx_tmp.ads_fr_channel_code_01;

select * from csx_tmp.ads_fr_channel_code;