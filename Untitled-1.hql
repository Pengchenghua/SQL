CREATE EXTERNAL TABLE `ods_ecc.ecc_ytbcustomer`(
  `mandt` string COMMENT '科目类型', 
  `keydt` string COMMENT '日期', 
  `bukrs` string COMMENT '公司代码', 
  `belnr` string COMMENT '凭证号', 
  `gjahr` string COMMENT '年度', 
  `buzei` string COMMENT '', 
  `budat` string COMMENT '回款日期', 
  `kunnr` string COMMENT '客户编码', 
  `lifnr` string COMMENT '', 
  `prctr` string COMMENT '地点', 
  `hkont` string COMMENT '科目类型', 
  `dmbtr` string COMMENT '回款金额', 
  `hwaer` string COMMENT '币种', 
  `wtime` string COMMENT '写入时间')
PARTITIONED BY ( 
  `sdt` string COMMENT '日期分区')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://nameservice1/user/hive/warehouse/ods_ecc.db/ecc_ytbcustomer'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///rawdata/db/ecc/ytbcustomer/.schemas/ecc_ytbcustomer.avsc', 
  'transient_lastDdlTime'='1551081247')

select  budat as cash_date,
        hkont as subject_code,
        bukrs as comp_code,
        prctr as shop_id,
        kunnr as customer_no,
        cast (dmbtr decimal(26,6)) as collection
 from  `ods_ecc.ecc_ytbcustomer`
 where 
 hkont LIKE '1122%'
        AND sdt = regexp_replace(to_date(date_sub(current_timestamp(),1)),'-','')
        AND substr(budat,1,4) = substr(regexp_replace(${hiveconf:sdate},'-',''),1,4)
        AND substr(a.belnr,1,1)<>'6'
        AND mandt ='800' 
        AND ( substr(kunnr,1,1) NOT IN ('G',
                                        'L',
                                        'V',
                                        'S')
             OR kunnr='S9961' )
