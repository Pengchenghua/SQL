select *, FROM_UNIXTIME(gcStartTime/1000,'%Y-%m-%d %H:%i:%s' ) gcStartTime from FINE_RECORD_GC frg where FROM_UNIXTIME(time/1000,'%Y-%m-%d %H:%i:%s' ) >='2021-02-03 00:00:00' and node ='192'

;
select *, FROM_UNIXTIME(time/1000,'%Y-%m-%d %H:%i:%s' ) from FINE_RECORD_EXECUTE fre where FROM_UNIXTIME(time/1000,'%Y-%m-%d %H:%i:%s' ) >='2021-02-04 00:00:00' and tname  like '%商品销售汇总查询(运营)%';
select * from FINE_REAL_TIME_USAGE frtu ;