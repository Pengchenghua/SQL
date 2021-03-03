
--   and move_type in ('107A','114A') 
--   and reservoir_area_code ='TS01';
   
 --11 12 盘盈 盘亏 不含税 amt_no_tax,---含税 amt
 
select a.location_code ,shop_name,case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
sum(case when amt_no_tax>=0 then -amt_no_tax end )/10000  inventory_p_no, --盘盈  
sum(case when amt_no_tax<0 then -amt_no_tax end )/10000  inventory_l_no, --盘亏
sum(case when amt>=0 then -amt end )/10000  inventory_p, --盘盈  
sum(case when amt<0 then -amt end )/10000  inventory_l --盘亏
from
(select a.*
from csx_ods.source_sync_r_d_data_sync_inventory_item a
where a.sdt = '19990101'
and a.reservoir_area_code IN('PD01','PD02' )
-- and ( a.purchase_group_code like 'H%' or a.purchase_group_code like 'U%' ) 
and a.posting_time >= '2020-05-01 00:00:00' 
and a.posting_time < '2020-06-01 00:00:00')a
join 
(select location_code,shop_name,province_name
from csx_dw.csx_shop where sdt = 'current' and location_code ='W0A5') b on b.location_code=a.location_code
group by case when a.location_code='W0H4' then '供应链' else b.province_name end,a.location_code ,shop_name;