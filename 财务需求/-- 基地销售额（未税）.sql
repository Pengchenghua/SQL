-- 基地销售额（未税）
SELECT concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) qq,
substr(sdt,1,6) as mon,
goods_code,
goods_name,
sum(excluding_tax_sales) as sale,
sum(excluding_tax_profit) as profit,
sum(excluding_tax_profit) /sum(excluding_tax_sales)
 from csx_dw.dws_sale_r_d_detail
where sdt>='20200101' and sdt<'20211001'
and goods_code in 
('1168186','1168187','1168188','1186702','1194210',
'1201070','1201071','1201072','1201073','1201074','1216354','1216633','1216634','762651','1194209')
GROUP BY
concat(substr(sdt,1,4),'Q',floor(substr(sdt,5,2)/3.1)+1) ,
goods_code,
goods_name,
substr(sdt,1,6) 