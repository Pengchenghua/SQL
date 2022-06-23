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

-- 基地商品入库平均成本
select months, province_name,goods_code,goods_name,sum(receive_amt)/sum(receive_qty) price from  csx_tmp.report_fr_r_m_financial_purchase_detail 
where  1=1
and province_name in ( '四川省' )
and business_type_name !='云超配送'
and source_type_code  not in ('4','18')
and goods_code in ('3695','1330713','262352','2230','1330712','153890',
    '1065513','1251396','576','263859','2112','562','538','883188','620',
    '1134244','1356734','317132','1374480')
and months>='202106'
group by months, province_name,goods_code,goods_name
;
