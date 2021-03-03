create table csx_tmp.temp_goods
(goodsid string 
);

select * from csx_tmp.temp_goods;


SET hive.execution.engine=tez; 

-- insert overwrite directory '/tmp/pengchenghua/sale03' row format delimited fields terminated by '\t'
;
-- INVALIDATE METADATA csx_tmp.yuncao_sale;
drop table csx_tmp.yuncao_sale;
create table csx_tmp.yuncao_sale as 
select shop_id,
        a.goodsid,
        goodsname,
       c.div_id,
       c.div_name,
       c.catg_l_id,
       c.catg_l_name,
       c.catg_m_id,
       c.catg_m_name,
       c.catg_s_id,
        sum(case when sdt>='20201201' then a.tax_value + a.sales_val - a.subtatal_5 end ) sale,
        sum(case when sdt>='20201201' then a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt end) profit,
         sum(case when sdt>=regexp_replace(date_sub(current_date(),31),'-','') then a.tax_value + a.sales_val - a.subtatal_5 end ) sale_01,
        sum(case when sdt>=regexp_replace(date_sub(current_date(),31),'-','') then a.tax_value + a.sales_val - a.subtatal_5 - a.cost_amt + a.pro_chg_amt end) profit_01
        from dw.sale_sap_dtl_fct a
join 
csx_tmp.temp_goods b on a.goodsid=b.goodsid
join 
(SELECT goodsid,
       goodsname,
       div_id,
       div_name,
       catg_l_id,
       catg_l_name,
       catg_m_id,
       catg_m_name,
       catg_s_id,
       catg_s_name
FROM dim.dim_goods
WHERE edate='9999-12-31') c on a.goodsid=c.goodsid
WHERE
        a.bill_type IN ('',
        'S1',
        'S2',
        'ZF1',
        'ZF2',
        'ZR1',
        'ZR2',
        'ZFP',
        'ZFP1')
        AND sdt <= '20201207'
        AND a.sdt >= '20201101'
and a.shop_id in ('90V3','9893','9839','9700','9549','9531','9440','9439','9387','9296','9199','9165','9163','9144','9139','9129','9116','9078','9047','9046','9045','9039','9038','9024','9023','9019','9018','9017','9015','9012','9009')
group by
        shop_id,
        a.goodsid,
        goodsname,
      c.div_id,
      c.div_name,
      c.catg_l_id,
      c.catg_l_name,
      c.catg_m_id,
      c.catg_m_name,
      c.catg_s_id;