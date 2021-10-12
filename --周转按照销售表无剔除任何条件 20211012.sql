--周转按照销售表无剔除任何条件 20211012
drop table csx_tmp.temp_aa ;
create temporary  table csx_tmp.temp_aa as 
select province_code,
    province_name,
    a.dc_code,
    dc_name,
    goods_id,
    goods_name,
    a.division_code,
    a.division_name,
    classify_large_code,
    classify_large_name,
    classify_middle_code,
    classify_middle_name,
    classify_small_code,
    classify_small_name,
    dept_id,
    dept_name,
    period_inv_amt_30day,
    final_amt,
    final_qty,
    a.material_take_amt,
    a.receipt_amt,
    cost_30,
    a.dc_uses
from csx_tmp.ads_wms_r_d_goods_turnover a
left join
(select dc_code,goods_code,
    sum(sales_cost) cost_30
from csx_dw.dws_sale_r_d_detail
where sdt>='20210901'
and sdt<='20210930'
group by  dc_code,goods_code
)b 
on a.dc_code=b.dc_code and a.goods_id=b.goods_code
where sdt='20210930'
;

select *, period_inv_amt_30day/(material_take_amt+receipt_amt+ cost_30) as turn_day from csx_tmp.temp_aa a where  a.division_code in ('10','11');
select *, period_inv_amt_30day/(cost_30) as turn_day from csx_tmp.temp_aa a where  a.division_code in ('12','13','14','15');
