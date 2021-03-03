/* 进出比 ， 异常的标识 ， 高库存有入库 生鲜商品两天未移动 食百15天未移动 
进售价 ： 日常进价和异常进价 进售价 不匹配的 报价异常 进售价 ， 总进价额比预算进价额超过1万元 ； 
趋势 入库 采购报价 / 中台报价 库存平均价 
售价 - 销售毛利低时受那部分影响 */
set
  i_edate = '2019-12-22';
set
  i_sdate = '2019-11-30';
drop table b2b_tmp.temp_newsystem_jxc;
CREATE table b2b_tmp.temp_newsystem_jxc as
select
  goods_code,
  b.goodsname,
  bar_code,
  brand,
  brand_name,
  division_code,
  division_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  department_id,
  department_name,
  dc_code,
  c.shop_name dc_name,
  reservoir_area_code,
  sum(qty_qc) qty_qc,
  sum(amt_qc) amt_qc,
  sum(amt_no_tax_qc) amt_no_tax_qc,
  sum(shrk_qty) shrk_qty,
  sum(shrk_amt) shrk_amt,
  sum(dbrk_qty) dbrk_qty,
  sum(dbrk_amt) dbrk_amt,
  sum(thrk_qty) thrk_qty,
  sum(thrk_amt) thrk_amt,
  sum(qcrk_qty) qcrk_qty,
  sum(qcrk_amt) qcrk_amt,
  sum(cprk_qty) cprk_qty,
  sum(cprk_amt) cprk_amt,
  sum(sale_qty) sale_qty,
  sum(sale_amt) sale_amt,
  sum(dbck_qty) dbck_qty,
  sum(dbck_amt) dbck_amt,
  sum(zt_qty) zt_qty,
  sum(zt_amt) zt_amt,
  sum(lyck_qty) lyck_qty,
  sum(lyck_amt) lyck_amt,
  sum(py_qty) py_qty,
  sum(py_amt) py_amt,
  sum(pk_qty) pk_qty,
  sum(pk_amt) pk_amt,
  sum(bs_qty) bs_qty,
  sum(bs_amt) bs_amt,
  sum(move_qty) move_qty,
  sum(move_amt) move_amt,
  sum(zmp_qty) zmp_qty,
  sum(zmp_amt) zmp_amt,
  sum(qty_qm) qty_qm,
  sum(amt_qm) amt_qm,
  sum(amt_no_tax_qm) amt_no_tax_qm
from (
    select
      goods_code,
      goods_name,
      bar_code,
      brand,
      brand_name,
      division_code,
      division_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      department_id,
      department_name,
      case
        when dc_code like 'E%' then concat(9, substr(dc_code, 2, 3))
        else dc_code
      end dc_code,
      dc_name,
      reservoir_area_code,
      sum (
        case
          when sdt = regexp_replace($ { hiveconf :i_sdate }, '-', '') then qty
          else 0
        end
      ) qty_qc,
      sum (
        case
          when sdt = regexp_replace($ { hiveconf :i_sdate }, '-', '') then amt
          else 0
        end
      ) amt_qc,
      sum (
        case
          when sdt = regexp_replace($ { hiveconf :i_sdate }, '-', '') then amt_no_tax
          else 0
        end
      ) amt_no_tax_qc,
      0 shrk_qty,
      0 shrk_amt,
      0 dbrk_qty,
      0 dbrk_amt,
      0 thrk_qty,
      0 thrk_amt,
      0 qcrk_qty,
      0 qcrk_amt,
      0 cprk_qty,
      0 cprk_amt,
      0 sale_qty,
      0 sale_amt,
      0 dbck_qty,
      0 dbck_amt,
      0 zt_qty,
      0 zt_amt,
      0 lyck_qty,
      0 lyck_amt,
      0 py_qty,
      0 py_amt,
      0 pk_qty,
      0 pk_amt,
      0 bs_qty,
      0 bs_amt,
      0 move_qty,
      0 move_amt,
      0 zmp_qty,
      0 zmp_amt,
      sum (
        case
          when sdt = regexp_replace($ { hiveconf :i_edate }, '-', '') then qty
          else 0
        end
      ) qty_qm,
      sum (
        case
          when sdt = regexp_replace($ { hiveconf :i_edate }, '-', '') then amt
          else 0
        end
      ) amt_qm,
      sum (
        case
          when sdt = regexp_replace($ { hiveconf :i_edate }, '-', '') then amt_no_tax
          else 0
        end
      ) amt_no_tax_qm
    from csx_dw.wms_accounting_stock_m
    where
      sdt in (
        regexp_replace($ { hiveconf :i_sdate }, '-', ''),
        regexp_replace($ { hiveconf :i_edate }, '-', '')
      )
      and sys = 'new'
      and substr(reservoir_area_code, 1, 2) <> 'PD'
    group by
      goods_code,
      goods_name,
      bar_code,
      brand,
      brand_name,
      division_code,
      division_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      department_id,
      department_name,
      case
        when dc_code like 'E%' then concat(9, substr(dc_code, 2, 3))
        else dc_code
      end,
      dc_name,
      reservoir_area_code
    union all
    select
      goods_code,
      goods_name,
      bar_code,
      brand,
      brand_name,
      division_code,
      division_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      department_id,
      department_name,
      case
        when dc_code like 'E%' then concat(9, substr(dc_code, 2, 3))
        else dc_code
      end dc_code,
      dc_name,
      reservoir_area_code,
      0 qty_qc,
      0 amt_qc,
      0 amt_no_tax_qc,
      sum (
        case
          when substr(move_type, 1, 3) = '101'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '101'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) shrk_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '101'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '101'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) shrk_amt,
      --调拨入库
      sum (
        case
          when substr(move_type, 1, 3) in ('102', '105')
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) in ('102', '105')
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) dbrk_qty,
      sum (
        case
          when substr(move_type, 1, 3) in ('102', '105')
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) in ('102', '105')
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) dbrk_amt,
      --退货入库
      sum (
        case
          when substr(move_type, 1, 3) = '108'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '108'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) thrk_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '108'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '108'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) thrk_amt,
      --库存期初导入
      sum (
        case
          when substr(move_type, 1, 3) = '201'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '201'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) qcrk_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '201'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '201'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) qcrk_amt,
      --成品入库
      sum (
        case
          when substr(move_type, 1, 3) = '120'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '120'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) cprk_qty,
      sum(
        case
          when substr(move_type, 1, 3) = '120'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '120'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) cprk_amt,
      --销售出库
      sum (
        case
          when substr(move_type, 1, 3) = '107'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '107'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) sale_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '107'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '107'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) sale_amt,
      --调拨出库
      sum (
        case
          when substr(move_type, 1, 3) in ('104', '106')
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) in ('104', '106')
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) dbck_qty,
      sum (
        case
          when substr(move_type, 1, 3) in ('104', '106')
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) in ('104', '106')
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) dbck_amt,
      --出库到在途
      sum (
        case
          when substr(move_type, 1, 3) = '114'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '114'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) zt_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '114'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '114'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) zt_amt,
      --领用/原料消耗/退料差异
      sum (
        case
          when substr(move_type, 1, 3) in (
            '118',
            '119',
            '121'
          )
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) in (
            '118',
            '119',
            '121'
          )
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) lyck_qty,
      sum (
        case
          when substr(move_type, 1, 3) in (
            '118',
            '119',
            '121'
          )
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) in (
            '118',
            '119',
            '121'
          )
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) lyck_amt,
      --盘盈
      sum (
        case
          when substr(move_type, 1, 3) = '111'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '111'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) py_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '111'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '111'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) py_amt,
      --盘亏
      sum (
        case
          when substr(move_type, 1, 3) = '110'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '110'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) pk_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '110'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '110'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) pk_amt,
      --报损
      sum (
        case
          when substr(move_type, 1, 3) = '117'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '117'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) bs_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '117'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '117'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) bs_amt,
      --移库
      sum (
        case
          when substr(move_type, 1, 3) = '109'
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) = '109'
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) move_qty,
      sum (
        case
          when substr(move_type, 1, 3) = '109'
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) = '109'
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) move_amt,
      --子母品转换
      sum (
        case
          when substr(move_type, 1, 3) in ('112', '113')
          and in_or_out = 0 then qty
          when substr(move_type, 1, 3) in ('112', '113')
          and in_or_out = 1 then -1 * qty
          else 0
        end
      ) zmp_qty,
      sum (
        case
          when substr(move_type, 1, 3) in ('112', '113')
          and in_or_out = 0 then amt
          when substr(move_type, 1, 3) in ('112', '113')
          and in_or_out = 1 then -1 * amt
          else 0
        end
      ) zmp_amt,
      0 qty_qm,
      0 amt_qm,
      0 amt_no_tax_qm
    from csx_dw.wms_accounting_stock_operation_item_m a
    where
      sdt <= regexp_replace($ { hiveconf :i_edate }, '-', '')
      and sdt > regexp_replace($ { hiveconf :i_sdate }, '-', '')
      and substr(reservoir_area_code, 1, 2) <> 'PD'
    group by
      goods_code,
      goods_name,
      bar_code,
      brand,
      brand_name,
      division_code,
      division_name,
      category_large_code,
      category_large_name,
      category_middle_code,
      category_middle_name,
      category_small_code,
      category_small_name,
      department_id,
      department_name,
      case
        when dc_code like 'E%' then concat(9, substr(dc_code, 2, 3))
        else dc_code
      end,
      dc_name,
      reservoir_area_code
  ) t
left join (
    select
      goodsid,
      regexp_replace(regexp_replace(goodsname, '\n', ''), '\r', '') goodsname
    from dim.dim_goods
    where
      edate = '9999-12-31'
  ) b on t.goods_code = b.goodsid
left join (
    select
      shop_id,
      shop_name
    from csx_dw.shop_m
    where
      sdt = 'current'
  ) c on t.dc_code = c.shop_id
group by
  goods_code,
  b.goodsname,
  bar_code,
  brand,
  brand_name,
  division_code,
  division_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  department_id,
  department_name,
  dc_code,
  c.shop_name,
  reservoir_area_code;
select
  goods_code,
  goodsname,
  bar_code,
  brand,
  brand_name,
  division_code,
  division_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  department_id,
  department_name,
  dc_code,
  dc_name,
  sum(qty_qc) qty_qc,
  sum(amt_qc) amt_qc,
  sum(amt_no_tax_qc) amt_no_tax_qc,
  sum(shrk_qty) shrk_qty,
  sum(shrk_amt) shrk_amt,
  sum(dbrk_qty) dbrk_qty,
  sum(dbrk_amt) dbrk_amt,
  sum(thrk_qty) thrk_qty,
  sum(thrk_amt) thrk_amt,
  sum(qcrk_qty) qcrk_qty,
  sum(qcrk_amt) qcrk_amt,
  sum(cprk_qty) cprk_qty,
  sum(cprk_amt) cprk_amt,
  sum(sale_qty) sale_qty,
  sum(sale_amt) sale_amt,
  sum(dbck_qty) dbck_qty,
  sum(dbck_amt) dbck_amt,
  sum(zt_qty) zt_qty,
  sum(zt_amt) zt_amt,
  sum(lyck_qty) lyck_qty,
  sum(lyck_amt) lyck_amt,
  sum(py_qty) py_qty,
  sum(py_amt) py_amt,
  sum(pk_qty) pk_qty,
  sum(pk_amt) pk_amt,
  sum(bs_qty) bs_qty,
  sum(bs_amt) bs_amt,
  sum(move_qty) move_qty,
  sum(move_amt) move_amt,
  sum(zmp_qty) zmp_qty,
  sum(zmp_amt) zmp_amt,
  sum(qty_qm) qty_qm,
  sum(amt_qm) amt_qm,
  sum(amt_no_tax_qm) amt_no_tax_qm
from b2b_tmp.temp_newsystem_jxc a
where
  (
    qty_qc <> 0
    or amt_qc <> 0
    or shrk_qty <> 0
    or dbrk_qty <> 0
    or thrk_qty <> 0
    or qcrk_qty <> 0
    or cprk_qty <> 0
    or sale_qty <> 0
    or dbck_qty <> 0
    or zt_qty <> 0
    or lyck_qty <> 0
    or py_qty <> 0
    or pk_qty <> 0
    or bs_qty <> 0
    or move_qty <> 0
    or zmp_qty <> 0
    or qty_qm <> 0
    or amt_qm <> 0
  )
  and substr(reservoir_area_code, 1, 2) <> 'TS'
group by
  goods_code,
  goodsname,
  bar_code,
  brand,
  brand_name,
  division_code,
  division_name,
  category_large_code,
  category_large_name,
  category_middle_code,
  category_middle_name,
  category_small_code,
  category_small_name,
  department_id,
  department_name,
  dc_code,
  dc_name;
  /* 
        PD仓里面的库存 ；                                                                                                                             
         --如果某商品本质是库存期初导入那么他的link_wms_move_type='201A'link_wms_entry_order_type='期初结存'该记录无盘点过账-盘盈记录，
        同样如果是119 ， 120 成品原料转换也不计入过账 - 盘盈记录中 ， 也就是真实的盘点过账 
        -- 盘盈只记录link_wms_move_type = '111A' B是反向的冲销
        -- 101A 收货入库 
        -- 102A 调拨入库 
        -- 103A 退货出库 
        104A 调拨出库 --
        105A 调拨退货入库                                                                                                                         
        106A 调拨退货出库                                                                                                                         
        107A 销售出库                                                                                                                             
        108A 退货入库 - 109A 移库                                                                                                                 
        110A 盘亏从正常仓到待处理仓                                                                                                               
        111A 盘盈从待处理仓到正常仓                                                                                                               
        112A 子品转母品                                                                                                                           
        113A 母品转子品                                                                                                                           
        114A 出库到在途                                                                                                                           
        115A 盘点过账 - 盘盈 116A 盘点过账 - 盘亏 117A 报损                                                                                       
        118A 领用                                                                                                                                 
        119A 原料转成品 - 原料消耗                                                                                                                
        120A 原料转成品 - 成品入库                                                                                                                
        121A 退料差异                                                                                                                             
        201A 库存期初导入                                                                                                                         
        主导架构 新人熟悉 数据支持 */
