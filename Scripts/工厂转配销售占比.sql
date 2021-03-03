drop table
    b2b_tmp.temp_sale;

CREATE temporary table
    b2b_tmp.temp_sale as select
        sdt ,
        case
            when shopid_orig = 'W0B6' then 'BBC'
            when source_id_new = '22' then 'M端'
            else c.sflag
        end qdflag ,
        case
            when c.dist is not null
            and c.sflag <> 'M端' then substr(dist ,
            1 ,
            2)
            when a.cust_id like 'S%'
            and substr(b.prov_name ,
            1 ,
            2) in ('重庆' ,
            '四川' ,
            '北京' ,
            '福建' ,
            '上海' ,
            '浙江' ,
            '江苏' ,
            '安徽') then substr(b.prov_name ,
            1 ,
            2)
            else substr(d.prov_name ,
            1 ,
            2)
        end dist ,
        shopid_orig ,
        goodsid ,
        sum(sale_mtd)sale_mtd ,
        sum(sale_ytd)sale_ytd
    from
        (
        select
            sdt ,
            shop_id ,
            cust_id ,
            shopid_orig ,
            goodsid ,
            sum (
            case
                when sdt >= '20190701' then tax_salevalue
                else 0
            end ) sale_mtd ,
            sum(tax_salevalue) sale_ytd
        from
            csx_ods.sale_b2b_dtl_fct
        where
            sdt >= '20190101'
            and shop_id <> 'W098'
            and sflag in('qyg' ,
            'qyg_c' ,
            'gc')
        group by
            shop_id ,
            cust_id ,
            shopid_orig ,
            goodsid ,
            sdt) a
    left join csx_ods.b2b_customer_new c on
        lpad(a.cust_id ,
        10 ,
        '0')= lpad(c.cust_id ,
        10 ,
        '0')
    left join (
        select
            shop_id ,
            prov_name
        from
            dim.dim_shop
        where
            edate = '9999-12-31' ) d on
        a.shop_id = d.shop_id
    left join (
        select
            shop_id ,
            case
                when shop_id in ('W055' ,
                'W056') then '上海市'
                else prov_name
            end prov_name ,
            case
                when prov_name like '%市' then prov_name
                else city_name
            end city_name
        from
            dim.dim_shop
        where
            edate = '9999-12-31' ) b on
        a.cust_id = concat('S' ,
        b.shop_id)
    group by
        case
            when shopid_orig = 'W0B6' then 'BBC'
            when source_id_new = '22' then 'M端'
            else c.sflag
        end ,
        case
            when c.dist is not null
            and c.sflag <> 'M端' then substr(dist ,
            1 ,
            2)
            when a.cust_id like 'S%'
            and substr(b.prov_name ,
            1 ,
            2) in ('重庆' ,
            '四川' ,
            '北京' ,
            '福建' ,
            '上海' ,
            '浙江' ,
            '江苏' ,
            '安徽') then substr(b.prov_name ,
            1 ,
            2)
            else substr(d.prov_name ,
            1 ,
            2)
        end ,
        shopid_orig ,
        goodsid ,
        sdt ;
-- 商超的成品/转配 按月查询
 select
    substr(a.sdt ,
    1 ,
    6) as mon ,
    a.qdflag ,
    dist ,
    case
        when b.mat_type = '成品'
        and a.goodsid not in ('5990' ,
        '877589') then '成品'
        else '转配'
    end mat_type ,
    count(distinct a.goodsid)sku ,
    sum(sale_mtd) sale_mtd ,
    sum(sale_ytd) sale_ytd
from
    (
    select
        *
    from
        b2b_tmp.temp_sale
    where
        qdflag = 'M端' ) a
left join csx_ods.marc_ecc b on
    a.shopid_orig = b.shop_id
    and a.goodsid = b.goodsid
group by
    a.qdflag ,
    dist ,
    case
        when b.mat_type = '成品'
        and a.goodsid not in ('5990' ,
        '877589') then '成品'
        else '转配'
    end,
    substr(a.sdt ,
    1 ,
    6)
union all select
    substr(a.sdt ,
    1 ,
    6) as mon,
    case
        when a.qdflag is null then 'B端'
        else a.qdflag
    end qdflag ,
    dist ,
    case
        when b.goodsid is not null then '成品'
        else '采购'
    end mat_type ,
    count(distinct a.goodsid)sku ,
    sum(sale_mtd) sale_mtd ,
    sum(sale_ytd) sale_ytd
from
    (
    select
        *
    from
        b2b_tmp.temp_sale
    where
        qdflag <> 'M端'
        or qdflag is null ) a
left join (
    select
        distinct goodsid
    from
        csx_ods.marc_ecc
    where
        mat_type = '成品'
        and goodsid not in ('5990' ,
        '877589') ) b on
    a.goodsid = b.goodsid
group by
    substr(a.sdt ,
    1 ,
    6) ,
    case
        when a.qdflag is null then 'B端'
        else a.qdflag
    end ,
    dist ,
    case
        when b.goodsid is not null then '成品'
        else '采购'
    end ;
-- 商超的成品/转配 渠道省区查询
 select
    a.qdflag ,
    dist ,
    case
        when b.mat_type = '成品'
        and a.goodsid not in ('5990' ,
        '877589') then '成品'
        else '转配'
    end mat_type ,
    count(distinct a.goodsid)sku ,
    sum(sale_mtd) sale_mtd ,
    sum(sale_ytd) sale_ytd
from
    (
    select
        *
    from
        b2b_tmp.temp_sale
    where
        qdflag = 'M端' ) a
left join csx_ods.marc_ecc b on
    a.shopid_orig = b.shop_id
    and a.goodsid = b.goodsid
group by
    a.qdflag ,
    dist ,
    case
        when b.mat_type = '成品'
        and a.goodsid not in ('5990' ,
        '877589') then '成品'
        else '转配'
    end

union all select
    case
        when a.qdflag is null then 'B端'
        else a.qdflag
    end qdflag ,
    dist ,
    case
        when b.goodsid is not null then '成品'
        else '采购'
    end mat_type ,
    count(distinct a.goodsid)sku ,
    sum(sale_mtd) sale_mtd ,
    sum(sale_ytd) sale_ytd
from
    (
    select
        *
    from
        b2b_tmp.temp_sale
    where
        qdflag <> 'M端'
        or qdflag is null ) a
left join (
    select
        distinct goodsid
    from
        csx_ods.marc_ecc
    where
        mat_type = '成品'
        and goodsid not in ('5990' ,
        '877589') ) b on
    a.goodsid = b.goodsid
group by
    case
        when a.qdflag is null then 'B端'
        else a.qdflag
    end ,
    dist ,
    case
        when b.goodsid is not null then '成品'
        else '采购'
    end ;
-- 商超的成品/转配  渠道汇总
 select
    a.qdflag ,
    case
        when b.mat_type = '成品'
        and a.goodsid not in ('5990' ,
        '877589') then '成品'
        else '转配'
    end mat_type ,
    count(distinct a.goodsid)sku ,
    sum(sale_mtd) sale_mtd ,
    sum(sale_ytd) sale_ytd
from
    (
    select
        *
    from
        b2b_tmp.temp_sale
    where
        qdflag = 'M端' ) a
left join csx_ods.marc_ecc b on
    a.shopid_orig = b.shop_id
    and a.goodsid = b.goodsid
group by
    a.qdflag ,
    case
        when b.mat_type = '成品'
        and a.goodsid not in ('5990' ,
        '877589') then '成品'
        else '转配'
    end
union all select
    case
        when a.qdflag is null then 'B端'
        else a.qdflag
    end qdflag ,
    case
        when b.goodsid is not null then '成品'
        else '采购'
    end mat_type ,
    count(distinct a.goodsid)sku ,
    sum(sale_mtd) sale_mtd ,
    sum(sale_ytd) sale_ytd
from
    (
    select
        *
    from
        b2b_tmp.temp_sale
    where
        qdflag <> 'M端'
        or qdflag is null ) a
left join (
    select
        distinct goodsid
    from
        csx_ods.marc_ecc
    where
        mat_type = '成品'
        and goodsid not in ('5990' ,
        '877589') ) b on
    a.goodsid = b.goodsid
group by
    case
        when a.qdflag is null then 'B端'
        else a.qdflag
    end ,
    case
        when b.goodsid is not null then '成品'
        else '采购'
    end ;