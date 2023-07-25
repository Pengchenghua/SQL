
select
	channel ,
	sales_province ,
	sales_name ,
	work_no ,
	customer_no ,
	customer_name ,
	in_value,
	in_amt
from
	(
	select
		customer_no ,
		customer_name ,
		sales_province ,
		channel ,
		sales_name ,
		work_no
	from
		csx_dw.customer_m as cm
	where
		sdt = ${hiveconf:edate}
		and customer_no <> '' )a
join (
	SELECT
		kunnr,
		in_value,
		in_amt
	FROM
		b2b_tmp.temp_current02 ) c ON
	regexp_replace(a.customer_no ,
	'(^0*)',
	'')= regexp_replace(kunnr ,
	'(^0*)',
	'')
;
select
		*
	from
		ods_ecc.ecc_ytbcustomer a
	where
		hkont like '1122%'
		and sdt = '20200108'
		and a.budat >= '20191201' and a.budat < '20200101'
		and substr(a.belnr,1,1)<> '6'
		and mandt = '800'
		and kunnr in ('0000102215',
'0000103875',
'0000103945',
'0000104141',
'0000104222',
'0000104229',
'0000104239',
'0000104241',
'0000104251',
'0000104255',
'0000104538',
'0000104752',
'0000104843',
'0000105005',
'0000105024',
'0000105098',
'0000105756',
'0000107579',
'0000107634')
		;
SELECT * FROM csx_dw.customer_m where sdt='20200108' and customer_name like '重庆朝阳%';

select * from csx_dw.receivables_collection where sdt>='20191201'  and customer_no='104538'
;



select
    a.hkont          ,
    a.bukrs comp_code,
    case
        when length(a.kunnr)<3
            then a.lifnr
            else a.kunnr
    end kunnr   ,
    a.budat     ,
    'A'prctr    ,
    'A'shop_name,
    a.dmbtr     ,
    case
        when kunnr in ('V7126',
                       'V7127',
                       'V7128',
                       'V7129',
                       'V7130',
                       'V7131',
                       'V7132',
                       'V7000')
            then 'Y004'
            else coalesce(c.zterm,d.zterm)
    end zterm,
    case
        when kunnr in ('V7126',
                       'V7127',
                       'V7128',
                       'V7129',
                       'V7130',
                       'V7131',
                       'V7132',
                       'V7000')
            then 45
            else coalesce(c.diff,d.diff)
    end                                                                         diff ,
    concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)) sdate,
    case
        when kunnr in ('V7126',
                       'V7127',
                       'V7128',
                       'V7129',
                       'V7130',
                       'V7131',
                       'V7132',
                       'V7000')
            then to_date(date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),45))
        when coalesce(c.zterm,d.zterm) like 'Y%'
            then to_date(date_add(last_day(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2))),COALESCE(c.diff,d.diff,0)))
            else to_date(date_add(concat(substr(a.budat,1,4),'-',substr(a.budat,5,2),'-',substr(a.budat,7,2)),COALESCE(c.diff,d.diff,0)))
    end edate
from
    (
        select *
        from
                    ods_ecc.ecc_ytbcustomer
        where
            sdt      ='20200423'
            and budat<'20200423'
            and mandt='800'
            AND kunnr ='0000109342'
            and
            (
                substr(hkont,1,3)<>'139'
                or
                (
                    substr(hkont,1,3)='139'
                    and budat       >='20190201'
                )
            )
    )
    a
    left join
        (
            select
                customer_number    ,
                company_code       ,
                payment_terms             zterm,
                cast(payment_days as int) diff
            from
                csx_dw.customer_account_day a
            where
                sdt                 ='current'
                and customer_number<>''
        )
        c
        on
            (
                lpad(a.kunnr,10,'0')=lpad(c.customer_number,10,'0')
                and a.bukrs         =c.company_code
            )
    left join
        (
            select
                customer_no        ,
                payment_terms             zterm,
                cast(payment_days as int) diff
            from
                csx_dw.dws_crm_w_a_customer_m
            where
                sdt             =regexp_replace(to_date(date_sub(CURRENT_TIMESTAMP() ,1)),'-','')
                and customer_no<>''
        )
        d
        on
            lpad(a.kunnr,10,'0')=lpad(d.customer_no,10,'0')
;

select *
        from
                    ods_ecc.ecc_ytbcustomer
        where
            sdt      ='20200604'
            and budat<'20200603'
            and mandt='800'
          AND kunnr ='0000104601'
   and  concat_ws('-',
        hkont ,
        gjahr,
        belnr,
        bukrs)  in (
    '1122010000-2020-0090526358-1933',
    '1122010000-2020-0090526357-1933',
    '1122010000-2020-0090446438-1933',
    '1122010000-2020-0090446437-1933',
    '1122010000-2020-0090446436-1933',
    '1122010000-2020-0101042210-2200',
    '1122010000-2020-0100794408-2121',
    '1122010000-2020-0100794407-2121',
    '1122010000-2020-0100698829-2121',
    '1122010000-2020-0100698828-2121',
    '1122010000-2020-0100698815-2121',
    '1122010000-2020-0100698814-2121',
    '1122010000-2020-0100698811-2121',
    '1122010000-2020-0100698810-2121',
    '1122010000-2020-0100698807-2121',
    '1122010000-2020-0100698806-2121',
    '1122010000-2020-0100599788-2202',
    '1122010000-2020-0100387789-2400',
    '1122010000-2020-0100384016-2300',
    '1122010000-2020-0100343582-2403',
    '1122010000-2020-0100343559-2403',
    '1122010000-2020-0100343558-2403',
    '1122010000-2020-0100339686-2402',
    '1122010000-2020-0100245041-2303',
    '1122010000-2020-0100154283-2700',
    '1122010000-2020-0100066952-2105',
    '1122010000-2020-0100004543-2800',
    -- 20200526 增加
    '1122010000-2020-0100183238-2700',
    '1122010000-2020-0100404461-2402',
    '1122010000-2020-0100467273-2400',
    '1122010000-2020-0100468834-2300',
    '1122010000-2020-0100755372-2202',
    '1122010000-2020-0100873656-2121',
    '1122010000-2020-0101263298-2200',
    '1122010000-2020-0090572072-1933')
    ;


--逾期前5
    select
        calmonth,
        sales_province_code,
        sales_province,
        sum(ac_all )/10000 ac_all ,
        sum(ac_wdq  )/10000 ac_wdq,
        sum(ac_all-ac_wdq )/10000 ac_ovendue
    FROM
        csx_dw.account_age_dtl_fct_new a 
        join 
     (select max(m.calday)as max_sdt ,m.calmonth from csx_dw.dws_w_a_date_m as m where calmonth >='201912' and  calmonth<='202006'group by calmonth )c on a.sdt=c.max_sdt
     join
     (select customer_no,cm.province_code ,cm.province_name,cm.sales_province,cm.sales_province_code 
        from csx_dw.dws_crm_w_a_customer_m as cm where sdt='20200608' and customer_no !='' and channel like '%大%')b on regexp_replace(kunnr,'^0*','')=b.customer_no
 group by  calmonth,
       sales_province_code,
        sales_province;
        
    
select * from ods_ecc.ecc_ytbcustomer where sdt='20200814' and budat <='20200701' and kunnr ='0000103010';