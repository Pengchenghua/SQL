select * from csx_dw.wms_shipped_order
where sdt>='20200301' and sys ='new' 
and shipped_location_code ='W0A3'
and order_no ='OM200626005653';