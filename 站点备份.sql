-- 备份站点表backup_dws_crm_r_d_customer_site_info
insert into data_analysis_prd.backup_dws_crm_r_d_customer_site_info
SELECT *,DATE_FORMAT(CURRENT_DATE()-1,"%Y%m%d") FROM data_analysis_prd.dws_crm_r_d_customer_site_info;



-- 备份站点表backup_dws_crm_r_d_customer_site_info
insert into data_analysis_prd.backup_dws_crm_r_d_customer_site_info
SELECT *,DATE_FORMAT(CURRENT_DATE()-1,"%Y%m%d") FROM data_analysis_prd.dws_crm_r_d_customer_site_info


=if($mon=format(today()-1,"yyyyMM"),format(today()-1,"yyyy-MM-dd"),DATEINMONTH(format(CONCATENATE($mon,"01"),"yyyy-MM-dd"),-1))

format(today()-1,"yyyyMM")

32
3