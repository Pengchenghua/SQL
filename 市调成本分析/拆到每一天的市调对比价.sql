select 
                gg2.sdt,
                gg1.performance_region_code,
                gg1.performance_province_code,
                gg1.performance_city_code,
                gg1.goods_code,
                avg(gg1.price) as pro_db_price 
         from 
                        (select 
                            g3.performance_region_code,
                            g3.performance_province_code,
                            g3.performance_city_code,
                            g4.goods_code,
                            g1.price,
                            g1.price_begin_date,
                            g1.price_end_date 
                        from 
                                (-- 非云超报价
                                select 
                                         shop_code,product_id,price,
                                         regexp_replace(substr(price_begin_time,1,10),'-','') as price_begin_date,
                                         regexp_replace(substr(price_end_time,1,10),'-','') as price_end_date 
                                from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_di 
                                where sdt>=regexp_replace(date_add(add_months(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-2),1-${days_of_month}),'-','')   
                                and regexp_replace(substr(price_end_time,1,10),'-','')>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-14-dayofweek(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'))),'-','') 
                                and shop_code in ('9149','9272','9294','9396','95E0','RW7','YS33','YW173','ZD106','ZD21','ZD281','ZD95','ZD101','9201','RS26') 
                                union all 
                                -- 云超报价
                                select 
                                         shop_code,product_id,price,
                                         regexp_replace(substr(price_begin_time,1,10),'-','') as price_begin_date,
                                         regexp_replace(substr(price_end_time,1,10),'-','') as price_end_date 
                                from csx_ods.csx_ods_csx_price_prod_market_research_price_di 
                                where sdt>=regexp_replace(date_add(add_months(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-2),1-${days_of_month}),'-','')   
                                and regexp_replace(substr(price_end_time,1,10),'-','')>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-14-dayofweek(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'))),'-','') 
                                and shop_code in ('9149','9272','9294','9396','95E0','RW7','YS33','YW173','ZD106','ZD21','ZD281','ZD95','ZD101','9201','RS26') 
                                ) g1 
                                left join 
                                (select * 
                                from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
                                where sdt='${yes_date}'
                                ) g2 
                                on g1.product_id=g2.id 
                                left join 
                                (select *  
                                from csx_dim.csx_dim_shop  
                                where sdt='current' 
                                ) g3 
                                on g2.location_code=g3.shop_code 
                                left join 
                                (select * 
                                from csx_dim.csx_dim_basic_goods 
                                where sdt='current'
                                ) g4 
                                on g2.product_code=g4.goods_code 
                                -- where g4.classify_middle_name='${classify_name}' 
                        ) gg1 
                        cross join 
                        (select distinct calday as sdt 
                         from csx_dim.csx_dim_basic_date 
                         where calday>=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-14-dayofweek(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'))),'-','')  
                         and calday<=regexp_replace(date_add(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'),-1-dayofweek(from_unixtime(unix_timestamp('${yes_date}','yyyyMMdd'),'yyyy-MM-dd'))),'-','') 
                        ) gg2 
                        on gg1.price_begin_date<=gg2.sdt and gg1.price_end_date>=gg2.sdt 
        group by 
                gg2.sdt,
                gg1.performance_region_code,
                gg1.performance_province_code,
                gg1.performance_city_code,
                gg1.goods_code