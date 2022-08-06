-- 现金录入&预付款录入
create table source_scm_w_a_no_ticket_cash_limit_input  
(
    id BIGINT NOT NULL auto_increment COMMENT 'id',
    months VARCHAR(10) NOT NULL default COMMENT '可用月份',
    province_name VARCHAR(10) NOT NULL default COMMENT '省区名称',
    city_name VARCHAR(10) NOT NULL default COMMENT '城市名称',
    limit_amount VARCHAR(10) NOT NULL default COMMENT '额度',
    create_by VARCHAR(10) NOT NULL default 'sys' COMMENT '创建人',
    create_time TIMESTAMP NOT NULL default '0000-00-00 00:00:00'   COMMENT '创建时间',
    update_by VARCHAR(10) NOT NULL default 'sys' COMMENT '更新人',
    update_time TIMESTAMP NOT NULL default  '0000-00-00 00:00:00' on update CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY (province_name,months) using  btree
) ENGINE=InnoDB auto_increment CHARSET=utf8mb4 comment ='无票现金可用额度录入'
;

create table source_scm_w_a_advances_limit_input  
(
    id BIGINT NOT NULL auto_increment COMMENT 'id',
    QUARTER VARCHAR(10) NOT NULL default COMMENT '季度',
    province_name VARCHAR(10) NOT NULL default COMMENT '省区名称',
    limit_amount VARCHAR(10) NOT NULL default COMMENT '额度',
    create_by VARCHAR(10) NOT NULL default 'sys' COMMENT '创建人',
    create_time TIMESTAMP NOT NULL default '0000-00-00 00:00:00'   COMMENT '创建时间',
    update_by VARCHAR(10) NOT NULL default 'sys' COMMENT '更新人',
    update_time TIMESTAMP NOT NULL default  '0000-00-00 00:00:00' on update CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (id),
    KEY (province_name,months) using  btree
) ENGINE=InnoDB  CHARSET=utf8mb4 comment ='预付款额度录入'
;