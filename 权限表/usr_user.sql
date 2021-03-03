SET FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS `usr_user`;
CREATE TABLE `usr_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` varchar(32) NOT NULL COMMENT '用户id',
  `user_name` varchar(32) NOT NULL COMMENT '用户名称',
  `user_work_no` varchar(32) NOT NULL COMMENT '用户工号',
  `position` varchar(32) NOT NULL COMMENT '用户职务',
  `phone` varchar(16) NOT NULL COMMENT '手机号',
  `org` varchar(32) NOT NULL COMMENT '部门',
  `is_deleted` tinyint(1) NOT NULL COMMENT '是否删除',
  `is_able` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否启用：1.启动；0.不启用',
  `create_by` varchar(32) NOT NULL COMMENT '添加人',
  `gmt_create` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '添加时间',
  `update_by` varchar(32) NOT NULL COMMENT '修改人',
  `gmt_modified` datetime NOT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=618 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户表';
