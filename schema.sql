CREATE TABLE `clusters` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `name` varchar(150) NOT NULL DEFAULT '' COMMENT '集群名称',
    `listen_host` varchar(150) NOT NULL COMMENT 'dal监听host',
    `listen_port` int(11) NOT NULL COMMENT 'dal监听端口',
    `username` varchar(50) NOT NULL COMMENT '登录dal用户名称',
    `password` varchar(128) NOT NULL COMMENT '登录dal的密码',
    `db_name` varchar(150) NOT NULL DEFAULT '' COMMENT '使用的数据库名称',
    `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `udx_name` (`name`),
    KEY `idx_username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COMMENT='集群';

CREATE TABLE `groups` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `cluster_id` bigint(20) NOT NULL COMMENT '关联哪个集群',
    `db_name` varchar(150) NOT NULL DEFAULT '' COMMENT '使用的数据库名称',
    `gno` int(11) NOT NULL COMMENT '组号',
    `shards` text COMMENT '有哪些分片: 1-10,13,15,20-50',
    `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idx_cluster_id` (`cluster_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COMMENT='组';

CREATE TABLE `nodes` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `group_id` bigint(20) NOT NULL COMMENT '自增ID',
    `role` tinyint(4) NOT NULL DEFAULT '2' COMMENT '角色: 1:master. 2.slave',
    `is_candidate` tinyint(4) NOT NULL DEFAULT '1' COMMENT '是否是候选master: 0:否. 1.是',
    `username` varchar(50) NOT NULL COMMENT '登录dal用户名称',
    `password` varchar(128) NOT NULL COMMENT '登录dal的密码',
    `host` varchar(150) NOT NULL COMMENT 'host',
    `port` int(11) NOT NULL COMMENT 'port',
    `db_name` varchar(150) NOT NULL DEFAULT '' COMMENT '使用的数据库名称',
    `read_weight` int(11) NOT NULL DEFAULT '0' COMMENT '读权重',
    `charset` varchar(50) NOT NULL DEFAULT 'utf8mb4' COMMENT '节点字符集',
    `auto_commit` tinyint(4) NOT NULL DEFAULT '1' COMMENT '是否是自动提交',
    `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idx_group_id` (`group_id`),
    KEY `idx_username` (`username`),
    KEY `idx_host_port` (`host`,`port`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COMMENT='节点名称';

CREATE TABLE `shard_tables` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `cluster_id` bigint(20) NOT NULL COMMENT '关联哪个集群',
    `db_name` varchar(150) NOT NULL DEFAULT '' COMMENT '使用的数据库名称',
    `name` varchar(200) NOT NULL COMMENT '需要shard的表',
    `shard_cols` varchar(2000) NOT NULL COMMENT '用来做shard的字段名称',
    `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    KEY `idx_cluster_id` (`cluster_id`),
    KEY `idx_db_name` (`db_name`),
    KEY `idx_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COMMENT='需要shard的表';

-- ------------------------- 分库分表 --------------------
-- clusters
INSERT INTO clusters VALUES (1, 'cluster-name-01', '0.0.0.0', 13306, 'shop', 'chenhao', 'chenhao1', DEFAULT, DEFAULT);
-- group
INSERT INTO groups VALUES
(1, 1, 'shop_0', 0, '1-10, 12, 15', DEFAULT, DEFAULT),
(2, 1, 'shop_1', 1, '11, 12-14, 16-20', DEFAULT, DEFAULT),
(3, 1, 'shop_2', 2, '31-40', DEFAULT, DEFAULT),
(4, 1, 'shop_3', 3, '21, 22, 23-28, 29, 30', DEFAULT, DEFAULT);
-- nodes
INSERT INTO nodes VALUES
(NULL, 1, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_0', 1, 'utf8mb4', 1, DEFAULT, DEFAULT),
(NULL, 2, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_1', 1, 'utf8mb4', 1, DEFAULT, DEFAULT),
(NULL, 3, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_2', 1, 'utf8mb4', 1, DEFAULT, DEFAULT),
(NULL, 4, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_3', 1, 'utf8mb4', 1, DEFAULT, DEFAULT);
-- shard tables
INSERT INTO shard_tables VALUES
(NULL, 1, 'shop', 'store', 'city, name', DEFAULT, DEFAULT),
(NULL, 1, 'shop', 'staff', 'city, name', DEFAULT, DEFAULT);

-- ------------------------- 非分库分表 --------------------
-- clusters
INSERT INTO clusters VALUES (2, 'cluster-name-02', '0.0.0.0', 23306, 'employees', 'root', 'root12', DEFAULT, DEFAULT);
-- group
INSERT INTO groups VALUES(5, 2, 'employees', 0, '', DEFAULT, DEFAULT);
-- nodes
INSERT INTO nodes VALUES(NULL, 5, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'employees', 1, 'utf8mb4', 1, DEFAULT, DEFAULT);
