CREATE TABLE clusters (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    name VARCHAR(150) NOT NULL DEFAULT '' COMMENT '集群名称',
    db_name VARCHAR(150) NOT NULL DEFAULT '' COMMENT '使用的数据库名称',
    username VARCHAR(50) NOT NULL COMMENT '登录dal用户名称',
    `password` VARCHAR(128) NOT NULL COMMENT '登录dal的密码',
    is_shard tinyint NOT NULL DEFAULT '0' COMMENT '是否是shard: 0否, 1是',
    created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY(id),
    UNIQUE INDEX udx_name(name),
    INDEX idx_username(username)
)COMMENT='集群';

CREATE TABLE groups(
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    cluster_id BIGINT NOT NULL COMMENT '关联哪个集群',
    db_name VARCHAR(150) NOT NULL DEFAULT '' COMMENT '使用的数据库名称',
    gno INT NOT NULL COMMENT '组号',
    shards VARCHAR(2000) NOT NULL DEFAULT '' COMMENT '有哪些分片: 1-10,13,15,20-50',
    created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY(id),
    INDEX idx_cluster_id(cluster_id)
)COMMENT='组';

CREATE TABLE nodes(
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    group_id BIGINT NOT NULL COMMENT '自增ID',
    role TINYINT NOT NULL DEFAULT '2' COMMENT '角色: 1:master. 2.slave',
    is_candidate TINYINT NOT NULL DEFAULT '1' COMMENT '是否是候选master: 0:否. 1.是',
    username VARCHAR(50) NOT NULL COMMENT '登录dal用户名称',
    `password` VARCHAR(128) NOT NULL COMMENT '登录dal的密码',
    host VARCHAR(150) NOT NULL COMMENT 'host',
    port INT NOT NULL COMMENT 'port',
    db_name VARCHAR(150) NOT NULL DEFAULT '' COMMENT '使用的数据库名称',
    read_weight INT NOT NULL DEFAULT 0 COMMENT '读权重',
    `charset` VARCHAR(50) NOT NULL DEFAULT 'utf8mb4' COMMENT '节点字符集',
    is_auto_commit TINYINT NOT NULL DEFAULT '1' COMMENT '是否是自动提交',
    created_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY(id),
    INDEX idx_group_id(group_id),
    INDEX idx_username(username),
    INDEX idx_host_port(host, port)
)COMMENT='节点名称';

-- clusters
INSERT INTO clusters VALUES (NULL, 'first', 'shop', 'chenhao', 'chenhao1', 1, DEFAULT, DEFAULT);

-- shard tables
INSERT INTO shard_tables VALUES
(NULL, 1, 'store', 'city, name', DEFAULT, DEFAULT),
(NULL, 1, 'staff', 'city, name', DEFAULT, DEFAULT);

-- group
INSERT INTO groups VALUES
(NULL, 1, 'shop_0', 0, '1-10, 12, 15', DEFAULT, DEFAULT),
(NULL, 1, 'shop_1', 1, '11, 12-14, 16-20', DEFAULT, DEFAULT),
(NULL, 1, 'shop_2', 2, '31-40', DEFAULT, DEFAULT),
(NULL, 1, 'shop_3', 3, '21, 22, 23-28, 29, 30', DEFAULT, DEFAULT);

-- nodes
INSERT INTO nodes VALUES
(NULL, 1, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_0', 1, 'utf8mb4', 1, DEFAULT, DEFAULT),
(NULL, 2, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_1', 1, 'utf8mb4', 1, DEFAULT, DEFAULT),
(NULL, 3, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_2', 1, 'utf8mb4', 1, DEFAULT, DEFAULT),
(NULL, 4, 1, 1, 'HH', 'oracle12', '127.0.0.1', 3306, 'shop_3', 1, 'utf8mb4', 1, DEFAULT, DEFAULT);
