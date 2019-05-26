package algorithm

type Algorithm interface {
	/* 将字段进行hash获取分片值
	 * Args:
	 *     shardCNT: 总共的分片数
	 *     cols: 需要进行hash的字段
	 * Return:
	 *     int: hash 后的分片号(代表数据属于那个分片)
	 *     error: 错误
	 */
	GetShardNo(cols ...interface{}) (int, error)
	GetShardNoByCnt(shardCnt int, cols ...interface{}) (int, error)
}
