


## client
* 增加 ErrWrongGroup 处理，以及插叙对应的 group。 其它的没有啥事。另外肯定是 server 端先自动用最新的配置。后面的client 端才会跟进



## server
* logcomapaction (需要清理掉对应不属于当前的 shard 的记录)
* shardkv persiste （需要处理掉不属于当前 shard 的记录）， raft persiste 不需要做任何修改
* shard 之间同步数据直接把 state machine 中的数据同步过去


## tips
* 注意需要持久化



