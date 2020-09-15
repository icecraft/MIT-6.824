# MIT-6.824
Basic Sources for MIT 6.824 Distributed Systems Class

MIT 6.824 课程的学习资料

## 课程安排 Schedule

[课程安排](https://pdos.csail.mit.edu/6.824/schedule.html)

## 视频 Videos

[2020年lectures视频地址](https://www.bilibili.com/video/av87684880)

## 讲座 Lectures

- [Lec1: 入门介绍(以MapReduce为例)](https://github.com/chaozh/MIT-6.824/issues/2)
- [Lec2: RPC与线程机制(Go语言实战)](https://github.com/chaozh/MIT-6.824/issues/3)
- [Lec3: GFS](https://github.com/chaozh/MIT-6.824/issues/6)
- [Lec4：主从备份](https://github.com/chaozh/MIT-6.824/issues/7)
- [Lec 5：Raft基本](https://github.com/chaozh/MIT-6.824/issues/9)
- [Lec6：Raft实现](https://github.com/chaozh/MIT-6.824/issues/10)

## 问题 Questions

记录在issues中

- 课前问题：[对分布式系统课程有啥想说的？](https://github.com/chaozh/MIT-6.824/issues/1)
- [Lab0 完成Crawler与KV的Go语言实验](https://github.com/chaozh/MIT-6.824/issues/4)
- Lab1 MapReduce实验
- [Lec3 请描述客户端从GFS读数据的大致流程？](https://github.com/chaozh/MIT-6.824/issues/6)
- [Lec4 论文中VM FT如何处理网络分区问题？](https://github.com/chaozh/MIT-6.824/issues/7)
- [Lec5 Raft什么机制会阻止他们当选？](https://github.com/chaozh/MIT-6.824/issues/9)
- [Lec6 Figure13中第8步能否导致状态机重置，即接收InstallSnapshot RPC消息能否导致状态回退](https://github.com/chaozh/MIT-6.824/issues/10)

## 参考资料 Related

- [MapReduce(2004)](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)
- [GFS(2003)](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/gfs-sosp2003.pdf)
- [Fault-Tolerant Virtual Machines(2010)](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)
- [Raft Extended(2014)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)



## 学习小组 Group

mit6.824自学互助 QQ群：258537180(群已满)
mit6.824自学互助二群 QQ群：682413790


## Raft 实验 Misc
* go test --count 100 实验时，每次 go test 中的 raft instance 不会被回收会导致以下的问题
  * 拖慢系统，每次实验都会起一些 goroutine
  * 日志很乱
  * 上次实验的 rpc 信号疑似会发送本次实验 and vice versa
  * 存在死锁问题，目前只有 测试用例 TestPersist22C 出现过


## 出现的 bug
* 5 节点集群， leader 3 接受客户端的提交信息 A 后，在节点 0， 2 返回 replicated 成功后。 leader 3 提交信息 A 到状态机。准备在第二次心跳后通知节点 0， 2 提交到状态机。 结果期间 leader 3 宕机。 leader 4 当选为新的 leader，在接受client 提交的信息 B 时， 通知节点 0， 2 上的消息 A 改写为 B ！！！！ （没有严格按照 raft 论文实现，在选主时不应该按照最新 commited 消息的 index 和 term 来比较， 而是应该用 最后一个 log 的）[fixed]
* request vote 和 append msg handler 可以并发进行，没有上锁会导致问题（这两个 handler 里面会先用锁获取 相关的信息后解锁。但是 handler 里面有不确定的 chan send 和 recieve 方法，可能会带来时序的问题～）[fixed]
* TestFigure82C 在跑了 500 次后还是出现了 apply commit error [fixed]
* go test --run 2C 还有有概率会 死锁，但是没有定位出死锁的原因 [fixed]
* 大量这种日志需要修复
```
INFO[0184] IndexHint: 370, matchIndex: 371
INFO[0184] IndexHint: 370, matchIndex: 371
INFO[0187] IndexHint: 527, matchIndex: 529
INFO[0187] IndexHint: 524, matchIndex: 527
INFO[0195] IndexHint: 583, matchIndex: 584
INFO[0195] IndexHint: 583, matchIndex: 584
INFO[0195] IndexHint: 583, matchIndex: 584
INFO[0195] IndexHint: 583, matchIndex: 584
INFO[0195] IndexHint: 583, matchIndex: 584
INFO[0199] IndexHint: 841, matchIndex: 846
INFO[0199] IndexHint: 841, matchIndex: 851
```


