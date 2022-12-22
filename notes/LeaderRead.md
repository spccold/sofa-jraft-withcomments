## Leader Read：
1. 将当前自己的 commit index 记录到一个 local 变量 ReadIndex 里面。
2. 向其他节点发起一次 heartbeat，如果大多数节点返回了对应的 heartbeat response，那么 leader 就能够确定现在自己仍然是 leader。
3. Leader 等待自己的状态机执行，直到 apply index 超过了 ReadIndex，这样就能够安全的提供 linearizable read 了。
4. Leader 执行 read 请求，将结果返回给 client。


## 疑问：
### 问:
为啥一定记录下当前的commit index, 而且等到当前commit index应用到状态机之后才进行读取操作？确定当前节点是leader之后, 直接进行读取操作不行吗？

### 答:
不行的, 先看下线性一致性读的定义

1. 任何一个读取返回新值后，所有后续读取（在相同或其他客户端上）也必须返回新值

2. linearizable:  one of the strongest single-object consistency models, and implies that every operation appears to take place atomically, in some order, consistent with the real-time ordering of those operations: e.g., if operation A completes before operation B begins, then B should logically take effect after A.

3. 个人理解: 自然时钟顺序上, 如果某个操作A已经执行完毕, 那么后续和A操作相关的操作都会受到影响(即: 可以感知到A的操作)，关键就在自然时钟顺序上


在raft算法中, 如果当前term下的一条log复制到majority, 就认为当前的写入已经成功了, 和是否应用到状态机无关，猜想一种场景, 如果此时的读取操作没有记录当前的commit index并等到应用状态机完成，此时读的某个变量(比如x)的值为1, 而commit index写入了x=2，此时client读取的值为x=1，此时当前leader crash, 另外一个节点当选为leader, 新的leader会向其他follower同步日志, 并应用历史已经committed的日志到状态机，这时client再次读取x，获取的值是2，但是在此期间并没有新增任何写入操作, 那这样其实是不满足线性一致性的(<font color=#FF0000>p.s. 解释2可以，但是无法解释1？</font>)

记录下当前的commit index的核心原因就是因为在raft算法中, commit index代表的就是对应的操作已经完成, 但是可能还没有应用到状态机, client还不可见, 所有就一定得等到应用状态机完成之后, 再返回客户端，保障写入完成的操作在接下来的读取过程中一定可见