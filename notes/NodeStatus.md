## 所有服务器上的易失性状态
### lastCommittedIndex
1. 节点状态发生重置的时候(follower -> leader, leader -> follower), lastCommittedIndex不会被重置
2. 新启动的节点, 或者节点重启, 初始值为0, 比如一个新的leader, 在当前term下未成功commit任何log的前提下, 新的leader是无法感知lastCommittedIndex到底是多少

### lastAppliedIndex
1. 节点状态发生重置的时候(follower -> leader, leader -> follower), lastAppliedIndex不会被重置
2. 新启动的节点, 或者节点重启, 默认值会重置为0