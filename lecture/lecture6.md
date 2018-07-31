## 6.824 2018 Lecture 6: Raft (2)
#### 回顾整体计划
提供一个像lab3中一样的键值服务<br>
目标：在客户端看来如通过单机服务一样<br>
目标：即使少数机器宕机或者连接不上，服务也依然可用<br>
要注意网络分区和脑裂<br>
[分层: 客户端, k/v 存储, k/v 表, raft 层, raft 日志]<br>
[[client RPC -> Start() -> majority commit protocol -> applyCh]]<br>
状态机，应用，服务

###几个提醒
leader在多数成员回复后进行commits/executes操作<br>
leader通知follower提交<br>
为什么等待多数成员而不是所有成员？<br>
&emsp;即使少数成员宕机也需要保证可用
为什么多数成员即可？<br>
&emsp;两个有多数成员组成的集合一定有公共成员<br>
&emsp;前后相邻的两个leader达成一致的多数派成员至少已一个相同<br>
&emsp;因此后一个leader一定可以看见前一个leader所提交的日志<br>
注意这里的“多数”指的的是包括所有成员在内的大多数（包括在线的和离线的），而不是仅仅指在线的。<br>

#### topic: the Raft log (Lab 2B)
只要leader在线<br>
&emsp;客户端只和leader交互<br>
&emsp;客户端不受follower的影响

当leader发生改变的时候，事情变得有意思了<br>
&emsp;例如，老的leader宕机了<br>
&emsp;如何在客户端没有察觉的情况下选举出新的leader<br>
&emsp;&emsp;读取待过旧的值，重复操作，乱序操作，丢失操作等

我们想保证什么？
&emsp;如果一个成员对于某条日志执行了某个操作<br>
&emsp;&emsp;则不会有其他成员对这条日志执行不同的操作<br>
(Figure 3's State Machine Safety)<br>
为什么需要？<br>
&emsp;如果成员之间的操作不一致，一旦发生leader的变更，那么可能使得客户端看到的状态发生改变<br>
&emsp;这违反了我们想让集群表现得如同单台服务器的目标。<br>
例如<br>
S1: put(k1,v1) | put(k1,v2) | ...<br>
&emsp;  S2: put(k1,v1) | put(k2,x)  | ...<br>
&emsp;不能允许他们执行第二条日志<br>

在发生了宕机之后，成员的日志会变得怎样不一致？<br>
&emsp;一个leader在发送所有日志给其他成员之前就宕机了<br>
&emsp;&emsp;S1:3<br>
&emsp;&emsp;S2: 3 3<br>
&emsp;&emsp;S3: 3 3<br>
&emsp;更糟糕的一种情况，由于一连串的leader崩溃，同一编号的日志在不同机器上的内容可能不同，例如<br>
&emsp;&emsp;10 11 12 13  <- log entry #<br>
&emsp;S1:  3<br>
&emsp;S2:  3  3  4<br>
&emsp;S3:  3  3  5<br>

Raft通过让follower遵照leader的日志修改自己的日志来强制日志保持一致<br>
&emsp;例如<br>
&emsp;s3在第6个周期当选为leader<br>
&emsp;s3发送了编号为13的一条日志<br>
&emsp;&emsp;prevLogIndex=12<br>
&emsp;&emsp;prevLogTerm=5<br>
&emsp;s2返回错误<br>
&emsp;s3将nextIndex[S2]减小为12<br>
&emsp;s3发送日志12-13，prevLogIndex=11, prevLogTerm=3<br>
&emsp;s2删除自己的12号日志（并接收s3发送的日志）<br>
&emsp;s1也发生了类似的事情<br>

回滚的结果<br>
&emsp;每个在线的follower都删除尾部与leader不同的日志<br>
&emsp;每个在线的follower都接收leader在这之后的日志<br>
&emsp;现在所有follower的日志都和leader对齐了<br>

问题: 为什么可以丢弃 S2的日志index=12 term=4 ?（me:选出的新主一定拥有已经复制到大多数成员的最新的日志）<br>
一个新leader可以回滚上一个周期已经提交过的日志吗？<br>
&emsp;例如？一个已经提交的日志有可能在leader的日志重找不到吗？<br>
&emsp;这将是一个灾难，老的leader可能已经对客户端回复了yes<br>
&emsp;因此，raft保证选举出来的leader一定具有所有已经提交的日志<br>

为什么不选举具有最长的日志的server作为leader<br>
&emsp;例如<br>
&emsp;&emsp;S1: 5 6 7<br>
&emsp;&emsp;S2: 5 8<br>
&emsp;&emsp;S3: 5 8<br>
&emsp;首先，这种情况可能发生吗？如何发生<br>
&emsp;&emsp;s1在term6崩溃了，然后后重启后在term7继续作为leader然后再次崩溃，这两次崩溃前它都刚好接受了一个log但并没有复制到其他成员。<br>
&emsp;&emsp;下一个term将是8，s2或者s3中的某一个成为了leader然后将日志复制到了另一个并提交，之后都崩溃。
&emsp;全体重启<br>
&emsp;谁将会成为下一个leader<br>
&emsp;s1有最长的日志，但是8已经提交了！！<br>
&emsp;因此新leader只能是s2和s3中的一个<br>
&emsp;所以规则不能简化为“最长”<br>

论文5.4.1解释了选举限制<br>
&emsp;接受投票的处理者只对“日志至少和自己同样新”的候选者投票<br>
&emsp;&emsp;候选者的最新的log拥有更大的term或者拥有相同的term以及相同或更大的index<br>
&emsp;因此<br>
&emsp;&emsp;s2和s3不会投票给s1<br>
&emsp;&emsp;s2和s3相互之间可以投票<br>
&emsp;只有s2和s3可能成为leader，迫使s1丢弃6和7<br>
&emsp;&emsp;这样做OK，6和7没有复制到大多数->6和7没有提交->没有响应客户端->客户端之后会重发请求<br>

关键点：<br>
&emsp;"至少同样新"的规则保证了新的leader有所有潜在的可能已经提交的日中<br>
&emsp;因此新的leader不会回滚掉已经提交的日志<br>

问题(上一讲留下的)<br>
&emsp;Figure 7 中，顶部的leader宕机后哪一个能被选举为新leader<br>
&emsp;Figure 7中不同的leader被选举出来，会导致不同的日志最终被提交或者丢弃（部分c在term 6的日志和d在term 7的日志可能，但是11145566将会保证被提交）<br>

怎样快速回退<br>
&emsp;Figure 2设计了一种每次回退一条日志的RPC------慢<br>
&emsp;lab的测试用例或许需要更块的回退才能通过测试<br>
&emsp;论文的Section 5.3的末尾提出了一种方法<br>
&emsp;&emsp;大致：这是我的想法，可能有更好的方法<br>

  &emsp;&emsp;S1: 4 5 5      4 4 4      4<br>
  &emsp;&emsp;S2: 4 6 6  or  4 6 6  or  4 6 6<br>
  &emsp;&emsp;S3: 4 6 6      4 6 6      4 6 6<br>
&emsp;&emsp;S3是term6的leader，S1重启<br>
&emsp;&emsp;如果follower拒绝了leader的心跳，其回复为：<br>
&emsp;&emsp;&emsp;follower与leader心跳冲突的日志的term<br>
&emsp;&emsp;&emsp;follower在这个周期的中的第一个日志<br>
&emsp;&emsp;如果leader有与follower对应的日志<br>
&emsp;&emsp;&emsp;将leader的nextIndex[i]回退到冲突的term的最后一个日志<br>
&emsp;&emsp;否则<br>
&emsp;&emsp;&emsp;将leader的nextIndex[i]回退到冲突的term的一个日志<br>

#### *** topic: persistence (Lab 2C)
在发生宕机之后我们期望什么？<br>
&emsp;Raft能在失去一台机器的情况下继续运行<br>
&emsp;&emsp;但我们必须尽快修复它以免机器数量少于一半<br>
&emsp;两种策略<br>
&emsp;* 用一个新的空的成员来替换<br>
&emsp;&emsp;需要传输所有日志(或者快照)到新的成员(速度慢)<br>
&emsp;&emsp;我们必须支持这种情况，因为成员可能永久性宕机<br>
&emsp;* 重启宕机的成员，重新加入，让状态追上<br>
&emsp;&emsp;需要状态能够不受宕机影响，得到持久化<br>
&emsp;&emsp;我们必须支持这种情况，因为可能发生停电之类的故障<br>
&emsp;让我们讨论下第二种策略，持久化<br>

如果一个成员，崩溃并且重启了，有哪些东西是它必须记住的？<br>
&emsp;Figure 2 列出了“持久状态”<br>
&emsp;&emsp;log[], currentTerm, votedFor<br>
&emsp;一个成员只有在完好无损的重启后才能加入<br>
&emsp;也就是说必须在非易失性的存储器中存储<br>
&emsp;&emsp;非易失性 = 磁盘，SSD，battery-backed RAM（带电源你的RAM？），&c（这个不知道是啥。。。）<br>
&emsp;&emsp;每次改变后就保存<br>
&emsp;&emsp;每次进行RPC调用和收到RPC调用回复之前<br>
&emsp;为什么要保存log[]<br>
&emsp;&emsp;如果一个成员在一次commit中是leader所认为的大多数之一<br>
&emsp;&emsp;&emsp;即使重启也必须保存日志，以便未来的leader能够看到已经提交的日志<br>
&emsp;为什么要保存votedFor<br>
&emsp;&emsp;为了避免一个成员在同一个term内，重启前投给了一个候选者，重启后又投给了另一个候选者<br>
&emsp;为什么要保存currentTerm<br>
&emsp;&emsp;为了保证term是递增的<br>
&emsp;&emsp;为了检测出过时的leader和candidate传递过来的rpc调用<br>

有些raft的state是易失的<br>
&emsp;commitIndex, lastApplied, next/matchIndex[]<br>
&emsp;raft的算法从初始化的值来重建他们<br>

持久化通常都是性能瓶颈<br>
&emsp;硬盘的一次写通常耗费10ms，SSD耗费0.1ms<br>
&emsp;因此持久化限制我们每秒只能做100~10000次操作<br>

一个服务（例如KV存储）在崩溃并重启后怎样恢复它的状态？<br>
&emsp;简单的方法:空白状态启动它，然后重放所有已经持久化的日志<br>
&emsp;&emsp;上次应用的已经丢失，从零开始启动，不需要任何额外代码<br>
&emsp;但是对一个运行了很久的系统来说，重放非常慢<br>
&emsp;更快的方法，使用raft的快照，只回放尾部的log<br>

#### *** topic: log compaction and Snapshots (Lab 3B)
问题：<br>
&emsp;日志将会逐渐变大----比状态机大得多<br>
&emsp;在重启或者发送给一个新的服务器的时候，回放会变得很慢<br>
幸运的是：<br>
&emsp;一个服务器并不总是既需要完整的日志又需要服务状态<br>
&emsp;&emsp;执行过的日志可以认为已经保存在状态中了<br>
&emsp;&emsp;客户端只看到了状态而不是日志<br>
&emsp;服务状态通常比日志要小得多，我们值保存这个就行<br>

服务器丢弃日志有什么什么限制呢？<br>
&emsp;不能丢弃尚未提交的日志-----可能是leader确定的大多数之一<br>
&emsp;不能丢弃尚未执行的日志-----还没反应到状态中<br>
&emsp;已经执行的日志可能需要用于让其他服务器更新<br>

解决办法：服务周期性的创建持久化的“快照”<br>
&emsp;  一整个状态机的副本可以作为一个特殊的已经执行的日志<br>
&emsp;&emsp;例如 K/V表<br>
&emsp;服务将快照写到持久化存储中<br>
&emsp;服务告诉raft到某些编号为止的日志已经被保存到快照中<br>
&emsp;raft抛弃在这编号之前的日志<br>
&emsp;服务可以在任何时间创建快照并且抛弃不需要的日志<br>
&emsp;&emsp;例如在日志增长到太大的情况下<br>

快照与日志的关系<br>
&emsp;快照仅仅反应了执行过的日志<br>
&emsp;&emsp;当然肯定是提交过的日志<br>
&emsp;一个服务器丢弃的日志一定是已经提交了的<br>
&emsp;&emsp;任何不知道是否提交的日志都将得到保存<br>

因此，一个服务器在磁盘上的状态包括：<br>
&emsp;服务到某一个特定日志为止的快照<br>
&emsp;raft持久化保存下来的在快照之后的日志<br>
&emsp;两者结合起来等价于完整的日志<br>

崩溃重启的时候发生了什么<br>
&emsp;服务从磁盘读快照<br>
&emsp;raft从磁盘读持久化的日志<br>
&emsp;&emsp;发送(原文是sends，是否应该翻译成回放的意思)已经提交但是却没有在快照中的日志<br>

如果一个follower有延迟而leader已经丢掉了它想要获取的日志该如何处理?<br>
&emsp;nextIndex[i]将会回退到leader日志开始的位置<br>
&emsp;因此leader不能通过AppendEntries RPCs来修复follower<br>
&emsp;因此需要使用InstallSnapshot RPC发送快照<br>
&emsp;（问题：为什么leader不仅仅丢弃已经复制到所有成员的日志？）

InstallSnapshot RPC中有哪些东西？Figures 12, 13<br>
&emsp;term<br>
&emsp;lastIncludedIndex<br>
&emsp;lastIncludedTerm<br>
&emsp;snapshot data<br>

收到InstallSnapshot的follower如何处理?<br>
&emsp;如果term太旧(不是当前的leader)就拒绝<br>
&emsp;如果follower已经包含了快照中的index/term,则拒绝（或者忽略）<br>
&emsp;&emsp;这是一个过期的RPC<br>
&emsp;清空日志，用虚假的“前一条”日志来替换<br>
&emsp;将lastApplied设置为lastIncludedIndex<br>
&emsp;用快照的内容替换服务状态<br>

注意状态和历史操作是完全等价的<br>
&emsp;设计者可以决定发送哪个<br>
&emsp;例如对有一定延迟的副本发送最近的几条日志<br>
&emsp;&emsp;但对一个已经丢失了磁盘上的数据的副本发送快照<br>
&emsp;修复副本是一个代价很大的操作，需要注意<br>

问题：<br>
&emsp;收到InstallSnapshot的follower是否可能造成回退？即是说figure 13中的第八步是否可能造成follower的快照反应的数据比它已经执行的日志小？<br>

#### *** topic: configuration change (not needed for the labs)
改变配置<br>
&emsp;配置 = 一整个集群<br>
&emsp;有时候你需要<br>
&emsp;&emsp;移动成员或者增删成员<br>
&emsp;人工发起改变配置的任务，Raft管理它<br>
&emsp;我们希望即使在配置变更的过程中发生了宕机，Raft依然保持正确<br>
&emsp;&emsp;例如，客户端不应该察觉到（除了性能可能会有所降低）<br>
&emsp;难题：成员们将会在不同的时间点得知新的配置<br>
&emsp;例：将s3更换为s4<br>
&emsp;&emsp;我们只告诉S1和S4关于新的配置S1,S2,S4<br>
&emsp;&emsp; S1: 1,2,3  1,2,4<br>
&emsp;&emsp; S2: 1,2,3  1,2,3<br>
&emsp;&emsp; S3: 1,2,3  1,2,3<br>
&emsp;&emsp; S4:        1,2,4<br>
&emsp;现在我们可能选出两个leader<br>
&emsp;&emsp;S2和S3选举出S2<br>
&emsp;&emsp;S1和S4选举出S1<br>

Raft的配置变更<br>
&emsp;思路：定义一个“joint consensus”阶段，即包括新的定义又包括老的定义，以避免新老两部分同时选出leader的情况。
&emsp;系统最开始以老配置启动<br>
&emsp;系统管着者发指令让系统leader切换到新配置<br>
&emsp;Raft有一条特殊的配置定义日志<br>
&emsp;每个成员都使用自己日志中最新的定义<br>
&emsp;1、leader把新和老配置的日志Cold,new通过日志复制发给大多数成员<br>
&emsp;2、Cold,new提交后，leader再将包含新配置的Cnew复制到大多数成员，使得新配置生效<br>

在这个过程中如果leader在各个时间点宕机了怎么办？<br>
&emsp;&emsp;我们可能在下一个term选举出两个leader吗？<br>
&emsp;如果这种情况发生，那么每个leader一定是下面情况中的一个：<br>
&emsp;&emsp;A、在Cold中，但是日志中没有Cold,new<br>
&emsp;&emsp;B、在Cold或者Cnew中，日志有Cold,Cnew<br>
&emsp;&emsp;C、在Cnew中，日志有Cnew<br>
&emsp;我们知道在正常的选举规则下不可能出现A+A和C+C的情况<br>
&emsp;A+B？不可能，因为B既需要Cold中的大多数又需要Cnew中的大多数<br>
&emsp;A+C？不可能，因为在Cold,new提交前不可能前进到Cnew的状态<br>
&emsp;B+B？不可能，因为B既需要Cold中的大多数又需要Cnew中的大多数<br>
&emsp;B+C？不可能，因为B既需要Cold中的大多数又需要Cnew中的大多数<br>

好~！Raft现在可以切换到一组新的服务器定义而不用担心出现两个leader了<br>

#### *** topic: performance
注意：许多场景可能并不需要高性能<br>
&emsp;K/V存储可能需要<br>
&emsp;但GFS或者MapRedue的master不需要<br> 

许多复制系统都有类似的性能场景<br>
&emsp;每一次达成一致的RPC调用和磁盘写<br>
&emsp;因此Raft是消息复杂度的典型代表<br>

Raft做了一些牺牲性能降低进行简化的选择<br>
&emsp;follower拒绝超过本地最大序号的AppendEntries RPC.<br>
&emsp;&emsp;而不是在空洞填起来之前保存起来以供之后使用<br>
&emsp;&emsp;通过网络重新排序的开销可能比较重<br>
&emsp;没有为AppendEntries提供打包或者流水线的方式来发送<br>
&emsp;量很大的状态的快照很浪费<br>
&emsp;leader很慢的话对Raft影响很大，例如geo-replication（不知道是啥）中<br>

以下几个方面对性能有比较大的影响<br>
&emsp;为了持久化而进行的磁盘写<br>
&emsp;消息/网络包/RPC的负荷<br>
&emsp;必须得顺序执行日志的指令<br>
&emsp;只读操作的快速路径<br>

在性能方面花了更多注意力的论文<br>
&emsp;Zookeeper/ZAB; Paxos Made Live; Harp<br>