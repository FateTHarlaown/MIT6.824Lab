### 2018 Lecture 7: Raft (3) -- Snapshots, Linearizability, Duplicate Detection

#### *** Raft log compaction and snapshots (Lab 3B)
问题：<br>
&emsp;日志将会变大--比状态机本身的状态大得多<br>
&emsp;在重启回放和发送给一个新的机器的时候将会耗费许多时间<br>

幸运的是：<br>
&emsp;一个服务器并不同时需要完整的日志和服务状态<br>
&emsp;&emsp;被执行过的日志已经被包含在状态中了<br>
&emsp;&emsp;客户端只看状态而非日志<br>
&emsp;服务状态通常要小得多，所以可以只保存状态<br>

哪些日志不能被丢弃？<br>
&emsp;尚未执行的日志 -- 还未反应在状态机中<br>
&emsp;尚未提交的日志 -- 可能是leader已认为的大多数之一<br>

解决方案：服务周期性的创建持久化“快照”<br>
&emsp;一个服务的快照可以代表已经执行的日志<br>
&emsp;&emsp;例如K/V表<br>
&emsp;服务将快照写到持久化的存储器上<br>
&emsp;服务告诉raft到某些序号的日志已经存储到快照中<br>
&emsp;raft丢弃掉这之前的日志<br>
&emsp;一个服务可以在任何时间创建快照并丢弃之前的日志<br>
&emsp;&emsp;例如 当日志增长到过长的时候<br>

在崩溃和重启的时候发生了什么<br>
&emsp;服务从磁盘读取快照<br>
&emsp;raft读取持久化的日志<br>
&emsp;raft日志可能比快照的位置更靠前（但不会更后）<br>
&emsp;&emsp;Raft将会（重新）应用已经提交的日志。<br>
&emsp;&emsp;&emsp;因为已经应用的日志编号applyIndex在重启后从0开始<br>
&emsp;&emsp;服务将会看到重复的操作，必须检测到这些index，并且忽略它<br>

如果follower的日志结束得比leader的日志开始位置更早?<br>
&emsp;nextIndex[i]将会回退到比leader日志开始的位置更早<br>
&emsp;因此leader不能通过AppendEntries RPCs来修复从库<br>
&emsp;因此需要InstallSnapshot RPC<br>

什么是InstallSnapshot RPC？Figures 12，13<br>
&emsp;term<br>
&emsp;lastIncludedIndex<br>
&emsp;lastIncludedTerm<br>
&emsp;snapshot data<br>

当follower收到InstallSnapshot时做什么？<br>
&emsp;如果term太老就忽略(不是当前的leader)<br>
&emsp;如果follower已经有lastIncludedIndex则忽略<br>
&emsp;&emsp;这是一个太老的/有延迟的RPC<br>
&emsp;如果不忽略<br>
&emsp;清空日志，替换为“伪前一条日志”<br>
&emsp;将lastApplied设置为lastIncludedIndex<br>
&emsp;将服务状态（例如 k/v表）替换为快照的正文<br>

哲学意义上：<br>
&emsp;状态通常等价于操作的历史<br>
&emsp;你总能够选择二者中的一个进行存储和交互<br>
&emsp;再接下来的课程中可以看到这种二元性的例子<br>

实践：<br>
&emsp;raft层和服务层协作来保存快照<br>
&emsp;raft的快照方式是合理的，因为状态比较小<br>
&emsp;对于一个大的数据库，例如在复制GB大小的数据时，缓慢的创建并写入磁盘并不好<br>
&emsp;或许服务数据应该以B树的形式保存在磁盘中<br>
&emsp;&emsp;并不需要显式的持久化，因为磁盘已经做了<br>
&emsp;处理落后的副本很困难，尽管:<br>
&emsp;(不太理解这两句想表达啥意思)<br>
&emsp;&emsp;leader should save the log for a while<br>
&emsp;&emsp;or save identifiers of updated records<br>
&emsp;新的服务器需要完整的状态<br>

#### *** linearizability
对于lab3的“正确”，我们需要有一个定义<br>
&emsp;客户端会希望Put和Get如何运作？<br>
&emsp;通常被称为一致性协议<br>
&emsp;帮助我们推算出如何正确地处理复杂场景<br>
&emsp;&emsp;例如，并发，副本，宕机，RPC重发<br>
&emsp;&emsp;&emsp;leader改变，优化<br>
&emsp;我们将会在6.824中看到许多的一致性定义<br>
&emsp;&emsp;例如Spinnaker的timeline consistency（翻译成时间线一致？）<br>

对一个单机服务的行为来讲，“线性一致”(或者可线性化)是最常见和最直观的定义。<br>

线性一致的定义:<br>
&emsp;满足下面条件的一系列执行记录是线性一致的<br>:
&emsp;&emsp;可以找到一个全局的操作序列，与物理时间发生的操作先后相同(对于不重叠的操作)，在这之中，每一个读都可以看见在它之前最近一次写入的顺序<br>

操作序列是客户端的一系列操作的记录，每一个包含：参数，返回值，开始时间，完成时间。

例子1：<br>
&emsp;|-Wx1-|&emsp;|-Wx2-|<br>
&emsp;&emsp;|---Rx2---|<br>
&emsp;&emsp;&emsp;|-Rx1-|<br>
&emsp;Wx1代表向x中写入1<br>
&emsp;Rx1表示从x中读出了1<br>
&emsp;顺序：Wx1, Rx1, Wx2, Rx2<br>
&emsp;这个顺序遵循值约束<W->R><br>
&emsp;这个顺序遵循真实时间约束<Wx1->Wx2><br>
&emsp;所以操作序列是可线性化的<br>

列子2：<br>
&emsp;|-Wx1-|&emsp;|-Wx2-|<br>
&emsp;&emsp;&emsp;|--Rx2--|<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;|-Rx1-|<br>
&emsp;Wx2之后才能是Rx2（值约束），Rx2之后才能是Rx1（时间约束），Rx1又必须在Wx2之前。。。这是一个循环-----所以这不能被证明为一个线性顺序，这个例子不是线性一致的<br>

例子3：<br>
|--Wx0--|&emsp;|--Wx1--|<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;|--Wx2--|<br>
&emsp;&emsp;&emsp;&emsp;|-Rx2-|&emsp;|-Rx1-|<br>
顺序：Wx0 Wx2 Rx2 Wx1 Rx1<br>
因此，是可线性化的<br>
注意，服务可以选择并发写的顺序<br>
&emsp;例如，Raft可以在日志里（按一定顺序）放置并发操作<br>

例子4：<br>
|--Wx0--|&emsp;|--Wx1--|<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;|--Wx2--|<br>
C1:&emsp;&emsp;&emsp;|-Rx2-| |-Rx1-|<br>
C2:&emsp;&emsp;&emsp;|-Rx1-| |-Rx2-|<br>
我们必须把所有操作放到一个单独的序列中<br>
&emsp;或许:Wx2 C1:Rx2 Wx1 C1:Rx1 C2:Rx1<br>
&emsp;但C2:Rx2该放在哪里?<br>
&emsp;&emsp;从时间上看应该放在C2:Rx1之后<br>
&emsp;&emsp;但那之后应该读到1<br>
&emsp;没有任何序列满足条件<br>
&emsp;&emsp;C1的读取操作要求Wx2在Wx1之前<br>
&emsp;&emsp;C2的读取操作要求Wx1在Wx2之前<br>
&emsp;&emsp;这是一个循环，因此没有序列满足<br>
&emp;不是可线性化的<br>
因此，所有的客户端必须看见同样的并发写顺序<br>

例子5：<br>
忽略了最近的写入不是可线性化的<br>
Wx1&emsp;&emsp;&emsp;Rx1<br>
&emsp;&emsp;&emsp;Wx2<br>
这可以排除脑裂以及丢失已经提交的写。<br>

这篇文章可能会有用<br>
https://www.anishathalye.com/2017/06/04/testing-distributed-systems-for-linearizability/

#### *** duplicate RPC detection (Lab 3)
当一个PUT或者GET的调用超时客户端应该怎么办?<br>
&emsp;例如Call操作返回错误<br>
&emsp;如果服务端宕机或者请求丢失：重发<br>
&emsp;如果服务端已经执行了，但是答复信息丢失了:重发很危险<br>

问题在于：<br>
&emsp;这两种情况对于客户端看起来是一样的<br>
&emsp;如果已经执行了，客户端也需要执行的结果<br>

想法：重复RPC的检测<br>
&emsp;让K/V服务能够检测重复的客户端请求<br>
&emsp;客户端每次请求都选取一个ID，在RPC中包含这个ID<br>
&emsp;&emsp;重复发送的同一个RPC有同样的ID<br>
&emsp;K/V服务维护一个以ID索引的表<br>
&emsp;每个RPC一条记录<br>
&emsp;&emsp;每次执行之后记录值<br>
&emsp;如果第二次RPC带着同样的ID到达，那么它重复<br>
&emsp;直接使用表中的回复<br>

设计上使人困惑的点<br>
&emsp;什么时候可以删除表中的记录<br>
&emsp;如果新的leader接管，它如何获得重复检测的表<br>
&emsp;如果服务端宕机，如何恢复这个表<br>

使重复检测的表大小保持较小的尺寸的方法<br>
&emsp;每个客户端一条记录而不是每个RPC调用<br>
&emsp;每个客户端在每个时刻只有一个RPC<br>
&emsp;每次RPC调用使客户端的序号顺序递增<br>
&emsp;当服务端收到一个客户端的RPC #10<br>
&emsp;&emsp;它可以丢弃这个客户端的更小的记录<br>
&emsp;&emsp;这意味着客户端不会重发老的RPC<br>

一些细节:<br>
&emsp;每个客户端有唯一的ID ---- 或许是一个64位的随机数<br>
&emsp;客户端在每次RPC调用中发送ID和一个序号seq<br>
&emsp;&emsp;如果重发的话seq也重复<br>
&emsp;服务端的重复检测表用clinet的ID作为索引<br>
&emsp;&emsp;仅仅存储一个seq,表示执行过的RPC<br>
&emsp;RPC处理函数首先检查重复检测表，只在seq大于表中的记录的时候调用Start()<br>
&emsp;每条日志必须包含客户端的ID和seq<br>
&emsp;当从applyChan中取得操作命令的时候<br>
&emsp;&emsp;更新在重复检测表中的seq的值<br>
&emsp;&emsp;唤醒在等待的RPC处理函数(如果有的话)<br>

如果一个重复的请求比原始的请求更早到达会怎样？<br>
&emsp;能直接调用start()（再次）吗?<br>
&emsp;它将会在日志中出现两次(同样的client Id,同样的seq)<br>
&emsp;当命令在applyCh冲出现的时候，如果发现已经执行过则不执行它<br>

一个新的leader怎么获取重复检测表?<br>
&emsp;一个副本在执行的时候需要更新它的重复检测表<br>
&emsp;所以在他们成为leader的时候信息早就有了<br>

如果一个服务端宕机了它如何恢复它的表？<br>
&emsp;如果没有快照，日志回放将会重置这个表<br>
&emsp;如果有快照的话快照中需要保存一份重复检测表的副本<br>

但是等等：<br>
&emsp;K/V服务现在直接从重复检测表中返回老的值<br>
&emsp;如果回复的值已经过期呢？<br>
&emsp;这是否有问题？<br>
例子:<br>
&emsp;C1&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;C2<br>
&emsp;--&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;--<br>
&emsp;put(x,10)<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;首先发送一个get(x)，返回10然后删除<br>
&emsp;put(x,20)<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;再次发送get(x),从表中获取了10而不是20<br>

线性一致是如何定义的？<br>
C1: |-Wx10-|&emsp;&emsp;&emsp;&emsp;&emsp;|-Wx20-|<br>
C2: &emsp;&emsp;&emsp;&emsp;&emsp;|-Rx10-------------|<br>
顺序: Wx10 Rx10 Wx20<br>
因此：返回10是正确的(应该是重发使得Rx10这个动作本身的时间跨度变长)<br>

#### *** read-only operations (end of Section 8)
问题:Raft需要提交只读操作的日志吗？例如Get(key)？<br>

即是:leader是否要使用当前K/V表中的值立即响应Get()操作？<br>

A: 不是，先不考虑Figure 2或者labs中的框架<br>
&emsp;假设S1认为他是leader并且收到了一个Get(k)请求.<br>
&emsp;由于网络原因，它可能已经不在是leader但是自己却没有意识到<br>
&emsp;新的leader S2可能已经处理了一个这个Key的Put()操作<br>
&emsp;因此S1中的值是过期的<br>
&emsp;提供一个过期的值不是可线性化的；这属于脑裂现象<br>

因此:Figure 2 要求 Get()s 操作也要提交到日志中。<br>
&emsp;如果leader能够提交Get()的日志，那么（至少在这个时间点）它依然是leader。在上面S1的例子中它不能够获得大多数成员对AppendEntries 的确认回复以提交这个Get()日志，因此它将不会回复客户端<br>

但是:许多的应用都是读负载比较重，提交Get()的操作比较耗时。是否有办法对于避免提交只读操作？这在实际的系统中是一个重要的考虑<br>

思路:<br>
&emsp;像下面这样修改raft协议<br>
&emsp;定义一个租约周期,例如5秒<br>
&emsp;每次leader从一个大多数获取AppendEntries的回复之后<br>
&emsp;&emsp;在一个租约期内可以不提交只读的日志就回复<br>
&emsp;一个新的leader不能执行Put()操作直到上一次租约过期<br>
&emsp;因此follower们记住上一次他们应答AppendEntries的主机，并且告诉新的leader（通过RequestVote 的回复）.<br>

注意:对于labs,应当将get()提交到日志，不要实现租约。<br>

Spinnaker可以选择牺牲先行一致性使得读操作更快，它的"timeline reads"允许返回一个国企的值<br>
&emsp;Spinnaker用这个选择的自由去加速读取:<br>
&emsp;&emsp;任何副本都能够对于读操作进行回复， 使得读负载可以并行执行<br>
&emsp;但是timeline reads并不是可线性化的：<br>
&emsp;&emsp;副本可能没有获取到最近的写入<br>
&emsp;&emsp;副本可能与leader无法连通<br>

实践中，人们通常（但并不总是）希望用允许过期值来换取更高的性能<br>