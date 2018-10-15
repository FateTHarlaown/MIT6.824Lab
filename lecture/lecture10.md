### 6.824 2018 Lecture 10: Distributed Transactions 

主题：<br>
&emsp;分布式事务 = 并发控制 + 原子提交<br>

问题在哪里？<br>
&emsp;许多的数据记录，在多个服务端和客户端之间共享<br>
&emsp;客户端应用通常关系到许多的读和写<br>
&emsp;&emsp;银行转账借款和还款<br>
&emsp;&emsp;投票:核对是否已经投票，记录投票，增加计数<br>
&emsp;&emsp;在社会关系图中添加双向连接<br>
&emsp;我们想要隐藏来自应用的写入者的交错和失败(hide interleaving and failure from application writers这个不知到如何翻译)<br>
&emsp;这是一个经典的数据库关注的问题<br>
&emsp;&emsp;今天的材料来源于分布式数据库<br>
&emsp;&emsp;但是它的思想在许多分布式系统中都有应用<br>

示例场景<br>
&emsp;x和y是银行账户 -- 数据库表中的记录<br>
&emsp;x和y在不同的服务器上(可能在不同的银行)<br>
&emsp;x和y一开始都有10美元<br>
&emsp;客户端C1正在执行从x转账给y 1美元的操作<br>
&emsp;客户端C2正在做审计,检查钱没有丢失<br>
&emsp;C1:&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;C2:<br>
&emsp;add(x, 1)&emsp;&emsp;&emsp;tmp1 = get(x)<br>
&emsp;add(y, -1)&emsp;&emsp;&emsp;tmp2 = get(y)<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;print tmp1, tmp2<br>

我们希望怎样?<br>
&emsp;x=11;<br>
&emsp;y=9;<br>
&emsp;C2 打印出10,10或者11,9<br>

哪里可能发生错误?<br>
&emsp;C1和C2的操作产生不当的交错<br>
&emsp;&emsp;例如C2在C1的两个操作之间完整的进行，打印出11,10<br>
&emsp;服务器宕机或者网络中断<br>
&emsp;账户x或者y不存在<br>

经典的打算：事务<br>
&emsp;客户端告诉事务系统一个事务的开始和结束<br>
&emsp;系统进行安排，使得每个事务：<br>
&emsp;&emsp;原子性:所有的写入要么全部发生要么全部不发生，即使发生了失败<br>
&emsp;&emsp;可串行化：结果要就像事务一个接一个执行一样<br>
&emsp;&emsp;持久化：已经提交的写入操作即使在崩溃和重启后也不会丢失<br>
&emsp;这就是所谓的ACID特性<br>
&emsp;应用程序依赖这些特性<br>
&emsp;我们感兴趣的是*分布式*事务<br>
&emsp;&emsp;数据分布在多个服务器<br>

应用的代码可能看起来像这样：<br>
&emsp;T1：<br>
&emsp;&emsp;begin_transaction()<br>
&emsp;&emsp;add(x, 1)<br>
&emsp;&emsp;add(y, -1)<br>
&emsp;&emsp;end_transaction()<br>
&emsp;T2:<br>
&emsp;&emsp;begin_transaction()<br>
&emsp;&emsp;tmp1 = get(x)<br>
&emsp;&emsp;tmp2 = get(y)<br>
&emsp;&emsp;print tmp1, tmp2<br>
&emsp;&emsp;end_transaction()<br>

如果发生了某些错误，一个事务可能会“终止”<br>
&emsp;一次“终止”将会撤销任何对记录所做的修改<br>
&emsp;事务可能主动终止，例如，账户不存在<br>
&emsp;系统可能强制终止, 例如, 解除死锁<br>
&emsp;某些服务端的错误会导致终止<br>
&emsp;应用可能会（也可能不会）尝试重试事务<br>

分布式事务有两个大的方面：<br>
&emsp;并发控制<br>
&emsp;原子提交<br>

首先，并发控制<br>
&emsp;正确的执行并行事务<br>

传统事务的正确性的定义是“可串行化”<br>
&emsp;你并行执行某些事务，得到结果<br>
&emsp;&emsp;“结果”新的记录的值和输出<br>
&emsp;结果要是可串行化，需要满足下列条件:<br>
&emsp;&emsp;存在一个这些事务串行执行的结果与实际执行的结果相同<br>
&emsp;&emsp;(串行化意味着同一时间只有一个 -- 不会并行执行)<br>
&emsp;&emsp;(定义会让你想起可线性化)<br>

你可以通过这些方式检测一个执行的结果是否是可串行化的<br>
&emsp;找到一个执行顺序生成同样的结果<br>
&emsp;例如，可能的串行化执行顺序为<br>
&emsp;&emsp;T1;T2<br>
&emsp;&emsp;T2;T1<br>
&emsp;因此正确（可串行化）的结果是:<br>
&emsp;T1; T2 : x=11 y=9 "11,9"<br>
&emsp;T2; T1 : x=11 y=9 "10,10"<br>
&emsp;这两个结果不一样；但任意一个都是对的<br>
&emsp;没有其他结果是正确的<br>
&emsp;具体实现可能并行执行了T1和T2<br>;
&emsp;&emsp;但是依然生成了相当于串行执行的结果<br>

如果T1的操作完全在T2的两个get操作中间完成呢？<br>
&emsp;结果将会可串行化吗？<br>
&emsp;T2将会打印10，9<br>
&emsp;但是10，9并不是两个可串行化结果之一<br>
如果T2完全在T1的两个add操作中间完成呢?<br>
&emsp;T2将会打印出11，10<br>
&emsp;但是11，10不是两个可串行化结果之一<br>

可串行化对于编程者而言非常好<br>
&emsp;它使得程序员可以忽略并发<br>

两种对于事务的并发控制<br>
&emsp;悲观型:
&emsp;&emsp;使用之前对记录加锁<br>
&emsp;&emsp;冲突将造成延迟(等待锁)<br>
&emsp;乐观型<br>
&emsp;&emsp;不加锁使用记录<br>
&emsp;&emsp;提交的时候检查读/写是否可串行化<br>
&emsp;&emsp;冲突造成终止+重试，但是在没有冲突的情况下比加锁更快<br>
&emsp;&emsp;被称之为乐观并发控制(OCC)

今天：悲观并发控制<br>
下周：乐观并发控制<br>

“两阶段锁2PL”是一中实现可串行化的方式<br>
&emsp;2PL的定义:<br>
&emsp;&emsp;一个事务在使用记录钱必须获取它的锁<br>
&emsp;&emsp;一个事务必须一直持有它的锁直到事务提交或者终止<br>

2PL的一个例子<br>
&emsp;假设T1和T2在同一时刻开始<br>
&emsp;事务在需要的时候原子的获取锁<br>
&emsp;所以T1和T2中第一个使用X的将会获取锁<br>
&emsp;其他的等待<br>
&emsp;这阻止了非串行化的交错<br>

细节:<br>
&emsp;每个数据库中的记录都有锁<br>
&emsp;如果是分布式场景，锁存储在它所在的服务器<br>
&emsp;(但是2PL并不会被分布式影响太多)<br>
&emsp;一个执行的事务在第一次使用某记录的时候就需要获取锁<br>
&emsp;&emsp;add()和get()隐式的获取记录锁<br>
&emsp;&emsp;停止事务释放所有的锁<br>
&emsp;所有的锁都是排它锁(在这里的讨论中没有读写锁)<br>
&emsp;完整的名称是"严格的两阶段锁"<br>
&emsp;与线程锁相似（例如Go的Mutex），但是更简单<br>
&emsp;&emsp;显示的使用begin/end_transacion<br>
&emsp;&emsp;DB会知道进行加锁<br>
&emsp;&emsp;可能会终止而回滚(例如为了解除死锁)<br>

为什么需要一直持有锁直到提交或者回滚？<br>
&emsp;为什么不在对记录的操作完成后立即释放<br>
&emsp;一个有问题的结果的例子<br>
&emsp;&emsp;假如T2在get(x)释放了x的锁<br>
&emsp;&emsp;T1可能在T2的get()操作中间完成<br>
&emsp;&emsp;T2可能打印10，9<br>
&emsp;&emsp;这不是一个可串行化的执行：既不是T1;T2也不是T2;T1<br>
&emsp;另一个有问题的结果的例子<br>
&emsp;&emsp;假设T2写入了x，然后释放了x的锁<br>
&emsp;&emsp;T2读了x然后打印出来<br>
&emsp;&emsp;T1然后回滚了<br>
&emsp;&emsp;T2使用了一个从未存在的值<br>
&emsp;&emsp;我们必须回滚T2，这造成了连锁回滚<br>

2PL可能会禁止正确（可串行化的）执行吗？<br>
&emsp;是的，例如：<br>
&emsp;&emsp;T1&emsp;&emsp;&emsp;&emsp;&emsp;T2<br>
&emsp;&emsp;get(x)&emsp;&emsp;&emsp;&emsp;<br>
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;get(x)<br>
&emsp;&emsp;put(x,1)<br>
&emsp;锁将会禁止这种交错<br>
&emsp;但是结果(x=1)是可串行化(如同T2;T1)

加锁可以造成死锁，例如<br>
&emsp;T1&emsp;&emsp;&emsp;t2<BR>
&emsp;get(x)&emsp;get(y)<br>
&emsp;get(y)&emsp;get(x)<br>
系统必须要检测死锁（环？超时？）并且回滚其中的一个事务<br>

问题：描述一个场景，两阶段加锁的性能比简单的加锁方式性能更高. 简单加锁：在使用任何数据之前都先对每一条数据加锁；在回滚或者提交后释放.<br>

下一个主题：分布式事务如何对抗失败<br>
&emsp;假设，x和y在不同的工作服务器<br>
&emsp;假设x的服务器上执行add 1 操作，但是y在执行减法操作之前失败了<br>
&emsp;或者x的服务端执行了add 1操作，但是y却发现账户不存在<br>
&emsp;x和y都执行了他们自己的部分，但是并不确定其他人是否执行<br>

我们希望“原子提交”<br>
&emsp;许多服务器对同一个任务进行写作<br>
&emsp;每个计算机有不同的角色<br>
&emsp;要保证原子性：要么全做要么不做<br>
&emsp;挑战：容错，性能<br>

我们将要开发一个协议叫做“两阶段提交(2PC)”<br>
&emsp;分布式数据库使用它来完成跨服务器的事务<br>
&emsp;我们将假设数据库也是加锁的<br>

没有“失败”的情况下的两阶段提交:<br>
&emsp;事务由事务协调器(TC)驱动<br>
&emsp;TC通过RPC发送put(), get()给A, B<br>
&emsp;&emsp;所有修改被暂存，只有当提交后才会生效<br>
&emsp;TC观察到transaction_end()（这里是不是打错了？）<br>
&emsp;TC发送PREPARE 消息给A和B<br>
&emsp;如果A（或者B）将会提交，
&emsp;&emsp;回复Yes<br>
&emsp;&emsp;之后A/B进入“prepareed”状态<br>
&emsp;否则回复NO.<br>
&emsp;如果两者都回复YES,TC发送COMMIT消息<br>
&emsp;如果任意一个回复NO，TC发送ABORT消息<br>
&emsp;A/B在得到一个COMMIT消息后进行commit操作.<br>
&emsp;&emsp;例如，写暂存的记录到真实数据库<br>
&emsp;&emsp;释放他们在记录上的事务锁<br>

为什么它到目前为止是正确的<br>
&emsp;&emsp;除非他们都同意，否则A和B都不能提交<br>

如果B崩溃并且重启会怎样？<br>
&emsp;如果B在崩溃前发送了YES，B必须记住！<br>
&emsp;因为A可能收到了COMMIT并且提交了<br>
&emsp;同时，B必须继续持有事务锁<br>
&emsp;如果TC发送了COMMIT, B将暂存的数据写入真实的数据<br>

如果TC崩溃并且重启了会怎样?<br>
&emsp;如果TC可能已经发送了COMMIT,他必须记住<br>
&emsp;&emsp;因为worker可能已经提交了<br>
&emsp;如果任何人询问的话再次回复（COMMIT）<br>
&emsp;即是说，TC必须在发送COMMIT之前把COMMIT写入到磁盘<br>

如果TC从一直没有从B收到任何回复会怎样？<br>
&emsp;或许B奔溃了并且没有回复；或许网络断了<br>
&emsp;TC可能会超时，并且回滚（因为还没有发COMMIT）<br>
&emsp;好的：允许各个成员释放锁<br>

如果B在等待PREPRAE时超时或者崩溃？<br>
&emsp;B还没有回复PREPARE,因此TC不能决定提交<br>
&emsp;因此B能单独决定回滚，然后释放锁<br>
&emsp;对未来的PREPARE回复NO.<br>

如果B对于PREPARE回复了YES，但是没有收到COMMIT或者ABORT<br>
&emsp;B可以单方面的决定是否回滚吗？<br>
&emsp;&emsp;不行！TC可能已经从A和B获得了YES<br>
&emsp;&emsp;并且发送了COMMIT给A, 但是在发送给B之前崩溃了<br>
&emsp;&emsp;因此A将会提交而B回滚：不正确<br>
&emsp;B也不能单独决定提交：<br>
&emsp;&emsp;A可能投票了NO<br>

因此：如果B回复了YES，它必须“阻塞”：等待TC的决定<br>

两阶段提交的视图<br>
&emsp;分片的数据库里面使用，当食物使用了多个服务器上的数据<br>
&emsp;但是它的评价并不好<br>
&emp;&emsp;慢：传输多次消息<br>
&emsp;&emsp;慢：磁盘写<br>
&emsp;在prepare/commit的消息交互期间必须要持有锁，会阻塞其他需要获取锁的操作<br>
&emsp;&emsp;TC崩溃可能造成不确定的阻塞，并且还持有锁<br>
&emsp;因此只适用于少数领域<br>
&emsp;&emsp;例如跨银行等<br>
&emsp;更快的分布式事务是一个活跃的研究领域<br>
&emsp;&emsp;更少的消息传递和持久化消耗<br>
&emsp;&emsp;可以做更少的工作的特殊场景<br>
&emsp;&emsp; Wide-area transactions(不知道是啥)<br>
&emsp;更弱的一致性，对应用造成了更多的负担<br>

Raft和两阶段提交解决了不同的问题<br>
&emsp;使用Raft来通过复制获取高可用能力<br>
&emsp;&emsp;例如在有部分服务器宕机的情况下依然可以使用<br>
&emsp;&emsp;所有的服务器都做的同样的事情<br>
&emsp;当每个成员执行了操作不同的时候使用2PC<br>
&emsp;&emsp;并且每个成员都必须做他们自己的那部分<br>
&emsp;2PC对于高可用并没有帮助<br>
&emsp;&emsp;因为所有的服务器都必须保持存活来完成所有操作<br>
&emsp;Raft不保证所有服务器都做某些事<br>
&emsp;&emsp;因此只有需要大部分保持存活即可<br>

那么如果我们既想要高可用又想要原子提交呢<br>
&emsp;这是一个问题<br>
&emsp;每一个“服务器”应该是一个Raft复制服务<br>
&emsp;并且TC也应该由Raft复制<br>
&emsp;在复制服务之间运行两阶段提交<br>
&emsp;这样你既能够容错又能够继续进行事务<br>
&emsp;在lab4中你将建立类似的东西来传送分片<br>
&emsp;下一节在FaRM中有不同的方法<br>



 

