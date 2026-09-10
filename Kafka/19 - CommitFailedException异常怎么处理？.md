2019-7-16 胡夕 



你好，我是胡夕。今天我来跟你聊聊CommitFailedException异常的处理。 

说起这个异常，我相信用过Kafka Java Consumer客户端API的你一定不会感到陌生。所 ~~<mark>谓</mark>~~ CommitFailedException，顾 ~~名~~ <mark>思</mark> 义 ~~就~~ <mark>是</mark> Consumer ~~客户~~ 端在 <mark>提</mark> 交位移时出现了 ~~错~~ <mark>误或</mark> ~~异常~~ ， 而 ~~且~~ 还 <mark>是</mark> 那种不 ~~可~~ 恢 ~~复~~ 的严 ~~重异常~~ 。如果异常是可恢复的瞬时错误，提交位移的API自己就能 规避它们了，因为很多提交位移的API方法是支持自动错误重试的，比如我们在上一期中提到 的commitSync方法。 

每次和CommitFailedException一起出现的，还有一段非常著名的注释。为什么说它很“著 名”呢？第一，我想不出在近50万行的Kafka源代码中，还有哪个异常类能有这种待遇，可以 享有这么大段的注释，来阐述其异常的含义；第二，纵然有这么长的文字解释，却依然有很 多人对该异常想表达的含义感到困惑。 

现在，我们一起领略下这段文字的风采，看看社区对这个异常的最新解释： 

Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured 

max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records. 

这段话前半部分的意思是，本次提交位移失败了，原因是消费者组已经开启了Rebalance过 程，并且将要提交位移的分区分配给了另一个消费者实例。出现这个情况的原因是，你的消 费者实例连续两次调用poll方法的时间间隔超过了期望的max.poll.interval.ms参数值。这通常 表明，你的消费者实例花费了太长的时间进行消息处理，耽误了调用poll方法。 

在后半部分，社区给出了两个相应的解决办法（即橙色字部分）： 

1. 增加期望的时间间隔max.poll.interval.ms参数值。 

2. 减少poll方法一次性返回的消息数量，即减少max.poll.records参数值。 

在详细讨论这段文字之前，我还想提一句，实际上这段文字总共有3个版本，除了上面的这个 最新版本，还有2个版本，它们分别是： 

Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured session.timeout.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records. 

Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records. 

这两个较早的版本和最新版相差不大，我就不详细解释了，具体的差异我用橙色标注了。我 之所以列出这些版本，就是想让你在日后看到它们时能做到心中有数，知道它们说的是一个 事情。 

其实不论是哪段文字，它们都表征位移提交出现了异常。下面我们就来讨论下该异常是什么 时候被抛出的。从源代码方面来说，CommitFailedException异常通常发生在手动提交位移 时，即用户显式调用KafkaConsumer.commitSync()方法时。从使用场景来说，有两种典型的 场景可能遭遇该异常。 

我们先说说最常见的场景。当消息处理的总时间超过预设的max.poll.interval.ms参数值时， Kafka Consumer端会抛出CommitFailedException异常。这是该异常最“正宗”的登场方 式。你只需要写一个Consumer程序，使用KafkaConsumer.subscribe方法随意订阅一个主 题，之后设置Consumer端参数max.poll.interval.ms=5秒，最后在循环调用 KafkaConsumer.poll方法之间，插入Thread.sleep(6000)和手动提交位移，就可以成功复现这 个异常了。在这里，我展示一下主要的代码逻辑。 

Properties props = new Properties(); … props.put("max.poll.interval.ms", 5000); consumer.subscribe(Arrays.asList("test-topic")); 

while (true) { ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1)); // 使用Thread.sleep模拟真实的消息处理逻辑 Thread.sleep(6000L); consumer.commitSync(); } 

如果要防止这种场景下抛出异常，你需要简化你的消息处理逻辑。具体来说有4种方法。 

1. ~~缩~~ ~~<mark>短</mark> 单~~ 条 ~~消息~~ 处 ~~理~~ 的时间。比如，之前下游系统消费一条消息的时间是100毫秒，优化之后 成功地下降到50毫秒，那么此时Consumer端的TPS就提升了一倍。 

2. ~~增~~ 加Consumer端允许下游系统 ~~消费一~~ 批 ~~消息~~ 的 ~~<mark>最</mark>~~ 大时长。这取决于Consumer端参数 max.poll.interval.ms的值。在最新版的Kafka中，该参数的默认值是5分钟。如果你的消费 逻辑不能简化，那么提高该参数值是一个不错的办法。值得一提的是，Kafka 0.10.1.0之前 的版本是没有这个参数的，因此如果你依然在使用0.10.1.0之前的客户端API，那么你需要 增加session.timeout.ms参数的值。不幸的是，session.timeout.ms参数还有其他的含义， 因此增加该参数的值可能会有其他方面的“不良影响”，这也是社区在0.10.1.0版本引入 max.poll.interval.ms参数，将这部分含义从session.timeout.ms中剥离出来的原因之一。 

3. ~~减~~ 少下游系统 ~~一~~ 次性 ~~消费~~ 的 ~~消息总~~ 数。这取决于Consumer端参数max.poll.records的值。 当前该参数的默认值是500条，表明调用一次KafkaConsumer.poll方法，最多返回500条消 息。可以说，该参数规定了单次poll方法能够返回的消息总数的上限。如果前两种方法对你 都不适用的话，降低此参数值是避免CommitFailedException异常最简单的手段。 

4. 下游系统 ~~使用~~ 多线 <mark>程</mark> 来加 ~~速消费~~ 。这应该算是“最高级”同时也是最难实现的解决办法 了。具体的思路就是，让下游系统手动创建多个消费线程处理poll方法返回的一批消息。之 前你使用Kafka Consumer消费数据更多是单线程的，所以当消费速度无法匹及Kafka Consumer消息返回的速度时，它就会抛出CommitFailedException异常。如果是多线程， 你就可以灵活地控制线程数量，随时调整消费承载能力，再配以目前多核的硬件条件，该 方法可谓是防止CommitFailedException最高档的解决之道。事实上，很多主流的大数据流 处理框架使用的都是这个方法，比如Apache Flink在集成Kafka时，就是创建了多个 KafkaConsumerThread线程，自行处理多线程间的数据消费。不过，凡事有利就有弊，这 个方法实现起来并不容易，特别是在多个线程间如何处理位移提交这个问题上，更是极容 易出错。在专栏后面的内容中，我将着重和你讨论一下多线程消费的实现方案。 

综合以上这4个处理方法，我个人推荐你首先尝试采用方法1来预防此异常的发生。优化下游 系统的消费逻辑是百利而无一害的法子，不像方法2、3那样涉及到Kafka Consumer端TPS与 消费延时（Latency）的权衡。如果方法1实现起来有难度，那么你可以按照下面的法则来实 践方法2、3。 

首先，你需要弄清楚你的下游系统消费每条消息的平均延时是多少。比如你的消费逻辑是从 Kafka获取到消息后写入到下游的MongoDB中，假设访问MongoDB的平均延时不超过2秒，那 么你可以认为消息处理需要花费2秒的时间。如果按照max.poll.records等于500来计算，一批 消息的总消费时长大约是1000秒，因此你的Consumer端的max.poll.interval.ms参数值就不 能低于1000秒。如果你使用默认配置，那默认值5分钟显然是不够的，你将有很大概率遭遇 CommitFailedException异常。将max.poll.interval.ms增加到1000秒以上的做法就属于上面 的第2种方法。 

除了调整max.poll.interval.ms之外，你还可以选择调整max.poll.records值，减少每次poll方 法返回的消息数。还拿刚才的例子来说，你可以设置max.poll.records值为150，甚至更少， 这样每批消息的总消费时长不会超过300秒（150*2=300），即max.poll.interval.ms的默认值 5分钟。这种减少max.poll.records值的做法就属于上面提到的方法3。 

Okay，现在我们已经说完了关于CommitFailedException异常的经典发生场景以及应对办法。 从理论上讲，关于该异常你了解到这个程度，已经足以帮助你应对应用开发过程中由该异常 带来的“坑”了 。但其实，该异常还有一个不太为人所知的出现场景。了解这个冷门场景， 可以帮助你拓宽Kafka Consumer的知识面，也能提前预防一些古怪的问题。下面我们就来说 说这个场景。 

之前我们花了很多时间学习Kafka的消费者，不过大都集中在消费者组上，即所谓的 Consumer Group。其实，Kafka Java Consumer端还提供了一个名为Standalone Consumer 的独立消费者。它没有消费者组的概念，每个消费者实例都是独立工作的，彼此之间毫无联 系。不过，你需要注意的是，独立消费者的位移提交机制和消费者组是一样的，因此独立消 费者的位移提交也必须遵守之前说的那些规定，比如独立消费者也要指定group.id参数才能提 交位移。你可能会觉得奇怪，既然是独立消费者，为什么还要指定group.id呢？没办法，谁让 社区就是这么设计的呢？总之，消费者组和独立消费者在使用之前都要指定group.id。 

现在问题来了，如果你的应用中同时出现了设置相同group.id值的消费者组程序和独立消费者 程序，那么当独立消费者程序手动提交位移时，Kafka就会立即抛出CommitFailedException 异常，因为Kafka无法识别这个具有相同group.id的消费者实例，于是就向它返回一个错误， 表明它不是消费者组内合法的成员。 

虽然说这个场景很冷门，但也并非完全不会遇到。在一个大型公司中，特别是那些将Kafka作 为全公司级消息引擎系统的公司中，每个部门或团队都可能有自己的消费者应用，谁能保证 各自的Consumer程序配置的group.id没有重复呢？一旦出现不凑巧的重复，发生了上面提到 的这种场景，你使用之前提到的哪种方法都不能规避该异常。令人沮丧的是，无论是刚才哪 个版本的异常说明，都完全没有提及这个场景，因此，如果是这个原因引发的 CommitFailedException异常，前面的4种方法全部都是无效的。 

更为尴尬的是，无论是社区官网，还是网上的文章，都没有提到过这种使用场景。我个人认 为，这应该算是Kafka的一个bug。比起返回CommitFailedException异常只是表明提交位移失 

败，更好的做法应该是，在Consumer端应用程序的某个地方，能够以日志或其他方式友善地 提示你错误的原因，这样你才能正确处理甚至是预防该异常。 

总结一下，今天我们详细讨论了Kafka Consumer端经常碰到的CommitFailedException异 常。我们从它的含义说起，再到它出现的时机和场景，以及每种场景下的应对之道。当然， 我也留了个悬念，在专栏后面的内容中，我会详细说说多线程消费的实现方式。希望通过今 天的分享，你能清晰地掌握CommitFailedException异常发生的方方面面，从而能在今后更有 效地应对此异常。 



请比较一下今天我们提到的预防该异常的4种方法，并说说你对它们的理解。 

欢迎写下你的思考和答案，我们一起讨论。如果你觉得有所收获，也欢迎把文章分享给你的 朋友。 

© 版权归极客邦科技所有，未经许可不得传播售卖。 页面已增加防盗追踪，如有侵权极客邦将依法追究其法律责任。 

上一篇 18 | Kafka中位移提交那些事儿 

下一篇 20 | 多线程开发消费者实例 



1563351828 

max.poll.interval.ms是指两次poll()的最大间隔时间，kafka消费者以轮询的方式来拉取消息， 并且一次拉取批量的消息（默认500条），而批量的大小是通过max.poll.records来控制的。 两次poll()的实际时间取决于 单条消息的处理时间*一次拉取的消息量（500），当超过 max.poll.interval.ms配置的时间Kafka server认为kafka consumer掉线了，于是就执行分区再 均衡将这个consumer踢出消费者组。但是consumer又不知道服务端把自己给踢出了，下次 在执行poll()拉取消息的时候（在poll()拉取消息之前有个自动提交offset的操作），就会触发 该问题。 可见第2,3种方案是通过调整Kafka consumer的配置参数来缩短业务总的处理时间 或者增加服务端判断时长，比较容易实现；第1种就跟业务有关了，比较难搞，有些业务可能 就是要这么长的时间，很难再缩短；第4种方案就更复杂了，要把同步消息转换成异步，交给 其它线程来处理，这时需要把auto.commit.enable=false，手动提交offset，并且consumer是 线程不安全的，异步线程何时处理完，何时该提交，在哪提交，也是应用需要考虑的问题！ 希望胡老师针对第4种方案重点探讨一下！ 



### 1563289264 

老师，1、请问Standalone Consumer 的独立消费者一般什么情况会用到 2、Standalone Consumer 的独立消费者 使用跟普通消费者组有什么区别的。 

作者回复 1. 很多流处理框架的Kafka connector都没有使用consumer group，而是直接使用 standalone consumer，因为group机制不好把控 

2. standalone consumer没有rebalance，也没有group提供的负载均衡，你需要自己实现。其他方面 （比如位移提交）和group没有太大的不同 

1589212606 

为啥自动commit 不会抛 CommitFailedException？ 



## 作者回复 自动commit失败由Kafka内部消化处理 



1589128736 

“当消息处理的总时间超过预设的 max.poll.interval.ms 参数值时，Kafka Consumer 端会抛 出 CommitFailedException 异常”。 

其实逻辑是这样：消息处理的总时间超过预设的 max.poll.interval.ms 参数值 导致了 Rebalance‘； 

rebalance导致了 partition assgined 的consumer member变了； 

导致原来的consumer 想要commit都没法commit 。（因为元信息,比如连的broker都变了）. 

请老师指正下 

作者回复 嗯，差不多是这个道理：） 



1563237791 

希望老师可以更加具体的说说，rebalance的细节，比如某个consumer发生full gc的场景，它 的partition是怎么被分配走的，重连之后提交会发生什么 

作者回复 假设full gc导致所有线程STW，从而心跳中断，导致被踢出group，Coordinator向其他存活 consumer发送心跳response，通知它们开启新一轮rebalance。 



1563287855 

老师，我想问下max.poll.interval.ms两者session.timeout.ms有什么联系，可以说0.10.1.0 之 前的客户端 API，相当于session.timeout.ms代替了max.poll.interval.ms吗？ 比如说session.timeout.ms是5秒，如果消息处理超过5秒，也算是超时吗？ 

作者回复 嗯，我更愿意说是max.poll.interval.ms承担了session.timeout.ms的部分功能。在没有 max.poll.interval.ms和单独的心跳线程之前，如果session.timeout.ms = 5s，消息处理超过了5s，那 么consumer就算是超时 



### 1564524522 

To use this mode, instead of subscribing to the topic using subscribe, you just call assign(Collection) with the full list of partitions that you want to consume. 

String topic = "foo"; 

TopicPartition partition0 = new TopicPartition(topic, 0); TopicPartition partition1 = new TopicPartition(topic, 1); consumer.assign(Arrays.asList(partition0, partition1)); 

Once assigned, you can call poll in a loop, just as in the preceding examples to consume records. The group that the consumer specifies is still used for committing offsets, but now the set of partitions will only change with another call to assign. Manual partition assignment does not use group coordination, so consumer failures will not cause assigned partitions to be rebalanced. Each consumer acts independently even if it shares a groupId with another consumer. To avoid offset commit conflicts, you should usually ensure that the groupId is unique for each consumer instance. 

老师 standalone mode 是上面这段内容吗？ 

作者回复 是的。使用assign的consumer就是standalone consumer 



### 1563269996 

假如broker集群整个挂掉了，过段时间集群恢复后，consumer group会自动恢复消费吗？还 是需要手动重启consumer机器？ 

作者回复 consumer有重连机制 



1572942524 

A ：定义：所谓CommitFailedException，是指Consumer客户端在提交位移时出现了错误或 异常，并且并不可恢复的严重异常。 

B ：导致原因： 

- （1）消费者端处理的总时间超过预设的max.poll.interval.ms参数值 

- （2）出现一个Standalone Consumerd的独立消费者，配置的group.id重名冲突。 

# C ：解决方案： 

- （1）减少单条消息处理的时间 

- （2）增加Consumer端允许下游系统消费一批消息的最大时长 

- （3）减少下游系统一次性消费的消息总数。 

（4）下游使用多线程加速消费 



### 1563239011 

"不幸的是，session.timeout.ms 参数还有其他的含义，因此增加该参数的值可能会有其他方 面的“不良影响”，这也是社区在 0.10.1.0 版本引入 max.poll.interval.ms 参数，将这部分含 义从 session.timeout.ms 中剥离出来的原因之一。"--->能细述一下不良影响吗？ 

作者回复 之前版本中session.timeout.ms有多重含义，session过期时间、消息处理逻辑最大时间等 



1573174588 

老师，没有设置 group.id 话，会怎么样，系统会自动生成唯一的一个值吗 

作者回复 group.id是必须要设置的，否则会抛InvalidGroupIdException异常 



1564457788 

老师kafka死信该怎么去实现的？ 2.0之后增加了如下配置： errors.tolerance = all errors.deadletterqueue.topic.name = ""？ 

作者回复 还不够，你需要使用Kafka Connect组件才能实现。见： https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues 



1564429893 

我没在kafka官网、stackoverflow 、google 找到任何关于 standalone kafka consumer的 例 子，还望老师给个链接学习学习 

作者回复 Standalone consumer的提法并未出现在官方文档中，你可以在javadoc中看到一些： https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#manu 



1582361428 

老师您好，这边遇到个很奇怪的问题 

开启第一个消费者的时候，正常消费 

但是开启另一个后，触发了 rebalanced 

这时候第一个消费者会报出如下错误： 

The provided member is not known in the current generation 

是因为第一个消费者被踢出了 generation，但是它不知道，还在继续消费提交位移，或者做 着其他事情？这个其他事情可能是什么？ 

还有就是 rebalance发生的时候，消费者是立即暂停，还是会消费完整个poll？这时候 coordinator会等他吗？还是直接踢出去？ 

作者回复 两个consumer都是使用subscribe方法订阅的topic吗？ 

另外，rebalance发生时Coordinator会通过心跳response通知消费者。如果消费者此时正在处理消 息，肯定是不会响应的，毕竟还没有强行中断的机制 



1597882027 

老师poll一批消息，多线程处理并且是手动处理，会不会每个线程速度不一致，会导致提交位 移时，offset小得后提交，会有什么影响吗 

作者回复 如果是多线程提交位移，的确有可能出现这种情况。影响就是可能出现重复消费 



1588672607 

关于这个错误，我这边很奇怪，consumer用的是自动提交的配置，但是也出现了这个错误。 看错误应该是broker-2挂掉了，然后rediscovery。但是后面日志又说The coordinator is not aware of this member.再后面就是Commit cannot be completed 的错误了。我这里是有多个 broker，然后采用的域名的方式，ip可能会变。老师通过这些日志，能给点建议吗？ 错误日志： 

2020-05-04 18:49:56.592 INFO 6 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.AbstractCoordinator : [Consumer clientId=consumer-2, groupId=testgroup] Discovered group coordinator broker-2-lhfm0slmx1v4nyfz.kafka.svc01.local:9093 (id: 2147483645 rack: null) 2020-05-04 18:49:56.592 INFO 6 --- [ntainer#0-0-C-1] 

o.a.k.c.c.internals.AbstractCoordinator : [Consumer clientId=consumer-2, groupId=testgroup] Group coordinator broker-2-lhfm0slmx1v4nyfz.kafka.svc01.local:9093 (id: 2147483645 rack: null) is unavailable or invalid, will attempt rediscovery 2020-05-04 18:49:56.694 INFO 6 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.AbstractCoordinator : [Consumer clientId=consumer-2, groupId=testgroup] Discovered group coordinator broker-2-lhfm0slmx1v4nyfz.kafka.svc01.local:9093 (id: 2147483645 rack: null) 2020-05-04 18:49:56.735 ERROR 6 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator : [Consumer clientId=consumer-2, groupId=testgroup] Offset commit failed on partition test.topic-0 at offset 0: The coordinator is not aware of this member. 2020-05-04 18:49:56.735 WARN 6 --- [ntainer#0-0-C-1] o.a.k.c.c.internals.ConsumerCoordinator : [Consumer clientId=consumer-2, groupId=testgroup] Asynchronous auto-commit of offsets {test.topic-0=OffsetAndMetadata{offset=0, metadata=''}} failed: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing the session timeout or by reducing the maximum size of batches returned in poll() with max.poll.records. 

作者回复 能告知一下Kafka版本吗？目前社区对这部分代码改动很多，需要确认下是哪个版本碰到的 问题 



1574301314 

老师有两个疑问： 

1.相同的GroupId的Consumer 不应该就是同一个Consumer Group 组下的吗，或者有其他的 区分条件，比如订阅的Topic不同？ 

2.如果这个standalone Consumer 再给他添加一个同组的standalone Conusmer，会发生什 么？ 

作者回复 1. 设置相同group.id的consumer就是属于同一个group，你说的是对的：） 

2. 会出现位移提交失败的问题。严格来说这其实是一个问题，但是如果反馈到社区，社区会认为这 不是标准用法 



1593247458 

老师，我的程序使用的是自动提交offset，其他interval等相关参数（max poll, session timeout等）都是默认的。消息消费的速度也算快，500条应该就是一秒的事儿。可是这样也 还是偶尔会发生Offset commit failed异常，通常是在程序跑了一阵之后，请问您有没有遇到 过类似的情况？这是不是和单一一个consumer同时消费多个partition有关？ 

作者回复 这可能是因为broker端的问题而引起的，比如Coordinator变更了，或开启了新的 Rebalance。如果是偶发的，你不用太过担心~ 



1589990406 

老师。在学习这一章的时候。今天正好碰到一个commitFailedException的问题。kafka是 windows版本，0.10.0.0。kafka的持久化目录用的就是默认的/tmp目录。用Idea开发项目 时，会经常启停该kafka。今天我启动了一个消费者，生产者发送消息，消费者立马就报出了 commitFailedException异常。提示的就是poll loop的处理时间过长，提示我修改 session.timeout.ms的值。 

我的疑问点：我停止了消费者程序，此时就会发生reblance。然后，我又启动消费者。此时 再次发生reblance。应该是重新进行分区分配，不应该报出这个错误啊。我这个场景下，这 个错误发生的根本原因是什么？望老师解惑 (我最后的解决方案是：把tmp目录全部删除，然 后再启动消费者就可以了。) 

极客没有追评的功能。只能再重复一遍问题。老师，我是自动提交，什么原因会出现以上的 场景呢？忘了补充一句，我用的是：springboot，引入的是springboot整合kafka 的依赖 

作者回复 你的环境中是否存在这种情况：同时有同名的consumer group和standalone group。这种 情况下会抛出这个异常。据我所知，自动提交应该不会碰到这个异常，但你的环境是0.10，非常古 老，不确定那时候是否是这个设计。 

出现这个问题的原因在于你提交的时候Group在reblance。这种情况特别正常，所以后来社区修改了 代码，在自动提交时自行处理这个情况。但手动提交依然无法避免。 



### 1567751131 

Synchronous auto-commit of offsets {=OffsetAndMetadata{offset=6236, leaderEpoch=null, metadata=''}} failed: Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records. 

我们这边有几个项目，当用原生的kafka客户端经常出现这个报错，max.poll.interval.ms是默 认值300000，max.poll.records.是2000，但是实际上数据很少，每条数据处理的时间也很 短。heartbeat.interval.ms是2000，session.timeout.ms是12000。为什么经常出现这个错 误。 

重点是，另外几个项目用的是spring-kafka却重来没有出现过这样的报错，相关配置差不多， 业务场景差不多。求指点？怎么样避免这样的问题 

作者回复 还是要研究下你的环境中Rebalance多吗？ 



1564132678 

胡老师，手动提交时，当前后两次poll时间超过期望的max.poll.interval.ms时，会触发 Rebalance。 那么假如是自动提交时，会触发Rebalance吗？ 

假如，手动提交场景，consumer消费端处理业务时间过长（特殊case导致的），发生了 Rebalance，那么该consumer实例被踢出了，那么它永远‘死掉’了吗，还是会再次通过 heartbeat检测让它复活？ 

作者回复 不会 



### 1563283741 

老师好，我上一个问题的具体描述是这样的，今天讲CommitFailedException的例子是调用 consumer.commitSync();手动提交offset，确实当消息处理的总时间超过预设的 max.poll.interval.ms时会报这个异常，但是如果是自动提交offset的情况下，也就是把 enable.auto.commit=true，然后删除consumer.commitSync();代码，其它代码不变，也是 max.poll.interval.ms=5s，然后循环中sleep(6s)，发现不会报异常并且会一直重复消费，想问 下这是什么原因呢？ 

作者回复 嗯，是的。这个异常只是在手动提交时抛出的。 



1627796754 

好像只说了如何避免出现异常，但是异常出现了怎么处理呢，有类似回滚什么的吗？会重复 消费吗？ 



### 1627660295 

场景二，为什么只强调standalone 和 group之间可能会发生groupid相同，那group之间相同 怎么样呢？ 是不是会在coordinator注册group时就能发现组之间重复了groupid ?!!! 

作者回复 group之间相同？那么就会认为是同一个group 



1619693797 

无论哪种提交，最终的解决办法都是保守的，都是宁愿重复消费，也不让消息丢失，如果因 为位移提交导致消息丢失，那只能出现在手动提交的过程中，自己处理逻辑问题，没有处理 完消息，却提前提交了位移 



1616854156 

对于今天说的预防异常的四个种方法：总的来说，就是提高处理速度就可以解决问题，而如 

何提高速度呢？要不是max.poll.record一次的量级别少，然后处理逻辑快就可以避免；如果 处理逻辑慢了，且没办法优化，那么我们max.poll.interval.ms可以设置大一些，从而可以等 待我们的处理可以在有限时间内完成逻辑。同时我们采用多线程方式提高我们处理速度也是 一种比较复杂的方式。三种方式都可以尽量的去避免异常，但是如果我们的consumer standalone模式groupid名字跟我们的group consumer模式的groupid一样，这种情况就需要 我们改名字了。大体的三种优化方案，一种是模式方案。 



1609724719 

老师，场景二出现的原因是如果你的应用中同时出现了设置相同 group.id 值的【消费者组程 序】和【独立消费者程序】，还是说只要出现了设置相同的group.id就会出现呢？ 

作者回复 只要设置了就会出现 



1595384451 

我在 spark-sql-kafka-0-10_2.11-2.4.0-cdh6.2.0 看到如下代码： private def strategy(caseInsensitiveParams: Map[String, String]) = caseInsensitiveParams.find(x => STRATEGY_OPTION_KEYS.contains(x._1)).get match { case ("assign", value) => AssignStrategy(JsonUtils.partitions(value)) case ("subscribe", value) => SubscribeStrategy(value.split(",").map(_.trim()).filter(_.nonEmpty)) case ("subscribepattern", value) => SubscribePatternStrategy(value.trim()) case _ => // Should never reach here as we are already matching on // matched strategy names throw new IllegalArgumentException("Unknown option") } 是不是说，Spark 中的kafkaDataSource 实现是来自于调用的 option 参数呢？ 

作者回复 不太了解spark中的实现。代码中也没有出现kafkaDataSource？ 



1592313243 

打卡 



### 1589946136 

老师。在学习这一章的时候。今天正好碰到一个commitFailedException的问题。kafka是 windows版本，0.10.0.0。kafka的持久化目录用的就是默认的/tmp目录。用Idea开发项目 时，会经常启停该kafka。今天我启动了一个消费者，生产者发送消息，消费者立马就报出了 commitFailedException异常。提示的就是poll loop的处理时间过长，提示我修改 session.timeout.ms的值。 

我的疑问点：我停止了消费者程序，此时就会发生reblance。然后，我又启动消费者。此时 再次发生reblance。应该是重新进行分区分配，不应该报出这个错误啊。我这个场景下，这 个错误发生的根本原因是什么？望老师解惑 (我最后的解决方案是：把tmp目录全部删除，然 后再启动消费者就可以了。) 

作者回复 你是自动提交还是手动提交？应该是手动提交吧 



1583795654 

老师对于多线程消费的场景，如何保证消息的顺序呢。 

作者回复 通常无法保证，这也不属于典型的Kafka应用场景——Kafka追求高吞吐量。有诸多限制的 话会降低使用Kafka的收益：） 



### 1582011944 

有两个问题： 

1：请问消费者是不是只有：Consumer Group 及 Standalone Consumer 两类？ 

2：我在 Node 中，使用独立消费者配置相同的 groupId，启动了两个实例。使用生产者发布 消息时，两个消费者实例都可以正常处理，跟老师上面说的有点不太一样呀？这个是由于客 户端的不同导致的么？ 

作者回复 1. 是的 

2. 这是可能的。不同客户端设计机制不同。 



1581054209 

老师你好，我在使用spark streaming消费kafka消息会遇到某个batch消费耗时长。请老师帮 忙分析一下这个是什么原因，应该怎么去优化。谢谢老师。日志大体如下： 20/02/06 21:13:18 INFO KafkaRDD: Computing topic test_topic, partition 0 offsets 470985316 -> 470988502 20/02/06 21:13:18 DEBUG NetworkClient: Disconnecting from node 1 due to request timeout. 

20/02/06 21:13:18 DEBUG ConsumerNetworkClient: Cancelled FETCH request ClientRequest(expectResponse=true, callback=org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient$RequestFut request=RequestSend(header= {api_key=1,api_version=2,correlation_id=32,client_id=consumer-2}, body= {replica_id=-1,max_wait_time=3000,min_bytes=1,topics=[{topic=test_topic,partitions= [{partition=0,fetch_offset=470986525,max_bytes=10485760}]}]}), createdTimeMs=1580994741183, sendTimeMs=1580994741183) with correlation id 32 due to node 1 being disconnected 20/02/06 21:13:18 DEBUG Fetcher: Fetch failed org.apache.kafka.common.errors.DisconnectException 20/02/06 21:13:18 DEBUG NetworkClient: Initialize connection to node 2 for sending metadata request 

20/02/06 21:13:18 DEBUG NetworkClient: Initiating connection to node 2 at host1:9092. 

作者回复 从日志只能看到请求超时，具体原因看不出来，还是要结合其他因素进行分析。如果你能 确定是消费时间过长，那么优化这个点就可以了：） 



1577234353 

2019-12-23 16:43:56,367 consumer.py[line:792] WARNING Auto offset commit failed for group aff74e1e254e11ea9f47b827eb16d0ae: NodeNotReadyError: coordinator-1 2019-12-23 16:43:56,471 client_async.py[line:695] WARNING <BrokerConnection node_id=coordinator-1 host=39.104.137.50:9093 <connected> [IPv4 ('39.104.137.50', 9093)]> timed out after 305000 ms. Closing connection. 2019-12-23 16:43:56,473 client_async.py[line:327] WARNING Node coordinator-1 connection failed -- refreshing metadata 

2019-12-23 16:43:56,478 base.py[line:493] ERROR Error sending HeartbeatRequest_v1 to node coordinator-1 [[Error 7] RequestTimedOutError: Request timed out after 305000 ms] 2019-12-23 16:43:56,479 base.py[line:714] WARNING Marking the coordinator dead (node coordinator-1) for group aff74e1e254e11ea9f47b827eb16d0ae: [Error 7] 

RequestTimedOutError: Request timed out after 305000 ms. 出现上面的报错consumer就卡主了，不能接收数据了，是提交失败吧 

作者回复 看着像网络连接失败导致的问题。网络没问题吗？是持续性地报警吗？ 



1566030205 

CommitFailedException 表示，Consumer 客户端在提交位移时出现了错误或异常，而且还是 那种不可恢复的严重异常。 

如果框架在设计的时候，针对异常情况不但抛出异常信息，还给出相应的解决方案，那就更 人性化啦！ 

多谢分享。 



1563498324 

老师，请教2个问题，谢谢! 我想问下0.10.1.0之后的session.timeout.ms还有什么作用呢？ standalone consumer和group consumer在配置上如何区分? 

作者回复 用于侦测会话超时。standalone consumer和group的区分体现在API上 



### 1563360926 

[2019-07-17 18:53:36,230] ERROR [ReplicaManager broker=2] Error processing append operation on partition __consumer_offsets-49 (kafka.server.ReplicaManager) org.apache.kafka.common.errors.NotEnoughReplicasException: The size of the current ISR Set(2) is insufficient to satisfy the min.isr requirement of 2 for partition __consumer_offsets-49 

把min.insync.replicas = 2改成1，消费者就可以运行了，这种flower失效的情况怎么处理呢？ 



1563359413 

老师，kafka报错，哪个论坛比较火？ 



1563302056 

请问如何计算单条消息处理的时间， 比如收到一条消息之后要调用A,B,C,D,E 五个方法处理， 其中处理完B,E之后将结果发给别的kafka，处理时间是处理完A,B时间还是处理完A,B,C,D,E 的 总时间？ 

作者回复 这取决于你对如何才算处理完一条消息的定义。另外从Kafka中拿到消息后剩下的事情就完 全由你负责了，因此如何计算处理时间应该是你说了算的：） 



1563288674 

我在线上也遇到这个问题，想问一下老师：在新的consumer加入，发生repartition的时候， 是否也会抱这个错，谢谢了！ 



1563288177 

老师我这边遇到了一个奇怪的情况，kafka生成者发送消息能创建topic,但是消息怎么都发不 上去broker。并且在kafka-logs底下有一个和刚刚那条消息key值一样的文件夹, 

并且打印出如下的日志： 

[2019-07-16 22:34:16,380] INFO [Log partition=23bb7ffd-4aa5-42e2-9d84-c90f4566c15b-2, dir=/tmp/kafka-logs] Loading producer state till offset 0 with message format version 2 (kafka.log.Log) [2019-07-16 22:34:16,381] INFO [Log partition=23bb7ffd-4aa5-42e2-9d84-c90f4566c15b-2, dir=/tmp/kafka-logs] Completed load of log with 1 segments, log start offset 0 and log end offset 0 in 3 ms (kafka.log.Log) 

[2019-07-16 22:34:16,383] INFO Created log for partition 23bb7ffd-4aa5-42e2-9d84c90f4566c15b-2 in /tmp/kafka-logs with properties {compression.type -> producer, message.format.version -> 2.0-IV1, file.delete.delay.ms -> 60000, max.message.bytes -> 1000012, min.compaction.lag.ms -> 0, message.timestamp.type -> CreateTime, message.downconversion.enable -> true, min.insync.replicas -> 1, segment.jitter.ms -> 0, preallocate -> false, min.cleanable.dirty.ratio -> 0.5, index.interval.bytes -> 40 

作者回复 这都是info级别的log，没有看出有什么问题。最好确认去leader副本所在的broker去看日 志 





1563264045 

老师好，今天讲CommitFailedException的例子是调用consumer.commitSync();手动提交 offset，确实当消息处理的总时间超过预设的max.poll.interval.ms时会报这个异常，但是如果 是自动提交offset的情况下，不会报异常并且会一直重复消费，想问下这是什么原因呢？ 

作者回复 不知道你的代码是怎么写的。你是说循环调用poll然后返回相同的方法，是吗 



1563239383 

想进一步了解学习standalone consumer有什么资料推荐吗？ 

作者回复 没有。你可以自己试试。其实用法与consumer group差不多 



1563238513 

老师好，看你解释注释那段的时候提到消费者组已经开启了Rebalance过程，但是看后面的介 绍，出现异常好像并不一定有Rebalance，是这样吗？ 

作者回复 所以这段注释是有歧义或不准确的地方啊。 



1563234383 

通过什么方法计算单条消息处理时间呢？ 

作者回复 你可以自己加一些打点代码来计算 

