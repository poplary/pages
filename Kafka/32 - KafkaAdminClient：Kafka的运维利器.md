2019-8-15 胡夕 



你好，我是胡夕。今天我要和你分享的主题是：Kafka的运维利器KafkaAdminClient。 

在上一讲中，我向你介绍了Kafka自带的各种命令行脚本，这些脚本使用起来虽然方便，却有 一些弊端。 

首先，不论是Windows平台，还是Linux平台，命令行的脚本都只能运行在控制台上。如果你 想要在应用程序、运维框架或是监控平台中集成它们，会非常得困难。 

其次，这些命令行脚本很多都是通过连接ZooKeeper来提供服务的。目前，社区已经越来越不 推荐任何工具直连ZooKeeper了，因为这会带来一些潜在的问题，比如这可能会绕过Kafka的 安全设置。在专栏前面，我说过kafka-topics脚本连接ZooKeeper时，不会考虑Kafka设置的用 户认证机制。也就是说，任何使用该脚本的用户，不论是否具有创建主题的权限，都能成 功“跳过”权限检查，强行创建主题。这显然和Kafka运维人员配置权限的初衷背道而驰。 

最后，运行这些脚本需要使用Kafka内部的类实现，也就是Kafka服务 ~~<mark>器</mark>~~ 端的代码。实际上，社 区还是希望用户只使用Kafka ~~客户~~ 端代码，通过现有的请求机制来运维管理集群。这样的话， 所有运维操作都能纳入到统一的处理机制下，方便后面的功能演进。 

基于这些原因，社区于0.11版本正式推出了Java客户端版的AdminClient，并不断地在后续的 版本中对它进行完善。我粗略地计算了一下，有关AdminClient的优化和更新的各种提案，社 区中有十几个之多，而且贯穿各个大的版本，足见社区对AdminClient的重视。 

值得注意的是，服务 ~~<mark>器</mark>~~ 端也 ~~有一~~ 个AdminClient，包路径是kafka.admin。这是之前的老运维 工具类，提供的功能也比较有限，社区已经不再推荐使用它了。所以，我们最好统一使用客 户端的AdminClient。 

下面，我们来看一下如何在应用程序中使用AdminClient。我们在前面说过，它是Java客户端 提供的工具。想要使用它的话，你需要在你的工程中显式地增加依赖。我以最新的2.3版本为 例来进行一下展示。 

如果你使用的是Maven，需要增加以下依赖项： 

<dependency> 

<groupId>org.apache.kafka</groupId> <artifactId>kafka-clients</artifactId> <version>2.3.0</version> </dependency> 

如果你使用的是Gradle，那么添加方法如下： 

compile group: 'org.apache.kafka', name: 'kafka-clients', version: '2.3.0' 

鉴于社区还在不断地完善AdminClient的功能，所以你需要时刻关注不同版本的发布说明 （Release Notes），看看是否有新的运维操作被加入进来。在最新的2.3版本中， AdminClient提供的功能有9大类。 

1. 主题管理：包括主题的创建、删除和查询。 

2. 权限管理：包括具体权限的配置与删除。 

3. 配置参数管理：包括Kafka各种资源的参数设置、详情查询。所谓的Kafka资源，主要有 Broker、主题、用户、Client-id等。 

4. 副本日志管理：包括副本底层日志路径的变更和详情查询。 

5. 分区管理：即创建额外的主题分区。 

6. 消息删除：即删除指定位移之前的分区消息。 

7. Delegation Token管理：包括Delegation Token的创建、更新、过期和详情查询。 

8. 消费者组管理：包括消费者组的查询、位移查询和删除。 

9. Preferred领导者选举：推选指定主题分区的Preferred Broker为领导者。 

在详细介绍AdminClient的主要功能之前，我们先简单了解一下AdminClient的工作原理。从 设计上来 ~~看~~ ，AdminClient <mark>是</mark> ~~一~~ 个双线 <mark>程</mark> 的设计： ~~前~~ 端主线 <mark>程</mark> 和 ~~后~~ 端I/O线 <mark>程</mark> 。前端线程负责 将用户要执行的操作转换成对应的请求，然后再将请求发送到后端I/O线程的队列中；而后端 I/O线程从队列中读取相应的请求，然后发送到对应的Broker节点上，之后把执行结果保存起 来，以便等待前端线程的获取。 

值得一提的是，AdminClient在内部大量使用生产者-消费者模式将请求生成与处理解耦。我在 下面这张图中大致描述了它的工作原理。 



如图所示，前端主线程会创建名为Call的请求对象实例。该实例有两个主要的任务。 

1. 构 ~~建~~ 对应的 ~~请~~ 求对象。比如，如果要创建主题，那么就创建CreateTopicsRequest；如果是 查询消费者组位移，就创建OffsetFetchRequest。 

2. ~~指~~ 定响应的 <mark>回调</mark> 逻 ~~<mark>辑</mark>~~ 。比如从Broker端接收到CreateTopicsResponse之后要执行的动作。 一旦创建好Call实例，前端主线程会将其放入到新请求队列（New Call Queue）中，此 时，前端主线程的任务就算完成了。它只需要等待结果返回即可。 

剩下的所有事情就都是后端I/O线程的工作了。就像图中所展示的那样，该线程使用了3个队 列来承载不同时期的请求对象，它们分别是新请求队列、待发送请求队列和处理中请求队 列。为什么要使用3个呢？原因是目前新请求队列的线程安全是由Java的monitor锁来保证 的。为了 ~~确保前~~ 端主线 <mark>程</mark> 不 ~~会~~ <mark>因</mark> 为monitor锁被 ~~阻塞~~ ， ~~后~~ 端I/O线 <mark>程</mark> ~~会~~ 定 ~~期~~ 地将新 ~~请~~ 求队列 ~~中~~ 的所 ~~有~~ Call实例全 ~~部~~ 搬移到待发送 ~~请~~ 求队列 ~~中~~ 进 ~~行~~ 处 ~~理~~ 。图中的待发送请求队列和处理中请求 队列只由后端I/O线程处理，因此无需任何锁机制来保证线程安全。 

当I/O线程在处理某个请求时，它会显式地将该请求保存在处理中请求队列。一旦处理完成， I/O线程会自动地调用Call对象中的回调逻辑完成最后的处理。把这些都做完之后，I/O线程会 通知前端主线程说结果已经准备完毕，这样前端主线程能够及时获取到执行操作的结果。 AdminClient是使用Java Object对象的wait和notify实现的这种通知机制。 

严格来说，AdminClient并没有使用Java已有的队列去实现上面的请求队列，它是使用 ArrayList和HashMap这样的简单容器类，再配以monitor锁来保证线程安全的。不过，鉴于它 们充当的角色就是请求队列这样的主体，我还是坚持使用队列来指代它们了。 

了解AdminClient工作原理的一个好处在于，它 ~~能~~ 够帮 ~~助~~ 我们 ~~有~~ 针对性地对 <mark>调</mark> ~~用~~ AdminClient 的 <mark>程</mark> 序进 ~~行~~ <mark>调</mark> 试。 

我们刚刚提到的后端I/O线程其实是有名字的，名字的前缀是kafka-admin-client-thread。有 时候我们会发现，AdminClient程序貌似在正常工作，但执行的操作没有返回结果，或者hang 住了，现在你应该知道这可能是因为I/O线程出现问题导致的。如果你碰到了类似的问题，不 妨使用jstack ~~命~~ 令去查看一下你的AdminClient程序，确认下I/O线程是否在正常工作。 

这可不是我杜撰出来的好处，实际上，这是实实在在的社区bug。出现这个问题的根本原因， 就是I/O线程未捕获某些异常导致意外“挂”掉。由于AdminClient是双线程的设计，前端主 线程不受任何影响，依然可以正常接收用户发送的命令请求，但此时程序已经不能正常工作 了。 

如果你正确地引入了kafka-clients依赖，那么你应该可以在编写Java程序时看到AdminClient对 象。切记它的 ~~完整~~ 类 ~~路~~ 径 <mark>是</mark> org.apache.kafka.clients.admin.AdminClient，而不 <mark>是</mark> kafka.admin.AdminClient。后者就是我们刚才说的服务器端的AdminClient，它已经不被推荐 使用了。 

创建AdminClient实例和创建KafkaProducer或KafkaConsumer实例的方法是类似的，你需要 手动构造一个Properties对象或Map对象，然后传给对应的方法。社区专门为AdminClient提 供了几十个专属参数，最常见而且必须要指定的参数，是我们熟知的bootstrap.servers参数。 如果你想了解完整的参数列表，可以去官网查询一下。如果要销毁AdminClient实例，需要显 式调用AdminClient的close方法。 

你可以简单使用下面的代码同时实现AdminClient实例的创建与销毁。 

Properties props = new Properties(); props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-host:port"); props.put("request.timeout.ms", 600000); 

try (AdminClient client = AdminClient.create(props)) { // 执行你要做的操作…… } 

这段代码使用Java 7的try-with-resource语法特性创建了AdminClient实例，并在使用之后自动 关闭。你可以在try代码块中加入你想要执行的操作逻辑。 

讲完了AdminClient的工作原理和构造方法，接下来，我举几个实际的代码程序来说明一下如 何应用它。这几个例子，都是我们最常见的。 

首先，我们来看看如何创建主题，代码如下： 

String newTopicName = "test-topic"; 

try (AdminClient client = AdminClient.create(props)) { NewTopic newTopic = new NewTopic(newTopicName, 10, (short) 3); CreateTopicsResult result = client.createTopics(Arrays.asList(newTopic)); result.all().get(10, TimeUnit.SECONDS); } 

这段代码调用AdminClient的createTopics方法创建对应的主题。构造主题的类是NewTopic 类，它接收主题名称、分区数和副本数三个字段。 

注意这段代码倒数第二行获取结果的方法。目前，AdminClient各个方法的返回类型都是名为 ***Result的对象。这类对象会将结果以Java Future的形式封装起来。如果要获取运行结果， 你需要调用相应的方法来获取对应的Future对象，然后再调用相应的get方法来取得执行结 果。 

当然，对于创建主题而言，一旦主题被成功创建，任务也就完成了，它返回的结果也就不重 要了，只要没有抛出异常就行。 

接下来，我来演示一下如何查询指定消费者组的位移信息，代码如下： 

String groupID = "test-group"; try (AdminClient client = AdminClient.create(props)) { ListConsumerGroupOffsetsResult result = client.listConsumerGroupOffsets(groupID); Map<TopicPartition, OffsetAndMetadata> offsets = result.partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS); System.out.println(offsets); } 

和创建主题的风格一样，我们 <mark>调</mark> ~~用~~ AdminClient的listConsumerGroupOffsets方法去获 ~~取指~~ 定 ~~消费者组~~ 的位移数 ~~据~~ 。 

不过，对于这次返回的结果，我们不能再丢弃不管了， <mark>因</mark> 为它返 <mark>回</mark> 的Map对象 ~~中保~~ 存 ~~着~~ 按 <mark>照</mark> 分区分 ~~组~~ 的位移数 ~~据~~ 。你可以调用OffsetAndMetadata对象的offset()方法拿到实际的位移数 

据。 

现在，我们来使用AdminClient实现一个稍微高级一点的功能：获取某台Broker上Kafka主题 占用的磁盘空间量。有些遗憾的是，目前Kafka的JMX监控指标没有提供这样的功能，而磁盘 占用这件事，是很多Kafka运维人员要实时监控并且极为重视的。 

幸运的是，我们可以使用AdminClient来实现这一功能。代码如下： 

try (AdminClient client = AdminClient.create(props)) { 

DescribeLogDirsResult ret = client.describeLogDirs(Collections.singletonList(targetB long size = 0L; for (Map<String, DescribeLogDirsResponse.LogDirInfo> logDirInfoMap : ret.all().get( size += logDirInfoMap.values().stream().map(logDirInfo -> logDirInfo.replic topicPartitionReplicaInfoMap -> topicPartitionReplicaInfoMap.values().stream().map(replicaInfo -> .mapToLong(Long::longValue).sum(); } System.out.println(size); } 

这段代码的主要思想是，使用AdminClient的describeLogDirs方法获取指定Broker上所有分区 主题的日志路径信息，然后把它们累积在一起，得出总的磁盘占用量。 

好了，我们来小结一下。社区于0.11版本正式推出了Java客户端版的AdminClient工具，该工 具提供了几十种运维操作，而且它还在不断地演进着。如果可以的话，你最好统一使用 AdminClient来执行各种Kafka集群管理操作，摒弃掉连接ZooKeeper的那些工具。另外，我建 议你时刻关注该工具的功能完善情况，毕竟，目前社区对AdminClient的变更频率很高。 



请思考一下，如果我们要使用AdminClient去增加某个主题的分区，代码应该怎么写？请给出 主体代码。 

欢迎写下你的思考和答案，我们一起讨论。如果你觉得有所收获，也欢迎把文章分享给你的 朋友。 

© 版权归极客邦科技所有，未经许可不得传播售卖。 页面已增加防盗追踪，如有侵权极客邦将依法追究其法律责任。 

上一篇 31 | 常见工具脚本大汇总 下一篇 33 | Kafka认证机制用哪家？ 



1565971971 

可视化kafka管理工具，老师能推荐下吗？能支持2.0+版本 感谢！ 

作者回复 kafka manager 



1565872763 

想问下老师，在kafka某个topic下不小心创建了多个不用的消费组，怎么删除掉不用的消费组 呢？ 

作者回复 弃之不用，Kafka会自动删除它们的 



1573534033 

# 1 引入原因： 

A ：kafka自带的各种命令行脚本都只能运行在控制台上，不便于集成进应用程序或运维框架 B ：这些命令行脚本很多都是通过连接Zookeeper来提供服务，这存在一些潜在问题，如这可 能绕开Kafka的安全设置。 

C ：这些脚本需要使用Kafka内部的类实现，即Kafka服务端的代码。社区希望用户只使用 Kafka客户端代码，通过现有的请求机制来运维管理集群。 

2 如何使用： 

A ：要想使用，需要在工程中显示的地增加依赖。 

- 3 功能： 

A ：有九大类功能： 

- （1）主题管理：包括主题的创建，查询和删除 

- （2）权限管理：包括具体权限的配置与删除 

- （3）配置参数管理：包括Kafka各种资源的参数设置，详情查询。所谓的kafka资源主要有 Broker，主题，用户，Client-id等 

- （4）副本日志管理：包括副本底层日志路径的变更和详情查询 

- （5）分区管理：即创建额外的主题分区 

- （6）消息删除：删除指定位移之前的分区消息 

- （7）Delegation Token管理：包括Delegation Token的创建，更新，过期和详情查询 

- （8）消费者组管理：包括消费者组的查询，位移查询和删除 

- （9）Preferred领导者选举：推选指定主题分区的Preferred Broker为领导者。 

- 4 工作原理 

- A ：从设计上来看，AdminClient是一个双线程的设计：前端主线程和后端I/O线程。 

- （1）前端线程负责将用户要执行的操作转换成对应的请求，然后将请求发送到后端I/O线程 的队列中； 

- （2）后端I/O线程从队列中读取相应的请求，然后发送到对应的Broker节点上，之后把执行 结果保存起来，以便等待前端线程的获取。 

- B ：AdminClient在内部大量使用生产者—消费者模型将请求生产和处理解耦 

- C ：前端主线程会创建一个名为Call的请求对象实例。该实例的有两个主要任务 

- （1）构建对应的请求对象：如要创建主题，就创建CreateTopicRequest；要查询消费者位 移，就创建OffsetFetchRequest 

- （2）指定响应的回调逻辑：如Broker端接收到CreateTopicResponse之后要执行的动作。 

- （*）一旦创建好Call实例，前端主线程会将其放入到新请求队列（New Call Queue）中，此 时，前端主线程的任务就算完成了。他只需要等待结果返回即可。剩下的所有事情都是后端 I/O线程的工作了。 

- D ：后端I/O线程，该线程使用了3个队列来承载不同时期的请求对象，他们分别是新请求队 列，待发送请求队列和处理中请求队列。 

- （1）使用3个队列的原因：新请求队列的线程安全是有Java的monitor锁来保证的。为了确保 前端主线程不会因为monitor锁被阻塞，后端I/O线程会定期地将新请求队列中的所有Call实例 全部搬移到待发送请求队列中进行处理。 

- （2）待发送请求队列和处理中请求队列只由后端I/O线程处理，因此无需任何锁机制来保证 线程安全。 

- （3）当I/O线程在处理某个请求时，他会显式的将该请求保存在处理中请求队列。一旦处理 完成，I/O线程会自动地调用Call 对象中的回调完成最后的处理。 

- （4）最后，I/O线程会通知前端主线程处理完毕，这样前端主线程就能够及时的获取到执行 操作的结果。 

5 构造和销毁AdminClient实例 

A ：切记它的的完整路径是org.apche.kafka.clients.admin.AdminClient。 

B ：创建AdminClient实例和创建KafkaProducer或KafkaConsumer实例的方法是类似的，你 需要手动构造一个Properties对象或Map对象，然后传给对应的方法。 



1565829054 

老师，你好，这个只是提供了API是吧，那要是想可视化工具，还得基于它写代码是么 

作者回复 嗯嗯，是的 



1604487459 

这句话没懂，就算引入了其它两个队列，也无法避免锁阻塞啊，放进新请求队列的时候是一 定会存在锁争用的。我完全可以开启一个后台IO线程直接消费新请求的队列，因为新请求队 列一定是有序且线程安全的。 

为了确保前端主线程不会因为 monitor 锁被阻塞，后端 I/O 线程会定期地将新请求队列中的 所有 Call 实例全部搬移到待发送请求队列中进行处理。 

作者回复 我的意思是，至少不用Kafka自己实现线程同步了，交由Java类自行处理就好 



1590507644 

是不是存在有 高版本的 AdminClient 不能兼容低版本的 broker的问题？ 

我记得调用 2.x 的 AdminClient API去触发低版本（0.8.x.x）broker的reassign，会报错提示 “这个操作不被支持” 

作者回复 对的，不兼容。我记得只有在0.10.2.1之后才实现双向兼容 



老师 请问 org.apache.kafka 的kafka-clients 和 kafka_{scala版本号}这两个jar包的区别是啥 ? 

1566807701 

作者回复 前者是Clients端代码，后者是Server端代码 



## 1565834346 

老师可以简单对比一下pulsar 与kafka吗？感觉pulsar 的好多设计都是借鉴kafka的，最大的 一个区别是将broker 与数据存储分离，使得broker 可以更加容易扩展。另外，consumer 数 量的扩展也不受partition 数量的限制。pulsar 大有取代kafka之势，老师怎么看？ 

作者回复 哈哈，和Pulsar的郭总和翟总相识，不敢妄言。 



1565832834 

添加JMX指标以获取 Broker 磁盘占用这块感觉可以提个KIP 



1618999920 

2.7 看了源码的 增加分区。但是没有看明白逻辑，对于kafka设计的原理不理解导致嘛？就是 说为啥里面是这么分配。里面的集合的asList(1, 2),asList(2, 3), asList(3, 1) 表示的是什么意 思？第一位是broker嘛？第二位是分区数量？看了半天没整明白？ 

Increase the partition count for a topic to the given totalCount assigning the new partitions according to the given newAssignments. The length of the given newAssignments should equal totalCount - oldCount, since the assignment of existing partitions are not changed. Each inner list of newAssignments should have a length equal to the topic's replication factor. The first broker id in each inner list is the "preferred replica". 

For example, suppose a topic currently has a replication factor of 2, and has 3 partitions. The number of partitions can be increased to 6 using a NewPartition constructed like this: NewPartitions.increaseTo(6, asList(asList(1, 2), 

asList(2, 3), 

asList(3, 1))) 

In this example partition 3's preferred leader will be broker 1, partition 4's preferred leader will be broker 2 and partition 5's preferred leader will be broker 3. Params: 

totalCount – The total number of partitions after the operation succeeds. newAssignments – The replica assignments for the new partitions. 

public static NewPartitions increaseTo(int totalCount, List<List<Integer>> newAssignments) { 

return new NewPartitions(totalCount, newAssignments); } 

作者回复 没懂您的问题具体是什么。 



1609845196 

老师好，用adminclient中的creatacl实现授权好像没有效果，用kafka-acl.sh查不到记录，老 师在java代码中怎么动态授权呀 

作者回复 有具体代码吗？这些信息量无法进一步判断问题：（ 



1600321469 

增加分区数 

Map<String, NewPartitions> newPartitionsMap = new HashMap<>(); newPartitionsMap.put("test-topic", NewPartitions.increaseTo(13)); // 增加到x分区，x要比原 有分区数大 

CreatePartitionsResult result = adminClient.createPartitions(newPartitionsMap); result.all().get(); 

作者回复 ������ 



1593852313 

之前代码统计分区数好像不太对(代码未测试),修改为以上代码 

for (KafkaFuture<TopicDescription> kafkaFuture : kafkaFutures) { List<TopicPartitionInfo> topicPartitionInfos = kafkaFuture.get().partitions(); count += topicPartitionInfos.size(); } 

作者回复 👍 



1593851978 

使用 AdminClient 去增加某个主题的分区,暂时还没有测试 private void test() throws InterruptedException, ExecutionException, TimeoutException { Properties props = new Properties();props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafkahost:port");props.put("request.timeout.ms", 600000); //使用 AdminClient 去增加某个主题的分区 String newTopicName = "test-topic"; try (AdminClient client = AdminClient.create(props)) { int count = 0; DescribeTopicsResult result = client.describeTopics(Arrays.asList(newTopicName)); Map<String, KafkaFuture<TopicDescription>> kafkaFutureMap = result.values(); Collection<KafkaFuture<TopicDescription>> kafkaFutures = kafkaFutureMap.values(); for (KafkaFuture<TopicDescription> kafkaFuture : kafkaFutures) { List<TopicPartitionInfo> topicPartitionInfos = kafkaFuture.get().partitions(); for (TopicPartitionInfo topicPartitionInfo : topicPartitionInfos) { count += topicPartitionInfo.partition(); } } //新增一个分区 ++count; Map<String, NewPartitions> newPartitionsMap = new HashMap<>(); NewPartitions newPartition = NewPartitions.increaseTo(count); newPartitionsMap.put(newTopicName, newPartition); client.createPartitions(newPartitionsMap); } } 



1585015824 

老师10版本的kafka怎么可以通过JMX获取指定的监控对象值吗，所有api可以调用吗 

作者回复 有个JmxTool工具，你可以运行bin/kafka-run-class.sh kafka.tools.JmxTool去学习下它的用 法 



1583468727 

请问下，想写个java程序，该程序的功能是传入一个topic，能列出该topic下当前各个parition 最小的offset各是多少，请问用哪个类啊，谢谢您。 

作者回复 用KafkaConsumer就行，里面有endOffsets方法 



1574857655 

老师，请问怎么采集consumer group的性能指标呢？比如消息堆积数，需要了解到消费应用 程序的JMX端口才能采集吗？ 

作者回复 是的，典型的JMX指标包括lag, max-lag等 



1571799259 

👍 



1567828079 

手里的开发环境是这样：一台widnows 10的机器，在linux子系统中安装了kafka，在 windows中进行AdminClient调用，刚开始连接不上kakfa。后来通过在windows下，调用bat 脚本才发现是PCNAME.localdomain这个hostname识别不了。后来通过在hosts进行了一下配 置才ok。 

作者回复 嗯嗯，最好别在Windows上测试Kafka：） 



1567827925 

Map<String, NewPartitions> newPartitionsMap = new HashMap<>(); newPartitionsMap.put(topicName, NewPartitions.increaseTo(partitions)); CreatePartitionsResult createPartitionsResult = client.createPartitions(newPartitionsMap); KafkaFuture<Void> future1 = createPartitionsResult.values().get(topicName); 

future1.get(); System.out.println("ok"); 



1566876576 

老师，请教一下：可视化Kafka管理工具，是Kafka Manager更好还是Kafka Eagle更好，为什 么？ 

作者回复 前者用的比较多，后者坦率说没怎么用过，不好做推荐：） 



1566825289 

老师问下 AdminClient通过Java Object的 wait notify 实现通知机制,这里是 前端主线程进行条 件队列吗？是的话 主线程会阻塞吧 

作者回复 现在改成monitor锁的方式了。会阻塞，不过通常阻塞时间很短 



1566173942 

打卡，此节介绍了kafka的运维利器——AdminClient 1：AdminClient 的工作原理？ 从设计上来看，AdminClient 是一个双线程的设计：前端主线程和后端 I/O 线程。前端线程负 责将用户要执行的操作转换成对应的请求，然后再将请求发送到后端 I/O 线程的队列中；而 后端 I/O 线程从队列中读取相应的请求，然后发送到对应的 Broker 节点上，之后把执行结果 保存起来，以便等待前端线程的获取。 2：AdminClient的特点？ 社区于 0.11 版本正式推出了 Java 客户端版的 AdminClient 工具，该工具提供了几十种运维操 作，而且它还在不断地演进着——功能强悍，不断完善中。 



1565840708 

老师，有个问题，如果broker的端口号改变了，消费之前的 topic需要改动哪些参数 

作者回复 要修改consume端的连接配置即可 



