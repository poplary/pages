2019-7-20 胡夕 



你好，我是胡夕。今天我要和你分享的主题是：Kafka的Java消费者是如何管理TCP连接的。 

在专栏第13讲中，我们专门聊过“Java生产 ~~者~~ 是如何管理TCP连接资源的”这个话题，你应该 还有印象吧？今天算是它的姊妹篇，我们一起来研究下Kafka的Java ~~消费者~~ 管理TCP或Socket资 源的机制。只有完成了今天的讨论，我们才算是对Kafka客户端的TCP连接管理机制有了全面 的了解。 

和之前一样，我今天会无差别地混用TCP和Socket两个术语。毕竟，在Kafka的世界中，无论 是ServerSocket，还是SocketChannel，它们实现的都是TCP协议。或者这么说，Kafka的网络 传输是基于TCP协议的，而不是基于UDP协议，因此，当我今天说到TCP连接或Socket资源 时，我指的是同一个东西。 

我们先从消费者创建TCP连接开始讨论。消费者端主要的程序入口是KafkaConsumer类。和生 产 ~~者~~ 不 ~~<mark>同</mark>~~ 的 <mark>是</mark> ，构 ~~建~~ KafkaConsumer实例时 <mark>是</mark> 不 ~~会~~ 创 ~~建~~ 任何TCP连接的，也就是说，当你执行 完new KafkaConsumer(properties)语句后，你会发现，没有Socket连接被创建出来。这一点 和Java生产者是有区别的，主要原因就是生产者入口类KafkaProducer在构建实例的时候，会 在后台默默地启动一个Sender线程，这个Sender线程负责Socket连接的创建。 

从这一点上来看，我个人认为KafkaConsumer的设计比KafkaProducer要好。就像我在第13讲 中所说的，在Java构造函数中启动线程，会造成this指针的逃逸，这始终是一个隐患。 

如果Socket不是在构造函数中创建的，那么是在KafkaConsumer.subscribe或 KafkaConsumer.assign方法中创建的吗？严格来说也不是。我还是直接给出答案吧：TCP连接 <mark>是</mark> 在 <mark>调</mark> ~~用~~ KafkaConsumer.poll方法时被创 ~~建~~ 的。再细粒度地说，在poll方法内部有3个时机可 以创建TCP连接。 

# 1.发起FindCoordinator ~~请~~ 求时。 

还记得消费者端有个组件叫协调者（Coordinator）吗？它驻留在Broker端的内存中，负责消 费者组的组成员管理和各个消费者的位移提交管理。当消费者程序首次启动调用poll方法时， 它需要向Kafka集群发送一个名为FindCoordinator的请求，希望Kafka集群告诉它哪个Broker 是管理它的协调者。 

不过，消费者应该向哪个Broker发送这类请求呢？理论上任何一个Broker都能回答这个问 题，也就是说消费者可以发送FindCoordinator请求给集群中的任意服务器。在这个问题上， 社区做了一点点优化：消费者程序会向集群中当前负载最小的那台Broker发送请求。负载是 如何评估的呢？其实很简单，就是看消费者连接的所有Broker中，谁的待发送请求最少。当 然了，这种评估显然是消费者端的单向评估，并非是站在全局角度，因此有的时候也不一定 是最优解。不过这不并影响我们的讨论。总之，在这一步，消费者会创建一个Socket连接。 

# 2.连接协 <mark>调</mark> ~~者~~ 时。 

Broker处理完上一步发送的FindCoordinator请求之后，会返还对应的响应结果 （Response），显式地告诉消费者哪个Broker是真正的协调者，因此在这一步，消费者知晓 了真正的协调者后，会创建连向该Broker的Socket连接。只有成功连入协调者，协调者才能 开启正常的组协调操作，比如加入组、等待组分配方案、心跳请求处理、位移获取、位移提 交等。 

# 3. ~~消费~~ 数 ~~据~~ 时。 

消费者会为每个要消费的分区创建与该分区领导者副本所在Broker连接的TCP。举个例子，假 设消费者要消费5个分区的数据，这5个分区各自的领导者副本分布在4台Broker上，那么该消 费者在消费时会创建与这4台Broker的Socket连接。 

下面我们来说说消费者创建TCP连接的数量。你可以先思考一下大致需要的连接数量，然后我 们结合具体的Kafka日志，来验证下结果是否和你想的一致。 

我们来看看这段日志。 

[2019-05-27 10:00:54,142] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9092 (id: -1 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944) 

[2019-05-27 10:00:54,188] DEBUG [Consumer clientId=consumer-1, groupId=test] Sending metadata request MetadataRequestData(topics= [MetadataRequestTopic(name=‘t4’)], allowAutoTopicCreation=true, includeClusterAuthorizedOperations=false, includeTopicAuthorizedOperations=false) to node localhost:9092 (id: -1 rack: null) (org.apache.kafka.clients.NetworkClient:1097) 

[2019-05-27 10:00:54,188] TRACE [Consumer clientId=consumer-1, groupId=test] Sending FIND_COORDINATOR {key=test,key_type=0} with correlation id 0 to node -1 (org.apache.kafka.clients.NetworkClient:496) 

[2019-05-27 10:00:54,203] TRACE [Consumer clientId=consumer-1, groupId=test] Completed receive from node -1 for FIND_COORDINATOR with correlation id 0, received {throttle_time_ms=0,error_code=0,error_message=null, node_id=2,host=localhost,port=9094} (org.apache.kafka.clients.NetworkClient:837) 

[2019-05-27 10:00:54,204] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9094 (id: 2147483645 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944) 

[2019-05-27 10:00:54,237] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9094 (id: 2 rack: null) using address localhost/127.0.0.1 

(org.apache.kafka.clients.NetworkClient:944) 

[2019-05-27 10:00:54,237] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9092 (id: 0 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944) 

[2019-05-27 10:00:54,238] DEBUG [Consumer clientId=consumer-1, groupId=test] Initiating connection to node localhost:9093 (id: 1 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:944) 

这里我稍微解释一下，日志的第一行是消费者程序创建的第一个TCP连接，就像我们前面说 的，这个Socket用于发送FindCoordinator请求。由于这是消费者程序创建的第一个连接，此 时消费者对于要连接的Kafka集群一无所知，因此它连接的Broker节点的ID是-1，表示消费者 根本不知道要连接的Kafka Broker的任何信息。 

值得注意的是日志的第二行，消费者复用了刚才创建的那个Socket连接，向Kafka集群发送元 数据请求以获取整个集群的信息。 

日志的第三行表明，消费者程序开始发送FindCoordinator请求给第一步中连接的Broker，即 localhost:9092，也就是nodeId等于-1的那个。在十几毫秒之后，消费者程序成功地获悉协调 者所在的Broker信息，也就是第四行标为橙色的“node_id = 2”。 

完成这些之后，消费者就已经知道协调者Broker的连接信息了，因此在日志的第五行发起了 第二个Socket连接，创建了连向localhost:9094的TCP。只有连接了协调者，消费者进程才能 正常地开启消费者组的各种功能以及后续的消息消费。 

在日志的最后三行中，消费者又分别创建了新的TCP连接，主要用于实际的消息获取。还记得 我刚才说的吗？要消费的分区的领导者副本在哪台Broker上，消费者就要创建连向哪台 Broker的TCP。在我举的这个例子中，localhost:9092，localhost:9093和localhost:9094这3台 Broker上都有要消费的分区，因此消费者创建了3个TCP连接。 

看完这段日志，你应该会发现日志中的这些Broker节点的ID在不断变化。有时候是-1，有时候 是2147483645，只有在最后的时候才回归正常值0、1和2。这又是怎么回事呢？ 

前面我们说过了-1的来由，即消费者程序（其实也不光是消费者，生产者也是这样的机制）首 次启动时，对Kafka集群一无所知，因此用-1来表示尚未获取到Broker数据。 

那么2147483645是怎么来的呢？它是 ~~由~~ Integer.MAX_VALUE ~~减~~ 去协 <mark>调</mark> ~~者~~ 所在Broker的 ~~真~~ 实ID 计 ~~算~~ <mark>得</mark> 来的。看第四行标为橙色的内容，我们可以知道协调者ID是2，因此这个Socket连接的 节点ID就是Integer.MAX_VALUE减去2，即2147483647减去2，也就是2147483645。这种节 点ID的标记方式是Kafka社区特意为之的结果，目的就是要让组协调请求和真正的数据获取请 求使用不同的Socket连接。 

至于后面的0、1、2，那就很好解释了。它们表征了真实的Broker ID，也就是我们在 server.properties中配置的broker.id值。 

我们来简单总结一下上面的内容。通常来说，消费者程序会创建3类TCP连接： 

1. 确定协调者和获取集群元数据。 

2. 连接协调者，令其执行组成员管理操作。 

3. 执行实际的消息获取。 

那么，这三类TCP请求的生命周期都是相同的吗？换句话说，这些TCP连接是何时被关闭的 呢？ 

和生产者类似，消费者关闭Socket也分为主动关闭和Kafka自动关闭。主动关闭是指你显式地 调用消费者API的方法去关闭消费者，具体方式就是手 ~~动~~ <mark>调</mark> ~~用~~ KafkaConsumer.close()方法， <mark>或</mark> ~~者~~ <mark>是</mark> 执 ~~行~~ Kill ~~命~~ 令，不论是Kill -2还是Kill -9；而Kafka自动关闭是由 ~~消费者~~ 端参数 connection.max.idle.ms控制的，该参数现在的默认值是9分钟，即如果某个Socket连接上连 续9分钟都没有任何请求“过境”的话，那么消费者会强行“杀掉”这个Socket连接。 

不过，和生产者有些不同的是，如果在编写消费者程序时，你使用了循环的方式来调用poll方 法消费消息，那么上面提到的所有请求都会被定期发送到Broker，因此这些Socket连接上总 是能保证有请求在发送，从而也就实现了“长连接”的效果。 

针对上面提到的三类TCP连接，你需要注意的是，当 ~~第三~~ 类TCP连接成功创 ~~建后~~ ， ~~消费者~~ <mark>程</mark> 序 ~~就会~~ 废弃 ~~第一~~ 类TCP连接，之后在定期请求元数据时，它会改为使用第三类TCP连接。也就是 说，最终你会发现，第一类TCP连接会在后台被默默地关闭掉。对一个运行了一段时间的消费 者程序来说，只会有后面两类TCP连接存在。 

从理论上说，Kafka Java消费者管理TCP资源的机制我已经说清楚了，但如果仔细推敲这里面 的设计原理，还是会发现一些问题。 

我们刚刚讲过，第一类TCP连接仅仅是为了首次获取元数据而创建的，后面就会被废弃掉。最 根本的原因是，消费者在启动时还不知道Kafka集群的信息，只能使用一个“假”的ID去注 册，即使消费者获取了真实的Broker ID，它依旧无法区分这个“假”ID对应的是哪台 Broker，因此也就无法重用这个Socket连接，只能再重新创建一个新的连接。 

为什么会出现这种情况呢？主要是因为目前Kafka仅仅使用ID这一个维度的数据来表征Socket 连接信息。这点信息明显不足以确定连接的是哪台Broker，也许在未来，社区应该考虑使用< 主机 ~~名~~ 、端口、ID>三元组的方式来定位Socket资源，这样或许能够让消费者程序少创建一些 TCP连接。 

也许你会问，反正Kafka有定时关闭机制，这算多大点事呢？其实，在实际场景中，我见过很 多将connection.max.idle.ms设置成-1，即禁用定时关闭的案例，如果是这样的话，这些TCP 连接将不会被定期清除，只会成为永久的“僵尸”连接。基于这个原因，社区应该考虑更好 的解决方案。 

好了，今天我们补齐了Kafka Java客户端管理TCP连接的“拼图”。我们不仅详细描述了Java 消费者是怎么创建和关闭TCP连接的，还对目前的设计方案提出了一些自己的思考。希望今后 你能将这些知识应用到自己的业务场景中，并对实际生产环境中的Socket管理做到心中有数。 



假设有个Kafka集群由2台Broker组成，有个主题有5个分区，当一个消费该主题的消费者程序 启动时，你认为该程序会创建多少个Socket连接？为什么？ 

欢迎写下你的思考和答案，我们一起讨论。如果你觉得有所收获，也欢迎把文章分享给你的 朋友。 

- © 版权归极客邦科技所有，未经许可不得传播售卖。 页面已增加防盗追踪，如有侵权极客邦将依法追究其法律责任。 

上一篇 20 | 多线程开发消费者实例 

下一篇 22 | 消费者组消费进度监控都怎么实现？ 



1563576131 

整个生命周期里会建立4个连接，进入稳定的消费过程后，同时保持3个连接，以下是详细。 第一类连接：确定协调者和获取集群元数据。 

一个，初期的时候建立，当第三类连接建立起来之后，这个连接会被关闭。 

第二类连接：连接协调者，令其执行组成员管理操作。 一个 

第三类连接：执行实际的消息获取。 

两个分别会跟两台broker机器建立一个连接，总共两个TCP连接，同一个broker机器的不同分 区可以复用一个socket。 



1576639203 

消费者tcp连接一旦断开，就会导致rebalance，实际开发过程中，是不是需要尽量保证长连 接的模式？ 

作者回复 嗯，如果就是要长时间的消费，维持一个长连接是不错的选择 



注定非凡 1573000411 



- 1，何时创建 

A ：消费者和生产者不同，在创建KafkaConsumer实例时不会创建任何TCP连接。 

原因：是因为生产者入口类KafkaProducer在构建实例时，会在后台启动一个Sender线程，这 个线程是负责Socket连接创建的。 

- B ：TCP连接是在调用KafkaConsumer.poll方法时被创建。在poll方法内部有3个时机创建TCP 连接 

- （1）发起findCoordinator请求时创建 

Coordinator（协调者）消费者端主键，驻留在Broker端的内存中，负责消费者组的组成员管 理和各个消费者的位移提交管理。 

当消费者程序首次启动调用poll方法时，它需要向Kafka集群发送一个名为FindCoordinator的 请求，确认哪个Broker是管理它的协调者。 

## （2）连接协调者时 

Broker处理了消费者发来的FindCoordinator请求后，返回响应显式的告诉消费者哪个Broker 是真正的协调者。 

当消费者知晓真正的协调者后，会创建连向该Broker的socket连接。 

只有成功连入协调者，协调者才能开启正常的组协调操作。 

## （3）消费数据时 

消费者会为每个要消费的分区创建与该分区领导者副本所在的Broker连接的TCP. 

- 2 创建多少 

消费者程序会创建3类TCP连接： 

- （1） ：确定协调者和获取集群元数据 

- （2）：连接协调者，令其执行组成员管理操作 

- （3） ：执行实际的消息获取 

## 3 何时关闭TCP连接 

- A ：和生产者相似，消费者关闭Socket也分为主动关闭和Kafka自动关闭。 

- B ：主动关闭指通过KafkaConsumer.close()方法，或者执行kill命令，显示地调用消费者API 的方法去关闭消费者。 

- C ：自动关闭指消费者端参数connection.max.idle.ms控制的，默认为9分钟，即如果某个 socket连接上连续9分钟都没有任何请求通过，那么消费者会强行杀死这个连接。 

- D ：若消费者程序中使用了循环的方式来调用poll方法消息消息，以上的请求都会被定期的发 送到Broker，所以这些socket连接上总是能保证有请求在发送，从而实现“长连接”的效 果。 

- E ：当第三类TCP连接成功创建后，消费者程序就会废弃第一类TCP连接，之后在定期请求元 数据时，会改为使用第三类TCP连接。对于一个运行了一段时间的消费者程序来讲，只会有后 面两种的TCP连接。 

1563801238 



老师同一个消费组的客户端都只会连接到一个协调者吗？ 

作者回复 是的。每个group都有一个与之对应的coordinator 



1563757543 

我觉得作者可以跟学员的留言互动，然后每期课后思考可以在下期中贴出答案及分析，其实 留言讨论也是一个非常让人有收获的地方 



1587825812 

意思是即便知道了协调者在node 2上，还是会依然用2147483645这个id的TCP连接去跟协调 者通信吗。 

作者回复 这个数字不是固定的，而是用MAX - broker ID算出来的。对Coordinator的连接来说，是 的！ 它的ID就是这个算法 



1584417843 

老师您好，请问k8s这样的容器平台，适合部署kafka的消费者吗？如果容器平台起了二个一 模一样的消费者，对kafka来说会不会不知道自己通信的哪一个消费者? kafka通过什么来判断 不同的客户端? 

作者回复 每个客户端至少主机名和端口是不一样的，我是指在TCP连接这个层面。另外对于producer 而言，其实Kafka也不用区分它们，反正知道它们都再向集群发送消息就行了。对于consumer而言， 主要还是看group.id的设置以确定它们是否在同一个group。最后如果是你想区分客户端，那么可以 设置不同的client.id 



1563900443 

一共建过四次连接。若connection.max.idle.ms 不为-1，最终会断开第一次连的ID为-1的连 接。 



1563677848 

总共创建4个连接，最终保持3个连接： 

确定消费者所属的消费组对应的GroupCoordinator和获取集群的metadata时创建一个TCP连 接，由于此时的node id = -1，所以该连接无法重用。 

连接GroupCoordinator时，创建第二个TCP连接，node id值为Integer.MAX_VALUE-id 消费者会与每个分区的leader创建一个TCP连接来消费数据，node id为broker.id，由于kafka 只是用id这一维度来表征Socket连接信息，因此如果多个分区的leader在同一个broker上时， 会共用一个TCP连接，由于分区数大于broker的数量，所以会创建两个TCP连接消费数据。 



### 1579448135 

应该是3个tcp连接，第一个id=-1的没什么争议，然后是连接协调者的，但是broker，5个分区 的leader肯定会分布到这两台broker上，那么第三类tcp就是2个tcp连接，但是这2个中完全可 以有一个是直接使用连接协调者的那个tcp连接吧，但老师好像说过连接协调者的连接会和传 输数据的分开，id的计算都不相同，好吧，那就4个tcp连接吧。可这里真的不能复用吗？我觉 得可以。 

作者回复 目前与Coordinator和普通数据交互的TCP连接的确是分开的，你要说是否能复用，我觉得 当然可以复用，只不过现在没有这么设计：） 



### 1568856390 

元数据不包含协调者信息吗？为啥还要再请求一次协调者信息 什么设计思路？ 

作者回复 不包括，因为你请求元数据的broker可能不是Coordinator，没有Coordinator的信息 



1563752631 

“负载是如何评估的呢？其实很简单，就是看消费者连接的所有 Broker 中，谁的待发送请求 最少。” 

老师这个没太明白，这时候消费者不是还没连接么？那这部分信息是从哪获取到的呢？消费 者本地吗？ 

作者回复 刚开始的时候当然就类似于随机选broker了，但后面慢慢积累了一些数据之后这个小优化 还是会起一些作用的 



### 1592648167 

老师我有个疑问，consumer在FindCoordinator的时候会选择负载最小的broker进行连接， 文章说看消费者连接的所有 Broker 中，谁的待发送请求最少。请问consumer如何得知这个 消息？如果它想知道这个消息，不就得先和“某个东西”建立连接了？ 

作者回复 它首先倾向于使用已建立连接的节点。如果已建立连接的节点都在使用中，可能会创建新 的TCP连接 



### 1567073830 

我们要设计一个消息系统。有两个选择，更好的一种是每种不同schema的消息发一个topic。 但是有一种担心是consumer会为每个topic建立一个连接，造成连接数太多。请问胡老师， kafka client的consumer是每个集群固定数目的tcp连接，还是和topic数目相关？ 

作者回复 和它要订阅的topic分区数以及这些分区在broker上的散列情况有关。比如你订阅了100个 分区，但这个100个分区的leader副本都在一个broker上，那么长期来看consumer也就只和这1个 broker建立连接；相反如果这100分区散列在100个broker上，那么长期来看consumer会和100个 broker维持长连接 



### 1589876107 

老师，“消费者程序会向集群中当前负载最小的那台 Broker 发送请求”，消费者怎么单方面 知道服务器待发送的消息数量呢？而且应该只有leader才会实际发送消息吧，follower待发送 的都是0,消费者怎么在建立连接之前就知道服务器的角色呢？ 

作者回复 是这样判断的，就是看消费者与broker的TCP连接上的待处理请求的个数 



1570004896 

老师，想问下这里是不是笔误。 还记得消费者端有个组件叫Coordinator吗？协调者应该是位于Broker端的吧？ 

作者回复 嗯嗯，其实这里的消费者端指的是广义的消费者，我是想说在Kafka消费者的概念中有 Coordinator。当然如你所说Coordinator是Broker端的组件没错。这里的确有不严谨的地方，多谢指 出：） 



1563700099 

我认为也是3个连接，第一个是查找Coordinator的，这个会在后面断开。然后5个partition会 分布在2个broker上，那么客户端最多也就连接2次就能消费所有partition了，因此是连接3 个，最后保持2个。 



### 1563586144 

连接有三个阶段：首先获取协调者连接同时也获取元数据信息，这个连接后面会关闭；连接 协调者执行，等待分配分区，组协调等，这需要一个连接；后面真正消费五个分区两个 broker最多就两个连接，分区大于broker所以一定是两个，因为第一类连接没有id，所以无 法重用，会在第三类开启连接后关闭，所以开始四个连接最终保持三个连接 



1563555218 

3个tcp连接 一个查询协调着和获取元数据的tcp连接 一个连接协调写 管理组成员的tcp连接 主 题5个分区只有连接leader副本的broker需要创建连接 



1616900561 

第一个链接寻找协调器，第二个链接链接协调器进行管理，因为有两台broker索引需要创建 两个实际消费数据用的链接。总共四个链接。 



1608115106 

老师，我的kafka部在k8s上，版本是2.1.1，消费者通过serviceName+port的方式去访问，但 是kafka重启后Pod的IP变了，消费者连得还是原来的IP有什么办法吗 

作者回复 有办法通过域名访问吗? 



1605526866 

老师我们有这样一个场景，我们需要监测kafka是否存活，然后有一个程序没隔1s就通过tcp连 接kafka，然后断开！这样会不会把kakfa连接资源耗尽了啊 

作者回复 通过监测进程和端口号也可以判断吧。 



1595989429 

老师好，请教一个问题，现在对于producer和consumer都介绍了维持tcp连接的情况，那么 对于kafka集群 broker来说，这么多的tcp连接，是如何管理的呢？ 

作者回复 其实也没什么管理。如果一定要说管理，通过KafkaChannel对象来管理。 



1588672051 

胡老师，您好。我这边用的kafka 2.3 。Consumer一直出现这个错误信息在日志里面：202005-05 07:31:06.428 INFO 6 --- [ntainer#1-0-C-1] o.a.kafka.clients.FetchSessionHandler : [Consumer clientId=consumer-4, groupId=test-collections] Node 1 was unable to process the fetch request with (sessionId=2065156504, epoch=1204): 

FETCH_SESSION_ID_NOT_FOUND. 但是我看log是INFO级别的，请问是不是可以忽略掉？网 上说需要修改broker的max.incremental.fetch.session.cache.slots，有其他办法吗？ 

作者回复 看着很眼熟，应该是2.3已经修复的bug。你的客户端也升级到2.3版本了吗？ 



1579391914 

4个 



1575821680 

胡大佬，问下 "它连接的 Broker 节点的 ID 是 -1，表示消费者根本不知道要连接的 Kafka Broker 的任何信息。" 这边会有真实的broker机器与之对应嘛？ 

作者回复 会有的，只是暂时还不知道ID 



1568023986 

有2个 Broker，5个分区的领导者副本，由zookeeper分配Leader，所以默认是均匀的，第三 类会创建2个TCP连接，故共有4个TCP连接。 

请问胡老师 Leader副本 分配策略是什么？ 

作者回复 如果是不考虑机架信息，你基本上可以认为是round robin策略 



1564721961 

最终会保留三个吧，协调者链接需要与其他链接特地分开。 



- 1563863469 

老师，如果是调用consumer.createMessageStreams()这个方法，那这样也是建立min(broker 数，分区leader数)+1个tcp连接吗？还有建立一个连接会启动一个线程的吧，我看了我的java 进程下有几十个线程,但是好像也远远大于broker的数量。 



- 1563762187 

一个获取元数据的连接（之后会断开）+两个连接分区leader的连接+一个连接协调者的连接 



1563755652 

老师您好。我之前搭建了一个单机kafka，能正常收发消息。最近我按网上的介绍，把kafka改 成集群，出现一个问题：使用kafka自带的命令行生产者工具可以成功发送消息。但是用命令 行消费者工具总是接收不到数据（启动消费者一直不输出数据，仿佛没收到消息一样）。我 用strace跟踪发现消费者进程一在循环进行epoll（超时）调用，kafka服务器日志无异常。请 问这种情况要怎么检查问题。 



1563613650 

我觉得是3个TCP连接，查找协调者1个连接，连接两个Broker2个连接，且查找协调者的连接 慢慢会关闭 



1563548161 

同一个broker 一个topic下的分区可以复用一个连接 但是topic的不同分区的 leader 不一定都 在一个broker上 也就是消费时候 只有一个broker上的tcp连接起作用 

