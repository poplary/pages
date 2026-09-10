2019-7-2 胡夕 



你好，我是胡夕。今天我要和你分享的主题是：Kafka的Java生产者是如何管理TCP连接的。 

Apache Kafka的所有通信都是基于TCP的，而不是基于HTTP或其他协议。无论是生产者、消 费者，还是Broker之间的通信都是如此。你可能会问，为什么Kafka不使用HTTP作为底层的通 信协议呢？其实这里面的原因有很多，但最主要的原因在于TCP和HTTP之间的区别。 

从社区的角度来看，在开发客户端时，人们能够利用TCP本身提供的一些高级功能，比如多路 复用请求以及同时轮询多个连接的能力。 

所谓的多路复用请求，即multiplexing request，是指将两个或多个数据流合并到底层单一物 理连接中的过程。TCP的多路复用请求会在一条物理连接上创建若干个虚拟连接，每个虚拟连 接负责流转各自对应的数据流。其实严格来说，TCP并不能多路复用，它只是提供可靠的消息 交付语义保证，比如自动重传丢失的报文。 

更严谨地说，作为一个基于报文的协议，TCP能够被用于多路复用连接场景的前提是，上层的 应用协议（比如HTTP）允许发送多条消息。不过，我们今天并不是要详细讨论TCP原理，因 此你只需要知道这是社区采用TCP的理由之一就行了。 

除了TCP提供的这些高级功能有可能被Kafka客户端的开发人员使用之外，社区还发现，目前 已知的HTTP库在很多编程语言中都略显简陋。 

基于这两个原因，Kafka社区决定采用TCP协议作为所有请求通信的底层协议。 

Kafka的Java生产者API主要的对象就是KafkaProducer。通常我们开发一个生产者的步骤有4 步。 

第1步：构造生产者对象所需的参数对象。 

第2步：利用第1步的参数对象，创建KafkaProducer对象实例。 

第3步：使用KafkaProducer的send方法发送消息。 

第4步：调用KafkaProducer的close方法关闭生产者并释放各种系统资源。 

上面这4步写成Java代码的话大概是这个样子： 

Properties props = new Properties (); props.put(“参数1”, “参数1的值”)； props.put(“参数2”, “参数2的值”)； …… try (Producer<String, String> producer = new KafkaProducer<>(props)) { producer.send(new ProducerRecord<String, String>(……), callback); …… } 

这段代码使用了Java 7 提供的try-with-resource特性，所以并没有显式调用producer.close()方 法。无论是否显式调用close方法，所有生产者程序大致都是这个路数。 

现在问题来了，当我们开发一个Producer应用时，生产者会向Kafka集群中指定的主题 （Topic）发送消息，这必然涉及与Kafka Broker创建TCP连接。那么，Kafka的Producer客户 端是如何管理这些TCP连接的呢？ 

要回答上面这个问题，我们首先要弄明白生产者代码是什么时候创建TCP连接的。就上面的那 段代码而言，可能创建TCP连接的地方有两处：Producer producer = new KafkaProducer(props)和producer.send(msg, callback)。你觉得连向Broker端的TCP连接会是 哪里创建的呢？前者还是后者，抑或是两者都有？请先思考5秒钟，然后我给出我的答案。 

首先，生产者应用在创建KafkaProducer实例时是会建立与Broker的TCP连接的。其实这种表 述也不是很准确，应该这样说：在创 ~~建~~ KafkaProducer实例时，生产 ~~者~~ 应 ~~用会~~ 在 ~~后台~~ 创 ~~建并启 动一~~ 个 ~~名~~ 为Sender的线 <mark>程</mark> ，该Sender线 <mark>程</mark> 开 ~~始~~ <u>运</u> ~~行~~ 时 ~~首~~ 先 ~~会~~ 创 ~~建与~~ Broker的连接。我截取了 一段测试环境中的日志来说明这一点： 

[2018-12-09 09:35:45,620] DEBUG [Producer clientId=producer-1] Initialize connection to node localhost:9093 (id: -2 rack: null) for sending metadata request (org.apache.kafka.clients.NetworkClient:1084) 

[2018-12-09 09:35:45,622] DEBUG [Producer clientId=producer-1] Initiating connection to node localhost:9093 (id: -2 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:914) 

[2018-12-09 09:35:45,814] DEBUG [Producer clientId=producer-1] Initialize connection to node localhost:9092 (id: -1 rack: null) for sending metadata request (org.apache.kafka.clients.NetworkClient:1084) 

[2018-12-09 09:35:45,815] DEBUG [Producer clientId=producer-1] Initiating connection to node localhost:9092 (id: -1 rack: null) using address localhost/127.0.0.1 (org.apache.kafka.clients.NetworkClient:914) 

[2018-12-09 09:35:45,828] DEBUG [Producer clientId=producer-1] Sending metadata request (type=MetadataRequest, topics=) to node localhost:9093 (id: -2 rack: null) (org.apache.kafka.clients.NetworkClient:1068) 

你也许会问：怎么可能是这样？如果不调用send方法，这个Producer都不知道给哪个主题发 消息，它又怎么能知道连接哪个Broker呢？难不成它会连接bootstrap.servers参数指定的所有 Broker吗？嗯，是的，Java Producer目前还真是这样设计的。 

我在这里稍微解释一下bootstrap.servers参数。它是Producer的核心参数之一，指定了这个 Producer启动时要连接的Broker地址。请注意，这里的“启动时”，代表的是Producer启动 时会发起与这些Broker的连接。因此，如果你为这个参数指定了1000个Broker连接信息，那 么很遗憾，你的Producer启动时会首先创建与这1000个Broker的TCP连接。 

在实际使用过程中，我并不建议把集群中所有的Broker信息都配置到bootstrap.servers中，通 常你指定3～4台就足以了。因为Producer一旦连接到集群中的任一台Broker，就能拿到整个 集群的Broker信息，故没必要为bootstrap.servers指定所有的Broker。 

让我们回顾一下上面的日志输出，请注意我标为橙色的内容。从这段日志中，我们可以发 现，在KafkaProducer实例被创建后以及消息被发送前，Producer应用就开始创建与两台 Broker的TCP连接了。当然了，在我的测试环境中，我为bootstrap.servers配置了 localhost:9092、localhost:9093来模拟不同的Broker，但是这并不影响后面的讨论。另外， 日志输出中的最后一行也很关键：它表明Producer向某一台Broker发送了METADATA请求， 尝试获取集群的元数据信息——这就是前面提到的Producer能够获取集群所有信息的方法。 

讲到这里，我有一些个人的看法想跟你分享一下。通常情况下，我都不认为社区写的代码或 做的设计就一定是对的，因此，很多类似的这种“质疑”会时不时地在我脑子里冒出来。 

拿今天的这个KafkaProducer创建实例来说，社区的官方文档中提及KafkaProducer类是线程 安全的。我本人并没有详尽地去验证过它是否真的就是thread-safe的，但是大致浏览一下源 码可以得出这样的结论：KafkaProducer实例创建的线程和前面提到的Sender线程共享的可变 数据结构只有RecordAccumulator类，故维护了RecordAccumulator类的线程安全，也就实现 了KafkaProducer类的线程安全。 

你不需要了解RecordAccumulator类是做什么的，你只要知道它主要的数据结构是一个 ConcurrentMap<TopicPartition, Deque>。TopicPartition是Kafka用来表示主题分区的Java对 象，本身是不可变对象。而RecordAccumulator代码中用到Deque的地方都有锁的保护，所以 基本上可以认定RecordAccumulator类是线程安全的。 

说了这么多，我其实是想说，纵然KafkaProducer是线程安全的，我也不赞同创建 KafkaProducer实例时启动Sender线程的做法。写了《Java并发编程实践》的那位布赖恩·格茨 （Brian Goetz）大神，明确指出了这样做的风险：在对象构造器中启动线程会造成this指针的 逃逸。理论上，Sender线程完全能够观测到一个尚未构造完成的KafkaProducer实例。当然， 在构造对象时创建线程没有任何问题，但最好是不要同时启动它。 

好了，我们言归正传。针对TCP连接何时创建的问题，目前我们的结论是这样的：TCP连接 <mark>是</mark> 在创 ~~建~~ KafkaProducer实例时 ~~建~~ 立的。那么，我们想问的是，它只会在这个时候被创建吗？ 

当然不是！TCP连接还 ~~可能~~ 在 ~~两~~ 个地方被创 ~~建~~ ： ~~一~~ 个 <mark>是</mark> 在 ~~更~~ 新 ~~元~~ 数 ~~据后~~ ， <mark>另</mark> ~~一~~ 个 <mark>是</mark> 在 ~~消息~~ 发送 时。为什么说是可能？因为这两个地方并非总是创建TCP连接。当Producer更新了集群的元数 据信息之后，如果发现与某些Broker当前没有连接，那么它就会创建一个TCP连接。同样地， 当要发送消息时，Producer发现尚不存在与目标Broker的连接，也会创建一个。 

接下来，我们来看看Producer更新集群元数据信息的两个场景。 

场景一：当Producer尝试给一个不存在的主题发送消息时，Broker会告诉Producer说这个主 题不存在。此时Producer会发送METADATA请求给Kafka集群，去尝试获取最新的元数据信 息。 

场景二：Producer通过metadata.max.age.ms参数定期地去更新元数据信息。该参数的默认 值是300000，即5分钟，也就是说不管集群那边是否有变化，Producer每5分钟都会强制刷新 一次元数据以保证它是最及时的数据。 

讲到这里，我们可以“挑战”一下社区对Producer的这种设计的合理性。目前来看，一个 Producer默认会向集群的所有Broker都创建TCP连接，不管是否真的需要传输请求。这显然是 没有必要的。再加上Kafka还支持强制将空闲的TCP连接资源关闭，这就更显得多此一举了。 

试想一下，在一个有着1000台Broker的集群中，你的Producer可能只会与其中的3～5台 Broker长期通信，但是Producer启动后依次创建与这1000台Broker的TCP连接。一段时间之 后，大约有995个TCP连接又被强制关闭。这难道不是一种资源浪费吗？很显然，这里是有改 善和优化的空间的。 

说完了TCP连接的创建，我们来说说它们何时被关闭。 

Producer端关闭TCP连接的方式有两种： ~~一~~ 种 <mark>是</mark> ~~用户~~ 主 ~~动~~ 关闭； ~~一~~ 种 <mark>是</mark> Kafka ~~自动~~ 关闭。 

我们先说第一种。这里的主动关闭实际上是广义的主动关闭，甚至包括用户调用kill -9主 动“杀掉”Producer应用。当然最推荐的方式还是调用producer.close()方法来关闭。 

第二种是Kafka帮你关闭，这与Producer端参数connections.max.idle.ms的值有关。默认情况 下该参数值是9分钟，即如果在9分钟内没有任何请求“流过”某个TCP连接，那么Kafka会主 动帮你把该TCP连接关闭。用户可以在Producer端设置connections.max.idle.ms=-1禁掉这种 机制。一旦被设置成-1，TCP连接将成为永久长连接。当然这只是软件层面的“长连接”机 制，由于Kafka创建的这些Socket连接都开启了keepalive，因此keepalive探活机制还是会遵守 的。 

值得注意的是，在第二种方式中，TCP连接是在Broker端被关闭的，但其实这个TCP连接的发 起方是客户端，因此在TCP看来，这属于被动关闭的场景，即passive close。被动关闭的后果 就是会产生大量的CLOSE_WAIT连接，因此Producer端或Client端没有机会显式地观测到此连 接已被中断。 

我们来简单总结一下今天的内容。对最新版本的Kafka（2.1.0）而言，Java Producer端管理 TCP连接的方式是： 

1. KafkaProducer实例创建时启动Sender线程，从而创建与bootstrap.servers中所有Broker的 TCP连接。 

2. KafkaProducer实例首次更新元数据信息之后，还会再次创建与集群中所有Broker的TCP连 接。 

3. 如果Producer端发送消息到某台Broker时发现没有与该Broker的TCP连接，那么也会立即 创建连接。 

4. 如果设置Producer端connections.max.idle.ms参数大于0，则步骤1中创建的TCP连接会被 自动关闭；如果设置该参数=-1，那么步骤1中创建的TCP连接将无法被关闭，从而成为“僵 尸”连接。 



对于今天我们“挑战”的社区设计，你有什么改进的想法吗？ 

欢迎写下你的思考和答案，我们一起讨论。如果你觉得有所收获，也欢迎把文章分享给你的 朋友。 

- © 版权归极客邦科技所有，未经许可不得传播售卖。 页面已增加防盗追踪，如有侵权极客邦将依法追究其法律责任。 

上一篇 12 | 客户端都有哪些不常见但是很高级的功能？ 

下一篇 14 | 幂等生产者和事务生产者是一回事吗？ 



1572482869 

Apache Kafka的所有通信都是基于TCP的，而不是于HTTP或其他协议的 

- 1 为什采用TCP? 

（1）TCP拥有一些高级功能，如多路复用请求和同时轮询多个连接的能力。 

- （2）很多编程语言的HTTP库功能相对的比较简陋。 

名词解释： 

多路复用请求：multiplexing request，是将两个或多个数据合并到底层—物理连接中的过 程。TCP的多路复用请求会在一条物理连接上创建若干个虚拟连接，每个虚拟连接负责流转各 自对应的数据流。严格讲：TCP并不能多路复用，只是提供可靠的消息交付语义保证，如自动 重传丢失的报文。 

- 2 何时创建TCP连接？ 

- （1）在创建KafkaProducer实例时， 

A：生产者应用会在后台创建并启动一个名为Sender的线程，该Sender线程开始运行时，首 先会创建与Broker的连接。 

B：此时不知道要连接哪个Broker，kafka会通过METADATA请求获取集群的元数据，连接所 有的Broker。 

- （2）还可能在更新元数据后，或在消息发送时 

- 3 何时关闭TCP连接 

- （1）Producer端关闭TCP连接的方式有两种：用户主动关闭，或kafka自动关闭。 

- A：用户主动关闭，通过调用producer.close()方关闭，也包括kill -9暴力关闭。 

- B：Kafka自动关闭，这与Producer端参数connection.max.idles.ms的值有关，默认为9分 

- 钟，9分钟内没有任何请求流过，就会被自动关闭。这个参数可以调整。 

- C：第二种方式中，TCP连接是在Broker端被关闭的，但这个连接请求是客户端发起的，对 TCP而言这是被动的关闭，被动关闭会产生大量的CLOSE_WAIT连接。 

作者回复 总结得相当强：） 



1563852542 

Producer 通过 metadata.max.age.ms定期更新元数据，在连接多个broker的情况下， producer是如何决定向哪个broker发起该请求？ 

作者回复 向它认为当前负载最少的节点发送请求，所谓负载最少就是指未完成请求数最少的broker 



1562765896 

最近在使用kafka Connector做数据同步服务，在kafka中创建了许多topic，目前对kafka了解 还不够深入，不知道这个对性能有什么影响？topic的数量多大范围比较合适？ 

作者回复 topic数量只要不是太多，通常没有什么影响。如果单台broker上分区数超过2k，那么可能 要关注是否会出现性能问题了。 



1562072975 

老师好，看了今天的文章我有几个问题： 

1.Kafka的元数据信息是存储在zookeeper中的，而Producer是通过broker来获取元数据信息 的，那么这个过程是否是这样的，Producer向Broker发送一个获取元数据的请求给Broker， 之后Broker再向zookeeper请求这个信息返回给Producer? 

2.如果Producer在获取完元数据信息之后要和所有的Broker建立连接，那么假设一个Kafka集 群中有1000台Broker，对于一个只需要与5台Broker交互的Producer，它连接池中的链接数 量是不是从1000->5->1000->5?这样不是显得非常得浪费连接池资源？ 

作者回复 1. 集群元数据持久化在ZooKeeper中，同时也缓存在每台Broker的内存中，因此不需要请 求ZooKeeper 

2. 就我个人认为，的确有一些不高效。所以我说这里有优化的空间的。 



1562030273 

应该可以用懒加载的方式，实际发送时再进行TCP连接吧，虽然这样第一次发送时因为握手的 原因会稍慢一点 



## 1578559985 

老师有个问题请教下： 

Producer 通过 metadata.max.age.ms 参数定期地去更新元数据信息，默认5分钟更新元数 据，如果没建立TCP连接则会创建，而connections.max.idle.ms默认9分钟不使用该连接就会 关闭。那岂不是会循环往复地不断地在创建关闭TCP连接了吗？ 

作者回复 如果你的producer长时间没有消息需要发送，TCP连接确实会定期关闭再重建的 



## 1564920371 

老师，如果Broker端被动关闭，会导致client端产生close_wait状态，这个状态持续一段时间 之后，client端不是应该发生FIN完成TCP断开的正常四次握手吗？怎么感觉老师讲的这个FIN 就不会再发了，导致了僵尸连接的产生？ 

作者回复 问题在于客户端有可能一直hold住这个连接导致状态一直是CLOSE_WAIT。事实上，这是非 常正确的做法，至少符合TCP协议的设计。当然，如果客户端关闭了连接，就像你说的，OS会发起 FIN给远端 



## 1562056941 

试想一下，在一个有着 1000 台 Broker 的集群中，你的 Producer 可能只会与其中的 3～5 台 Broker 长期通信，但是 Producer 启动后依次创建与这 1000 台 Broker 的 TCP 连接。一段时 间之后，大约有 995 个 TCP 连接又被强制关闭。这难道不是一种资源浪费吗？很显然，这里 是有改善和优化的空间的。 

这段不敢苟同。作为消息服务器中国，连接应该是种必要资源，所以部署时就该充分给予， 而且创建连接会消耗CPU,用到时再创建不合适，我甚至觉得Kafka应该有连接池的设计。 

另外最后一部分关于TCP关闭第二种情况，客户端到服务端没有关闭，只是服务端到客户端关 闭了，tcp是四次断开，可以单方向关闭，另一方向继续保持连接 

作者回复 嗯嗯，欢迎不同意见。Kafka对于创建连接没有做任何限制。如果一开始就创建所有TCP连 接，之后因为超时的缘故又关闭这些连接，当真正使用时再次创建，那么为什么不把创建时机后延到 

真正需要的时候呢？实际场景中将TCP连接设置为长连接的情形并不多见，因此我说这种设计是可以 改进的。 



## 1566294076 

老是您好，咨询两个问题。 

1. Producer实例创建和维护的tcp连接在底层是否是多个Producer实例共享的，还是Jvm内， 多个Producer实例会各自独立创建和所有broker的tcp连接 

2.Producer实例会和所有broker维持连接，这里的所有，是指和topic下各个分区leader副本 所在的broker进行连接的，还是所有的broker，即使该broker下的所有topic分区都是flower 

作者回复 1. 这些TCP连接只会被Producer实例下的Sender线程使用。多个Producer实例会创建各自 的TCP连接 

2. 从长期来看，只和需要交互的Broker有连接 



## 1589352920 

胡夕老师，Kafka集群的元数据信息是保存在哪里的呢，以CDH集群为例，我比较菜：） 

作者回复 最权威的数据保存在ZooKeeper中，Controller会从ZooKeeper中读取并保存在它自己的内 存中，然后同步部分元数据给集群所有Broker 



1562472279 

谢谢老师。有几个问题请教一下： 

1. producer连接是每个broker一个连接，跟topic没有关系是吗？（consumer也是这样是 吗？） 

2. 我们运维在所有的broker之前放了一个F5做负载均衡，但其实应该也没用，他会自动去获 取kafka所有的broker，绕过这个F5，不知道我的理解是否正确？ 

3. 在线上我们有个kafka集群，大概200个topic，数据不是很均衡，有的一天才十几m，有的 一天500G，我们是想consumer读取所有的topic，然后后面做分发，但是consumer会卡死在 哪，也没有报错，也没有日志输出，不知道老师有没有思路可能是什么原因？ 谢谢了！ 

作者回复 1. 也不能说没有关系。客户端需要和topic下各个分区leader副本所在的broker进行连接的 

2. 嗯嗯，目前客户端是直连broker的 

3. 光看描述想不出具体的原因。有可能是频繁rebalance、long GC、消息corrupted或干脆就是一个 已知的bug 



1562033308 

觉得创建kafkaProducer的时候可以不用去创建sender线程去连接broker。 

1. 第一次更新元数据的时候，配置一个并发连接参数，比如说10，按照该连接参数的余数去 和配置中broker建立TCP连接。 

2. 获取到相应的metadata信息后，再去和相应的broker进行连接，连接建立后关闭掉无用的 连接。 

3. 按照原有设计，发送数据时再次检查连接。 

这样多余连接不会超过10，并且可配置。而且在更新metadata和发送数据时进行了连接的双 重监测，不用进行三次监测。 



1562031618 

看来无论在bootstrap.servers中是否写全部broker 的地址接下来producer 还是会跟所有的 broker 建立一次连接😂 



1563887683 

老师，请教一个问题，目前遇到一个文中所提的一个问题，就是broker端被直接kill -9,然后产 生不量的close_wait,导致重启broker后，producer和consumer都连不上，刷了大量的日志， 把机器磁盘给刷爆了，请问老师这个问题我应该怎么去处理？ 

作者回复 为什么连不上了呢？是ulimit打满了吗？如果是可否调大一下？ 



1562077910 

老师下面就有一个问题，KafkaProducer是建议创建实例后复用，像连接池那样使用，还是建 议每次发送构造一个实例？听完这讲后感觉哪个都不合理，每次new会有很大的开销，但是 

一次new感觉又有僵尸连接，KafkaProducer适合池化吗？还是建议单例？ 

作者回复 KafkaProducer是线程安全的，复用是没有问题的。只是要监控内存缓冲区的使用情况。毕 竟如果多个线程都使用一个KafkaProducer实例，缓冲器被填满的速度会变快。 



1562071835 

老师你好，kafka更新元数据的方法只有每5分钟的轮训吗，如果有监控zk节点之类的，是不 是可以把轮询元数据时间调大甚至取消 

作者回复 Clients端有个参数metadata.max.age.ms强制刷新元数据，默认的确是5分钟。新版本 Clients不会与ZooKeeper交互，所以感觉和ZooKeeper没什么关系。。。 



1562031455 

KafkaProducer 实例只是在首次更新元数据信息之后，创建与集群中所有 Broker 的 TCP 连 接，还是每次更新之后都要创建？为什么要创建与所有 Broker 的连接呢？ 



1589441409 

请问老师， 

第一次创建实例，获取metadata数据，比如有1000个Broker，则会创建1000个连接吗 然后跟不存在的主题发送消息，也会获取metadata数据，然后也是创建1000个连接吗 最后，定时更新metadada也是会创建1000个连接吗 

然后最大保活时间又删除无用的连接，是吧。 

作者回复 目前的设计是不会，它只会与需要访问的主题分区所在的broker建立连接 



1577236962 

整个集群 topic 的数量有限制嘛, 最大是多少 ? 单台broker上分区数最好不要超过 2k . 这个是根据经验来的嘛,还是官方有推荐.?? 

作者回复 官方给的经验：） 最好还是结合自己实际场景而定 



1562138337 

请问Producer会和同一台broker建立多个TCP连接吗？ 

作者回复 有可能，可能用于不同的目的，通常最终会收敛成一个 



1562030727 

producer是否会有类似于heart beat的机制去探测可能被broker关闭的连接然后建立重连呢？ 

作者回复 当需要用到连接而发现连接不可用的时候就会重建连接了 



1562026950 

我的想法这样的，先用客户端去连配置的第一broker server，连不上就连接第二个，一旦连 上了，就可以获取元数据信息了，这是创建produce实例时候动作，只发起一个TCP连接。再 send时候发现没连接的再连接，至于其他的都还是很合理的。 



1594002907 

老师，深入思考 🤔 再追问下: 

“如果某个 Socket 连接上连续 9 分钟都没有任何请求“过境”的话，那么消费者会强行“杀 掉”这个 Socket 连接”，这个socket连接被杀掉后，还会重新跟leader副本所在的broker节 点建立连接吗？如果会，这个应该是不会reblance因为跟心跳连接不是同一个。所以，会在 啥场景重新建立？ 

作者回复 “重新跟leader副本所在的broker节点建立连接？” -- 如果还需要发送请求则要建立连 接。会不会rebalance与tcp连接无关 



1585048599 

假如broker leader副本有100台机器，bootStrap.servers配置了10个broker地址， kafkaProducer创建实例时，是创建100个Tcp连接还是110个Tcp连接？ 

作者回复 长期来看，producer与N个broker通讯，那么就会维持与这N个Broker的TCP连接 



1583830585 

胡老师问一个问题，producer 异步发送数据时报这个错： The server disconnected before a response was received，想问一下这个可能导致的原因以及解决办法，kafka版本是 0.10.1.1，谢谢老师。 

作者回复 这个错误多是网络原因导致的，如果是瞬时错误可以不用理会，如果持续抛这个错误那么 需要进一步看是什么原因了导致的了。 



1582519994 

老师 有一点不明白，为什么更新元数据需要和所有的broker建立 tcp 连接呢？ 我感觉应该仅 仅是controller或者某个broker吧。 

作者回复 并不会与所有broker建立TCP连接去获取元数据。随机找一个broker去获取的~ 



1566872908 

broker有很多time_wait端口，甚至比established多，这是什么情况 

作者回复 可能有很多clients端连接过该broker，而clients又都没有正常关闭所致 



1565861745 

请问一下老师客户端是怎么选择broker的 

作者回复 一般先随机选一个broker获取到所有集群信息，然后再有针对性地连接特定的broker 



1565823876 

1：集群中的任意一台broker都拥有整个集群的所有broker的信息？ 通过评论了解到，所有的元数据是存储在zookeeper集群节点中的，broker是缓存了这部分信 息。元数据知都有主题的信息、都有broker的信息。 

2：勇敢的质疑精神是，独立思考的前提 

3：当Producer 尝试给一个不存在的主题发送消息时，Broker 会告诉 Producer 说这个主题 不存在。此时 Producer 会发送 METADATA 请求给 Kafka 集群，去尝试获取最新的元数据信 息。 

这种情况是怎么发生的，一个主题如果不存在，producer怎么知道给那个broker建立连接， 发送消息？ 

作者回复 producer会向任意一个broker请求元数据，因为所有broker都缓存了相同的集群元数据信 息 



1564666905 

问一下元数据具体指的是什么？存放在broker端的维护topic的信息的元数据么？ 

作者回复 嗯嗯，集群中所有主题信息、所有broker信息等 



1564620446 

老师，作为kafka的初学者来说。您建议使用kafka的原生API练习文中demo，还是使用被其 他框架整合的kafka练习？ 

作者回复 我建议直接使用kafka自己原生的api 



金小璐 1562601021 



tcp和http这部分的解释觉得有点怪。多路复用不是tcp协议栈特性，而且http也能做多路复用 （如spdy），不能算作选型原因。http是基于tcp协议之上的，我理解如果不需要http协议特 征，那选tcp一定比选http好，毕竟少了一层协议栈解析呀～ 



1562226255 

c 包在v1.0.0 已经对这个问题采取了对策。所有使用基础 c 包 librdkafka 都因此受益 

Sparse connections 

In previous releases librdkafka would maintain open connections to all brokers in the cluster and the bootstrap servers. 

With this release librdkafka now connects to a single bootstrap server to retrieve the full broker list, and then connects to the brokers it needs to communicate with: partition leaders, group coordinators, etc. 

For large scale deployments this greatly reduces the number of connections between clients and brokers, and avoids the repeated idle connection closes for unused connections. 

Sparse connections is on by default (recommended setting), the old behavior of connecting to all brokers in the cluster can be re-enabled by setting enable.sparse.connections=false. 

See Sparse connections in the manual for more information. 

Original issue librdkafka #825. 

为啥 java 目前还是这样或者一已经开始动手修改了？- -有点无法理解。 



1562217170 

老师，最近使用kakfa，报了个异常： 

Caused by: org.apache.kafka.common.KafkaException: Record batch for partition Notify-18 at offset 1803009 is invalid, cause: Record is corrupt (stored crc = 3092077514, computed crc = 2775748463) 

kafka的数据还会损坏，不是有校验吗？ 

作者回复 也可能是网络传输过程中出现的偶发情况，通常没有什么好的解决办法。。。 



## 1562146687 

老师您好，我在本地分别用1.x版本和2.x版本的生产者去测试，为什么结果和老师的不一样 呢。初始化KafkaProducer时，并没有与参数中设置的所有broker去建立连接，然后我sleep 十秒，让sender线程有机会多运行会。但是还是没有看到去连接所有的broker。只有当运行 到procuder.send时才会有Initialize connection日志输出，以及由于metadata的needUpdate 被更新成true，sender线程会开始有机会去更新metadata去连接broker（产生Initialize connection to node...for sending metadata request）。之前学习源码的时候，也只注意到 二个地方去连接broker（底层方法initiateConnect，更新metadata时建立连接以及发送数据 时判断待发送的node是否建立了连接）。老师是我哪里疏忽了吗，还是理解有问题。翻阅老 师的书上TCP管理这块貌似没有过多的讲解，求老师指导。。 

作者回复 我不知道您这边是怎么实验的，但是我这边的确会创建新的TCP连接~~ 



## 1562139637 

如果多个线程都使用一个KafkaProducer实例，缓冲器被填满的速度会变快。 老师看评论这句话不太理解，多个线程共用一个实例，没有再new新的实例，为什么缓冲器 很快填满，不是利用原有的实例的吗。 

作者回复 一个KafkaProducer实例会开辟一块内存缓冲区，如果多个线程共用这部分buffer，自然 buffer被填满的速度就会快很多。我是这个意思。。。。 



## 1562120957 

胡老师，我想请教一下。 

connections.max.idle.ms这个值如果设置为-1，按照您文章里所说，KafkaProducer会创建 bootstrap.servers中全部tcp连接，如果是1000个，那么就是说这1000个连接永远不会关闭 了？ 

作者回复 如果 broker端和clients端的connections.max.idle.ms都是-1，就真是永不关闭了。 



1562084385 

KafkaProducer 实例首次更新元数据信息之后，还会再次创建与集群中所有 Broker 的 TCP 连 接。 这里如果我有1000个broker，就建立1000个链接？ 为啥不是只根据元信息只建立有我 要发的主题的broke的链接？ 

作者回复 嗯嗯， 目前不是这样设计的。 



1562042717 

Producer应该只跟Controller节点更新元数据，以及相关的topic机器交互数据，而不应该跟 所有的机器创建连接；有个疑问，当 Producer 更新了集群的元数据信息之后，如果发现与某 些broker没有连接，就去创建，为什么会去创建跟producer无关的连接？ 

作者回复 有可能也会连接这些Broker的。Clients获取到集群元数据后知道了集群所有Broker的连接 信息。下次再次获取元数据时，它会选择一个负载最少的Broker进行连接。如果发现没有连接也会创 建Socket，但其实它并不需要向这个Broker发送任何消息。 



1562040007 

主要的资源浪费应该是在第一次获取元数据的时候创建所有的连接，应该是这个地方可以做 一些优化吧，可以做一个最小初始化数量，从元数据中随机获取配置的最少数据，然后进行 初始化。然后向Broker发送消息的时候在去判断，如果没有连接就创建连接，这样应该可以 折中一下吧。 



1562023615 

最近和同事梳理mq的中间件种类 提到使用Kafka还是rocketmq 同事强烈推荐使用rocketmq 其实我们的业务是可能有数据量较大情况的，之前不了解Rocketmq 老师能给些建议吗？ 

作者回复 这两款MQ的吞吐量都很大，都能满足你们的需求。RocketMQ是阿里研发的，国内支持上 要比Kafka好一些；Kafka比较成熟，社区也很完善，反正是各自有优缺点吧。至于网上那些比较的文 章，我觉得它们的缺点在实际场景中都能规避，不存在绝对的孰强孰劣。 



1628985229 

开放讨论的问题，是否可以设计一个Max connection 的参数比如3-5，如果因为各种网络原 因connection减少了就重连或者连接其他的broker，而不是一上来就链接所有的broker 



1624318400 

如果设置 Producer 端 connections.max.idle.ms 参数大于 0，则步骤 1 中创建的 TCP 连接会 被自动关闭？ 这里有点疑问？只会关闭第一次建立的tcp连接吗？通过元数据创建的tcp连接 不会被关闭嘛？ 

作者回复 任何tcp连接，只要一段时间没有请求流经其上，都会被关闭 



1622630549 

kafka Producer 启动后依次创建与这 1000 台 Broker 的 TCP 连接。但是producer短期内只与 其中3-5台通信，一段时间之后，大约有 995 个 TCP 连接又被强制关闭。这其中是否存在资源 的浪费？ 

是否是因为内网应用的长连接的资源损耗可以忽略？ 

作者回复 也不是了。总之，目前就是这样设计的，其实确实有一些可以优化的空间 



1619403286 

bootstrap.servers这个参数的有些歧义，本意是配置几个broker做备用就可以了，不是所有 的都配置上。这样的话，producer也就是和几个broker建立了链接，而不是所有。这样设计 应该也算合理。 



1618547734 

老师，kafka（v0.11.0.1）日志中有很多 

WARN Attempting to send response via channel for which there is no open connection, connection id 的告警，发现ip都来自同一个集群（以kafka为数据源进行60s poll一次）� 

这个现象持续很久了，是什么原因呢 

而且在这个现象前提下，有一天controller节点突然很多CLOSE_WAITE，打满了socket,然后 kafka就挂了，这个需要调整什么参数呢 

作者回复 增加下关闭连接的空闲时段阈值 



1616577897 

值得注意的是，在第二种方式中，TCP 连接是在 Broker 端被关闭的，但其实这个 TCP 连接的 发起方是客户端，因此在 TCP 看来，这属于被动关闭的场景，即 passive close。被动关闭的 后果就是会产生大量的 CLOSE_WAIT 连接，因此 Producer 端或 Client 端没有机会显式地观 测到此连接已被中断。 

请教下 Producer 或 Client 端没有机会显示关闭到此连接如何理解？如何这么说，在 netty 中 server 端关闭连接，client 端也检测不到？ 

# 作者回复 我的意思是以现有producer和consumer的代码实现，它hi无法观察到的 



1606305467 

老师，像一些大的 Kafka 集群，可能我们生产者只需要发送某个 Topic，是否可以增加这么一 个设计，让 Producer 只连接到目标 Topic 所在的 Broker 就可以了。 

作者回复 topic leader副本在哪些broker上本身就是不固定的 



1605364609 

请教一下大家，客户端send(test, data)后，报异常： 

java.util.concurrent.ExecutionException: 

org.apache.kafka.common.errors.TimeoutException: Topic test not present in metadata after 60000 ms 

服务端是允许自动创建topic的。这是什么原因导致的？ 

作者回复 通常都是因为无法连接Broker导致的。看下broker的连接信息是否是正确以及可连接的 



## 1599674056 

老师，假设此时 Producer 被多个线程共享，Metadata#update 方法是否会造成多次更新问 题？ 

我的理解是：update方法是线程安全的，在一条线程更新完元数据后，后面的线程会拿着他 们当前的元数据版本调用update，此时版本号不一样了，就会造成update方法内再次将 needUpdate设置为true。 

作者回复 producer被多个线程共享，但update不会被调用多次。 



## 1598164724 

老师你好，请问下broker收到消息后到然后consumer消费消息，中间发生了什么，broker如 何找到对应的consumer.的 

作者回复 broker不会找consumer。相反地，consumer会找broker的 



## 1597999722 

胡老师，对于“挑战“的问题：我觉得是不是默认建立所有与broker的连接再关闭空闲的， 会比需要时再一个个建立连接，权衡下来效益要高些，所以社区才会保留这种做法。 另外，我有一个不明白的点哈，建立连接需要先从bootstrap.servers获取信息（一般少许几 台就行），然后获得metadata。这样的话，producer端是不是知道了初次需要建立连接的 broker信息，直接建立需要的连接就行，没有必要建立所有连接吗？？还是说，默认场景和 这个场景是不同的啊？ 

作者回复 producer其实只和真正需要连接的broker建立长久的TCP连接 



这句话是什么意思？连接关了还是没关？ 什么是软件层面的长连接？ 

1597885951 

用户可以在 Producer 端设置 connections.max.idle.ms=-1 禁掉这种机制。一旦被设置成 -1，TCP 连接将成为永久长连接。当然这只是软件层面的“长连接”机制，由于 Kafka 创建的 这些 Socket 连接都开启了 keepalive，因此 keepalive 探活机制还是会遵守的。 

作者回复 只是想说明连接保活，Kafka通过这个参数在应用层面做了软件保活。还有一种保活机制是 通过TCP的keepalive机制。 



1597300754 

KafkaProducer对象创建的时候，会和参数 bootstrap.servers上的各个Broker建立TCP链接， 同时获取集群的元数据信息，之后跟新Producer端的元数据，并与集群中没有建立TCP连接的 Broker建立连接。也就是说无论 bootstrap.servers 指定几个Broker节点，Producer 都会和 集群上的所有节点建立连接，那么文章中提到的 不建议把集群中所有的 Broker 信息都配置到 bootstrap.servers 中，感觉这个建议意义不太大！ 

作者回复 嗯，如果集群有几百台机器，bootstrap.servers就太长了，这么想的话，这个建议还是有 可取之处吧：） 



## 1596757617 

所谓的多路复用请求，即 multiplexing request，是指将两个或多个数据流合并到底层单一物 理连接中的过程。TCP 的多路复用请求会在一条物理连接上创建若干个虚拟连接，每个虚拟 连接负责流转各自对应的数据流。其实严格来说，TCP 并不能多路复用，它只是提供可靠的 消息交付语义保证，比如自动重传丢失的报文 

io多路复用，是指一个线程监听多个socket连接，而不是一个连接建多个虚拟连接吧 

作者回复 嗯，你说的没错。另外，本文并未探讨IO多路复用。仅仅是讨论为什么使用TCP连接时谈到 了TCP的多路复用：） 



1594304274 

老师，你好，请教个问题，producer定时从某个broker获取元数据，当某个分区的leader发 生了切换，sender是怎么及使发现，和新broker建立连接的？ 

作者回复 它不会发现，而是通过broker告诉它的。producer向老leader所在的broker发送请求， broker返回异常告知producer：我已经不是leader了 



1592890413 

broker端通过prometheus监控到openfiles一直很高且降不下去，应该从哪个角度去找原因呢 

作者回复 你当前的open file limit设置了多少呢？ 另外你的Broker上的分区数大概是什么量级的。 Kafka的确会打开很多文件，而且是分区数越多open file handler占用越多，不过一般调大一点这个 都不会有太多问题 的 



1589442765 

请问更新元数据后,与某些broker没有连接,则创建一个tcp连接； 

比如3个broker没连接，则会创建3个连接？只是创建一个tcp连接多路复用。 

作者回复 不会复用一个tcp连接到多个broker上 



1589439121 

感觉那个功能好尴尬，获取所有broker并创建连接，然后又因为设置最大存活时间又断开连 接； 

感觉应该发送的时候创建连接，第一次可能会慢点。 

作者回复 大体上就是发送时候创建，只是获取元数据有时是先于发送事件的 



1587625288 

何时创建 TCP 连接？ 

KafkaProducer 实例创建时启动 Sender 线程，从而创建与 bootstrap.servers 中所有 Broker 的 TCP 连接。 

KafkaProducer 实例首次更新元数据信息之后，还会再次创建与集群中所有 Broker 的 TCP 连 

接。 如果 Producer 端发送消息到某台 Broker 时发现没有与该 Broker 的 TCP 连接，那么也会立 即创建连接。 

如果设置 Producer 端 connections.max.idle.ms 参数大于 0，则步骤 1 中创建的 TCP 连接会 被自动关闭；如果设置该参数 =-1，那么步骤 1 中创建的 TCP 连接将无法被关闭，从而成 为“僵尸”连接。 

何时关闭 TCP 连接？ 

Producer 端关闭 TCP 连接的方式有两种：一种是用户主动关闭；一种是 Kafka 自动关闭。 第一种。这里的主动关闭实际上是广义的主动关闭，甚至包括用户调用 kill -9 主动“杀 掉”Producer 应用。当然最推荐的方式还是调用 producer.close() 方法来关闭。 第二种是 Kafka 帮你关闭，这与 Producer 端参数 connections.max.idle.ms 的值有关。默认 情况下该参数值是 9 分钟，即如果在 9 分钟内没有任何请求“流过”某个 TCP 连接，那么 Kafka 会主动帮你把该 TCP 连接关闭。用户可以在 Producer 端设置 connections.max.idle.ms=-1 禁掉这种机制。一旦被设置成 -1，TCP 连接将成为永久长连 接。当然这只是软件层面的“长连接”机制，由于 Kafka 创建的这些 Socket 连接都开启了 keepalive，因此 keepalive 探活机制还是会遵守的。 



1586536361 

看了下最新版本的源码，貌似在初始化的时候，只是获取集群信息内ready状态的Leader节点 （存在未知leader的节点则会强制更新metadata信息）；然后与获取到的ready状态的Leader 节点们发送一次请求以此建立TCP连接。 

```java RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now); 

// if there are any partitions whose leaders are not known yet, force metadata update if (!result.unknownLeaderTopics.isEmpty()) { // The set of topics with unknown leader contains topics with leader election pending as well as // topics which may have expired. Add the topic again to metadata to ensure it is included // and request metadata update, since there are messages to send to the topic. for (String topic : result.unknownLeaderTopics) this.metadata.add(topic, now); 

log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}", result.unknownLeaderTopics); this.metadata.requestUpdate(); 

} 

// remove any nodes we aren't ready to send to Iterator<Node> iter = result.readyNodes.iterator(); ``` 

Kafka客户端版本：2.4.1 

是我理解的存在问题吗？ 

作者回复 你的理解没有问题：） 



1585053142 

作者你好，假如peoducer指定了connections.max.idle.ms = -1,bootStrap.servers指定了所有 broker地址信息（TCP连接的关闭和创建也是有开销的，因此很多时候我们想要禁掉自动关闭 机制）， 

假如boker数量是N，那么producer启动后创建2*N个TCP连接，不会被关闭，其中N个TCP连 接是有效的，N个TCP连接是无效的，不知道这种观点是否正确？还是和kafka版本有关？ 了社区的一些评论 

作者回复 也不算无效吧，只能说上面没有请求处理。版本的影响是存在的，不排除各个版本之间是 否有对连接做了什么修改，毕竟不算什么重大的功能改进。最好还是以实际试验结果为准 



1581394728 

对于为何采用TCP而不用Http的原因描述有点疑问：Http是应用层的协议，而tcp是传输控制 协议，即便使用http底层发送还是会用到tcp的不是吗？所以这个原因描述是否需要更正一 下。 

作者回复 Kafka不用http而选择tcp并非是因为tcp是传输层协议，http是应用层协议，因此我是这么 看的：我们说的都是对的，只是回应得不是一个层面上的问题：） 



1580082792 

connections.max.idle.ms 这个参数想确认一下, 是不是默认值为 600000 毫秒 

作者回复 Broker端和Client端都有这个参数。Broker端参数的默认值的确是10分钟，Client端的则不 是。 



1570335790 

我觉得在创建KafkaProducer时就通过Sender线程与某些Broker创建连接的主要目的是获取集 群元数据，不一定会和集群的所有broker创建连接。发送消息时，如果和leader节点没有连 接就会创建，发送完消息如果没有数据要发送，连接就会被关闭释放资源。 



1570259666 

Kafka强依赖zookeeper还是有很大风险的，之前公司就出现zookeeper被打挂的场景 



1570228905 

第二章有写到：生产者总是向领导者副本写消息；而消费者总是从领导者副本读消息。至于 追随者副本，它只做一件事：向领导者副本发送请求，请求领导者把最新生产的消息发给 它，这样它能保持与领导者的同步。如果bootstrap.servers 参数中没有设置领导者副本地 址，那么就得追随者副本同步数据到领导者副本？ 

作者回复 bootstrap.servers设置的是连接Kafka的broker信息，和副本没有关系啊 



1569803567 

Kafka自动关闭连接，被动关闭的后果就是会产生大量的 CLOSE_WAIT 连接，当发起端被确认 为空闲才会被被动关闭，理论数据传输量没有了，那为什么还有很多close wait？ 

作者回复 因为是被动关闭，所以才有CLOSE_WAIT，和是否有传输量关系不大 



1566829823 

针对第二种出现的大量无用链接，有什么好的解决办法吗？生产中发现，非java语音出现过这 种大量链接占满了broker机器的连接数 

作者回复 其他语言管理TCP资源的方式和Java版本的不一定相同。可以参考一下github官网，看看是 否已知的bug。另外也取决于是什么语言？ 



1566134809 

内容与2018年出版的那本kafka差不多。请教一个问题，线上环境（量比较大，每天的消息量 能达到1T）,连续性报“java.io.IOException: Connection to 2 was disconnected before the response was read”。我贴一下报错信息： 

2019-08-18 11:46:37,499] INFO [ReplicaFetcher replicaId=0, leaderId=2, fetcherId=2] Error sending fetch request (sessionId=88469530, epoch=INITIAL) to node 2: java.io.IOException: Connection to 2 was disconnected before the response was read. 

(org.apache.kafka.clients.FetchSessionHandler) 

......... t-0-logs-11=(offset=0, logStartOffset=0, maxBytes=1698029824), t0_pt_error-6= (offset=9999, logStartOffset=9999, maxBytes=1698029824), t0_pt_find_pid-5=(offset=4439, logStartOffset=4439, maxBytes=1698029824), t0_pt_extract_patient-15=(offset=18400, logStartOffset=18400, maxBytes=1698029824), __consumer_offsets-25=(offset=83, logStartOffset=0, maxBytes=1698029824), t0_qmjkda_extract_patient-9=(offset=877, logStartOffset=12, maxBytes=1698029824), t0_qmjkda_find_pid-5=(offset=5105, logStartOffset=5105, maxBytes=1698029824), t0_pt_find_pid-35=(offset=4321, logStartOffset=4321, maxBytes=1698029824), bigscreenRdData-18=(offset=1, logStartOffset=1, maxBytes=1698029824), t0_pt_etldr_mapping-4=(offset=16075, logStartOffset=16075, maxBytes=1698029824)}, isolationLevel=READ_UNCOMMITTED, toForget=, metadata=(sessionId=88469530, epoch=INITIAL)) 

(kafka.server.ReplicaFetcherThread) 

java.io.IOException: Connection to 2 was disconnected before the response was read at org.apache.kafka.clients.NetworkClientUtils.sendAndReceive(NetworkClientUtils.java:97) at 

kafka.server.ReplicaFetcherBlockingSend.sendRequest(ReplicaFetcherBlockingSend.scala:96 at kafka.server.ReplicaFetcherThread.fetch(ReplicaFetcherThread.scala:223) at kafka.server.ReplicaFetcherThread.fetch(ReplicaFetcherThread.scala:43) 

at 

kafka.server.AbstractFetcherThread.processFetchRequest(AbstractFetcherThread.scala:146) at kafka.server.AbstractFetcherThread.doWork(AbstractFetcherThread.scala:111) at kafka.utils.ShutdownableThread.run(ShutdownableThread.scala:82) 

作者回复 查看一下Broker id=2的broker进程是否启动吧，从日志上来说，follower无法连接到这个 broker上的leader了 



1564108277 

请问老师，除了Java库之外，其他语言的library，比如librdkafka的prodcuer管理TCP连接的 方式与Java库的方式是一样的吗？ 

作者回复 应该是不一样的 



1562687418 

“KafkaProducer 实例创建时启动 Sender 线程，从而创建与 bootstrap.servers 中所有 Broker 的 TCP 连接。”——我觉得这个完全可以弄成随机连接呀，如果一个连不通再随机连 另一个，不是任何一个broker都有元数据缓存吗，随机连一个取得元数据不就可以了，为什 么要全连呢？ 



1562162759 

老师、各位专家好，请问kafca适合做两个系统之间的转账处理吗？ 

另外，请问kafca的使用案例中，最多支持过什么数量级的消费者和生产者？ 



1562081996 

您好，老师，你说的第二种关闭producer的方法是一个被动关闭，发起方是客户端。 1. 这个客户端指的是producer客户端？所以broker此时会可能出现close wait，是这个意思 吗？ 

2. 还有这句“因此 Producer 端或 Client 端没有机会显式地.”也不是很理解，producer端和 client端不是一个意思吗？ 

作者回复 1. 是这个意思 

2. 我是想说consumer端也是这个原理。因此加上了clients 



