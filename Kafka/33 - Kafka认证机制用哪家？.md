2019-8-17 胡夕 



你好，我是胡夕。今天我要和你分享的主题是：Kafka的认证机制。 

所谓认证，又称“验证”“鉴权”，英文是authentication，是指通过一定的手段，完成对用 户身份的确认。认证的主要目的是确认当前声称为某种身份的用户确实是所声称的用户。 

在计算机领域，经常和认证搞混的一个术语就是授权，英文是authorization。授权一般是指 对信息安全或计算机安全相关的资源定义与授予相应的访问权限。 

举个简单的例子来区分下两者：认证要解决的是你要证明你是谁的问题，授权要解决的则是 你能做什么的问题。 

在Kafka中，认证和授权是两套独立的安全配置。我们今天主要讨论Kafka的认证机制，在专栏 的下一讲内容中，我们将讨论授权机制。 

自0.9.0.0版本开始，Kafka正式引入了认证机制，用于实现基础的安全用户认证，这是将Kafka 上云或进行多租户管理的必要步骤。截止到当前最新的2.3版本，Kafka支持基于SSL和基于 SASL的安全认证机制。 

~~基~~ 于SSL的认证主 ~~要~~ <mark>是</mark> ~~指~~ Broker和 ~~客户~~ 端的双 ~~路~~ 认证（2-way authentication）。通常来说， SSL加密（Encryption）已经启用了单向认证，即客户端认证Broker的证书（Certificate）。 如果要做SSL认证，那么我们要启用双路认证，也就是说Broker也要认证客户端的证书。 

对了，你可能会说，SSL不是已经过时了吗？现在都叫TLS（Transport Layer Security）了 吧？但是，Kafka的源码中依然是使用SSL而不是TLS来表示这类东西的。不过，今天出现的所 有SSL字眼，你都可以认为它们是和TLS等价的。 

Kafka还支持通过SASL做客户端认证。SASL <mark>是提</mark> 供认证和数 ~~据~~ 安全服务的框架。Kafka支持的 SASL机制有5种，它们分别是在不同版本中被引入的，你需要根据你自己使用的Kafka版本， 来选择该版本所支持的认证机制。 

1. GSSAPI：也就是Kerberos使用的安全接口，是在0.9版本中被引入的。 

2. PLAIN：是使用简单的用户名/密码认证的机制，在0.10版本中被引入。 

3. SCRAM：主要用于解决PLAIN机制安全问题的新机制，是在0.10.2版本中被引入的。 

4. OAUTHBEARER：是基于OAuth 2认证框架的新机制，在2.0版本中被引进。 

5. Delegation Token：补充现有SASL机制的轻量级认证机制，是在1.1.0版本被引入的。 

Kafka为我们提供了这么多种认证机制，在实际使用过程中，我们应该如何选择合适的认证框 架呢？下面我们就来比较一下。 

目前来看，使用SSL做信道加密的情况更多一些，但使用SSL实现认证不如使用SASL。毕竟， SASL能够支持你选择不同的实现机制，如GSSAPI、SCRAM、PLAIN等。因此，我的建议是你 ~~可~~ 以 ~~使用~~ SSL来做 ~~通信~~ 加密， ~~使用~~ SASL来做Kafka的认证实现。 

SASL下又细分了很多种认证机制，我们应该如何选择呢？ 

SASL/GSSAPI主要是给Kerberos使用的。如果你的公司已经做了Kerberos认证（比如使用 Active Directory），那么使用GSSAPI是最方便的了。因为你不需要额外地搭建Kerberos，只 要让你们的Kerberos管理员给每个Broker和要访问Kafka集群的操作系统用户申请principal就 

好了。总之，GSSAPI ~~适用~~ 于本 ~~身~~ 已经做了Kerberos认证的场 ~~<mark>景</mark>~~ ，这样的 ~~话~~ ，SASL/GSSAPI ~~可~~ 以实现无缝 ~~集~~ 成。 

而SASL/PLAIN，就像前面说到的，它是一个简单的用户名/密码认证机制，通常与SSL加密搭 配使用。注意，这里的PLAIN和PLAINTEXT是两回事。PLAIN在这 ~~里~~ <mark>是</mark> ~~一~~ 种认证机制，而 PLAINTEXT ~~说~~ 的 <mark>是</mark> 未 ~~使用~~ SSL时的 ~~明~~ 文传 ~~输~~ 。对于一些小公司而言，搭建公司级的Kerberos可 能并没有什么必要，他们的用户系统也不复杂，特别是访问Kafka集群的用户可能不是很多。 对于SASL/PLAIN而言，这就是一个非常合适的应用场景。 ~~总~~ 体来 ~~说~~ ，SASL/PLAIN的 ~~配置~~ 和运 ~~维~~ 成本 ~~相~~ 对较小， ~~适合~~ 于小型公 ~~司中~~ 的Kafka ~~集群~~ 。 

但是，SASL/PLAIN有这样一个弊端：它不能动态地增减认证用户，你必须重启Kafka集群才能 令变更生效。为什么呢？这是因为所有认证用户信息全部保存在静态文件中，所以只能重启 Broker，才能重新加载变更后的静态文件。 

我们知道，重启集群在很多场景下都是令人不爽的，即使是轮替式升级（Rolling Upgrade）。SASL/SCRAM就解决了这样的问题。它通过将认证用户信息保存在ZooKeeper的 方式，避免了动态修改需要重启Broker的弊端。在实际使用过程中，你可以使用Kafka提供的 命令动态地创建和删除用户，无需重启整个集群。因此，如 ~~果~~ 你打 ~~算使用~~ SASL/PLAIN，不妨 改 ~~用~~ SASL/SCRAM试试。不过 ~~要~~ 注 <mark>意</mark> 的 <mark>是</mark> ， ~~后者~~ <mark>是</mark> 0.10.2版本引入的。你至少 ~~要~~ 升级到这个版 本 ~~后~~ 才 ~~能使用~~ 。 

SASL/OAUTHBEARER是2.0版本引入的新认证机制，主要是为了实现与OAuth 2框架的集成。 OAuth是一个开发标准，允许用户授权第三方应用访问该用户在某网站上的资源，而无需将用 户名和密码提供给第三方应用。Kafka不提倡单纯使用OAUTHBEARER，因为它生成的不安全 的JSON Web Token，必须配以SSL加密才能用在生产环境中。当然，鉴于它是2.0版本才推出 来的，而且目前没有太多的实际使用案例，我们可以先观望一段时间，再酌情将其应用于生 产环境中。 

Delegation Token是在1.1版本引入的，它是一种轻量级的认证机制，主要目的是补充现有的 SASL或SSL认证。如果要使用Delegation Token，你需要先配置好SASL认证，然后再利用 Kafka提供的API去获取对应的Delegation Token。这样，Broker和客户端在做认证的时候，可 以直接使用这个token，不用每次都去KDC获取对应的ticket（Kerberos认证）或传输Keystore 文件（SSL认证）。 

为了方便你更好地理解和记忆，我把这些认证机制汇总在下面的表格里了。你可以对照着表 格，进行一下区分。 



接下来，我给出SASL/SCRAM的一个配置实例，来说明一下如何在Kafka集群中开启认证。其 他认证机制的设置方法也是类似的，比如它们都涉及认证用户的创建、Broker端以及Client端 特定参数的配置等。 

我的测试环境是本地Mac上的两个Broker组成的Kafka集群，连接端口分别是9092和9093。 

配置SASL/SCRAM的第一步，是创建能否连接Kafka集群的用户。在本次测试中，我会创建3个 用户，分别是admin用户、writer用户和reader用户。admin用户用于实现Broker间通信， writer用户用于生产消息，reader用户用于消费消息。 

我们使用下面这3条命令，分别来创建它们。 

> $ cd kafka_2.12-2.3.0/ 

> $ bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[passwo Completed Updating config for entity: user-principal 'admin'. 

$ bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[passwo Completed Updating config for entity: user-principal 'writer'. 

$ bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[passwo Completed Updating config for entity: user-principal 'reader'. 

在专栏前面，我们提到过，kafka-configs脚本是用来设置主题级别参数的。其实，它的功能还 有很多。比如在这个例子中，我们使用它来创建SASL/SCRAM认证中的用户信息。我们可以使 用下列命令来查看刚才创建的用户数据。 

$ bin/kafka-configs.sh --zookeeper localhost:2181 --describe --entity-type users  --entity-na Configs for user-principal 'writer' are SCRAM-SHA-512=salt=MWt6OGplZHF6YnF5bmEyam9jamRwdWlqZW 

这段命令包含了writer用户加密算法SCRAM-SHA-256以及SCRAM-SHA-512对应的盐值(Salt)、 ServerKey和StoreKey。这些都是SCRAM机制的术语，我们不需要了解它们的含义，因为它们 并不影响我们接下来的配置。 

配置了用户之后，我们需要为每个Broker创建一个对应的JAAS文件。因为本例中的两个 Broker实例是在一台机器上，所以我只创建了一份JAAS文件。但是你要切记，在实际场景 中，你需要为每台单独的物理Broker机器都创建一份JAAS文件。 

JAAS的文件内容如下： 

KafkaServer { 

org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin"; }; 

关于这个文件内容，你需要注意以下两点： 

不要忘记最后一行和倒数第二行结尾处的分号； 

JAAS文件中不需要任何空格键。 

这里，我们使用admin用户实现Broker之间的通信。接下来，我们来配置Broker的 server.properties文件，下面这些内容，是需要单独配置的： 

sasl.enabled.mechanisms=SCRAM-SHA-256 

sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256 

security.inter.broker.protocol=SASL_PLAINTEXT 

listeners=SASL_PLAINTEXT://localhost:9092 

第1项内容表明开启SCRAM认证机制，并启用SHA-256算法；第2项的意思是为Broker间通信 也开启SCRAM认证，同样使用SHA-256算法；第3项表示Broker间通信不配置SSL，本例中我 们不演示SSL的配置；最后1项是设置listeners使用SASL_PLAINTEXT，依然是不使用SSL。 

另一台Broker的配置基本和它类似，只是要使用不同的端口，在这个例子中，端口是9093。 

现在我们分别启动这两个Broker。在启动时，你需要指定JAAS文件的位置，如下所示： 

$KAFKA_OPTS=-Djava.security.auth.login.config=<your_path>/kafka-broker.jaas bin/kafka-server ...... 

[2019-07-02 13:30:34,822] INFO Kafka commitId: fc1aaa116b661c8a (org.apache.kafka.common.util [2019-07-02 13:30:34,822] INFO Kafka startTimeMs: 1562045434820 (org.apache.kafka.common.util [2019-07-02 13:30:34,823] INFO [KafkaServer id=0] started (kafka.server.KafkaServer) 

$KAFKA_OPTS=-Djava.security.auth.login.config=<your_path>/kafka-broker.jaas bin/kafka-server ...... 

[2019-07-02 13:32:31,976] INFO Kafka commitId: fc1aaa116b661c8a (org.apache.kafka.common.util [2019-07-02 13:32:31,976] INFO Kafka startTimeMs: 1562045551973 (org.apache.kafka.common.util [2019-07-02 13:32:31,978] INFO [KafkaServer id=1] started (kafka.server.KafkaServer) 

此时，两台Broker都已经成功启动了。 

在创建好测试主题之后，我们使用kafka-console-producer脚本来尝试发送消息。由于启用了 认证，客户端需要做一些相应的配置。我们创建一个名为producer.conf的配置文件，内容如 下： 

security.protocol=SASL_PLAINTEXT 

sasl.mechanism=SCRAM-SHA-256 

sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="w 

之后运行Console Producer程序： 

$ bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093 --topic test  --p >hello, world 

> 

可以看到，Console Producer程序发送消息成功。 

接下来，我们使用Console Consumer程序来消费一下刚刚生产的消息。同样地，我们需要为 kafka-console-consumer脚本创建一个名为consumer.conf的脚本，内容如下： 

security.protocol=SASL_PLAINTEXT sasl.mechanism=SCRAM-SHA-256 

sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=" 

# 之后运行Console Consumer程序： 

$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092,localhost:9093 --topic test hello, world 

很显然，我们是可以正常消费的。 

最后，我们来演示SASL/SCRAM动态增减用户的场景。假设我删除了writer用户，同时又添加 了一个新用户：new_writer，那么，我们需要执行的命令如下： 

- $ bin/kafka-configs.sh --zookeeper localhost:2181 --alter --delete-config 'SCRAM-SHA-256' --e Completed Updating config for entity: user-principal 'writer'. 

- $ bin/kafka-configs.sh --zookeeper localhost:2181 --alter --delete-config 'SCRAM-SHA-512' --e Completed Updating config for entity: user-principal 'writer'. 

- $ bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config 'SCRAM-SHA-256=[iterat Completed Updating config for entity: user-principal 'new_writer'. 

现在，我们依然使用刚才的producer.conf来验证，以确认Console Producer程序不能发送消 息。 

$ bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093 --topic test  --p >[2019-07-02 13:54:29,695] ERROR [Producer clientId=console-producer] Connection to node -1 

很显然，此时Console Producer已经不能发送消息了。因为它使用的producer.conf文件指定 的是已经被删除的writer用户。如果我们修改producer.conf的内容，改为指定新创建的 new_writer用户，结果如下： 

$ bin/kafka-console-producer.sh --broker-list localhost:9092,localhost:9093 --topic test  --p >Good! 

现在，Console Producer可以正常发送消息了。 

这个过程完整地展示了SASL/SCRAM是如何在不重启Broker的情况下增减用户的。 

至此，SASL/SCRAM配置就完成了。在专栏下一讲中，我会详细介绍一下如何赋予writer和 reader用户不同的权限。 

好了，我们来小结一下。今天，我们讨论了Kafka目前提供的几种认证机制，我给出了它们各 自的优劣势以及推荐使用建议。其实，在真实的使用场景中，认证和授权往往是结合在一起 使用的。在专栏下一讲中，我会详细向你介绍Kafka的授权机制，即ACL机制，敬请期待。 



请谈一谈你的Kafka集群上的用户认证机制，并分享一个你遇到过的“坑”。 

欢迎写下你的思考和答案，我们一起讨论。如果你觉得有所收获，也欢迎把文章分享给你的 朋友。 

© 版权归极客邦科技所有，未经许可不得传播售卖。 页面已增加防盗追踪，如有侵权极客邦将依法追究其法律责任。 

上一篇 32 | KafkaAdminClient：Kafka的运维利器 

下一篇 34 | 云环境下的授权该怎么做？ 



1566205890 

老师，对于多个消费者，每个消费者分配的消息数量一样，每个消费者消费完的数据最快和 最慢的大概有3s的差距，出现这个消费快慢差距会有哪些原因呢 

作者回复 如果你确定是3s的差距，看看是不是这个consumer参数导致的： 

group.initial.rebalance.delay.ms。当前这个参数值默认是3秒。 



1566007037 

老师说的这SCRAM认证用户名和密码直接保存在zookeeper上的，如果zookeeper不做安全控 制，岂不是失去意义了？目前我们没有做认证的，研究过一段时间的ssl认证，很麻烦，还影 响性能 

作者回复 不是明文保存的。当然做ZooKeeper的认证也是很有必要的 



1577782163 

老师，请问再java代码里怎么使用认证？比如 producer，是配置好了 conf 文件，然后传入参 数吗？ 

Properties props = new Properties(); props.put("producer.config", "<your_path>/producer.conf"); Producer<String, String> producer = new KafkaProducer<>(props) 类似这样可以吗？ 

作者回复 可以使用这样的方式： consumerProperties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"reader2\" password=\"reader-pwd\";"); 



1574093046 

还没做过，后续应该会做 



1566037336 

No JAAS configuration section named 'Client' was found in specified JAAS configuration file: '/usr/local/kafka/config/kafka-broker.jaas'. Will continue connection to Zookeeper server without SASL authentication, if Zookeeper server allows it. 



1604307041 

kafka broker jaas 文件中admin 用户明文密码，如果别人能看到这个文件，相当于有了管理 员的权限，安全性存在很大的风险，这块怎么考虑的 

作者回复 就可以考虑使用SASL/Kerberos的认证方式 



1594022691 

老师，我这边认证过后，就可以使用producer 使用 writer 去发送消息了，那是不是相当于也 是给 writer授权了呀（发送消息的权限） 

作者回复 这篇文章没有配置ACL，因此不存在授权的问题，只有认证的问题：） 



打卡，中间学习有断档，感觉模式了，学习还是得持续+专注。 

1569197860 



1566223137 

老师你好，我们生产上kafka总是发生leader切换，频率大概和zk fsync的告警日志一致，请问 有经验吗？zk隔一段时间会有个fsync慢的告警日志，然后差不多同一个时间点，收到 partition leader切换的告警 

作者回复 查看一下磁盘的性能，以及可以查一下文件系统的vm.dirty_ratio，看看是不是flush频率过 高导致的 



1582039279 

请问下老师，认证后java代码如何访问？ 

作者回复 和之前类似，只不过你需要配置相应的Clients端参数，如sasl.jaas.config（如果用了 SASL），security.protocol，sasl.mechanism（如果用了SASL） 



## 1576839352 

有没有 哪个大佬 使用 SCRAM-SHA-512 的Python 消费客户端实践？ 这边实现了好几个版本，总是失败。 



## 1571796560 

老师，您讲的上面的例子中，reader和writer用户只是做了认证，没有做授权，它们默认的权 限是什么呢？如果不授权就能收发消息么？ 

作者回复 默认没有任何权限。不授权不能收发消息 



## 1566186521 

（1）之前做kafak/ Sasl-Plain认证，几经转折才发现，这个认证用户跟linux用户名没关系， 而且不能动态添加减少用户，最重要的是租户可以自己修改acl权限，目前也只是把客户端的 kafka-topics.sh给禁用了，一叶障目吧，=。=； 

（2）还有就是sasl-plain这个acl权限感觉肯定，明明给认证用户a赋予了所有topic的在所有 

host的读写权限，但重启时发现有部分topic突然无法消费写入了，提示没权限，再重启就好 了； 

- （3）接（2）情况，还有就是用kafka-acls.sh去查看topic的所有acl权限时，有的acl完全为 空，但是用户a还能写入消费数据，这块完全不懂 

（4）目前kafa-acls.sh 只是用的基础的 Write和Read权限，像Cluster这个权限不知道干啥用 的，其他的了解也不深入 

（5）最后就是做kafka sasl plain 认证的时候给zk也加了认证，具体如下： zkserver.sh加入这个 

"-Djava.security.auth.login.config=/opt/beh/core/zookeeper/conf/kafka_zoo_jaas.conf" \ zoo.cfg加入这个： 

authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider requireClientAuthScheme=sasl 

jaasLoginRenew=3600000 

但是有点疑惑的就是不知道zk 这个认证是用在那块的？我发现加不加kafka sasl plain都能正 常用 



1614765959 

老师，配置成功启动，出现Connection to node 1(kafka01/192.168.100.101:9092) failed authentication due to : Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256 

(org.apache.kafka.clients.NetworkClient) 这个报错。这是什么原因呢？ 

作者回复 还是认证配置错误了，看看是否和专栏中的一样 



## 1614593787 

基于 SSL 的认证主要是指 Broker 和客户端的双路认证（2-way authentication）。通常来 说，SSL 加密（Encryption）已经启用了单向认证，即客户端认证 Broker 的证书 （Certificate）。 

这里不是很理解。何谓： SSL 已经启用了单向认证？ 

作者回复 就是ssl默认情况下，broker不会验证client端的证书 



1614587182 

老师的测试中 SCRAM-SHA-256 以及 SCRAM-SHA-512 两个算法都用到了，其实使用其中之一 是不是就足够了 

作者回复 对，用一个就行 



1608871790 

是不是用户信息只能建到zookeeper节点上？ 

作者回复 不是，可以使用外部的认证框架，比如kerberos 



1592450398 

本低环境用apache kafka配置很简单，但是用cdh反而搞不定，一直说 security.inter.broker.protocol can not be set to SASL_PLAINTEXT, as Kerberos is not enabled on this Kafka broker。求帮助。 

作者回复 调整下这个broker参数的值，security.inter.broker.protocol 



1587977452 

动态增减用户，是否可以使用java api编码调用的方式？ 

作者回复 目前不支持~ 



## 1575779888 

做了认证后，使用 bin/kafka-topics.sh --create --bootstrap-server localhost:9092 -- replication-factor 3 --partitions 3 --topic test 创建主题失败 

提示错误： 

[2019-12-08 12:20:39,172] INFO [SocketServer brokerId=0] Failed authentication with 

/127.0.0.1 (Unexpected Kafka request of type METADATA during SASL handshake.) (org.apache.kafka.common.network.Selector) 

[2019-12-08 12:20:39,587] INFO [SocketServer brokerId=0] Failed authentication with /127.0.0.1 (Unexpected Kafka request of type METADATA during SASL handshake.) (org.apache.kafka.common.network.Selector) [2019-12-08 12:20:39,998] INFO [SocketServer brokerId=0] Failed authentication with /127.0.0.1 (Unexpected Kafka request of type METADATA during SASL handshake.) (org.apache.kafka.common.network.Selector) 

作者回复 什么版本的Kafka呢？另外是否设置了security.protocol=sasl_plaintext 



1574733447 

这个应该用的不多吧 



1571884403 

顺序是Broker是停止的，然后修改配置文件，创建用户，之后启动Broker。 

我是单台环境 2.2.0 

Broker server.properties 除了下面的其他都是默认配置 

listeners=SASL_PLAINTEXT://172.16.247.100:9092 sasl.enabled.mechanisms=SCRAM-SHA-256 sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256 security.inter.broker.protocol=SASL_PLAINTEXT 

# 这里我使用的是sasl.jaas.config配置形式，而不是kafka_server_jaas.conf形式 # 官网中http://kafka.apache.org/documentation/#security_jaas_broker支持这种配置方式 listener.name.sasl_plaintext.scram-sha- 

256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \ username="admin" \ password="admin-secret"; 

创建admin账号 

./kafka-configs.sh --zookeeper localhost:2181 --alter --add-config \ 

'SCRAM-SHA-256=[password=admin-secret],SCRAM-SHA-512=[password=admin-secret]' \ --entity-type users --entity-name admin 

创建完我再ZK中的 config\users节点可以看到这个用户. 

启动后的server.log，日志里kafka成功注册到zk节点 [2019-10-24 10:25:52,210] INFO Registered broker 0 at path /brokers/ids/0 with addresses: ArrayBuffer(EndPoint(172.16.247.100,9092,ListenerName(SASL_PLAINTEXT),SASL_PLAINTEXT czxid (broker epoch): 148 (kafka.zk.KafkaZkClient) [2019-10-24 10:25:52,462] INFO [KafkaServer id=0] started (kafka.server.KafkaServer) [2019-10-24 10:25:52,582] INFO [SocketServer brokerId=0] Failed authentication with /172.16.247.100 (Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256) (org.apache.kafka.common.network.Selector) [2019-10-24 10:25:52,583] INFO [Controller id=0, targetBrokerId=0] Failed authentication with srv01.contoso.com/172.16.247.100 (Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256) (org.apache.kafka.common.network.Selector) [2019-10-24 10:25:52,584] ERROR [Controller id=0, targetBrokerId=0] Connection to node 0 (srv01.contoso.com/172.16.247.100:9092) failed authentication due to: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA256 (org.apache.kafka.clients.NetworkClient) 

作者回复 我觉得你可以试一下jaas文件的方式，看看整个流程能否走通。如果可以，说明还是参数指 定jaas设置的问题。总之先窄化可能的问题点 



## 1571813646 

老师我按上面的方式配置，Kafka起来了，但是日志全是错误 [2019-10-23 14:46:39,465] ERROR [Controller id=0, targetBrokerId=0] Connection to node 0 (srv01.contoso.com/172.16.247.100:9092) failed authentication due to: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA256 (org.apache.kafka.clients.NetworkClient) [2019-10-23 14:46:39,578] INFO [SocketServer brokerId=0] Failed authentication with /172.16.247.100 (Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-256) (org.apache.kafka.common.network.Selector) [2019-10-23 14:46:39,578] INFO [Controller id=0, targetBrokerId=0] Failed authentication with srv01.contoso.com/172.16.247.100 (Authentication failed during authentication due 

to invalid credentials with SASL mechanism SCRAM-SHA-256) (org.apache.kafka.common.network.Selector) 

作者回复 能把配置的命令发下吗？ 



1566982330 

老师求助，，win10环境 执行命令： 

.\bin\windows\kafka-configs.bat --zookeeper localhost:2181 --alter --add-config 'SCRAMSHA-256=[iterations=8192,password=admin],SCRAM-SHA-512=[password=admin]' --ent ity-type users --entity-name admin 报错如下： 

requirement failed: Unknown Dynamic Configuration: Set('SCRAM-SHA-256). 网上搜了很久，没有找到解决方案，，请老师解惑。感谢 

作者回复 似乎是个已知问题，最好不要在Windows平台上跑Kafka，问题超多而且还无法解决 



1566556613 

老胡，看到你的博客园了，什么时候把你的博客地址全分享出来，让大家学习下呗 



1566172297 

胡老师，kafka平滑升级后面会讲吗？ 

作者回复 可能涉及不是很多。有什么问题只管问吧 

