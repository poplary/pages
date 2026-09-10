2019-8-27 胡夕 



你好，我是胡夕。今天我要和你分享的主题是：那些主流的Kafka监控框架。 

在上一讲中，我们重点讨论了如何监控Kafka集群，主要是侧重于讨论监控原理和监控方法。 今天，我们来聊聊具体的监控工具或监控框架。 

令人有些遗憾的是，Kafka社区似乎一直没有在监控框架方面投入太多的精力。目前，Kafka的 新功能提议已超过500个，但没有一个提议是有关监控框架的。当然，Kafka的确提供了超多 的JMX指标，只是，单独查看这些JMX指标往往不是很方便，我们还是要依赖于框架统一地提 供性能监控。 

也许，正是由于社区的这种“不作为”，很多公司和个人都自行着手开发Kafka监控框架，其 中并不乏佼佼者。今天我们就来全面地梳理一下主流的监控框架。 

首先，我向你推荐JMXTool工具。严格来说，它并不是一个框架，只是社区自带的一个工具罢 了。JMXTool工具能够实时查看Kafka JMX指标。倘若你一时找不到合适的框架来做监控， JMXTool可以帮你“临时救急”一下。 

Kafka官网没有JMXTool的任何介绍，你需要运行下面的命令，来获取它的使用方法的完整介 绍。 

bin/kafka-run-class.sh kafka.tools.JmxTool 

JMXTool工具提供了很多参数，但你不必完全了解所有的参数。我把主要的参数说明列在了下 面的表格里，你至少要了解一下这些参数的含义。 



现在，我举一个实际的例子来说明一下如何运行这个命令。 

假设你要查询Broker端每秒入站的流量，即所谓的JMX指标BytesInPerSec，这个JMX指标能帮 助你查看Broker端的入站流量负载，如果你发现这个值已经接近了你的网络带宽，这就说明 该Broker的入站负载过大。你需要降低该Broker的负载，或者将一部分负载转移到其他 Broker上。 

下面这条命令，表示每5秒查询一次过去1分钟的BytesInPerSec均值。 

bin/kafka-run-class.sh kafka.tools.JmxTool --object-name kafka.server:type=BrokerTopicMetrics 

在这条命令中，有几点需要你注意一下。 

设置 --jmx-url参数的值时，需要指定JMX端口。在这个例子中，端口是9997，在实际操作 中，你需要指定你的环境中的端口。 

由于我是直接在Broker端运行的命令，因此就把主机名忽略掉了。如果你是在其他机器上 运行这条命令，你要记得带上要连接的主机名。 

关于 --object-name参数值的完整写法，我们可以直接在Kafka官网上查询。我们在前面说 过，Kafka提供了超多的JMX指标，你需要去官网学习一下它们的用法。我以 

ActiveController JMX指标为例，介绍一下学习的方法。你可以在官网上搜索关键词 ActiveController，找到它对应的 --object-name，即 

kafka.controller:type=KafkaController,name=ActiveControllerCount，这样，你就可以执 行下面的脚本，来查看当前激活的Controller数量。 

$ bin/kafka-run-class.sh kafka.tools.JmxTool --object-name kafka.controller:type=KafkaControl Trying to connect to JMX url: service:jmx:rmi:///jndi/rmi://:9997/jmxrmi. "time","kafka.controller:type=KafkaController,name=ActiveControllerCount:Value" 2019-08-05 15:08:30,1 2019-08-05 15:08:31,1 

总体来说，JMXTool是社区自带的一个小工具，对于一般简单的监控场景，它还能应付，但是 它毕竟功能有限，复杂的监控整体解决方案，还是要依靠监控框架。 

说起Kafka监控框架，最有名气的当属Kafka Manager了。Kafka Manager是雅虎公司于2015 年开源的一个Kafka监控框架。这个框架用Scala语言开发而成，主要用于管理和监控Kafka集 群。 

应该说Kafka Manager是目前众多Kafka监控工具中最好的一个，无论是界面展示内容的丰富 程度，还是监控功能的齐全性，它都是首屈一指的。不过，目前该框架已经有4个月没有更新 

了，而且它的活跃的代码维护者只有三四个人，因此，很多Bug或问题都不能及时得到修复， 更重要的是，它无法追上Apache Kafka版本的更迭速度。 

当前，Kafka Manager最新版是2.0.0.2。在其Github官网上下载tar.gz包之后，我们执行解压 缩，可以得到kafka-manager-2.0.0.2目录。 

之后，我们需要运行sbt工具来编译Kafka Manager。sbt是专门用于构建Scala项目的编译构建 工具，类似于我们熟知的Maven和Gradle。Kafka Manager自带了sbt命令，我们直接运行它 构建项目就可以了： 

./sbt clean dist 

经过漫长的等待之后，你应该可以看到项目已经被成功构建了。你可以在Kafka Manager的 target/universal目录下找到生成的zip文件，把它解压，然后修改里面的 conf/application.conf文件中的kafka-manager.zkhosts项，让它指向你环境中的ZooKeeper地 址，比如： 

kafka-manager.zkhosts="localhost:2181" 

之后，运行以下命令启动Kafka Manager： 

bin/kafka-manager -Dconfig.file=conf/application.conf -Dhttp.port=8080 

该命令指定了要读取的配置文件以及要启动的监听端口。现在，我们打开浏览器，输入对应 的IP:8080，就可以访问Kafka Manager了。下面这张图展示了我在Kafka Manager中添加集群 的主界面。 



注意，要勾选上Enable JMX Polling，这样你才能监控Kafka的各种JMX指标。下图就是Kafka Manager框架的主界面。 



从这张图中，我们可以发现，Kafka Manager清晰地列出了当前监控的Kafka集群的主题数 量、Broker数量等信息。你可以点击顶部菜单栏的各个条目去探索其他功能。 

除了丰富的监控功能之外，Kafka Manager还提供了很多运维管理操作，比如执行主题的创 建、Preferred Leader选举等。在生产环境中，这可能是一把双刃剑，毕竟这意味着每个访问 Kafka Manager的人都能执行这些运维操作。这显然是不能被允许的。因此，很多Kafka Manager用户都有这样一个诉求：把Kafka Manager变成一个纯监控框架，关闭非必要的管理 功能。 

庆幸的是，Kafka Manager提供了这样的功能。你 ~~可~~ 以修改config下的application.conf文件， 删除application.features ~~中~~ 的 ~~值~~ 。比如，如果我想禁掉Preferred Leader选举功能，那么我就 可以删除对应KMPreferredReplicaElectionFeature项。删除完之后，我们重启Kafka Manager，再次进入到主界面，我们就可以发现之前的Preferred Leader Election菜单项已经 没有了。 



总之，作为一款非常强大的Kafka开源监控框架，Kafka Manager提供了丰富的实时监控指标 以及适当的管理功能，非常适合一般的Kafka集群监控，值得你一试。 

我要介绍的第二个Kafka开源监控框架是Burrow。Burrow <mark>是</mark> LinkedIn开 ~~源~~ 的 ~~一~~ 个专门监控 ~~消 费者~~ 进 ~~度~~ 的框架。事实上，当初其开源时，我对它还是挺期待的。毕竟是LinkedIn公司开源的 一个框架，而LinkedIn公司又是Kafka创建并发展壮大的地方。Burrow应该是有机会成长为很 好的Kafka监控框架的。 

然而令人遗憾的是，它后劲不足，发展非常缓慢，目前已经有几个月没有更新了。而且这个 框架是用Go写的，安装时要求必须有Go运行环境，所以，Burrow在普及率上不如其他框架。 另外，Burrow没有UI界面，只是开放了一些HTTP Endpoint，这对于“想偷懒”的运维来 说，更是一个减分项。 

如果你要安装Burrow，必须要先安装Golang语言环境，然后依次运行下列命令去安装 Burrow： 

$ go get github.com/linkedin/Burrow 

$ cd $GOPATH/src/github.com/linkedin/Burrow 

- $ dep ensure 

- $ go install 

等一切准备就绪，执行Burrow启动命令就可以了。 

$GOPATH/bin/Burrow --config-dir /path/containing/config 

总体来说，Burrow目前提供的功能还十分有限，普及率和知名度都是比较低的。不过，它的 好处是，该项目的主要贡献者是LinkedIn团队维护Kafka集群的主要负责人，所以质量是很有 保证的。如果你恰好非常熟悉Go语言生态，那么不妨试用一下Burrow。 

除了刚刚说到的专属开源Kafka监控框架之外，其实现在更流行的做法是，在 ~~一~~ 套 ~~通用~~ 的监控 框架 ~~中~~ 监控Kafka，比如使用JMXTrans + InfluxDB + Grafana的 ~~组合~~ 。由于Grafana支持对JMX ~~指标~~ 的监控，因此很容易将Kafka各种JMX指标集成进来。 

我们来看一张生产环境中的监控截图。图中集中了很多监控指标，比如CPU使用率、GC收集 数据、内存使用情况等。除此之外，这个仪表盘面板还囊括了很多关键的Kafka JMX指标，比 如BytesIn、BytesOut和每秒消息数等。将这么多数据统一集成进一个面板上直观地呈现出 来，是这套框架非常鲜明的特点。 



与Kafka Manager相比，这套监控框架的优势在于，你可以在一套监控框架中同时监控企业的 多个关键技术组件。特别是对于那 ~~<u>些</u>~~ 已经 ~~搭建~~ 了该监控 ~~组合~~ 的企业来 ~~说~~ ， ~~直~~ 接 ~~复用~~ 这套框架 ~~可~~ 以极大地节 ~~省~~ <u>运</u> ~~维~~ 成本，不失为 ~~一~~ 个好的选择。 

最后，我们来说说Confluent公司发布的Control Center。这是目前已知的最强大的Kafka监控 框架了。 



<!-- Start of picture text -->
Control Center不 但能 够实时地监控Kafka 集群 ，而 且 还 能 够帮 助 你 操 作和 搭建基 于Kafka的实<br>时流处 理 应 用 。 更 棒的 是 ，Control Center 提 供了统 一 式的主题 管理 功 能 。你 可 以在这 里享 受<br>到Kafka主题和Schema的 一站 式 管理 服务。<br><!-- End of picture text -->

下面这张图展示了Control Center的主题管理主界面。从这张图中，我们可以直观地观测到整 个Kafka集群的主题数量、ISR副本数量、各个主题对应的TPS等数据。当然，Control Center 提供的功能远不止这些，你能想到的所有Kafka运维管理和监控功能，Control Center几乎都 能提供。 



不过，如果你要使用Control Center，就必须使用Confluent Kafka Platform企业版。换句话 说，Control Center不是免费的，你需要付费才能使用。如果你需要一套很强大的监控框架， 你可以登录Confluent公司官网，去订购这套真正意义上的企业级Kafka监控框架。 

其实，除了今天我介绍的Kafka Manager、Burrow、Grafana和Control Center之外，市面上 还散落着很多开源的Kafka监控框架，比如Kafka Monitor、Kafka Offset Monitor等。不过， 这些框架基本上已经停止更新了，有的框架甚至好几年都没有人维护了，因此我就不详细展 开了。如果你是一名开源爱好者，可以试着到开源社区中贡献代码，帮助它们重新焕发活 力。 

值得一提的是，国内最近有个Kafka Eagle框架非常不错。它是国人维护的，而且目前还在积 极地演进着。根据Kafka Eagle官网的描述，它支持最新的Kafka 2.x版本，除了提供常规的监 控功能之外，还开放了告警功能（Alert），非常值得一试。 

总之，每个框架都有自己的特点和价值。Kafka Manager框架适用于基本的Kafka监控， Grafana+InfluxDB+JMXTrans的组合适用于已经具有较成熟框架的企业。对于其他的几个监控 框架，你可以把它们作为这两个方案的补充，加入到你的监控解决方案中。 



如果想知道某台Broker上是否存在请求积压，我们应该监控哪个JMX指标？ 

欢迎写下你的思考和答案，我们一起讨论。如果你觉得有所收获，也欢迎把文章分享给你的 朋友。 

© 版权归极客邦科技所有，未经许可不得传播售卖。 页面已增加防盗追踪，如有侵权极客邦将依法追究其法律责任。 

上一篇 36 | 你应该怎么监控Kafka？ 

下一篇 38 | 调优Kafka，你做到了吗？ 



1566889578 

感觉Grafana+InfluxDB这一套，可以用于任何语言，还可以自定义接口出来加入监控。 



1613358712 

https://github.com/didi/Logi-KafkaManager 一站式Apache Kafka集群指标监控与运维管控平 台，1600+Star，650+ 用户的选择，绝对的好用的Kafka监控利器！ 

作者回复 👍 



1604049711 

搜了一下kafka manager，已经找不到了，最后发现已经改名叫CMAK(Cluster Manager for Apache Kafka) 

作者回复 嗯嗯，各类框架发展得太快了 



1569285316 

kafka集群监控工具，免费的功能少，功能强大的收费。看自己的情况选择了，作为技术关注 点还在于这些工具的实现原理。 不过任何监控工具，估计都类似，以下是猜测的： 1：获取监控数据，通常是日志信息，加埋点或者利用OS的功能获取 

2：存储监控数据，未经清洗的数据 

- 3：清洗数据，格式化数据，聚合数据，汇总数据 

- 4：展示监控信息 

- 5：功能需求没问题后就是各种优化了，比如：UI展示优化/获取数据不丢消息的优化/展示数 据的性能优化/功能优化，可以加各种报警设置，给出问题产生的主要场景和解决思路。 



1583168212 

Grafana监控，kafka manager运维管理 



1571275522 

有个地方不太准确 BytesInPerSec是leader副本的入流量 并不等于网卡流量 要关注带宽指标 还是需要具体看网卡的流量指标 

作者回复 hmmmm.... 好像我没有说BytesInPerSec=网卡流量，BytesInPerSec是broker端的入站流 量。如果接近带宽，需要调整broker上的负载。 



1566953498 

请求积压，监控两个idle就好吧？但是具体哪些请求积压和哪些ip的请求，这块还不清楚，求 指教。 

作者回复 目前只能监控是否存在请求积压，无法确认到底是那些请求积压的 



1566889902 

监控records-lag-max 和 records-lead-min，它们分别表示此消费者在测试窗口时间内曾经达 到的最大的 Lag 值和最小的 Lead 值。 



1566866799 

老师 生产环境建议用confluent免费版本的kafka么 比如5.3版本基于apache kafka 2.3的？我 们想自己搭kafka 在confluent和apache里面选一个，都是免费的 

作者回复 confluent免费版不错的，可以用：） 



1586187455 

老师你好，我们最近在做kafka数据层面的监控，就是消息情况的监控，包括状态，生产消费 时差，消息趋势等，这一方面的工具，老师这边有好的建议吗？ 

作者回复 目前监控基本上是找主流的监控框架，支持JMX监控的就行，比如Prometheus 



1566865563 

老师，Kafka Manager貌似不支持Kafka 2.x版本吧 

作者回复 可以支持 



1620026630 

尝试了一把kafka eagle 



1619101729 

滴滴新开源了一个，可以参考(https://github.com/didi/Logi-KafkaManager) 



1593934860 

broker存在请求积压是指客户端发送的请求没及时处理吗. 

作者回复 是的。都积压在broker端等待后续处理 





# ~~行~~ 1590450564则将至 

老师，您说的去kafka官网搜索--object-names，这个参数在官网怎么搜索啊？官网左侧的tab 都点开搜了，没有看到。☹ 

作者回复 主要是这里：http://kafka.apache.org/documentation/#monitoring 



1575455640 

我用kafka-manager链接集群后，topics显示为0。请教老师，该如何去排查问题。 

作者回复 先确定问题出现在那端吧？比如使用kafka-topics脚本确认下topic数量。 



1574836015 

你好，请问kafka Consume 进度的监控工具有哪些？我的版本是kafka_2.11-1.1.0，我使用 kafkaOffsetMonitor拿不到consumer数据，查了资料好像kafkaOffsetMonitor不支持0.9以上 版本 

作者回复 kafkaOffsetMonitor已经很久没有维护了。可以使用原生的Kafka命令或kafka-manager 



## 1572358688 

老师，Kafka集群如果重启的，3台机器，每个主题3个副本，假设A主题的 ISR是 0 1 2 ，如果 我同时重启2台broker，那么此时主题A的ISR就剩下一个，这种情况下，集群还可用么？ 

作者回复 你指的可用是指什么含义呢？从Kafka的角度，只要ISR中依然有副本，理论上数据依然可 以正常收发 



1570766668 

老师，请问下为什么我的kafka manager 里面的Latest Offset 为空？如图 https://www.processon.com/view/link/5d9ffd05e4b0893e992642c3 

作者回复 要确认下这些分区是否正常吧，比如leader是否存在 



1566952712 

你好，我们生产request handle idle过低的原因找到了，是由于磁盘坏道导致的？能否加些关 于kafkaApi的监控？看下请求的分布情况及哪些请求占用requesthandler过多吗？ 

作者回复 好问题！hmm.... 目前暂时做不到或者非常不方便。你可以启动KafkaApis的TRACE日志， 然后汇总统计哪类请求占用了过多的线层 



1566914123 

前公司携程grafana用的就很好 



1566869582 

Kafka Eagle会导致zookeeper连接占满不释放 

