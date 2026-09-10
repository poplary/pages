2019-9-3 胡夕 



你好，我是胡夕。今天我要和你分享的主题是：从0搭建基于Kafka的企业级实时日志流处理 平台。 

简单来说，我们要实现一些大数据组件的组合，就如同玩乐高玩具一样，把它们“插”在一 起，“拼”成一个更大一点的玩具。 

在任何一个企业中，服务器每天都会产生很多的日志数据。这些数据内容非常丰富，包含了 我们的线上业务数 ~~据~~ 、 ~~用户行~~ 为数 ~~据~~ 以及 ~~后~~ 端系统数 ~~据~~ 。实时分析这些数据，能够帮助我们 更快地洞察潜在的趋势，从而有针对性地做出决策。今天，我们就使用Kafka搭建一个这样的 平台。 

如果在网上搜索实时日志流处理，你应该能够搜到很多教你搭建实时流处理平台做日志分析 的教程。这些教程使用的技术栈大多是Flume+Kafka+Storm、Spark Streaming或Flink。特别 是Flume+Kafka+Flink的组合，逐渐成为了实时日志流处理的标配。不过，要搭建这样的处理 平台，你需要用到3个框架才能实现，这既增加了系统复杂度，也提高了运维成本。 

今天，我来演示一下如何使用Apache Kafka这一个框架，实现一套实时日志流处理系统。换 句话说，我使用的技术栈是Kafka Connect+Kafka Core+Kafka Streams的组合。 

下面这张图展示了基于Kafka的实时日志流处理平台的流程。 



从图中我们可以看到，日志先从Web服务器被不断地生产出来，随后被实时送入到Kafka Connect组件，Kafka Connect组件对日志进行处理后，将其灌入Kafka的某个主题上，接着发 送到Kafka Streams组件，进行实时分析。最后，Kafka Streams将分析结果发送到Kafka的另 一个主题上。 

我在专栏前面简单介绍过Kafka Connect和Kafka Streams组件，前者可以实现外部系统与 Kafka之间的数据交互，而后者可以实时处理Kafka主题中的消息。 

现在，我们就使用这两个组件，结合前面学习的所有Kafka知识，一起构建一个实时日志分析 平台。 

我们先利用Kafka Connect组件收 ~~集~~ 数 ~~据~~ 。如前所述，Kafka Connect组件负责连通Kafka与外 部数据系统。连接外部数据源的组件叫连接器（Connector）。 ~~常~~ 见的外 ~~部~~ 数 ~~据源包括~~ 数 ~~据~~ 

今天我们使用文件连接器（File Connector）实时读取Nginx的access日志。假设access日志的 格式如下： 

10.10.13.41 - - [13/Aug/2019:03:46:54 +0800] "GET /v1/open/product_list?user_key=****&user_ph 

在这段日志里，请求参数中的os_type字段目前有两个值：ios和android。我们的目标是实时 计算当天所有请求中ios端和android端的请求数。 

当前，Kafka Connect支持单机版（Standalone）和集群版（Cluster），我们用集群的方式来 启动Connect组件。 

首先，我们要启动Kafka集群，假设Broker的连接地址是localhost:9092。 

启动好Kafka集群后，我们启动Connect组件。在Kafka安装目录下有个config/connectdistributed.properties文件，你需要修改下列项： 

bootstrap.servers=localhost:9092 rest.host.name=localhost rest.port=8083 

第1项是指定 ~~要~~ 连接的Kafka ~~集群~~ ，第2项和第3项分别指定Connect组件开放的REST服务的主 机 ~~名~~ 和端口。保存这些变更之后，我们运行下面的命令启动Connect。 

cd kafka_2.12-2.3.0 bin/connect-distributed.sh config/connect-distributed.properties 

如果一切正常，此时Connect应该就成功启动了。现在我们在浏览器访问localhost:8083的 Connect REST服务，应该能看到下面的返回内容： 

{"version":"2.3.0","commit":"fc1aaa116b661c8a","kafka_cluster_id":"XbADW3mnTUuQZtJKn9P-hA"} 

看到该JSON串，就表明Connect已经成功启动了。此时，我们打开一个终端，运行下面这条 命令来查看一下当前都有哪些Connector。 

$ curl http://localhost:8083/connectors 

[] 

结果显示，目前我们没有创建任何Connector。 

现在，我们来创建对应的File Connector。该Connector读取指定的文件，并为每一行文本创 建一条消息，并发送到特定的Kafka主题上。创建命令如下： 

$ curl -H "Content-Type:application/json" -H "Accept:application/json" http://localhost:8083/ {"name":"file-connector","config":{"connector.class":"org.apache.kafka.connect.file.FileStrea 

这条命令本质上是向Connect REST服务发送了一个POST请求，去创建对应的Connector。在 这个例子中，我们的Connector类是Kafka默认提供的FileStreamSourceConnector。我们要读 取的日志文件在/var/log目录下，要发送到Kafka的主题名称为access_log。 

现在，我们再次运行curl http: // localhost:8083/connectors， 验证一下刚才的Connector是 否创建成功了。 

- $ curl http://localhost:8083/connectors 

- ["file-connector"] 

显然，名为file-connector的新Connector已经创建成功了。如果我们现在使用Console Consumer程序去读取access_log主题的话，应该会发现access.log中的日志行数据已经源源不 断地向该主题发送了。 

如果你的生产环境中有多台机器，操作也很简单，在每台机器上都创建这样一个Connector， 只要保证它们被送入到相同的Kafka主题以供消费就行了。 

数据到达Kafka还不够，我们还需要对其进行实时处理。下面我演示一下如何编写Kafka Streams程序来实时分析Kafka主题数据。 

我们知道，Kafka Streams是Kafka提供的用于实时流处理的组件。 

与其他流处理框架不同的是，它仅仅是一个类库，用它编写的应用被编译打包之后就是一个 普通的Java应用程序。你可以使用任何部署框架来运行Kafka Streams应用程序。 

同时，你只需要简单地启动多个应用程序实例，就能自动地获得负载均衡和故障转移，因 此，和Spark Streaming或Flink这样的框架相比，Kafka Streams自然有它的优势。 

下面这张来自Kafka官网的图片，形象地展示了多个Kafka Streams应用程序组合在一起，共同 实现流处理的场景。图中清晰地展示了3个Kafka Streams应用程序实例。一方面，它们形成 一个组，共同参与并执行流处理逻辑的计算；另一方面，它们又都是独立的实体，彼此之间 毫无关联，完全依靠Kafka Streams帮助它们发现彼此并进行协作。 



关于Kafka Streams的原理，我会在专栏后面进行详细介绍。今天，我们只要能够学会利用它 提供的API编写流处理应用，帮我们找到刚刚提到的请求日志中ios端和android端发送请求数 量的占比数据就行了。 

要使用Kafka Streams，你需要在你的Java项目中显式地添加kafka-streams依赖。我以最新的 2.3版本为例，分别演示下Maven和Gradle的配置方法。 

Maven: <dependency> <groupId>org.apache.kafka</groupId> <artifactId>kafka-streams</artifactId> <version>2.3.0</version> </dependency> 

Gradle: 

compile group: 'org.apache.kafka', name: 'kafka-streams', version: '2.3.0' 

现在，我先给出完整的代码，然后我会详细解释一下代码中关键部分的含义。 

package com.geekbang.kafkalearn; 

import com.google.gson.Gson; import org.apache.kafka.common.serialization.Serdes; import org.apache.kafka.streams.KafkaStreams; import org.apache.kafka.streams.StreamsBuilder; import org.apache.kafka.streams.StreamsConfig; import org.apache.kafka.streams.Topology; import org.apache.kafka.streams.kstream.KStream; import org.apache.kafka.streams.kstream.Produced; import org.apache.kafka.streams.kstream.TimeWindows; import org.apache.kafka.streams.kstream.WindowedSerdes; 

import java.time.Duration; import java.util.Properties; import java.util.concurrent.CountDownLatch; public class OSCheckStreaming { 

public static void main(String[] args) { 

Properties props = new Properties(); props.put(StreamsConfig.APPLICATION_ID_CONFIG, "os-check-streams"); props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass() props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.StringSerde.cl 

final Gson gson = new Gson(); 

final StreamsBuilder builder = new StreamsBuilder(); 

KStream<String, String> source = builder.stream("access_log"); source.mapValues(value -> gson.fromJson(value, LogLine.class)).mapValues(LogLine::get .groupBy((key, value) -> value.contains("ios") ? "ios" : "android") .windowedBy(TimeWindows.of(Duration.ofSeconds(2L))) .count() .toStream() .to("os-check", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.cla 

final Topology topology = builder.build(); 

final KafkaStreams streams = new KafkaStreams(topology, props); 

final CountDownLatch latch = new CountDownLatch(1); 

Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") { @Override public void run() { streams.close(); latch.countDown(); } }); try { streams.start(); latch.await(); } catch (Exception e) { System.exit(1); } System.exit(0); } } class LogLine { private String payload; private Object schema; public String getPayload() { return payload; } } 

这段代码会实时读取access_log主题，每2秒计算一次ios端和android端请求的总数，并把这 些数据写入到os-check主题中。 

首先，我们构造一个Properties对象。这个对象负责初始化Streams应用程序所需要的关键参 数设置。比如，在上面的例子中，我们设置了bootstrap.servers参数、application.id参数以 及默认的序列化器（Serializer）和解序列化器（Deserializer）。 

bootstrap.servers参数你应该已经很熟悉了，我就不多讲了。这里的application.id是Streams 程序中非常关键的参数，你必须要指定一个集群范围内唯一的字符串来标识你的Kafka Streams程序。序列化器和解序列化器设置了默认情况下Streams程序执行序列化和反序列化 时用到的类。在这个例子中，我们设置的是String类型，这表示，序列化时会将String转换成 字节数组，反序列化时会将字节数组转换成String。 

构建好Properties实例之后，下一步是创建StreamsBuilder对象。稍后我们会用这个Builder去 实现具体的流处理逻辑。 

在这个例子中，我们实现了这样的流计算逻辑：每2秒去计算一下ios端和android端各自发送 的总请求数。还记得我们的原始数据长什么样子吗？它是一行Nginx日志，只不过Connect组 件在读取它后，会把它包装成JSON格式发送到Kafka，因此，我们需要借助Gson来帮助我们 把JSON串还原为Java对象，这就是我在代码中创建LogLine类的原因。 

代码中的mapValues调用将接收到的JSON串转换成LogLine对象，之后再次调用mapValues方 法，提取出LogLine对象中的payload字段，这个字段保存了真正的日志数据。这样，经过两 次mapValues方法调用之后，我们成功地将原始数据转换成了实际的Nginx日志行数据。 

值得注意的是，代码使用的是Kafka Streams提供的mapValues方法。顾名思义，这个方法 ~~就~~ <mark>是只</mark> 对 ~~消息~~ 体（Value）进 ~~行~~ 转换，而不变 ~~更消息~~ 的 ~~键~~ （Key）。 

其实，Kafka Streams也提供了map方法，允许你同时修改消息Key。通常来说，我们认为 mapValues ~~要~~ 比map方法 ~~更~~ ~~<mark>高</mark>~~ 效，因为Key的变更可能导致下游处理算子（Operator）的重分 区，降低性能。如果可能的话最好尽量使用mapValues方法。 

拿到真实日志行数据之后，我们调用groupBy方法进行统计计数。由于我们要统计双端（ios 端和android端）的请求数，因此，我们groupBy的Key是ios或android。在上面的那段代码 中，我仅仅依靠日志行中是否包含特定关键字的方式来确定是哪一端。更正宗的做法应该 是，分析Nginx ~~日~~ 志 ~~格~~ 式， <mark>提</mark> ~~取~~ 对应的参数 ~~值~~ ，也 ~~就~~ <mark>是</mark> os_type的 ~~值~~ 。 

做完groupBy之后，我们还需要限定要统计的时间窗口范围，即我们统计的双端请求数是在哪 个时间窗口内计算的。在这个例子中，我调用了windowedBy方法，要求Kafka Streams每2秒 统计一次双端的请求数。设定好了时间窗口之后，下面就是调用count方法进行统计计数了。 

这一切都做完了之后，我们需要调用toStream方法将刚才统计出来的表（Table）转换成事件 流，这样我们就能实时观测它里面的内容。我会在专栏的最后几讲中解释下流处理领域内的 流和表的概念以及它们的区别。这里你只需要知道toStream是将一个Table变成一个Stream即 可。 

最后，我们调用to方法将这些时间窗口统计数据不断地写入到名为os-check的Kafka主题中， 从而最终实现我们对Nginx日志进行实时分析处理的需求。 

由于Kafka Streams应用程序就是普通的Java应用，你可以用你熟悉的方式对它进行编译、打 包和部署。本例中的OSCheckStreaming.java就是一个可执行的Java类，因此直接运行它即 可。如果一切正常，它会将统计数据源源不断地写入到os-check主题。 

如果我们想要查看统计的结果，一个简单的方法是使用Kafka自带的kafka-console-consumer 脚本。命令如下： 

$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic os-check --from-beg [android@1565743788000/9223372036854775807] 1522 [ios@1565743788000/9223372036854775807] 478 [ios@1565743790000/9223372036854775807] 1912 [android@1565743790000/9223372036854775807] 5313 [ios@1565743792000/9223372036854775807] 780 [android@1565743792000/9223372036854775807] 1949 [android@1565743794000/9223372036854775807] 37 

由于我们统计的结果是某个时间窗口范围内的，因此承载这个统计结果的消息的Key封装了该 时间窗口信息，具体格式是：[ios <mark>或</mark> android@开 ~~始~~ 时间/ ~~结束~~ 时间]，而消息的Value就是一个 简单的数字，表示这个时间窗口内的总请求数。 

如果把上面ios相邻输出行中的开始时间相减，我们就会发现，它们的确是每2秒输出一次，每 次输出会同时计算出ios端和android端的总请求数。接下来，你可以订阅这个Kafka主题，将 结果实时导出到你期望的其他数据存储上。 

至此，基于Apache Kafka的实时日志流处理平台就简单搭建完成了。在搭建的过程中，我们 只使用Kafka这一个大数据框架就完成了所有组件的安装、配置和代码开发。比起 Flume+Kafka+Flink这样的技术栈，纯Kafka的方案在运维和管理成本上有着极大的优势。如果 你打算从0构建一个实时流处理平台，不妨试一下Kafka Connect+Kafka Core+Kafka Streams 的组合。 

其实，Kafka Streams提供的功能远不止做计数这么简单。今天，我只是为你展示了Kafka Streams的冰山一角。在专栏的后几讲中，我会重点向你介绍Kafka Streams组件的使用和管 理，敬请期待。 



请比较一下Flume+Kafka+Flink方案和纯Kafka方案，思考一下它们各自的优劣之处。在实际场 景中，我们该如何选择呢？ 

欢迎写下你的思考和答案，我们一起讨论。如果你觉得有所收获，也欢迎把文章分享给你的 朋友。 

- © 版权归极客邦科技所有，未经许可不得传播售卖。 页面已增加防盗追踪，如有侵权极客邦将依法追究其法律责任。 

上一篇 38 | 调优Kafka，你做到了吗？ 

下一篇 40 | Kafka Streams与其他流处理平台的差异在哪里？ 



1580738613 

老师好，说下我这边的一些实践。19年一直在做容器日志平台，我们目前的方案是 Fluentd + Kafka + ELK。使用Fluentd做为容器平台的采集器实时采集数据，采集完之后数据写入Kafka 通过Kafka进行解耦，使用Logstash消费后写入ES。这套方案目前在容器环境下应该可以说是 标配 

作者回复 棒棒：） 



1567509889 

老师，上述Kafka Connect+Kafka Core+Kafka Streams例子中，生产者和消费者分别是什么？ 

作者回复 此时，生产者和消费者化身成这个大平台的小组件了。Connect中只有producer，将读取 的日志行数据写入到Kafka源主题中。Streams中既有producer也有consumer：producer负责将计算 结果实时写入到目标Kafka主题；consumer负责从源主题中读取消息供下游实时计算之用。 



1588647850 

胡老师，对于Stream的处理和之前的topic-message，我感觉没什么大的区别，感觉流程是类 似的。只不过是在以前的consumer中额外添加了producer的逻辑，把处理结果发送到另一个 topic中。感觉不用这里说的stream也能实现一样的效果。我不是个明白本质的区别是什么， 麻烦能解释一下吗？谢谢 

作者回复 Kafka Streams提供了consume-process-produce的原子性操作，也就是端到端的EOS。如 果你自己实现代价很高 



1568475245 

老师，示例中开启Connect后启动读取的是本机的nginx日志，但如果nginx日志是在其他机器 上面，那Connect是不是支持远程读的还是怎么样可以读取到其他机器的日志？ 

作者回复 在Nginx日志机器上开启，因为目前File Connector只支持从本地文件读取 



1583830225 

写的不太明白啊, 难道每一个nginx服务器上都要部署kafka吗 

作者回复 是的 



1582622640 

老师我想问一下。Kafka Connect 是一个单独的组件么？类似agent一样，可以在目标采集机 器（非kafka集群）上部署？那如果要用，岂不是每个业务机器都要装个kafka？ 单机模式跟集群模式有啥区别呢？没太懂。比如kafka集群上启动单机模式的connect ？不行 么？ 

最终操作，都往同一个topic里扔就好了? 

作者回复 是单独的组件。需要单独部署。 

kafka集群上启动单机模式的connect --- 这是可以的 



1578989450 



老师我有问题要请教下，添加 Connector 步骤里面是用 http REST 接口新建的，那新建的 Connector 是跑在 Broker 里面还是说又启动了一个新的 Java 进程执行 Connector？ 

作者回复 新的java进程 



1601721023 

老师如果有多个分区，并且消息写入是随机的。那么多个kafka streams实例在对os_type进行 group_by统计时，需要相互之间传输数据进行shuffle操作吗？ 

作者回复 需要的。Kafka Streams使用特殊的repartition topics来保存shuffle后的数据 



1592743226 

“另一方面，它们又都是独立的实体，彼此之间毫无关联，完全依靠 Kafka Streams 帮助它 们发现彼此并进行协作。” 

完全依靠“kafka cluster”这个吧！？ 

作者回复 依靠Kafka Streams找到Kafka Cluster上的彼此。这么说是不是好点：） 



1583169095 

connect与filebeat比单文件收集性能比较来说哪个更高？我们用filebeat单文件收集性能到瓶 颈了，我们现在的做法是减小单文件的，大小，来提高并行度，不知道connect性能如何 

作者回复 hmmm..... 这个只能靠测试才能知道了：） 



1616602307 

老师 想咨询个问题 我这边如果日志不是来自于文件 而是来自于telenet的输出 需要怎么做日 志实时分析。 而且需要有上万个telenet实例一起在输出 我需要分别分析每个实例按照某种统 

一规则 

作者回复 可以为每个telnet的数据流编制一个key来管理 



1611823082 

为什么消费者取出来的值是乱码的 

作者回复 因为是二进制字节序列，需要反序列化 



1603419908 

问个大数据相关的问题，一定要读取和写入都在hdfs上才是离线计算吗？ 

作者回复 不一定。离线计算通常是指延时比较高的batch computing，和用什么存储没有强绑定关系 



1602726350 

老师，如果要将处理后的结果写到另一个 kafka 集群的 topic，应该怎么做呢？ 

作者回复 使用KStream的to方法 



1598104724 

这个案例运行了下没有两秒钟就输出，老师能怎么排查下是什么问题吗？ 

作者回复 .windowedBy(TimeWindows.of(Duration.ofSeconds(2L))) 

改长点 





1597151775杨逸林 

我看了有点多的 SpringCloud + Kafka Stream 整合的，按照 Spring 官方给的教学视频学，结 果到头来还是只会改下字符串（从一个 Topic 监听数据，送到另一个 Topic 做数据清洗）。 Kafka 官网都是像你写的 demo 一样的，其实还是有点懵的。。。 

作者回复 其实我一直有个想法：在学习任何大数据流式处理框架前，我们至少要对Java的Stream和 Lambda表达式有一定的了解，这样我们才能更好地理解如何把操作算子组合在一起。比如要理解基 本的有状态操作算子和无状态操作算子都有哪些等 



1594433192 

用过kafka connector，直接在集群上启动单机的connector连接mqtt，但感觉这种方案的扩 展性不高 

作者回复 Connect是可以集群搭建的 



1593009895 

说好的从0 开始的，kafka 集群怎么装。 

作者回复 哈哈哈，有关搭建集群的内容在前面的课程中：） 



1587118544 

胡哥： 

开发的一个弹幕系统，分为server和connect，connect是专门维护socket链接和推消息的，可 以横向扩展。但是目前遇到了系统瓶颈，单机就支持3000左右连接。在推消息的过程中，系 统每次把消息给tcp就不管客户端是否能收到消息，但是操作系统tcp连接在发消息失败的时候 会重试，所以在推消息的过程中消息就堆积了起来，导致单机连接数一直上不去。这种能指 点一下吗？ 

作者回复 不确定完全理解了意思。听上去是否可以在推消息和发消息之间加一个队列解耦下？这样 server只管向队列推消息就可以了。connect负责从队列取消息然后发出去。如果单机性能不够，还 可以水平扩展成多机的情况 



1586057002 

connect只能支持json格式的数据吗 

作者回复 不是的，支持各种形式 



1576017489 

终于等到了。 



1575902837 

胡老师，请问对于迟到的数据，os_check主题会生成多条记录吗？此时消费者应用程序应该 如何处理？ 

作者回复 不会生成多条记录，但是的确可能会被丢弃（如果late太多） 



1573526558 

老师，您在处理json串的时候为什么用Gson，而不用Alibaba的fastjson呢？ 

作者回复 只是举个例子而已，没说一定只能用Gson做JSON的序列化 



1569751121 

老师，请教connect读取mysql数据库中，我的添加connector命令是curl -X POST http://llzw:8083/connectors -H "Content-Type: application/json" -d '{"name": "mysqlconnector","config": {"connector.class": 

"io.debezium.connector.mysql.MySqlConnector","tasks.max": "1","database.hostname": "lzw-mysql","database.port": "3306","database.user": "root","database.password": "123456","database.server.id": "1","database.server.name": "pydata","database.whitelist": "employee","topic.prefix": "test-mysql-","database.history.kafka.bootstrap.servers": "l- 

lzw:9092,l-lzw2:9092","database.history.kafka.topic": "db.history.mysql"}}' 

消费topic时返回如下： {"source": 

{"version":"0.9.5.Final","connector":"mysql","name":"pydata","server_id":0,"ts_sec":0,"gtid":n bin.000004","pos":1021,"row":0,"snapshot":true,"thread":null,"db":null,"table":null,"query": character_set_server=latin1, collation_server=latin1_swedish_ci;"}} 

没有正常可能原因是什么？ 



1569293012 

打卡，仅仅使用kafka这一个大数据组件就能实现一个企业级的实时日志流处理平台。 获取——存储——清洗——转存——展示 



1567728723 

请教一下，集群版的connector是说每个kafka节点都启动一个吗？还有它读取的nginx日志就 在本地？谢谢 

作者回复 在Nginx日志本地 



1567471363 

请问胡老师，console-consumer输出的message，为什么结束时间是一个很大的整数？从开 始时间看，它应该是millisecond epoch，原本以为结束时间应该也是开始时间+2 second，但 是文章中的例子看着不像： 

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic os-check --frombeginning --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property 

key.deserializer=org.apache.kafka.streams.kstream.TimeWindowedDeserializer --property key.deserializer.default.windowed.key.serde.inner=org.apache.kafka.common.serialization. [android@1565743788000/9223372036854775807] 1522 [ios@1565743788000/9223372036854775807] 478 

[ios@1565743790000/9223372036854775807] 1912 [android@1565743790000/9223372036854775807] 5313 [ios@1565743792000/9223372036854775807] 780 [android@1565743792000/9223372036854775807] 1949 [android@1565743794000/9223372036854775807] 37 …… 

作者回复 这里的结束时间在代码中没有指定，因此默认值是Long.MAX_VALUE 

