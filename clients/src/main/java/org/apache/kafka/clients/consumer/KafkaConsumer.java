/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.internals.ConsumerDelegate;
import org.apache.kafka.clients.consumer.internals.ConsumerDelegateCreator;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.metrics.KafkaConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.time.Duration;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * 从kafka集群消费记录的客户端。
 *
 * <p>该客户端透明地处理Kafka代理的故障，并透明地适应其提取的主题分区在集群内的迁移。该客户端还与代理交互，
 * 以允许消费者组使用consumer groups对消费进行负载均衡。
 *
 * <p>消费者维护TCP连接到所需的broker来获取记录。使用后忘记关闭将导致连接泄露。
 *
 * <p>跨版本兼容性：
 * 客户端可以与版本为0.10.0或更新的broker进行通信。更老或更新的broker将核心功能。例如：0.10.0 brokers 不支持 offsetsFromTimes，
 * 因为该功能是在0.10.1版本中加入的。你将收到{@link org.apache.kafka.common.errors.UnsupportedVersionException}当调用
 * 的API在指定的broker版本中不可用时。
 *
 * <p>offsets和消费者位置：
 * kafka在一个分区中为每条记录维护一个数字的偏移量。该偏移量作为在该分区中记录的唯一标识符，并标识消费者在分区中的位置。
 * 例如：一个消费者在偏移量为5时，代表已经消费了0到4的偏移量的记录，下一次就接受偏移量为5的记录。
 * 实际上与消费者用户相关的位置概念有两种：
 * <ul>
 *     <li>
 *         消费者的{@link #position(TopicPartition) position}给出了将发出的下一条记录的偏移量，它将比消费者在该分区中看到的最大偏移量大1。
 *         每当消费者在调用{@link #poll(Duration)}收到消息时，它都会自动前进
 *     </li>
 *     <li>
 *          {@link #commitSync() committed position}是已安全存储的最后一个偏移量。如果进程失败并重新启动，则消费者将恢复到此偏移量。
 *          消费者可以定期自动提交偏移量；也可以选择通过调用其中一个提交API手动控制此提交位置。
 *     </li>
 * </ul>
 *
 * <p>这种区别使得消费者能够控制何时将记录视为已消费。下文将对此进行更详细的谈论。
 *
 * <p>消费者组和主题订阅
 * kafka使用{@code consumer groups}的概念来允许一组进程划分消费和处理记录的工作。这些处理可以在同一台及其或在多台机器构成的分布式环境中
 * 为处理数据提供伸缩性和故障容错。所有共享相同的{@code group.id}消费者实例将被视作同一消费者的一部分。
 *
 * <p>在分组中每个消费者可以动态地设置想要订阅的主题列表，通过{@link #subscribe(Collection, ConsumerRebalanceListener)}API。
 * kafka将订阅的主题中的每条消息发送给每个消费者组中的一个进程（类似于RocketMQ的集群消费模式）。所以如果一个主题存在四个分区，一个消费者组
 * 中有两个进程，每个进程将消费两个分区上的记录。
 *
 * <p>消费者组中的成员被动态维护的：如果一个进程失败，被分配给它的分区将被重新分配给同一消费者分组下的其他消费者。类似的，如果一个消费者加入到分组中，
 * 分片将从已有消费者移动到新加入的消费者中。这成为{@code rebalancing}组，将在下文中详细谈论。组重平衡同时被用于当一个新的分区被加入到已订阅的主题
 * 其中之一时，或当一个匹配{@link #subscribe(Pattern, ConsumerRebalanceListener)}的新主题被创建。组将通过定期的元数据刷新自动检测新的分区
 * 并将其分配给组内的成员。
 *
 * <p>从概念上来讲，您可以将消费者组视为由多个进程组成的单个逻辑订阅者。作为多订阅者的系统，kafka天然的支持一个给定主题存在任意数量的消费者组，而无需
 * 重复数据（额外的消费者实际上非常便宜）。
 *
 * <p>这是消息传递系统中常见功能的轻微概括。为了获取与传统消息系统中队列的类似语义，所有的进程都成为单个消费者组的一部分，因此记录传递将像队列一样在组
 * 之间平衡。但与传统消息系统不同的是，你可以拥有多个这样的分组。为了获得与传统消息中的发布-订阅类似的语义，每个进程都有自己的消费者组，因此每个进程将订阅
 * 发布到该主题的所有记录（这种场景说的是，这个消费者组中仅有一个消费者，这样子所有的记录才能被一个队列去消费。）。
 *
 * <p>此外，当组的重新分配是自动发生的，消费者们可以通过{@link ConsumerRebalanceListener}收到通知，该监听器允许消费者们完成必须的应用级逻辑，例如状态清理，
 * 手动偏移量提交等等。查阅 <a href="#rebalancecallback">Storing Offsets Outside Kafka</a> 获取更多的细节.
 *
 * <p>消费者通过{@link #assign(Collection)}来<a href="#manualassignment">手动分配</a>特定分区是可能的。在这种场景下，动态分区分配和消费者组协调将被禁用。
 *
 * <h3>检测消费者故障</h3>
 * <p>在订阅一系列的主题后，当调用{@link #poll(Duration)}方法后，消费者会自动加入到消费者分组中。Poll API被设计用于确保消费者活跃。只要你持续调用poll，
 * 消费者就一直位于分组中，并持续从被分配的分区中读取消息。在底层，消费者向服务器定期发送心跳。如果消费者故障或者在指定时间内{@code session.timeout.ms}
 * 无法发送心跳，此时消费者被认为死亡，之前分配给它的分区将被重新分配。
 *
 * <p>消费者还有可能碰到活锁的情况，即它一直发送心跳，但是没有任何进展。为了防止消费者在这种情况下无限期地持有其分区，我们提供了一种使用{@code max.poll.interval.ms}
 * 的活跃度检测机制。基本上，如果你不至少按照配置的最大间隔频率调用轮询，那么客户端将主动离开消费者分组，以便另一个消费者可以接管该分区。当发生上述情况时，
 * 你可能看到一个偏移量提交失败的异常（在调用{@link #commitSync()}时抛出{@link CommitFailedException}）。这是一种安全机制来确保消费者分组中只有活跃的成员才能提交偏移量。
 * 因此呆在消费者分组中，消费者才能继续执行poll。
 *
 * <p>消费者提供两种配置来控制poll循环的行为：
 * <ul>
 *     <li>
 *         {@code max.poll.interval.ms}：通过在预期的poll中增加间隔，用户可以让消费者更多时间处理来自{@link #poll(Duration)}返回的批次记录。
 *          这种方式的缺点就是增加该参数值，可能延迟组平衡，因为消费者仅会在轮询调用中加入重新平衡，你可以使用该配置来限制完成重新平衡的时间，但是如果消费者
 *          实际上不能足够频繁的调用{@link #poll(Duration)}，则可能面临进度变慢的风险。
 *     </li>
 *     <li>
 *         {@code max.poll.records}：使用该设置限制单次从{@link #poll(Duration)}中返回总的记录数。则可以更容易地预测每个轮询间隔内必须处理的最大值。
 *         通过调整该值，你可能能够减少poll间隔，从而减少分组重平衡带来的影响。
 *     </li>
 * </ul>
 *
 * <p>对于消息处理时间不可预测地变化的场景，这两个选项可能都不够用。处理这些情况的推荐方法是将消息处理移至另一个线程。
 * 这允许消费者持续调用{@link #poll(Duration)}的同时处理消息。必须小心谨慎，确保承诺的偏移不会超出实际位置。
 * 通常，用户必须禁用自动提交，仅在线程完成处理后才手动提交已处理的记录偏移量（取决于您需要的传递语义）。
 * 还要注意，您需要{@link #pause(Collection)}分区，以便在线程处理完先前返回的记录之前不会从轮询中收到新的记录。
 *
 * <h3>使用示例</h3>
 * <p>消费者API提供了灵活性，可以涵盖各种消费用例。下面是一些示例演示如何使用它们。
 *
 * <h4>自动提交偏移量</h4>
 * <p>该示例演示了kafka消费者api依赖于自动偏移量提交的简单使用。
 * <pre>{@code
 *  Properties props = new Properties();
 *  props.setProperty("bootstrap.servers", "localhost:9092");
 *  props.setProperty("group.id", "test");
 *  props.setProperty("enable.auto.commit", "true");
 *  props.setProperty("auto.commit.interval.ms", "1000");
 *  props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 *  props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 *  KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
 *  consumer.subscribe(Arrays.asList("foo", "bar"));
 *  while(true) {
 *      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
 *      for (ConsumerRecord record : records) {
 *          System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
 *      }
 *  }
 * }</pre>
 *
 * <p>通过使用{@code bootstrap.servers}指定要联系的一个或多个代理的列表来引导与集群的连接。
 * 这个列表被用于发现集群中部分的broker，而不需要是集群中服务器的详尽列表。(不过你可能希望指定多个，以防客户端连接时服务端宕机)
 *
 * <p>参数{@code enable.auto.commit}意味着由参数{@code auto.commit.interval.ms}控制的频率自动提交偏移量。
 *
 * <p>在上述示例中，消费者通过配置的{@code group.id}作为消费者分组的成员，订阅了{@code foo}和{@code bar}主题。
 *
 * <p>反序列化参数制定了如何将字节数组转化为对象。例如：通过指定String反序列化器，我们可以说记录的键和值均是简单字符串。
 *
 * <h4>手动控制偏移量</h4>
 * <p>用户不必依赖消费者定期提交已消费的偏移量，还可以控制何时应将记录视为已消费，从而提交其偏移量。
 * 当消息的消费伴随着一些处理逻辑，这会很有用，因此在消息完成处理前不应被是为消费完成。
 * <pre>{@code
 *  Properties props = new Properties();
 *  props.setProperty("bootstrap.servers", "localhost:9092");
 *  props.setProperty("group.id", "test");
 *  props.setProperty("enable.auto.commit", "false");
 *  props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 *  props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
 *  KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
 *  consumer.subscribe(Arrays.asList("foo", "bar"));
 *  final int minBatchSize = 200;
 *  List<ConsumeRecord<String, String>> buffer = new ArrayList<>();
 *  while(true) {
 *      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
 *      for (ConsumerRecord<String, String> record: records) {
 *          buffer.add(record);
 *      }
 *      if (buffer.size >= minBatchSize) {
 *          insertIntoDb(buffer);
 *          consumer.commitSync();
 *          buffer.clear();
 *      }
 *  }
 *
 * }</pre>
 *
 * <p>在上述示例中，我们消费一批记录，并在内存中对它们进行处理。当我们由足够的记录进行批次处理时，我们将它们保存到数据库中。
 * 如果我们像之前的示例那样允许偏移量自动提交，当它们从{@link #poll(Duration)}方法返回时，记录会被视为已消费。
 * 之后我们的进程在批次处理记录后，在将它们插入到数据库之前失败，那么消息就丢失了。
 *
 * <p>为了避免这种情况，我们将仅在对应记录被插入到数据库后手动提交偏移量。这使我们能够精确控制何时记录被视为已消耗。
 * 这就提出了相反的可能性：在将数据插入到数据库后，但在提交偏移量之前，进程在此期间宕机了（尽管则只需要几毫秒，但这是一种可能性）。
 * 在这种情况下，接管消费的进程将从最后提交的偏移量开始消费，并重复插入最后一批数据。通过这种方式使用，Kafka提供了通常所说的"至少一次"
 * 的交付保证，因为每条数据可能只交付一次，但在失败的情况下可能重复。
 *
 * <p>使用自动偏移量提交也可以提供"至少一次"的交付保证，但要求是，你必须在任何后续调用之前，或在{@link #close()}消费者之前，消费每次调用
 * 返回的所有数据。如果您未能做到以上任意一点
 *
 * <p>
 * <b>Note: Using automatic offset commits can also give you "at-least-once" delivery, but the requirement is that
 * you must consume all data returned from each call to {@link #poll(Duration)} before any subsequent calls, or before
 * {@link #close() closing} the consumer. If you fail to do either of these, it is possible for the committed offset
 * to get ahead of the consumed position, which results in missing records. The advantage of using manual offset
 * control is that you have direct control over when a record is considered "consumed."</b>
 * <p>
 * The above example uses {@link #commitSync() commitSync} to mark all received records as committed. In some cases
 * you may wish to have even finer control over which records have been committed by specifying an offset explicitly.
 * In the example below we commit offset after we finish handling the records in each partition.
 * <p>
 * <pre>
 *     try {
 *         while(running) {
 *             ConsumerRecords&lt;String, String&gt; records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
 *             for (TopicPartition partition : records.partitions()) {
 *                 List&lt;ConsumerRecord&lt;String, String&gt;&gt; partitionRecords = records.records(partition);
 *                 for (ConsumerRecord&lt;String, String&gt; record : partitionRecords) {
 *                     System.out.println(record.offset() + &quot;: &quot; + record.value());
 *                 }
 *                 long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
 *                 consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
 *             }
 *         }
 *     } finally {
 *       consumer.close();
 *     }
 * </pre>
 *
 * <b>Note: The committed offset should always be the offset of the next message that your application will read.</b>
 * Thus, when calling {@link #commitSync(Map) commitSync(offsets)} you should add one to the offset of the last message processed.
 *
 * <h4><a name="manualassignment">Manual Partition Assignment</a></h4>
 * <p>
 * In the previous examples, we subscribed to the topics we were interested in and let Kafka dynamically assign a
 * fair share of the partitions for those topics based on the active consumers in the group. However, in
 * some cases you may need finer control over the specific partitions that are assigned. For example:
 * <p>
 * <ul>
 * <li>If the process is maintaining some kind of local state associated with that partition (like a
 * local on-disk key-value store), then it should only get records for the partition it is maintaining on disk.
 * <li>If the process itself is highly available and will be restarted if it fails (perhaps using a
 * cluster management framework like YARN, Mesos, or AWS facilities, or as part of a stream processing framework). In
 * this case there is no need for Kafka to detect the failure and reassign the partition since the consuming process
 * will be restarted on another machine.
 * </ul>
 * <p>
 * To use this mode, instead of subscribing to the topic using {@link #subscribe(Collection) subscribe}, you just call
 * {@link #assign(Collection)} with the full list of partitions that you want to consume.
 *
 * <pre>
 *     String topic = &quot;foo&quot;;
 *     TopicPartition partition0 = new TopicPartition(topic, 0);
 *     TopicPartition partition1 = new TopicPartition(topic, 1);
 *     consumer.assign(Arrays.asList(partition0, partition1));
 * </pre>
 * <p>
 * Once assigned, you can call {@link #poll(Duration) poll} in a loop, just as in the preceding examples to consume
 * records. The group that the consumer specifies is still used for committing offsets, but now the set of partitions
 * will only change with another call to {@link #assign(Collection) assign}. Manual partition assignment does
 * not use group coordination, so consumer failures will not cause assigned partitions to be rebalanced. Each consumer
 * acts independently even if it shares a groupId with another consumer. To avoid offset commit conflicts, you should
 * usually ensure that the groupId is unique for each consumer instance.
 * <p>
 * Note that it isn't possible to mix manual partition assignment (i.e. using {@link #assign(Collection) assign})
 * with dynamic partition assignment through topic subscription (i.e. using {@link #subscribe(Collection) subscribe}).
 *
 * <h4><a name="rebalancecallback">Storing Offsets Outside Kafka</h4>
 * <p>
 * The consumer application need not use Kafka's built-in offset storage, it can store offsets in a store of its own
 * choosing. The primary use case for this is allowing the application to store both the offset and the results of the
 * consumption in the same system in a way that both the results and offsets are stored atomically. This is not always
 * possible, but when it is it will make the consumption fully atomic and give "exactly once" semantics that are
 * stronger than the default "at-least once" semantics you get with Kafka's offset commit functionality.
 * <p>
 * Here are a couple of examples of this type of usage:
 * <ul>
 * <li>If the results of the consumption are being stored in a relational database, storing the offset in the database
 * as well can allow committing both the results and offset in a single transaction. Thus either the transaction will
 * succeed and the offset will be updated based on what was consumed or the result will not be stored and the offset
 * won't be updated.
 * <li>If the results are being stored in a local store it may be possible to store the offset there as well. For
 * example a search index could be built by subscribing to a particular partition and storing both the offset and the
 * indexed data together. If this is done in a way that is atomic, it is often possible to have it be the case that even
 * if a crash occurs that causes unsync'd data to be lost, whatever is left has the corresponding offset stored as well.
 * This means that in this case the indexing process that comes back having lost recent updates just resumes indexing
 * from what it has ensuring that no updates are lost.
 * </ul>
 * <p>
 * Each record comes with its own offset, so to manage your own offset you just need to do the following:
 *
 * <ul>
 * <li>Configure <code>enable.auto.commit=false</code>
 * <li>Use the offset provided with each {@link ConsumerRecord} to save your position.
 * <li>On restart restore the position of the consumer using {@link #seek(TopicPartition, long)}.
 * </ul>
 *
 * <p>
 * This type of usage is simplest when the partition assignment is also done manually (this would be likely in the
 * search index use case described above). If the partition assignment is done automatically special care is
 * needed to handle the case where partition assignments change. This can be done by providing a
 * {@link ConsumerRebalanceListener} instance in the call to {@link #subscribe(Collection, ConsumerRebalanceListener)}
 * and {@link #subscribe(Pattern, ConsumerRebalanceListener)}.
 * For example, when partitions are taken from a consumer the consumer will want to commit its offset for those partitions by
 * implementing {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)}. When partitions are assigned to a
 * consumer, the consumer will want to look up the offset for those new partitions and correctly initialize the consumer
 * to that position by implementing {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)}.
 * <p>
 * Another common use for {@link ConsumerRebalanceListener} is to flush any caches the application maintains for
 * partitions that are moved elsewhere.
 *
 * <h4>Controlling The Consumer's Position</h4>
 * <p>
 * In most use cases the consumer will simply consume records from beginning to end, periodically committing its
 * position (either automatically or manually). However Kafka allows the consumer to manually control its position,
 * moving forward or backwards in a partition at will. This means a consumer can re-consume older records, or skip to
 * the most recent records without actually consuming the intermediate records.
 * <p>
 * There are several instances where manually controlling the consumer's position can be useful.
 * <p>
 * One case is for time-sensitive record processing it may make sense for a consumer that falls far enough behind to not
 * attempt to catch up processing all records, but rather just skip to the most recent records.
 * <p>
 * Another use case is for a system that maintains local state as described in the previous section. In such a system
 * the consumer will want to initialize its position on start-up to whatever is contained in the local store. Likewise
 * if the local state is destroyed (say because the disk is lost) the state may be recreated on a new machine by
 * re-consuming all the data and recreating the state (assuming that Kafka is retaining sufficient history).
 * <p>
 * Kafka allows specifying the position using {@link #seek(TopicPartition, long)} to specify the new position. Special
 * methods for seeking to the earliest and latest offset the server maintains are also available (
 * {@link #seekToBeginning(Collection)} and {@link #seekToEnd(Collection)} respectively).
 *
 * <h4>Consumption Flow Control</h4>
 * <p>
 * If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all of them at the same time,
 * effectively giving these partitions the same priority for consumption. However in some cases consumers may want to
 * first focus on fetching from some subset of the assigned partitions at full speed, and only start fetching other partitions
 * when these partitions have few or no data to consume.
 *
 * <p>
 * One of such cases is stream processing, where processor fetches from two topics and performs the join on these two streams.
 * When one of the topics is long lagging behind the other, the processor would like to pause fetching from the ahead topic
 * in order to get the lagging stream to catch up. Another example is bootstrapping upon consumer starting up where there are
 * a lot of history data to catch up, the applications usually want to get the latest data on some of the topics before consider
 * fetching other topics.
 *
 * <p>
 * Kafka supports dynamic controlling of consumption flows by using {@link #pause(Collection)} and {@link #resume(Collection)}
 * to pause the consumption on the specified assigned partitions and resume the consumption
 * on the specified paused partitions respectively in the future {@link #poll(Duration)} calls.
 *
 * <h3>Reading Transactional Messages</h3>
 *
 * <p>
 * Transactions were introduced in Kafka 0.11.0 wherein applications can write to multiple topics and partitions atomically.
 * In order for this to work, consumers reading from these partitions should be configured to only read committed data.
 * This can be achieved by setting the {@code isolation.level=read_committed} in the consumer's configuration.
 *
 * <p>
 * In <code>read_committed</code> mode, the consumer will read only those transactional messages which have been
 * successfully committed. It will continue to read non-transactional messages as before. There is no client-side
 * buffering in <code>read_committed</code> mode. Instead, the end offset of a partition for a <code>read_committed</code>
 * consumer would be the offset of the first message in the partition belonging to an open transaction. This offset
 * is known as the 'Last Stable Offset'(LSO).</p>
 *
 * <p>
 * A {@code read_committed} consumer will only read up to the LSO and filter out any transactional
 * messages which have been aborted. The LSO also affects the behavior of {@link #seekToEnd(Collection)} and
 * {@link #endOffsets(Collection)} for {@code read_committed} consumers, details of which are in each method's documentation.
 * Finally, the fetch lag metrics are also adjusted to be relative to the LSO for {@code read_committed} consumers.
 *
 * <p>
 * Partitions with transactional messages will include commit or abort markers which indicate the result of a transaction.
 * There markers are not returned to applications, yet have an offset in the log. As a result, applications reading from
 * topics with transactional messages will see gaps in the consumed offsets. These missing messages would be the transaction
 * markers, and they are filtered out for consumers in both isolation levels. Additionally, applications using
 * {@code read_committed} consumers may also see gaps due to aborted transactions, since those messages would not
 * be returned by the consumer and yet would have valid offsets.
 *
 * <h3><a name="multithreaded">Multi-threaded Processing</a></h3>
 * <p>
 * The Kafka consumer is NOT thread-safe. It is the responsibility of the user to ensure that multi-threaded access
 * is properly synchronized. Un-synchronized access will result in {@link ConcurrentModificationException}.
 *
 * <p>
 * The only exception to this rule is {@link #wakeup()}, which can safely be used from an external thread to
 * interrupt an active operation. In this case, a {@link org.apache.kafka.common.errors.WakeupException} will be
 * thrown from the thread blocking on the operation. This can be used to shutdown the consumer from another thread.
 * The following snippet shows the typical pattern:
 *
 * <pre>
 * public class KafkaConsumerRunner implements Runnable {
 *     private final AtomicBoolean closed = new AtomicBoolean(false);
 *     private final KafkaConsumer consumer;
 *
 *     public KafkaConsumerRunner(KafkaConsumer consumer) {
 *       this.consumer = consumer;
 *     }
 *
 *     {@literal}@Override
 *     public void run() {
 *         try {
 *             consumer.subscribe(Arrays.asList("topic"));
 *             while (!closed.get()) {
 *                 ConsumerRecords records = consumer.poll(Duration.ofMillis(10000));
 *                 // Handle new records
 *             }
 *         } catch (WakeupException e) {
 *             // Ignore exception if closing
 *             if (!closed.get()) throw e;
 *         } finally {
 *             consumer.close();
 *         }
 *     }
 *
 *     // Shutdown hook which can be called from a separate thread
 *     public void shutdown() {
 *         closed.set(true);
 *         consumer.wakeup();
 *     }
 * }
 * </pre>
 * <p>
 * Then in a separate thread, the consumer can be shutdown by setting the closed flag and waking up the consumer.
 *
 * <p>
 * <pre>
 *     closed.set(true);
 *     consumer.wakeup();
 * </pre>
 *
 * <p>
 * Note that while it is possible to use thread interrupts instead of {@link #wakeup()} to abort a blocking operation
 * (in which case, {@link InterruptException} will be raised), we discourage their use since they may cause a clean
 * shutdown of the consumer to be aborted. Interrupts are mainly supported for those cases where using {@link #wakeup()}
 * is impossible, e.g. when a consumer thread is managed by code that is unaware of the Kafka client.
 *
 * <p>
 * We have intentionally avoided implementing a particular threading model for processing. This leaves several
 * options for implementing multi-threaded processing of records.
 *
 * <h4>1. One Consumer Per Thread</h4>
 * <p>
 * A simple option is to give each thread its own consumer instance. Here are the pros and cons of this approach:
 * <ul>
 * <li><b>PRO</b>: It is the easiest to implement
 * <li><b>PRO</b>: It is often the fastest as no inter-thread co-ordination is needed
 * <li><b>PRO</b>: It makes in-order processing on a per-partition basis very easy to implement (each thread just
 * processes messages in the order it receives them).
 * <li><b>CON</b>: More consumers means more TCP connections to the cluster (one per thread). In general Kafka handles
 * connections very efficiently so this is generally a small cost.
 * <li><b>CON</b>: Multiple consumers means more requests being sent to the server and slightly less batching of data
 * which can cause some drop in I/O throughput.
 * <li><b>CON</b>: The number of total threads across all processes will be limited by the total number of partitions.
 * </ul>
 *
 * <h4>2. Decouple Consumption and Processing</h4>
 * <p>
 * Another alternative is to have one or more consumer threads that do all data consumption and hands off
 * {@link ConsumerRecords} instances to a blocking queue consumed by a pool of processor threads that actually handle
 * the record processing.
 * <p>
 * This option likewise has pros and cons:
 * <ul>
 * <li><b>PRO</b>: This option allows independently scaling the number of consumers and processors. This makes it
 * possible to have a single consumer that feeds many processor threads, avoiding any limitation on partitions.
 * <li><b>CON</b>: Guaranteeing order across the processors requires particular care as the threads will execute
 * independently an earlier chunk of data may actually be processed after a later chunk of data just due to the luck of
 * thread execution timing. For processing that has no ordering requirements this is not a problem.
 * <li><b>CON</b>: Manually committing the position becomes harder as it requires that all threads co-ordinate to ensure
 * that processing is complete for that partition.
 * </ul>
 * <p>
 * There are many possible variations on this approach. For example each processor thread can have its own queue, and
 * the consumer threads can hash into these queues using the TopicPartition to ensure in-order consumption and simplify
 * commit.
 */
public class KafkaConsumer<K, V> implements Consumer<K, V> {

    private static final ConsumerDelegateCreator CREATOR = new ConsumerDelegateCreator();

    private final ConsumerDelegate<K, V> delegate;

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >here</a>. Values can be
     * either strings or objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs The consumer configs
     */
    public KafkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties The consumer configuration properties
     */
    public KafkaConsumer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A consumer is instantiated by providing a {@link java.util.Properties} object as configuration, and a
     * key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param properties        The consumer configuration properties
     * @param keyDeserializer   The deserializer for key that implements {@link Deserializer}. The configure() method
     *                          won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *                          won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Properties properties,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    /**
     * A consumer is instantiated by providing a set of key-value pairs as configuration, and a key and a value {@link Deserializer}.
     * <p>
     * Valid configuration strings are documented at {@link ConsumerConfig}.
     * <p>
     * Note: after creating a {@code KafkaConsumer} you must always {@link #close()} it to avoid resource leaks.
     *
     * @param configs           The consumer configs
     * @param keyDeserializer   The deserializer for key that implements {@link Deserializer}. The configure() method
     *                          won't be called in the consumer when the deserializer is passed in directly.
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}. The configure() method
     *                          won't be called in the consumer when the deserializer is passed in directly.
     */
    public KafkaConsumer(Map<String, Object> configs,
                         Deserializer<K> keyDeserializer,
                         Deserializer<V> valueDeserializer) {
        this(new ConsumerConfig(ConsumerConfig.appendDeserializerToConfig(configs, keyDeserializer, valueDeserializer)),
                keyDeserializer, valueDeserializer);
    }

    KafkaConsumer(ConsumerConfig config, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        delegate = CREATOR.create(config, keyDeserializer, valueDeserializer);
    }

    KafkaConsumer(LogContext logContext,
                  Time time,
                  ConsumerConfig config,
                  Deserializer<K> keyDeserializer,
                  Deserializer<V> valueDeserializer,
                  KafkaClient client,
                  SubscriptionState subscriptions,
                  ConsumerMetadata metadata,
                  List<ConsumerPartitionAssignor> assignors) {
        delegate = CREATOR.create(
                logContext,
                time,
                config,
                keyDeserializer,
                valueDeserializer,
                client,
                subscriptions,
                metadata,
                assignors
        );
    }

    /**
     * Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning
     * partitions using {@link #assign(Collection)} then this will simply return the same partitions that
     * were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned
     * to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the
     * process of getting reassigned).
     *
     * @return The set of partitions currently assigned to this consumer
     */
    public Set<TopicPartition> assignment() {
        return delegate.assignment();
    }

    /**
     * Get the current subscription. Will return the same topics used in the most recent call to
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, or an empty set if no such call has been made.
     *
     * @return The set of topics currently subscribed to
     */
    public Set<String> subscription() {
        return delegate.subscription();
    }

    /**
     * Subscribe to the given list of topics to get dynamically
     * assigned partitions. <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> Note that it is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     * <p>
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * As part of group management, the consumer will keep track of the list of consumers that belong to a particular
     * group and will trigger a rebalance operation if any one of the following events are triggered:
     * <ul>
     * <li>Number of partitions change for any of the subscribed topics
     * <li>A subscribed topic is created or deleted
     * <li>An existing member of the consumer group is shutdown or fails
     * <li>A new member is added to the consumer group
     * </ul>
     * <p>
     * When any of these events are triggered, the provided listener will be invoked first to indicate that
     * the consumer's assignment has been revoked, and then again when the new assignment has been received.
     * Note that rebalances will only occur during an active call to {@link #poll(Duration)}, so callbacks will
     * also only be invoked during that time.
     * <p>
     * The provided listener will immediately override any listener set in a previous call to subscribe.
     * It is guaranteed, however, that the partitions revoked/assigned through this interface are from topics
     * subscribed in this call. See {@link ConsumerRebalanceListener} for more details.
     *
     * @param topics   The list of topics to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If topics is null or contains null or empty elements, or if listener is null
     * @throws IllegalStateException    If {@code subscribe()} is called previously with pattern, or assign is called
     *                                  previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                                  configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        delegate.subscribe(topics, listener);
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * <b>Topic subscriptions are not incremental. This list will replace the current
     * assignment (if there is one).</b> It is not possible to combine topic subscription with group management
     * with manual partition assignment through {@link #assign(Collection)}.
     * <p>
     * If the given list of topics is empty, it is treated the same as {@link #unsubscribe()}.
     *
     * <p>
     * This is a short-hand for {@link #subscribe(Collection, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Collection, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param topics The list of topics to subscribe to
     * @throws IllegalArgumentException If topics is null or contains null or empty elements
     * @throws IllegalStateException    If {@code subscribe()} is called previously with pattern, or assign is called
     *                                  previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                                  configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Collection<String> topics) {
        delegate.subscribe(topics);
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of check.
     * This can be controlled through the {@code metadata.max.age.ms} configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching topics.
     * <p>
     * See {@link #subscribe(Collection, ConsumerRebalanceListener)} for details on the
     * use of the {@link ConsumerRebalanceListener}. Generally rebalances are triggered when there
     * is a change to the topics matching the provided pattern and when consumer group membership changes.
     * Group rebalances only take place during an active call to {@link #poll(Duration)}.
     *
     * @param pattern  Pattern to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation for the
     *                 subscribed topics
     * @throws IllegalArgumentException If pattern or listener is null
     * @throws IllegalStateException    If {@code subscribe()} is called previously with topics, or assign is called
     *                                  previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                                  configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        delegate.subscribe(pattern, listener);
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against topics existing at the time of check.
     * <p>
     * This is a short-hand for {@link #subscribe(Pattern, ConsumerRebalanceListener)}, which
     * uses a no-op listener. If you need the ability to seek to particular offsets, you should prefer
     * {@link #subscribe(Pattern, ConsumerRebalanceListener)}, since group rebalances will cause partition offsets
     * to be reset. You should also provide your own listener if you are doing your own offset
     * management since the listener gives you an opportunity to commit offsets before a rebalance finishes.
     *
     * @param pattern Pattern to subscribe to
     * @throws IllegalArgumentException If pattern is null
     * @throws IllegalStateException    If {@code subscribe()} is called previously with topics, or assign is called
     *                                  previously (without a subsequent call to {@link #unsubscribe()}), or if not
     *                                  configured at-least one partition assignment strategy
     */
    @Override
    public void subscribe(Pattern pattern) {
        delegate.subscribe(pattern);
    }

    /**
     * Unsubscribe from topics currently subscribed with {@link #subscribe(Collection)} or {@link #subscribe(Pattern)}.
     * This also clears any partitions directly assigned through {@link #assign(Collection)}.
     *
     * @throws org.apache.kafka.common.KafkaException for any other unrecoverable errors (e.g. rebalance callback errors)
     */
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    /**
     * Manually assign a list of partitions to this consumer. This interface does not allow for incremental assignment
     * and will replace the previous assignment (if there is one).
     * <p>
     * If the given list of topic partitions is empty, it is treated the same as {@link #unsubscribe()}.
     * <p>
     * Manual topic assignment through this method does not use the consumer's group management
     * functionality. As such, there will be no rebalance operation triggered when group membership or cluster and topic
     * metadata change. Note that it is not possible to use both manual partition assignment with {@link #assign(Collection)}
     * and group assignment with {@link #subscribe(Collection, ConsumerRebalanceListener)}.
     * <p>
     * If auto-commit is enabled, an async commit (based on the old assignment) will be triggered before the new
     * assignment replaces the old one.
     *
     * @param partitions The list of partitions to assign this consumer
     * @throws IllegalArgumentException If partitions is null or contains null or empty topics
     * @throws IllegalStateException    If {@code subscribe()} is called previously with topics or pattern
     *                                  (without a subsequent call to {@link #unsubscribe()})
     */
    @Override
    public void assign(Collection<TopicPartition> partitions) {
        delegate.assign(partitions);
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     * @param timeoutMs The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
     *                  If 0, returns immediately with any records that are available currently in the buffer, else returns empty.
     *                  Must not be negative.
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if the offset for a partition or set of
     *                                                                  partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException           if {@link #wakeup()} is called before or while this
     *                                                                  function is called
     * @throws org.apache.kafka.common.errors.InterruptException        if the calling thread is interrupted before or while
     *                                                                  this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException   if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException    if caller lacks Read access to any of the subscribed
     *                                                                  topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                   for any other unrecoverable errors (e.g. invalid groupId or
     *                                                                  session timeout, errors deserializing key/value pairs, or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException                       if the timeout value is negative
     * @throws java.lang.IllegalStateException                          if the consumer is not subscribed to any topics or manually assigned any
     *                                                                  partitions to consume from
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     * @deprecated Since 2.0. Use {@link #poll(Duration)}, which does not block beyond the timeout awaiting partition
     * assignment. See <a href="https://cwiki.apache.org/confluence/x/5kiHB">KIP-266</a> for more information.
     */
    @Deprecated
    @Override
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return delegate.poll(timeoutMs);
    }

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs. It is an error to not have
     * subscribed to any topics or partitions before polling for data.
     * <p>
     * On each poll, consumer will try to use the last consumed offset as the starting offset and fetch sequentially. The last
     * consumed offset can be manually set through {@link #seek(TopicPartition, long)} or automatically set as the last committed
     * offset for the subscribed list of partitions
     *
     * <p>
     * This method returns immediately if there are records available or if the position advances past control records
     * or aborted transactions when isolation.level=read_committed.
     * Otherwise, it will await the passed timeout. If the timeout expires, an empty record set will be returned.
     * Note that this method may block beyond the timeout in order to execute custom
     * {@link ConsumerRebalanceListener} callbacks.
     *
     * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
     * @return map of topic to records since the last fetch for the subscribed list of topics and partitions
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException   if the offset for a partition or set of
     *                                                                    partitions is undefined or out of range and no offset reset policy has been configured
     * @throws org.apache.kafka.common.errors.WakeupException             if {@link #wakeup()} is called before or while this
     *                                                                    function is called
     * @throws org.apache.kafka.common.errors.InterruptException          if the calling thread is interrupted before or while
     *                                                                    this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException     if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException      if caller lacks Read access to any of the subscribed
     *                                                                    topics or to the configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                     for any other unrecoverable errors (e.g. invalid groupId or
     *                                                                    session timeout, errors deserializing key/value pairs, your rebalance callback thrown exceptions,
     *                                                                    or any new error cases in future versions)
     * @throws java.lang.IllegalArgumentException                         if the timeout value is negative
     * @throws java.lang.IllegalStateException                            if the consumer is not subscribed to any topics or manually assigned any
     *                                                                    partitions to consume from
     * @throws java.lang.ArithmeticException                              if the timeout is greater than {@link Long#MAX_VALUE} milliseconds.
     * @throws org.apache.kafka.common.errors.InvalidTopicException       if the current subscription contains any invalid
     *                                                                    topic (per {@link org.apache.kafka.common.internals.Topic#validate(String)})
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *                                                                    when the broker doesn't support this feature
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException   if this consumer instance gets fenced by broker.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        return delegate.poll(timeout);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException     if the commit failed and cannot be retried.
     *                                                                     This fatal error can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *                                                                     or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *                                                                     when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *                                                                     consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *                                                                     so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *                                                                     complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *                                                                     NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *                                                                     and also for those partitions that are still assigned their fetch positions may have changed too
     *                                                                     if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException              if {@link #wakeup()} is called before or while this
     *                                                                     function is called
     * @throws org.apache.kafka.common.errors.InterruptException           if the calling thread is interrupted before or while
     *                                                                     this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException      if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException       if not authorized to the topic or to the
     *                                                                     configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                      for any other unrecoverable errors (e.g. if offset metadata
     *                                                                     is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException             if the timeout specified by {@code default.api.timeout.ms} expires
     *                                                                     before successful completion of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException    if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync() {
        delegate.commitSync();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the passed timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @throws org.apache.kafka.clients.consumer.CommitFailedException     if the commit failed and cannot be retried.
     *                                                                     This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *                                                                     or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *                                                                     when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *                                                                     consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *                                                                     so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *                                                                     complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *                                                                     NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *                                                                     and also for those partitions that are still assigned their fetch positions may have changed too
     *                                                                     if more records are returned from the {@link #poll(Duration)} call.
     * @throws org.apache.kafka.common.errors.WakeupException              if {@link #wakeup()} is called before or while this
     *                                                                     function is called
     * @throws org.apache.kafka.common.errors.InterruptException           if the calling thread is interrupted before or while
     *                                                                     this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException      if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException       if not authorized to the topic or to the
     *                                                                     configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                      for any other unrecoverable errors (e.g. if offset metadata
     *                                                                     is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException             if the timeout expires before successful completion
     *                                                                     of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException    if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(Duration timeout) {
        delegate.commitSync(timeout);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @throws org.apache.kafka.clients.consumer.CommitFailedException     if the commit failed and cannot be retried.
     *                                                                     This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *                                                                     or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *                                                                     when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *                                                                     consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *                                                                     so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *                                                                     complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *                                                                     NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *                                                                     and also for those partitions that are still assigned their fetch positions may have changed too
     *                                                                     if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *                                                                     you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException              if {@link #wakeup()} is called before or while this
     *                                                                     function is called
     * @throws org.apache.kafka.common.errors.InterruptException           if the calling thread is interrupted before or while
     *                                                                     this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException      if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException       if not authorized to the topic or to the
     *                                                                     configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException                          if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException                      for any other unrecoverable errors (e.g. if offset metadata
     *                                                                     is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException             if the timeout expires before successful completion
     *                                                                     of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException    if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is a synchronous commit and will block until either the commit succeeds, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     * <p>
     * Note that asynchronous offset commits sent previously with the {@link #commitAsync(OffsetCommitCallback)}
     * (or similar) are guaranteed to have their callbacks invoked prior to completion of this method.
     *
     * @param offsets A map of offsets by partition with associated metadata
     * @param timeout The maximum amount of time to await completion of the offset commit
     * @throws org.apache.kafka.clients.consumer.CommitFailedException     if the commit failed and cannot be retried.
     *                                                                     This can only occur if you are using automatic group management with {@link #subscribe(Collection)},
     *                                                                     or if there is an active group with the same <code>group.id</code> which is using group management. In such cases,
     *                                                                     when you are trying to commit to partitions that are no longer assigned to this consumer because the
     *                                                                     consumer is for example no longer part of the group this exception would be thrown.
     * @throws org.apache.kafka.common.errors.RebalanceInProgressException if the consumer instance is in the middle of a rebalance
     *                                                                     so it is not yet determined which partitions would be assigned to the consumer. In such cases you can first
     *                                                                     complete the rebalance by calling {@link #poll(Duration)} and commit can be reconsidered afterwards.
     *                                                                     NOTE when you reconsider committing after the rebalance, the assigned partitions may have changed,
     *                                                                     and also for those partitions that are still assigned their fetch positions may have changed too
     *                                                                     if more records are returned from the {@link #poll(Duration)} call, so when you retry committing
     *                                                                     you should consider updating the passed in {@code offset} parameter.
     * @throws org.apache.kafka.common.errors.WakeupException              if {@link #wakeup()} is called before or while this
     *                                                                     function is called
     * @throws org.apache.kafka.common.errors.InterruptException           if the calling thread is interrupted before or while
     *                                                                     this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException      if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException       if not authorized to the topic or to the
     *                                                                     configured groupId. See the exception for more details
     * @throws java.lang.IllegalArgumentException                          if the committed offset is negative
     * @throws org.apache.kafka.common.KafkaException                      for any other unrecoverable errors (e.g. if offset metadata
     *                                                                     is too large or if the topic does not exist).
     * @throws org.apache.kafka.common.errors.TimeoutException             if the timeout expires before successful completion
     *                                                                     of the offset commit
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException    if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout) {
        delegate.commitSync(offsets, timeout);
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration)} for all the subscribed list of topics and partition.
     * Same as {@link #commitAsync(OffsetCommitCallback) commitAsync(null)}
     *
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync() {
        delegate.commitAsync();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for the subscribed list of topics and partitions.
     * <p>
     * This commits offsets only to Kafka. The offsets committed using this API will be used on the first fetch after
     * every rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        delegate.commitAsync(callback);
    }

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * <p>
     * This commits offsets to Kafka. The offsets committed using this API will be used on the first fetch after every
     * rebalance and also on startup. As such, if you need to store offsets in anything other than Kafka, this API
     * should not be used. The committed offset should be the next message your application will consume,
     * i.e. lastProcessedMessageOffset + 1. If automatic group management with {@link #subscribe(Collection)} is used,
     * then the committed offsets must belong to the currently auto-assigned partitions.
     * <p>
     * This is an asynchronous call and will not block. Any errors encountered are either passed to the callback
     * (if provided) or discarded.
     * <p>
     * Offsets committed through multiple calls to this API are guaranteed to be sent in the same order as
     * the invocations. Corresponding commit callbacks are also invoked in the same order. Additionally note that
     * offsets committed through this API are guaranteed to complete before a subsequent call to {@link #commitSync()}
     * (and variants) returns.
     *
     * @param offsets  A map of offsets by partition with associate metadata. This map will be copied internally, so it
     *                 is safe to mutate the map after returning.
     * @param callback Callback to invoke when the commit completes
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this consumer instance gets fenced by broker.
     */
    @Override
    public void commitAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        delegate.commitAsync(offsets, callback);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets
     * <p>
     * The next Consumer Record which will be retrieved when poll() is invoked will have the offset specified, given that
     * a record with that offset exists (i.e. it is a valid offset).
     * <p>
     * {@link #seekToBeginning(Collection)} will go to the first offset in the topic.
     * seek(0) is equivalent to seekToBeginning for a TopicPartition with beginning offset 0,
     * assuming that there is a record at offset 0 still available.
     * {@link #seekToEnd(Collection)} is equivalent to seeking to the last offset of the partition, but behavior depends on
     * {@code isolation.level}, so see {@link #seekToEnd(Collection)} documentation for more details.
     * <p>
     * Seeking to the offset smaller than the log start offset or larger than the log end offset
     * means an invalid offset is reached.
     * Invalid offset behaviour is controlled by the {@code auto.offset.reset} property.
     * If this is set to "earliest", the next poll will return records from the starting offset.
     * If it is set to "latest", it will seek to the last offset (similar to seekToEnd()).
     * If it is set to "none", an {@code OffsetOutOfRangeException} will be thrown.
     * <p>
     * Note that, the seek offset won't change to the in-flight fetch request, it will take effect in next fetch request.
     * So, the consumer might wait for {@code fetch.max.wait.ms} before starting to fetch the records from desired offset.
     *
     * @param partition the TopicPartition on which the seek will be performed.
     * @param offset    the next offset returned by poll().
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException    if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, long offset) {
        delegate.seek(partition, offset);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next {@link #poll(Duration) poll(timeout)}. If this API
     * is invoked for the same partition more than once, the latest offset will be used on the next poll(). Note that
     * you may lose data if this API is arbitrarily used in the middle of consumption, to reset the fetch offsets. This
     * method allows for setting the leaderEpoch along with the desired offset.
     *
     * @throws IllegalArgumentException if the provided offset is negative
     * @throws IllegalStateException    if the provided TopicPartition is not assigned to this consumer
     */
    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        delegate.seek(partition, offsetAndMetadata);
    }

    /**
     * Seek to the first offset for each of the given partitions. This function evaluates lazily, seeking to the
     * first offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException    if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        delegate.seekToBeginning(partitions);
    }

    /**
     * Seek to the last offset for each of the given partitions. This function evaluates lazily, seeking to the
     * final offset in all partitions only when {@link #poll(Duration)} or {@link #position(TopicPartition)} are called.
     * If no partitions are provided, seek to the final offset for all of the currently assigned partitions.
     * <p>
     * If {@code isolation.level=read_committed}, the end offset will be the Last Stable Offset, i.e., the offset
     * of the first message with an open transaction.
     *
     * @throws IllegalArgumentException if {@code partitions} is {@code null}
     * @throws IllegalStateException    if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        delegate.seekToEnd(partitions);
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position for the given partition.
     * <p>
     * This call will block until either the position could be determined or an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout specified by {@code default.api.timeout.ms} expires
     * (in which case a {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partition The partition to get the position for
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException                                      if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException   if no offset is currently defined for
     *                                                                    the partition
     * @throws org.apache.kafka.common.errors.WakeupException             if {@link #wakeup()} is called before or while this
     *                                                                    function is called
     * @throws org.apache.kafka.common.errors.InterruptException          if the calling thread is interrupted before or while
     *                                                                    this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException     if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException      if not authorized to the topic or to the
     *                                                                    configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *                                                                    when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException                     for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException            if the position cannot be determined before the
     *                                                                    timeout specified by {@code default.api.timeout.ms} expires
     */
    @Override
    public long position(TopicPartition partition) {
        return delegate.position(partition);
    }

    /**
     * Get the offset of the <i>next record</i> that will be fetched (if a record with that offset exists).
     * This method may issue a remote call to the server if there is no current position
     * for the given partition.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to get the position for
     * @param timeout   The maximum amount of time to await determination of the current position
     * @return The current position of the consumer (that is, the offset of the next record to be fetched)
     * @throws IllegalStateException                                    if the provided TopicPartition is not assigned to this consumer
     * @throws org.apache.kafka.clients.consumer.InvalidOffsetException if no offset is currently defined for
     *                                                                  the partition
     * @throws org.apache.kafka.common.errors.WakeupException           if {@link #wakeup()} is called before or while this
     *                                                                  function is called
     * @throws org.apache.kafka.common.errors.InterruptException        if the calling thread is interrupted before or while
     *                                                                  this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException          if the position cannot be determined before the
     *                                                                  passed timeout expires
     * @throws org.apache.kafka.common.errors.AuthenticationException   if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException    if not authorized to the topic or to the
     *                                                                  configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                   for any other unrecoverable errors
     */
    @Override
    public long position(TopicPartition partition, final Duration timeout) {
        return delegate.position(partition, timeout);
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call will do a remote call to get the latest committed offset from the server, and will block until the
     * committed offset is gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partition The partition to check
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException         if {@link #wakeup()} is called before or while this
     *                                                                function is called
     * @throws org.apache.kafka.common.errors.InterruptException      if the calling thread is interrupted before or while
     *                                                                this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the topic or to the
     *                                                                configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                 for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException        if the committed offset cannot be found before
     *                                                                the timeout specified by {@code default.api.timeout.ms} expires.
     * @deprecated since 2.4 Use {@link #committed(Set)} instead
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition) {
        return delegate.committed(partition);
    }

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or
     * another). This offset will be used as the position for the consumer in the event of a failure.
     * <p>
     * This call will block until the position can be determined, an unrecoverable error is
     * encountered (in which case it is thrown to the caller), or the timeout expires.
     *
     * @param partition The partition to check
     * @param timeout   The maximum amount of time to await the current committed offset
     * @return The last committed offset and metadata or null if there was no prior commit
     * @throws org.apache.kafka.common.errors.WakeupException         if {@link #wakeup()} is called before or while this
     *                                                                function is called
     * @throws org.apache.kafka.common.errors.InterruptException      if the calling thread is interrupted before or while
     *                                                                this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the topic or to the
     *                                                                configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                 for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException        if the committed offset cannot be found before
     *                                                                expiration of the timeout
     * @deprecated since 2.4 Use {@link #committed(Set, Duration)} instead
     */
    @Deprecated
    @Override
    public OffsetAndMetadata committed(TopicPartition partition, final Duration timeout) {
        return delegate.committed(partition, timeout);
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will do a remote call to get the latest committed offsets from the server, and will block until the
     * committed offsets are gotten successfully, an unrecoverable error is encountered (in which case it is thrown to
     * the caller), or the timeout specified by {@code default.api.timeout.ms} expires (in which case a
     * {@link org.apache.kafka.common.errors.TimeoutException} is thrown to the caller).
     *
     * @param partitions The partitions to check
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     * partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException             if {@link #wakeup()} is called before or while this
     *                                                                    function is called
     * @throws org.apache.kafka.common.errors.InterruptException          if the calling thread is interrupted before or while
     *                                                                    this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException     if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException      if not authorized to the topic or to the
     *                                                                    configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the consumer attempts to fetch stable offsets
     *                                                                    when the broker doesn't support this feature
     * @throws org.apache.kafka.common.KafkaException                     for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException            if the committed offset cannot be found before
     *                                                                    the timeout specified by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return delegate.committed(partitions);
    }

    /**
     * Get the last committed offsets for the given partitions (whether the commit happened by this process or
     * another). The returned offsets will be used as the position for the consumer in the event of a failure.
     * <p>
     * If any of the partitions requested do not exist, an exception would be thrown.
     * <p>
     * This call will block to do a remote call to get the latest committed offsets from the server.
     *
     * @param partitions The partitions to check
     * @param timeout    The maximum amount of time to await the latest committed offsets
     * @return The latest committed offsets for the given partitions; {@code null} will be returned for the
     * partition if there is no such message.
     * @throws org.apache.kafka.common.errors.WakeupException         if {@link #wakeup()} is called before or while this
     *                                                                function is called
     * @throws org.apache.kafka.common.errors.InterruptException      if the calling thread is interrupted before or while
     *                                                                this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the topic or to the
     *                                                                configured groupId. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                 for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException        if the committed offset cannot be found before
     *                                                                expiration of the timeout
     */
    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions, final Duration timeout) {
        return delegate.committed(partitions, timeout);
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * this specific client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destinations.
     * <p>
     * If telemetry is enabled, this will first require a connection to the cluster to generate
     * the unique client instance ID. This method waits up to {@code timeout} for the consumer
     * client to complete the request.
     * <p>
     * Client telemetry is controlled by the {@link ConsumerConfig#ENABLE_METRICS_PUSH_CONFIG}
     * configuration option.
     *
     * @param timeout The maximum time to wait for consumer client to determine its client instance ID.
     *                The value must be non-negative. Specifying a timeout of zero means do not
     *                wait for the initial request to complete if it hasn't already.
     * @return The client's assigned instance id used for metrics collection.
     * @throws InterruptException       If the thread is interrupted while blocked.
     * @throws KafkaException           If an unexpected error occurs while trying to determine the client
     *                                  instance ID, though this error does not necessarily imply the
     *                                  consumer client is otherwise unusable.
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws IllegalStateException    If telemetry is not enabled ie, config `{@code enable.metrics.push}`
     *                                  is set to `{@code false}`.
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        return delegate.clientInstanceId(timeout);
    }

    /**
     * Get the metrics kept by the consumer
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return delegate.metrics();
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException         if {@link #wakeup()} is called before or while this
     *                                                                function is called
     * @throws org.apache.kafka.common.errors.InterruptException      if the calling thread is interrupted before or while
     *                                                                this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the specified topic. See the exception for more details
     * @throws org.apache.kafka.common.KafkaException                 for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException        if the offset metadata could not be fetched before
     *                                                                the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return delegate.partitionsFor(topic);
    }

    /**
     * Get metadata about the partitions for a given topic. This method will issue a remote call to the server if it
     * does not already have any metadata about the given topic.
     *
     * @param topic   The topic to get partition metadata for
     * @param timeout The maximum of time to await topic metadata
     * @return The list of partitions, which will be empty when the given topic is not found
     * @throws org.apache.kafka.common.errors.WakeupException         if {@link #wakeup()} is called before or while this
     *                                                                function is called
     * @throws org.apache.kafka.common.errors.InterruptException      if the calling thread is interrupted before or while
     *                                                                this function is called
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the specified topic. See
     *                                                                the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException        if topic metadata cannot be fetched before expiration
     *                                                                of the passed timeout
     * @throws org.apache.kafka.common.KafkaException                 for any other unrecoverable errors
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        return delegate.partitionsFor(topic, timeout);
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     *
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException    if {@link #wakeup()} is called before or while this
     *                                                           function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *                                                           this function is called
     * @throws org.apache.kafka.common.KafkaException            for any other unrecoverable errors
     * @throws org.apache.kafka.common.errors.TimeoutException   if the offset metadata could not be fetched before
     *                                                           the amount of time allocated by {@code default.api.timeout.ms} expires.
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return delegate.listTopics();
    }

    /**
     * Get metadata about partitions for all topics that the user is authorized to view. This method will issue a
     * remote call to the server.
     *
     * @param timeout The maximum time this operation will block to fetch topic metadata
     * @return The map of topics and its partitions
     * @throws org.apache.kafka.common.errors.WakeupException    if {@link #wakeup()} is called before or while this
     *                                                           function is called
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
     *                                                           this function is called
     * @throws org.apache.kafka.common.errors.TimeoutException   if the topic metadata could not be fetched before
     *                                                           expiration of the passed timeout
     * @throws org.apache.kafka.common.KafkaException            for any other unrecoverable errors
     */
    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        return delegate.listTopics(timeout);
    }

    /**
     * Suspend fetching from the requested partitions. Future calls to {@link #poll(Duration)} will not return
     * any records from these partitions until they have been resumed using {@link #resume(Collection)}.
     * Note that this method does not affect partition subscription. In particular, it does not cause a group
     * rebalance when automatic assignment is used.
     * <p>
     * Note: Rebalance will not preserve the pause/resume state.
     *
     * @param partitions The partitions which should be paused
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void pause(Collection<TopicPartition> partitions) {
        delegate.pause(partitions);
    }

    /**
     * Resume specified partitions which have been paused with {@link #pause(Collection)}. New calls to
     * {@link #poll(Duration)} will return records from these partitions if there are any to be fetched.
     * If the partitions were not previously paused, this method is a no-op.
     *
     * @param partitions The partitions which should be resumed
     * @throws IllegalStateException if any of the provided partitions are not currently assigned to this consumer
     */
    @Override
    public void resume(Collection<TopicPartition> partitions) {
        delegate.resume(partitions);
    }

    /**
     * Get the set of partitions that were previously paused by a call to {@link #pause(Collection)}.
     *
     * @return The set of paused partitions
     */
    @Override
    public Set<TopicPartition> paused() {
        return delegate.paused();
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     * <p>
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     * than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     * such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException     if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException      if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException                                   if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException            if the offset metadata could not be fetched before
     *                                                                    the amount of time allocated by {@code default.api.timeout.ms} expires.
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *                                                                    the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return delegate.offsetsForTimes(timestampsToSearch);
    }

    /**
     * Look up the offsets for the given partitions by timestamp. The returned offset for each partition is the
     * earliest offset whose timestamp is greater than or equal to the given timestamp in the corresponding partition.
     * <p>
     * This is a blocking call. The consumer does not have to be assigned the partitions.
     * If the message format version in a partition is before 0.10.0, i.e. the messages do not have timestamps, null
     * will be returned for that partition.
     *
     * @param timestampsToSearch the mapping from partition to the timestamp to look up.
     * @param timeout            The maximum amount of time to await retrieval of the offsets
     * @return a mapping from partition to the timestamp and offset of the first message with timestamp greater
     * than or equal to the target timestamp. {@code null} will be returned for the partition if there is no
     * such message.
     * @throws org.apache.kafka.common.errors.AuthenticationException     if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException      if not authorized to the topic(s). See the exception for more details
     * @throws IllegalArgumentException                                   if the target timestamp is negative
     * @throws org.apache.kafka.common.errors.TimeoutException            if the offset metadata could not be fetched before
     *                                                                    expiration of the passed timeout
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException if the broker does not support looking up
     *                                                                    the offsets by timestamp
     */
    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        return delegate.offsetsForTimes(timestampsToSearch, timeout);
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @param partitions the partitions to get the earliest offsets.
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException        if the offset metadata could not be fetched before
     *                                                                expiration of the configured {@code default.api.timeout.ms}
     * @see #seekToBeginning(Collection)
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return delegate.beginningOffsets(partitions);
    }

    /**
     * Get the first offset for the given partitions.
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @param partitions the partitions to get the earliest offsets
     * @param timeout    The maximum amount of time to await retrieval of the beginning offsets
     * @return The earliest available offsets for the given partitions
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException        if the offset metadata could not be fetched before
     *                                                                expiration of the passed timeout
     * @see #seekToBeginning(Collection)
     */
    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return delegate.beginningOffsets(partitions, timeout);
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @param partitions the partitions to get the end offsets.
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException        if the offset metadata could not be fetched before
     *                                                                the amount of time allocated by {@code default.api.timeout.ms} expires
     * @see #seekToEnd(Collection)
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return delegate.endOffsets(partitions);
    }

    /**
     * Get the end offsets for the given partitions. In the default {@code read_uncommitted} isolation level, the end
     * offset is the high watermark (that is, the offset of the last successfully replicated message plus one). For
     * {@code read_committed} consumers, the end offset is the last stable offset (LSO), which is the minimum of
     * the high watermark and the smallest offset of any open transaction. Finally, if the partition has never been
     * written to, the end offset is 0.
     *
     * <p>
     * This method does not change the current consumer position of the partitions.
     *
     * @param partitions the partitions to get the end offsets.
     * @param timeout    The maximum amount of time to await retrieval of the end offsets
     * @return The end offsets for the given partitions.
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws org.apache.kafka.common.errors.AuthorizationException  if not authorized to the topic(s). See the exception for more details
     * @throws org.apache.kafka.common.errors.TimeoutException        if the offsets could not be fetched before
     *                                                                expiration of the passed timeout
     * @see #seekToEnd(Collection)
     */
    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return delegate.endOffsets(partitions, timeout);
    }

    /**
     * Get the consumer's current lag on the partition. Returns an "empty" {@link OptionalLong} if the lag is not known,
     * for example if there is no position yet, or if the end offset is not known yet.
     *
     * <p>
     * This method uses locally cached metadata. If the log end offset is not known yet, it triggers a request to fetch
     * the log end offset, but returns immediately.
     *
     * @param topicPartition The partition to get the lag for.
     * @return This {@code Consumer} instance's current lag for the given partition.
     * @throws IllegalStateException if the {@code topicPartition} is not assigned
     */
    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        return delegate.currentLag(topicPartition);
    }

    /**
     * Return the current group metadata associated with this consumer.
     *
     * @return consumer group metadata
     * @throws org.apache.kafka.common.errors.InvalidGroupIdException if consumer does not have a group
     */
    @Override
    public ConsumerGroupMetadata groupMetadata() {
        return delegate.groupMetadata();
    }

    /**
     * Alert the consumer to trigger a new rebalance by rejoining the group. This is a nonblocking call that forces
     * the consumer to trigger a new rebalance on the next {@link #poll(Duration)} call. Note that this API does not
     * itself initiate the rebalance, so you must still call {@link #poll(Duration)}. If a rebalance is already in
     * progress this call will be a no-op. If you wish to force an additional rebalance you must complete the current
     * one by calling poll before retrying this API.
     * <p>
     * You do not need to call this during normal processing, as the consumer group will manage itself
     * automatically and rebalance when necessary. However there may be situations where the application wishes to
     * trigger a rebalance that would otherwise not occur. For example, if some condition external and invisible to
     * the Consumer and its group changes in a way that would affect the userdata encoded in the
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription Subscription}, the Consumer
     * will not be notified and no rebalance will occur. This API can be used to force the group to rebalance so that
     * the assignor can perform a partition reassignment based on the latest userdata. If your assignor does not use
     * this userdata, or you do not use a custom
     * {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor ConsumerPartitionAssignor}, you should not
     * use this API.
     *
     * @param reason The reason why the new rebalance is needed.
     * @throws java.lang.IllegalStateException if the consumer does not use group subscription
     */
    @Override
    public void enforceRebalance(final String reason) {
        delegate.enforceRebalance(reason);
    }

    /**
     * @see #enforceRebalance(String)
     */
    @Override
    public void enforceRebalance() {
        delegate.enforceRebalance();
    }

    /**
     * Close the consumer, waiting for up to the default timeout of 30 seconds for any needed cleanup.
     * If auto-commit is enabled, this will commit the current offsets if possible within the default
     * timeout. See {@link #close(Duration)} for details. Note that {@link #wakeup()}
     * cannot be used to interrupt close.
     *
     * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted
     *                                                           before or while this function is called
     * @throws org.apache.kafka.common.KafkaException            for any other error during close
     */
    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Tries to close the consumer cleanly within the specified timeout. This method waits up to
     * {@code timeout} for the consumer to complete pending commits and leave the group.
     * If auto-commit is enabled, this will commit the current offsets if possible within the
     * timeout. If the consumer is unable to complete offset commits and gracefully leave the group
     * before the timeout expires, the consumer is force closed. Note that {@link #wakeup()} cannot be
     * used to interrupt close.
     *
     * @param timeout The maximum time to wait for consumer to close gracefully. The value must be
     *                non-negative. Specifying a timeout of zero means do not wait for pending requests to complete.
     * @throws IllegalArgumentException               If the {@code timeout} is negative.
     * @throws InterruptException                     If the thread is interrupted before or while this function is called
     * @throws org.apache.kafka.common.KafkaException for any other error during close
     */
    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    /**
     * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long poll.
     * The thread which is blocking in an operation will throw {@link org.apache.kafka.common.errors.WakeupException}.
     * If no thread is blocking in a method which can throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method will raise it instead.
     */
    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    // Functions below are for testing only
    String clientId() {
        return delegate.clientId();
    }

    Metrics metricsRegistry() {
        return delegate.metricsRegistry();
    }

    KafkaConsumerMetrics kafkaConsumerMetrics() {
        return delegate.kafkaConsumerMetrics();
    }

    boolean updateAssignmentMetadataIfNeeded(final Timer timer) {
        return delegate.updateAssignmentMetadataIfNeeded(timer);
    }
}
