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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.clients.producer.internals.KafkaProducerMetrics;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;


/**
 * kafka client发送记录到kafka cluster
 *
 * <p>该生产者是线程安全的，并在多个线程中共享一个生产者实例。其处理速度超过多个实例。
 *
 * <p>代码实例:
 * <pre>{@code
 *  Properties props = new Properties();
 *  props.put("bootstrap.servers", "localhost:9092");
 *  props.put("linger.ms", 1);
 *  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 *  Producer<String, String> producer = new KafkaProducer<>(props);
 *  for(int i = 0; i < 100; i++) {
 *      producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 *  producer.close();
 * }</pre>
 *
 * <p>producer保存了缓冲区，用于保存尚未被发送到kafka cluster的记录，这些记录会被后台I/O线程构造到请求，并传递给kafka cluster。
 * 在使用客户端后未能关闭将导致资源泄露。
 *
 * <p>{@link Producer#send(ProducerRecord)}是异步的，在调用该方法时，producer会将记录保存到待发送记录的缓冲区，然后立刻返回。
 * 这使得生产者能够将各个记录分批处理以提高效率。
 *
 * <p>{@code acks}配置控制了请求是否被是为完成。默认的设置{@code all}将导致记录的完整提交被阻止，速度最慢但是最持久化（最安全）的配置
 *
 * <p>如果请求失败，生产者会自动重试。重试次数{@code retries}最大为{@link Integer#MAX_VALUE}，推荐使用{@code delivery.timeout.ms}
 * 来控制重试行为，而不是通过重试次数{@code retries}来控制。
 *
 * <p>生产者为每个分片维护尚未发送的记录的缓冲区。这些缓冲区大小由{@code batch.size}参数指定。将该参数增大会产生更多的批处理，但需要更多的内存（
 * 因为生产者通常会为每一个活跃分片维护一个缓冲区）
 *
 * <p>默认情况下，缓冲区会立刻发送记录，即使还有空间未使用。但是如果想要减少发送请求的数量，可以将{@code linger.ms}设置为大于0。该参数用于控制
 * 生产者等待多少毫秒以确保该批次中可以携带更多的记录。这是类似于TCP的Nagle's算法。例如：在上述代码示例中，假如100条记录被加入到一个请求中，因为
 * 我们设置了{@code linger.ms}=1ms。然而该参数将让请求延迟1ms以等待更多的请求加入，如果缓冲池未满的话。需要注意即使将{@code linger.ms}设置为0，
 * 时间接近的记录通常也会一起批处理。所以在高负载下，无论{@code linger.ms}配置延迟如何，批处理都会发生。然后将{@code linger.ms}设置大于0，
 * 可以导致更少的批处理，当未处于最大负载时，更多高效的请求将消耗更少的延迟。
 *
 * <p>{@code buffer.memory}将控制生产者用于缓冲区的累计可用内存。如果记录发送速度超过了它们被传递到kafak集群的速度，缓冲区空间将很快被耗尽。
 * 如果缓冲区空间被耗尽，额外的{@link #send(ProducerRecord)}将会被阻塞。阻塞的时间阈值由参数{@code max.block.ms}决定，达到阻塞上限后便会抛出{@link TimeoutException}。
 *
 * <p>{@code key.serializer}和{@code value.serializer}指示如何将用户提供的{@link ProducerRecord#key()}和{@link ProducerRecord#value()}转换为字节数组。
 * 用户可以使用内部提供的{@link org.apache.kafka.common.serialization.ByteArraySerializer}或{@link org.apache.kafka.common.serialization.StringSerializer}
 * 用于简单的字节数组或字符串类型。
 *
 * <p>从kafka 0.11开始，KafkaProducer支持两种额外的模式：幂等生产者和事务生产者。幂等生产者强化了kafka的交付语义，从至少一次交付变为精确一次交付。
 * 特别是生产者重试将不再引入重复。事务生产者允许应用原子性的发送消息到多个分片（和主题）。
 *
 * <p>从kafka 3.0开始，{@code enable.idempotence}配置默认为true。当开启{@code enable.idempotence}，{@code retries}将默认为{@link Integer#MAX_VALUE}。
 * 且{@code acks}配置默认为{@code all}。对于幂等生产者来说没有API变更，因此已存在的应用无需修改即可利用此功能。
 *
 * <p>为了利用幂等生产者，必须避免应用级别的重新发送，因为这些重新发送无法进行重复数据删除。例如：如果一个应用开启了幂等生产者，推荐不要设置{@code retries}，
 * 因为其默认被设置为{@link Integer#MAX_VALUE}。此外，如果一个{@link #send(ProducerRecord)}即使无限重试仍然返回错误（例如，缓冲区中的消息在发送前已过期），
 * 此时推荐停止生产者，检查最后发送发送的消息确保没有重复。最终，kafka生产者只能保证在一个会话中被发送的消息的幂等。
 *
 * <p>为了使用事务生产者和附带的API，用户必须设置{@code transactional.id}属性。如果{@code transactional.id}被设置，将自动启用幂等及其相关配置。
 * 此外，事务中包含的主题应该被设置持久性。具体来说，{@code replication.factor}应该被至少设置为3。并且{@code min.insync.replicas}应该被至少设置为2。
 * 最后，为了实现端到端的事务保证，消费者必须也要被配置为仅读取已提交的消息。
 *
 * <p>{@code transactional.id}的目的是为跨越多个会话的单个生产者实例开启事务恢复。它通常来自于分区、有状态的应用程序中分片的标识符。
 * 因此，它对于分区应用程序内运行的每个生产者实例都是唯一的。
 *
 * <p>所有新的事务API都是阻塞的，并且在失败时抛出异常。以下的代码实例揭示了相关API如何被使用。它与上面的实例相似，除了是100条记录作为单个事务的一部分。
 * <pre>{@code
 *  Properties props = new Properties();
 *  props.put("bootstrap.servers", "localhost:9092");
 *  props.put("transactional.id", "my-transactional-id");
 *  Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 *  producer.initTransactions();
 *
 *  try {
 *      producer.beginTransaction();
 *      for (int i = 0; i < 100; i++)
 *          producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *      producer.commitTransaction();
 *  } catch(ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *      // 我们不能从异常恢复，唯一的选择就是关闭生产者并推出。
 *      producer.close();
 *  } catch(KafkaException e) {
 *      // 对于其余所有异常，仅需拒绝事务并重试
 *      producer.abortTransaction();
 *  }
 *  producer.close();
 * }</pre>
 *
 * <p>正如上述示例中所暗示的，每个生产者只能有一个打开的事务。在{@link #beginTransaction()}和{@link #commitTransaction()}之间发送的
 * 所有消息都被视作一个事务。当{@code transactional.id}被指定，被该生产者发送的所有消息必须是一个事务的一部分。
 *
 * <p>事务生产者使用异常来交流错误状态。具体来说，不需要为{@link #send(ProducerRecord)}指定回调，或者对返回值调用{@link Future#get()}方法，
 * 如果任何{@link #send()}方法或在事务期间遇到不可恢复的错误，则会抛出{@link KafkaException}。
 * 查看{@link #send(ProducerRecord)}文档获取更多关于从事务发送时检测错误的信息。
 *
 * <p>当遇到{@link KafkaException}时调用{@link #abortTransaction()}确保任何成功写请求被标记为驳回，从而实现事务保证
 *
 * <p>与broker通信的客户端在0.10.0及以上的，更老的或更新的broker可能不支持特定的客户端功能。对于示例，事务API需要broker版本在0.11.0或以上。
 * 当调用的API在运行中的broker版本中不支持时，会抛出一个{@link org.apache.kafka.common.errors.UnsupportedVersionException}异常。
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private final String clientId;
    // Visible for testing
    final Metrics metrics;
    private final KafkaProducerMetrics producerMetrics;
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final ProducerMetadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Thread ioThread;
    private final Compression compression;
    private final Sensor errors;
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    private final boolean partitionerIgnoreKeys;
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(final Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(properties, null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(Utils.propsToMap(properties), keySerializer, valueSerializer);
    }

    /**
     * Check if partitioner is deprecated and log a warning if it is.
     */
    @SuppressWarnings("deprecation")
    private void warnIfPartitionerDeprecated() {
        // Using DefaultPartitioner and UniformStickyPartitioner is deprecated, see KIP-794.
        if (partitioner instanceof org.apache.kafka.clients.producer.internals.DefaultPartitioner) {
            log.warn("DefaultPartitioner is deprecated.  Please clear " + ProducerConfig.PARTITIONER_CLASS_CONFIG
                    + " configuration setting to get the default partitioning behavior");
        }
        if (partitioner instanceof org.apache.kafka.clients.producer.UniformStickyPartitioner) {
            log.warn("UniformStickyPartitioner is deprecated.  Please clear " + ProducerConfig.PARTITIONER_CLASS_CONFIG
                    + " configuration setting and set " + ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG
                    + " to 'true' to get the uniform sticky partitioning behavior");
        }
    }

    // visible for testing
    @SuppressWarnings({"unchecked", "this-escape"})
    KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors<K, V> interceptors,
                  Time time) {
        try {
            this.producerConfig = config;
            this.time = time;

            // 获取配置的事务ID
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);

            // 获取客户端ID
            this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);

            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));

            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            this.clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            this.clientTelemetryReporter.ifPresent(reporters::add);
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            this.producerMetrics = new KafkaProducerMetrics(metrics);
            this.partitioner = config.getConfiguredInstance(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    Partitioner.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            warnIfPartitionerDeprecated();
            this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                                                         Serializer.class);
                this.keySerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                                           Serializer.class);
                this.valueSerializer.configure(config.originals(Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            List<ProducerInterceptor<K, V>> interceptorList = ClientUtils.configuredInterceptors(config,
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                    interceptorList,
                    reporters,
                    Arrays.asList(this.keySerializer, this.valueSerializer));
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            this.compression = configureCompression(config);

            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            int deliveryTimeoutMs = configureDeliveryTimeout(config, log);

            this.apiVersions = new ApiVersions();
            this.transactionManager = configureTransactionState(config, logContext);
            // There is no need to do work required for adaptive partitioning, if we use a custom partitioner.
            boolean enableAdaptivePartitioning = partitioner == null &&
                config.getBoolean(ProducerConfig.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG);
            RecordAccumulator.PartitionerConfig partitionerConfig = new RecordAccumulator.PartitionerConfig(
                enableAdaptivePartitioning,
                config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)
            );
            // As per Kafka producer configuration documentation batch.size may be set to 0 to explicitly disable
            // batching which in practice actually means using a batch size of 1.
            int batchSize = Math.max(1, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
            this.accumulator = new RecordAccumulator(logContext,
                    batchSize,
                    compression,
                    lingerMs(config),
                    retryBackoffMs,
                    retryBackoffMaxMs,
                    deliveryTimeoutMs,
                    partitionerConfig,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    apiVersions,
                    transactionManager,
                    new BufferPool(this.totalMemorySize, batchSize, metrics, time, PRODUCER_METRIC_GROUP_NAME));

            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                this.metadata = new ProducerMetadata(retryBackoffMs,
                        retryBackoffMaxMs,
                        config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                        config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
                        logContext,
                        clusterResourceListeners,
                        Time.SYSTEM);
                this.metadata.bootstrap(addresses);
            }
            this.errors = this.metrics.sensor("errors");
            this.sender = newSender(logContext, kafkaClient, this.metadata);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(Duration.ofMillis(0), true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    KafkaProducer(ProducerConfig config,
                  LogContext logContext,
                  Metrics metrics,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  TransactionManager transactionManager,
                  Sender sender,
                  ProducerInterceptors<K, V> interceptors,
                  Partitioner partitioner,
                  Time time,
                  KafkaThread ioThread,
                  Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        this.producerConfig = config;
        this.time = time;
        this.clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
        this.log = logContext.logger(KafkaProducer.class);
        this.metrics = metrics;
        this.producerMetrics = new KafkaProducerMetrics(metrics);
        this.partitioner = partitioner;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.interceptors = interceptors;
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
        this.compression = configureCompression(config);
        this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
        this.partitionerIgnoreKeys = config.getBoolean(ProducerConfig.PARTITIONER_IGNORE_KEYS_CONFIG);
        this.apiVersions = new ApiVersions();
        this.transactionManager = transactionManager;
        this.accumulator = accumulator;
        this.errors = this.metrics.sensor("errors");
        this.metadata = metadata;
        this.sender = sender;
        this.ioThread = ioThread;
        this.clientTelemetryReporter = clientTelemetryReporter;
    }

    // visible for testing
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, ProducerMetadata metadata) {
        int maxInflightRequests = producerConfig.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
        KafkaClient client = kafkaClient != null ? kafkaClient : ClientUtils.createNetworkClient(producerConfig,
                this.metrics,
                "producer",
                logContext,
                apiVersions,
                time,
                maxInflightRequests,
                metadata,
                throttleTimeSensor,
                clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));

        short acks = Short.parseShort(producerConfig.getString(ProducerConfig.ACKS_CONFIG));
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                producerConfig.getInt(ProducerConfig.RETRIES_CONFIG),
                metricsRegistry.senderMetrics,
                time,
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager,
                apiVersions);
    }

    private static Compression configureCompression(ProducerConfig config) {
        CompressionType type = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
        switch (type) {
            case GZIP: {
                return Compression.gzip()
                        .level(config.getInt(ProducerConfig.COMPRESSION_GZIP_LEVEL_CONFIG))
                        .build();
            }
            case LZ4: {
                return Compression.lz4()
                        .level(config.getInt(ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG))
                        .build();
            }
            case ZSTD: {
                return Compression.zstd()
                        .level(config.getInt(ProducerConfig.COMPRESSION_ZSTD_LEVEL_CONFIG))
                        .build();
            }
            default:
                return Compression.of(type).build();
        }
    }

    private static int lingerMs(ProducerConfig config) {
        return (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE);
    }

    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        int lingerMs = lingerMs(config);
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int lingerAndRequestTimeoutMs = (int) Math.min((long) lingerMs + requestTimeoutMs, Integer.MAX_VALUE);

        if (deliveryTimeoutMs < lingerAndRequestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // throw an exception if the user explicitly set an inconsistent value
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                deliveryTimeoutMs = lingerAndRequestTimeoutMs;
                log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
            }
        }
        return deliveryTimeoutMs;
    }

    private TransactionManager configureTransactionState(ProducerConfig config,
                                                         LogContext logContext) {
        TransactionManager transactionManager = null;

        if (config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)) {
            final String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            final int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            final long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager = new TransactionManager(
                logContext,
                transactionalId,
                transactionTimeoutMs,
                retryBackoffMs,
                apiVersions
            );

            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        } else {
            // ignore unretrieved configurations related to producer transaction
            config.ignore(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
        }
        return transactionManager;
    }

    /**
     * Needs to be called before any other methods when the {@code transactional.id} is set in the configuration.
     * This method does the following:
     * <ol>
     * <li>Ensures any transactions initiated by previous instances of the producer with the same
     *      {@code transactional.id} are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.</li>
     * <li>Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.</li>
     * </ol>
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     *
     * @throws IllegalStateException if no {@code transactional.id} has been configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException error indicating that the configured
     *         transactional.id is not authorized, or the idempotent producer id is unavailable. See the exception for
     *         more details.  User may retry this function call after fixing the permission.
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void initTransactions() {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long now = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.initializeTransactions();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        producerMetrics.recordInit(time.nanoseconds() - now);
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws IllegalStateException if no {@code transactional.id} has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         {@code transactional.id} is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long now = time.nanoseconds();
        transactionManager.beginTransaction();
        producerMetrics.recordBeginTxn(time.nanoseconds() - now);
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
     * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     *
     * <p>
     * This method is a blocking call that waits until the request has been received and acknowledged by the consumer group
     * coordinator; but the offsets are not considered as committed until the transaction itself is successfully committed later (via
     * the {@link #commitTransaction()} call).
     *
     * @throws IllegalStateException if no transactional.id has been configured, no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized, or the consumer group id is not authorized.
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws TimeoutException if the time taken for sending the offsets has surpassed <code>max.block.ms</code>.
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     *
     * @deprecated Since 3.0.0, please use {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)} instead.
     */
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code groupMetadata} should be extracted from the used {@link KafkaConsumer consumer} via
     * {@link KafkaConsumer#groupMetadata()} to leverage consumer group metadata. This will provide
     * stronger fencing than just supplying the {@code consumerGroupId} and passing in {@code new ConsumerGroupMetadata(consumerGroupId)},
     * however note that the full set of consumer group metadata returned by {@link KafkaConsumer#groupMetadata()}
     * requires the brokers to be on version 2.5 or newer to understand.
     *
     * <p>
     * This method is a blocking call that waits until the request has been received and acknowledged by the consumer group
     * coordinator; but the offsets are not considered as committed until the transaction itself is successfully committed later (via
     * the {@link #commitTransaction()} call).
     *
     * <p>
     * Note, that the consumer should have {@code enable.auto.commit=false} and should
     * also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     * This method will raise {@link TimeoutException} if the producer cannot send offsets before expiration of {@code max.block.ms}.
     * Additionally, it will raise {@link InterruptException} if interrupted.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started.
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0) or
     *         the broker doesn't support the latest version of transactional API with all consumer group metadata
     *         (i.e. if its version is lower than 2.5.0).
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized, or the consumer group id is not authorized.
     * @throws org.apache.kafka.clients.consumer.CommitFailedException if the commit failed and cannot be retried
     *         (e.g. if the consumer has been kicked out of the group). Users should handle this by aborting the transaction.
     * @throws org.apache.kafka.common.errors.FencedInstanceIdException if this producer instance gets fenced by broker due to a
     *                                                                  mis-configured consumer instance id within group metadata.
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * @throws TimeoutException if the time taken for sending the offsets has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        throwIfInvalidGroupMetadata(groupMetadata);
        throwIfNoTransactionManager();
        throwIfProducerClosed();

        if (!offsets.isEmpty()) {
            long start = time.nanoseconds();
            TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, groupMetadata);
            sender.wakeup();
            result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
            producerMetrics.recordSendOffsets(time.nanoseconds() - start);
        }
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
     * <p>
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     * <p>
     * If the transaction is committed successfully and this method returns without throwing an exception, it is guaranteed
     * that all {@link Callback callbacks} for records in the transaction will have been invoked and completed.
     * Note that exceptions thrown by callbacks are ignored; the producer proceeds to commit the transaction in any case.
     * <p>
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be committed before expiration
     * of {@code max.block.ms}, but this does not mean the request did not actually reach the broker. In fact, it only indicates
     * that we cannot get the acknowledgement response in time, so it's up to the application's logic
     * to decide how to handle timeouts.
     * Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as abortTransaction)
     * since the commit may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     * @throws TimeoutException if the time taken for committing the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        long commitStart = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        producerMetrics.recordCommitTxn(time.nanoseconds() - commitStart);
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     * <p>
     * Note that this method will raise {@link TimeoutException} if the transaction cannot be aborted before expiration
     * of {@code max.block.ms}, but this does not mean the request did not actually reach the broker. In fact, it only indicates
     * that we cannot get the acknowledgement response in time, so it's up to the application's logic
     * to decide how to handle timeouts. Additionally, it will raise {@link InterruptException} if interrupted.
     * It is safe to retry in either case, but it is not possible to attempt a different operation (such as {@link #commitTransaction})
     * since the abort may already be in the progress of completing. If not retrying, the only option is to close the producer.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.InvalidProducerEpochException if the producer has attempted to produce with an old epoch
     *         to the partition leader. See the exception for more details
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for aborting the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        throwIfProducerClosed();
        log.info("Aborting incomplete transaction");
        long abortStart = time.nanoseconds();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await(maxBlockTimeMs, TimeUnit.MILLISECONDS);
        producerMetrics.recordAbortTxn(time.nanoseconds() - abortStart);
    }

    /**
     * 异步发送一个消息到主题，该方法与{@code send(record, null)}等价。
     *
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * 异步发送一个消息到主题，并在发送被确认时调用提供的回调。
     *
     * <p>发送是异步的，一旦记录被保存到缓冲区中等待发送到服务端，方法立刻返回。
     * 此方法允许以并行的方式发送大量记录而无需阻塞等待每一个响应。
     * 有两种场景会导致发生阻塞：
     * <ul>
     *     <li>对于第一条被发送到位于kafka集群上的主题的记录。如果此时kafka集群不可访达时，将阻塞最大{@code max.block.ms}毫秒</li>。
     *     <li>在分配缓冲区时当缓冲池中没有任何可用的缓冲区时</li>
     * </ul>
     *
     * <p>发送方法返回的结果是{@link RecordMetadata}，将指明消息被发送的分片、被分配的偏移量和记录的时间戳。
     * 如果生产者配置了{@code acks}=0，{@link RecordMetadata#offset()}=-1，因为生产者不会等待broker的确认，也就是不会等待broker的响应，
     * 也就无法得知broker分配的偏移量。如果{@link org.apache.kafka.common.record.TimestampType#CREATE_TIME}被主题使用，{@link RecordMetadata#timestamp()}
     * 就是用户提供的时间戳，或者是记录发送的时间当用户未提供时间戳时。如果{@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME}被主题使用，
     * {@link RecordMetadata#timestamp()}就是Kafka的broker在追加消息的本地时间。
     *
     * <p>由于发送方法是异步的，它返回{@link Future}用于保存被分配到该记录的{@link RecordMetadata}。调用{@link Future#get()}将会阻塞，
     * 直到关联的请求完成并返回记录的元信息或在发送消息时发生异常才会接触阻塞。
     *
     * <p>如果想要模拟简单的阻塞调用，可以立即使用{@link Future#get()}。
     * <pre>{@code
     *  byte[] key= "key".getBytes();
     *  byte[] value = "value".getBytes();
     *  ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("my-topic", key, value);
     *  producer.send(record).get();
     * }</pre>
     *
     * <p>对于非阻塞使用可以通过{@link Callback}参数，在请求完成时会进行回调。
     * <pre>{@code
     *  ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>("my-topic", key, value);
     *  producer.send(record, new Callback() {
     *      pubic void onCompletion(RecordMetadata metadata, Exception e) {
     *          if (e != null) {
     *              e.printStackTrace();
     *          }
     *          else {
     *              System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *          }
     *      }
     *  });
     * }</pre>
     *
     * <p>对发送到相同分片的记录的回调是有序的。在以下实例中确保了{@code callback1}先于{@code callback2}。
     * <pre>{@code
     *  producer.send(new ProducerRecord<byte[], byte[]>(topic, partition, key1, value1), callback1);
     *  producer.send(new ProducerRecord<byte[], byte[]>(topic, partition, key2, value2), callback2);
     * }</pre>
     *
     * <p>当作为事务的一部分使用时，无需定义回调或检查future的结果来确保发送正常。如果任何发送失败并带有一个不可恢复异常，
     * 最终的{@link #commitTransaction()}将会失败，并在最后一次失败发送时抛出异常。当出现这种情况时，我们的应用应该调用{@link #abortTransaction()}
     * 来重置状态和持续发送数据。
     *
     * <p>某些事务发送错误不能直接调用{@link #abortTransaction()}。具体来说，一个事务发送以{@link ProducerFencedException}、{@link org.apache.kafka.common.errors.OutOfOrderSequenceException}、
     * {@link org.apache.kafka.common.errors.UnsupportedVersionException}结束，或者是一个{@link AuthorizationException}异常时，唯一的选择就是调用{@link #close()}。
     * 上述的致命异常将导致生产者进入失效的状态，在该状态下，后续调用继续引发包装在{@link KafkaException}中的相同底层错误。
     *
     * <p>当启用幂等时，但是未配置{@code transactional.id}。{@link org.apache.kafka.common.errors.UnsupportedVersionException}和{@link AuthorizationException}被视为致命异常。
     * 然而{@link ProducerFencedException}无需被处理。此外，在接收到{@link org.apache.kafka.common.errors.OutOfOrderSequenceException}异常后，仍然继续发送消息的话，
     * 会导致消息乱序。为了确保消息有序性，需要关闭生产者并创建一个新的实例。
     *
     * <p>如果目标主题的消息格式未升级到0.11.0.0，幂等和事务生产者发送的消息将出现{@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException}异常。
     * 如果在事务生产者发送期间出现，可能需要调用{@link #abortTransaction()}并继续发送。但需要注意后续发送到相同主题的消息还是会返回该错误，知道主题进行了版本升级。
     *
     * <p>需要注意回调通常执行在生产者的I/O线程中，回调应该非常快，否则它们会延迟从其他线程发送消息。
     * 如果你想执行阻塞或计算量大的回调，建议在回调主体中使用您自己的{@link java.util.concurrent.Executor}来并行处理。
     *
     * @param record 带发送的记录
     * @param callback 当kafka broker确认该消息后，用户指定的回调会被执行（null 代表没有回调）
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException fatal error indicating that the producer is not allowed to write
     * @throws IllegalStateException if a transactional.id has been configured and no transaction has been started, or
     *                               when send is invoked after producer has been closed.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException If the record could not be appended to the send buffer due to memory unavailable
     *                          or missing metadata within {@code max.block.ms}.
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API exceptions.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // 拦截记录，记录可能会被编辑，方法不会抛出任何异常
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        // 发送消息
        return doSend(interceptedRecord, callback);
    }

    /**
     * 验证生产者实例已被关闭，该方法抛出{@link IllegalStateException}如果生产者已被关闭
     */
    private void throwIfProducerClosed() {
        if (sender == null || !sender.isRunning())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * Call deprecated {@link Partitioner#onNewBatch}
     */
    @SuppressWarnings("deprecation")
    private void onNewBatch(String topic, Cluster cluster, int prevPartition) {
        assert partitioner != null;
        partitioner.onNewBatch(topic, cluster, prevPartition);
    }

    /**
     * 异步发送记录到主题的实现
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        // 添加的回调需要注意以下几点：
        // - 在完成时调用拦截器和用户回调
        // - 注意分片被RecordAccumulator.append所计算
        AppendCallbacks appendCallbacks = new AppendCallbacks(callback, this.interceptors, record);

        try {
            // 如果生产者已被关闭，抛出异常
            throwIfProducerClosed();
            // 首先检查要发送的主题的元信息是否存在的
            long nowMs = time.milliseconds();
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                // 获取最新的主题信息
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), nowMs, maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            nowMs += clusterAndWaitTime.waitedOnMetadataMs;
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            Cluster cluster = clusterAndWaitTime.cluster;
            byte[] serializedKey;
            try {
                // 执行键序列化
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            byte[] serializedValue;
            try {
                // 执行值序列化
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }

            // 尝试计算分区，但请注意，在此调用之后它可能是RecordMetadata.UNKNOWN_PARTITION，则意味着RecordAccumulator将使用内部逻辑
            // 选择一个分区（这考虑到代理负载、每个分区生成的数据量等）
            int partition = partition(record, serializedKey, serializedValue, cluster);

            // 设置记录头只读，不可修改
            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            // 计算序列化的字节上限
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(), compression.type(), serializedKey, serializedValue, headers);
            // 检查并确保记录大小合法
            ensureValidRecordSize(serializedSize);

            long timestamp = record.timestamp() == null ? nowMs : record.timestamp();

            // A custom partitioner may take advantage on the onNewBatch callback.
            boolean abortOnNewBatch = partitioner != null;

            // 将记录追加到累积器，需要注意：实际分区可以在哪里计算，并且可以通过appendCallbacks.topicPartition去访问
            RecordAccumulator.RecordAppendResult result = accumulator.append(record.topic(), partition, timestamp, serializedKey,
                    serializedValue, headers, appendCallbacks, remainingWaitMs, abortOnNewBatch, nowMs, cluster);

            // 检查分片
            assert appendCallbacks.getPartition() != RecordMetadata.UNKNOWN_PARTITION;

            if (result.abortForNewBatch) {
                int prevPartition = partition;
                onNewBatch(record.topic(), cluster, prevPartition);
                partition = partition(record, serializedKey, serializedValue, cluster);
                if (log.isTraceEnabled()) {
                    log.trace("Retrying append due to new batch creation for topic {} partition {}. The old partition was {}", record.topic(), partition, prevPartition);
                }
                result = accumulator.append(record.topic(), partition, timestamp, serializedKey,
                    serializedValue, headers, appendCallbacks, remainingWaitMs, false, nowMs, cluster);
            }

            // Add the partition to the transaction (if in progress) after it has been successfully
            // appended to the accumulator. We cannot do it before because the partition may be
            // unknown or the initially selected partition may be changed when the batch is closed
            // (as indicated by `abortForNewBatch`). Note that the `Sender` will refuse to dequeue
            // batches from the accumulator until they have been added to the transaction.
            if (transactionManager != null) {
                transactionManager.maybeAddPartition(appendCallbacks.topicPartition());
            }

            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), appendCallbacks.getPartition());
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null) {
                TopicPartition tp = appendCallbacks.topicPartition();
                RecordMetadata nullMetadata = new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
                callback.onCompletion(nullMetadata, e);
            }
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            if (transactionManager != null) {
                transactionManager.maybeTransitionToErrorState(e);
            }
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw new InterruptException(e);
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            this.interceptors.onSendError(record, appendCallbacks.topicPartition(), e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     * 等待包括指定主题的分区在内的集群元数据可用
     *
     * @param topic The topic we want metadata for
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     * @param nowMs The current time in ms
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long nowMs, long maxWaitMs) throws InterruptedException {
        // 获取集群信息
        Cluster cluster = metadata.fetch();

        // 如果集群的黑名单主题中包含该主题，则抛出异常
        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);

        // 如果主题尚不存在，则将其添加到元数据主题列表并重置到期日期
        metadata.add(topic, nowMs);

        // 获取该主题的分片数
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // 当分片总数为不为空且指定的分片数是合法的（为空或在有效范围内），返回缓存的元数据
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        long remainingWaitMs = maxWaitMs;
        long elapsed = 0;

        // 触发元数据请求，直到我们有了主题和被请求分区的元信息，或是达到了maxWaitTimeMs。
        // 如果元数据过时并且该主题的分区数量同时增加，这种处理是必要的。
        long nowNanos = time.nanoseconds();
        do {
            if (partition != null) {
                log.trace("Requesting metadata update for partition {} of topic {}.", partition, topic);
            } else {
                log.trace("Requesting metadata update for topic {}.", topic);
            }
            // 如果主题尚不存在，则将其添加到元数据主题列表并重置到期日期
            metadata.add(topic, nowMs + elapsed);
            // 返回请求的版本
            int version = metadata.requestUpdateForTopic(topic);
            // 唤醒发送者
            sender.wakeup();
            try {
                // 等待更新置顶版本
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw new TimeoutException(String.format("Topic %s not present in metadata after %d ms.", topic, maxWaitMs));
            }
            // 获取更新后的集群信息
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - nowMs;
            // 检查是否超时
            if (elapsed >= maxWaitMs) {
                throw new TimeoutException(partitionsCount == null ?
                        String.format("Topic %s not present in metadata after %d ms.", topic, maxWaitMs) :
                        String.format("Partition %d of topic %s with partition count %d is not present in metadata after %d ms.", partition, topic, partitionsCount, maxWaitMs));
            }
            metadata.maybeThrowExceptionForTopic(topic);
            remainingWaitMs = maxWaitMs - elapsed;
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null || (partition != null && partition >= partitionsCount));

        // 记录等待元数据的指标
        producerMetrics.recordMetadataWait(time.nanoseconds() - nowNanos);

        // 返回结果
        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than " + maxRequestSize + ", which is the value of the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG + " configuration.");
        if (size > totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commitSync();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");

        long start = time.nanoseconds();
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        } finally {
            producerMetrics.recordFlush(time.nanoseconds() - start);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, time.milliseconds(), maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * this specific client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destinations.
     * <p>
     * If telemetry is enabled, this will first require a connection to the cluster to generate
     * the unique client instance ID. This method waits up to {@code timeout} for the producer
     * client to complete the request.
     * <p>
     * Client telemetry is controlled by the {@link ProducerConfig#ENABLE_METRICS_PUSH_CONFIG}
     * configuration option.
     *
     * @param timeout The maximum time to wait for producer client to determine its client instance ID.
     *                The value must be non-negative. Specifying a timeout of zero means do not
     *                wait for the initial request to complete if it hasn't already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        producer client is otherwise unusable.
     * @throws IllegalArgumentException If the {@code timeout} is negative.
     * @throws IllegalStateException If telemetry is not enabled ie, config `{@code enable.metrics.push}`
     *                               is set to `{@code false}`.
     * @return The client's assigned instance id used for metrics collection.
     */
    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (!clientTelemetryReporter.isPresent()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + ProducerConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");
        }

        return ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately. It will also abort the ongoing transaction if it's not
     * already completing.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(Duration.ofMillis(0))</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to close the client, this error should be treated
     *                        as fatal and indicate the client is no longer usable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     *
     */
    @Override
    public void close(Duration timeout) {
        close(timeout, false);
    }

    private void close(Duration timeout, boolean swallowException) {
        long timeoutMs = timeout.toMillis();
        if (timeoutMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs);

        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                        timeoutMs);
            } else {
                // Try to close gracefully.
                final Timer closeTimer = time.timer(timeout);
                if (this.sender != null) {
                    this.sender.initiateClose();
                    closeTimer.update();
                }
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(closeTimer.remainingMs());
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    } finally {
                        closeTimer.update();
                    }
                }
                clientTelemetryReporter.ifPresent(reporter -> reporter.initiateClose(closeTimer.remainingMs()));
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeoutMs);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        Utils.closeQuietly(interceptors, "producer interceptors", firstException);
        Utils.closeQuietly(producerMetrics, "producer metrics wrapper", firstException);
        Utils.closeQuietly(metrics, "producer metrics", firstException);
        Utils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        Utils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        Utils.closeQuietly(partitioner, "producer partitioner", firstException);
        clientTelemetryReporter.ifPresent(reporter -> Utils.closeQuietly(reporter, "producer telemetry reporter", firstException));
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
        log.debug("Kafka producer has been closed");
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * if custom partitioner is specified, call it to compute partition
     * otherwise try to calculate partition based on key.
     * If there is no key or key should be ignored return
     * RecordMetadata.UNKNOWN_PARTITION to indicate any partition
     * can be used (the partition is then calculated by built-in
     * partitioning logic).
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        if (record.partition() != null)
            return record.partition();

        if (partitioner != null) {
            int customPartition = partitioner.partition(
                record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
            if (customPartition < 0) {
                throw new IllegalArgumentException(String.format(
                    "The partitioner generated an invalid partition number: %d. Partition number should always be non-negative.", customPartition));
            }
            return customPartition;
        }

        if (serializedKey != null && !partitionerIgnoreKeys) {
            // hash the keyBytes to choose a partition
            return BuiltInPartitioner.partitionForKey(serializedKey, cluster.partitionsForTopic(record.topic()).size());
        } else {
            return RecordMetadata.UNKNOWN_PARTITION;
        }
    }

    private void throwIfInvalidGroupMetadata(ConsumerGroupMetadata groupMetadata) {
        if (groupMetadata == null) {
            throw new IllegalArgumentException("Consumer group metadata could not be null");
        } else if (groupMetadata.generationId() > 0
            && JoinGroupRequest.UNKNOWN_MEMBER_ID.equals(groupMetadata.memberId())) {
            throw new IllegalArgumentException("Passed in group metadata " + groupMetadata + " has generationId > 0 but member.id ");
        }
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    // Visible for testing
    String getClientId() {
        return clientId;
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * 被{@link RecordAccumulator}所调用的回调，追加函数有：
     * <ul>
     *     <li>用户自定义的回调</li>
     *     <li>拦截器回调</li>
     *     <li>分片回调</li>
     * </ul>
     */
    private class AppendCallbacks implements RecordAccumulator.AppendCallbacks {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final String topic;
        private final Integer recordPartition;
        private final String recordLogString;
        private volatile int partition = RecordMetadata.UNKNOWN_PARTITION;
        private volatile TopicPartition topicPartition;

        private AppendCallbacks(Callback userCallback, ProducerInterceptors<K, V> interceptors, ProducerRecord<K, V> record) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            // 提取记录信息，因为我们不想在批次整个生命周期保持一个记录的应用
            // 我们不希望存在NPE，因为拦截器可能无法被通知到
            topic = record != null ? record.topic() : null;
            recordPartition = record != null ? record.partition() : null;
            recordLogString = log.isTraceEnabled() && record != null ? record.toString() : "";
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata == null) {
                metadata = new RecordMetadata(topicPartition(), -1, -1, RecordBatch.NO_TIMESTAMP, -1, -1);
            }
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }

        @Override
        public void setPartition(int partition) {
            assert partition != RecordMetadata.UNKNOWN_PARTITION;
            this.partition = partition;

            if (log.isTraceEnabled()) {
                // Log the message here, because we don't know the partition before that.
                log.trace("Attempting to append record {} with callback {} to topic {} partition {}", recordLogString, userCallback, topic, partition);
            }
        }

        public int getPartition() {
            return partition;
        }

        public TopicPartition topicPartition() {
            if (topicPartition == null && topic != null) {
                if (partition != RecordMetadata.UNKNOWN_PARTITION)
                    topicPartition = new TopicPartition(topic, partition);
                else if (recordPartition != null)
                    topicPartition = new TopicPartition(topic, recordPartition);
                else
                    topicPartition = new TopicPartition(topic, RecordMetadata.UNKNOWN_PARTITION);
            }
            return topicPartition;
        }
    }
}
