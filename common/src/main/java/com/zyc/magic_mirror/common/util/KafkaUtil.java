package com.zyc.magic_mirror.common.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class KafkaUtil {
    private static final Logger logger = LoggerFactory.getLogger(KafkaUtil.class);

    private static final int DEFAULT_BATCH_SIZE = 16384;
    private static final int DEFAULT_LINGER_MS = 10;
    private static final int DEFAULT_BUFFER_MEMORY = 33554432;
    private static final int DEFAULT_REQUEST_TIMEOUT_MS = 30000;
    private static final int DEFAULT_SESSION_TIMEOUT_MS = 30000;
    private static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 300000;

    private static final Cache<String, KafkaProducer> PRODUCER_CACHE_WITH_EXPIRE;
    private static final Cache<String, KafkaConsumer> CONSUMER_CACHE_WITH_EXPIRE;

    static {
        PRODUCER_CACHE_WITH_EXPIRE = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<String, KafkaProducer>(){
                    @Override
                    public void onRemoval(RemovalNotification<String, KafkaProducer> removalNotification) {
                        try{
                            removalNotification.getValue().close(10, TimeUnit.SECONDS);
                        }catch (Exception e){
                            removalNotification.getValue().close();
                            logger.error("自动关闭kafka producer连接失败 - 路径: {}", removalNotification.getKey(), e);
                        }
                    }
                })
                .build();

        CONSUMER_CACHE_WITH_EXPIRE = CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<String, KafkaConsumer>(){

                    @Override
                    public void onRemoval(RemovalNotification<String, KafkaConsumer> removalNotification) {
                        try{
                            removalNotification.getValue().close();
                        }catch (Exception e){
                            logger.error("自动关闭kafka consumer连接失败 - 路径: {}", removalNotification.getKey(), e);
                        }
                    }
                })
                .build();

        logger.info("KafkaUtil初始化完成，Producer/Consumer缓存过期时间: 10分钟");
    }

    private KafkaUtil() {
    }

    public static Properties createDefaultProducerConfig(String bootstrap) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DEFAULT_BATCH_SIZE));
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(DEFAULT_LINGER_MS));
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DEFAULT_BUFFER_MEMORY));
        props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(DEFAULT_REQUEST_TIMEOUT_MS));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    public static Properties createDefaultConsumerConfig(String bootstrap, String groupId) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(DEFAULT_SESSION_TIMEOUT_MS));
        props.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(DEFAULT_HEARTBEAT_INTERVAL_MS));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return props;
    }

    public static KafkaConsumer<String, String> getKafkaConsumer(String bootstrap, String groupId) {
        return getKafkaConsumer(bootstrap, groupId, null);
    }

    public static <K, V> KafkaConsumer<K, V> getKafkaConsumer(String bootstrap, String groupId, Properties customProps) {
        if (bootstrap == null || bootstrap.trim().isEmpty()) {
            throw new IllegalArgumentException("Bootstrap地址不能为空");
        }
        if (groupId == null || groupId.trim().isEmpty()) {
            throw new IllegalArgumentException("Group ID不能为空");
        }

        String cacheKey = customProps.toString();
        KafkaConsumer<K, V> consumer = (KafkaConsumer<K, V>) CONSUMER_CACHE_WITH_EXPIRE.getIfPresent(cacheKey);

        if (consumer != null) {
            return consumer;
        }

        Properties props = customProps != null ? new Properties(customProps) : new Properties();
        if (props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        }
        if (props.getProperty(ConsumerConfig.GROUP_ID_CONFIG) == null) {
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        if (props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) == null) {
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        if (props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) == null) {
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }

        consumer = new KafkaConsumer<>(props);
        CONSUMER_CACHE_WITH_EXPIRE.put(cacheKey, consumer);
        logger.info("KafkaConsumer创建成功 - Bootstrap: {}, GroupId: {}", bootstrap, groupId);
        return consumer;
    }

    public static KafkaProducer<String, String> getKafkaProducer(String bootstrap) {
        Properties customProps = new Properties();
        customProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return getKafkaProducer(customProps);
    }

    public static <K, V> KafkaProducer<K, V> getKafkaProducer(Properties customProps) {
        if (!customProps.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException("Bootstrap地址不能为空");
        }


        String cacheKey = customProps.toString();
        KafkaProducer<K, V> producer = (KafkaProducer<K, V>) PRODUCER_CACHE_WITH_EXPIRE.getIfPresent(cacheKey);

        if (producer != null) {
            return producer;
        }

        Properties props = new Properties();
        if(customProps!=null && !customProps.isEmpty()){
            props.putAll(customProps);
        }
        if (props.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG) == null) {
            props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        if (props.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG) == null) {
            props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        if (props.getProperty(ProducerConfig.BATCH_SIZE_CONFIG) == null) {
            props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(DEFAULT_BATCH_SIZE));
        }
        if (props.getProperty(ProducerConfig.LINGER_MS_CONFIG) == null) {
            props.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(DEFAULT_LINGER_MS));
        }
        if (props.getProperty(ProducerConfig.BUFFER_MEMORY_CONFIG) == null) {
            props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(DEFAULT_BUFFER_MEMORY));
        }
        if (props.getProperty(ProducerConfig.ACKS_CONFIG) == null) {
            props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        }
        if (props.getProperty(ProducerConfig.RETRIES_CONFIG) == null) {
            props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        }
        if (props.getProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG) == null) {
            props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(DEFAULT_REQUEST_TIMEOUT_MS));
        }
        if (props.getProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG) == null) {
            props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        }

        producer = new KafkaProducer<>(props);

        //验证producer
        try{
            producer.partitionsFor("__consumer_offsets");
        }catch (Exception e){
            producer.close();
            throw new IllegalArgumentException("Kafka Producer 链接异常", e);
        }

        PRODUCER_CACHE_WITH_EXPIRE.put(cacheKey, producer);
        logger.info("KafkaProducer创建成功 - Bootstrap: {}", customProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        return producer;
    }

    public static RecordMetadata sendSync(KafkaProducer<String, String> kafkaProducer, String key,
                                             String value, String topic) throws ExecutionException, InterruptedException {
        return sendSync(kafkaProducer, key, value, topic);
    }

    public static <K, V> RecordMetadata sendSync(KafkaProducer<K, V> kafkaProducer, K key,
                                    V value, String topic) throws ExecutionException, InterruptedException {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> future = kafkaProducer.send(record);
        RecordMetadata recordMetadata = future.get();
        logger.debug("消息发送成功 - Topic: {}, Partition: {}, Offset: {}", 
                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        return recordMetadata;
    }

    public static void sendAsync(KafkaProducer<String, String> kafkaProducer, String key,
                               String value, String topic, Callback callback) {
        sendAsync(kafkaProducer, key, value, topic, callback);
    }

    public static <K, V> void sendAsync(KafkaProducer<K, V> kafkaProducer, K key,
                               V value, String topic, Callback callback) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        kafkaProducer.send(record, callback != null ? callback : new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("消息发送失败 - Topic: {}, Key: {}, Error: {}", 
                            topic, key, exception.getMessage(), exception);
                } else {
                    logger.debug("消息发送成功 - Topic: {}, Partition: {}, Offset: {}", 
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            }
        });
    }

    public static void sendAsync(KafkaProducer<String, String> kafkaProducer, String key,
                               String value, String topic) {
        sendAsync(kafkaProducer, key, value, topic, null);
    }

    public static <K, V> void sendAsync(KafkaProducer<K, V> kafkaProducer, K key,
                               V value, String topic) {
        sendAsync(kafkaProducer, key, value, topic, null);
    }

    public static <K, V> void sendBatch(KafkaProducer<K, V> kafkaProducer, String topic,
                                       List<K> keys, List<V> values) {
        if (keys == null || values == null || keys.size() != values.size()) {
            throw new IllegalArgumentException("Keys和Values列表长度必须相同且不为空");
        }

        for (int i = 0; i < keys.size(); i++) {
            ProducerRecord<K, V> record = new ProducerRecord<>(topic, keys.get(i), values.get(i));
            kafkaProducer.send(record);
        }
        logger.debug("批量发送消息完成 - Topic: {}, 数量: {}", topic, keys.size());
    }

    public static <K, V> void sendBatch(KafkaProducer<K, V> kafkaProducer, String topic,
                                       List<ProducerRecord<K, V>> records) {
        if (records == null || records.isEmpty()) {
            throw new IllegalArgumentException("Records列表不能为空");
        }

        for (ProducerRecord<K, V> record : records) {
            kafkaProducer.send(record);
        }
        logger.debug("批量发送消息完成 - Topic: {}, 数量: {}", topic, records.size());
    }

    public static <K, V> void consume(KafkaConsumer<K, V> kafkaConsumer, String topic,
                                      Consumer<ConsumerRecord<K, V>> recordHandler) {
        if (kafkaConsumer == null) {
            throw new IllegalArgumentException("KafkaConsumer不能为空");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic不能为空");
        }
        if (recordHandler == null) {
            throw new IllegalArgumentException("RecordHandler不能为空");
        }

        kafkaConsumer.subscribe(Lists.newArrayList(topic));
        logger.info("开始消费Kafka消息 - Topic: {}", topic);

        try {
            while (true) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(2000);
                if (!records.isEmpty()) {
                    for (ConsumerRecord<K, V> record : records) {
                        try {
                            recordHandler.accept(record);
                        } catch (Exception e) {
                            logger.error("处理Kafka消息异常 - Topic: {}, Partition: {}, Offset: {}", 
                                    record.topic(), record.partition(), record.offset(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Kafka消费异常 - Topic: {}", topic, e);
            throw new RuntimeException("Kafka消费异常", e);
        }
    }

    public static <K, V> void consumeOnce(KafkaConsumer<K, V> kafkaConsumer, String topic,
                                          Consumer<ConsumerRecord<K, V>> recordHandler) {
        if (kafkaConsumer == null) {
            throw new IllegalArgumentException("KafkaConsumer不能为空");
        }
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic不能为空");
        }
        if (recordHandler == null) {
            throw new IllegalArgumentException("RecordHandler不能为空");
        }

        kafkaConsumer.subscribe(Lists.newArrayList(topic));

        try {
            ConsumerRecords<K, V> records = kafkaConsumer.poll(2000);
            if (!records.isEmpty()) {
                for (ConsumerRecord<K, V> record : records) {
                    try {
                        recordHandler.accept(record);
                    } catch (Exception e) {
                        logger.error("处理Kafka消息异常 - Topic: {}, Partition: {}, Offset: {}", 
                                record.topic(), record.partition(), record.offset(), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Kafka消费异常 - Topic: {}", topic, e);
            throw new RuntimeException("Kafka消费异常", e);
        }
    }


    public static void closeAll() {
        int closedCount = 0;

        for (Map.Entry<String, KafkaProducer> entry : PRODUCER_CACHE_WITH_EXPIRE.asMap().entrySet()) {
            try {
                entry.getValue().close();
                logger.info("KafkaProducer已关闭 - Key: {}", entry.getKey());
                closedCount++;
            } catch (Exception e) {
                logger.error("关闭KafkaProducer失败 - Key: {}", entry.getKey(), e);
            }
        }
        PRODUCER_CACHE_WITH_EXPIRE.invalidateAll();

        for (Map.Entry<String, KafkaConsumer> entry : CONSUMER_CACHE_WITH_EXPIRE.asMap().entrySet()) {
            try {
                entry.getValue().close();
                logger.info("KafkaConsumer已关闭 - Key: {}", entry.getKey());
                closedCount++;
            } catch (Exception e) {
                logger.error("关闭KafkaConsumer失败 - Key: {}", entry.getKey(), e);
            }
        }
        CONSUMER_CACHE_WITH_EXPIRE.invalidateAll();

        logger.info("所有Kafka连接已关闭，共关闭 {} 个连接", closedCount);
    }

    public static int getActiveProducerCount() {
        return (int) PRODUCER_CACHE_WITH_EXPIRE.size();
    }

    public static int getActiveConsumerCount() {
        return (int) CONSUMER_CACHE_WITH_EXPIRE.size();
    }

    public static int getCacheExpireMinutes() {
        return 10;
    }
}
