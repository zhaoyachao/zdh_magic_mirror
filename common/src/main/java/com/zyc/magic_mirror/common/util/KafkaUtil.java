package com.zyc.magic_mirror.common.util;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *     Properties props = new Properties();
 *     props.put("bootstrap.servers", "localhost:9092");
 *     props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *     props.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
 *     props.put("linger.ms", 0);
 *     KafkaProducer<String, Long> producer = new KafkaProducer<>(props);
 *     producer.send(new ProducerRecord<>("visits", ip, System.currentTimeMillis() + i));
 */
public class KafkaUtil {

    private Properties props;

    private KafkaProducer<String, String> producer;

    private String lock="lock_producer";


    public KafkaUtil(Properties props){
        this.props = props;
    }

    public void init(Properties props){
        this.producer = new KafkaProducer<>(props);
    }


    public static KafkaConsumer getKafkaConsumer(String bootstrap, String groupId){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer(props);
    }

    public static KafkaConsumer getKafkaConsumer(String bootstrap, String groupId, Properties props) throws Exception {
        if(props == null){
            throw new Exception("配置信息不可为空");
        }
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new KafkaConsumer(props);
    }

    public static KafkaProducer getKafkaProducer(String bootstrap){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer(props);
    }

    public static <K, V> KafkaProducer<K, V> getKafkaProducer(String bootstrap, Class<K> keySerializerClass,
                                                              Class<V> valueSerializerClass, Properties props) throws Exception {
        if(props == null){
            throw new Exception("配置信息不可为空");
        }
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        // 设置默认序列化器
        if (keySerializerClass == null) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        } else {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        }

        if (valueSerializerClass == null) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        } else {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
        }
        return new KafkaProducer(props);
    }

    public static RecordMetadata send(KafkaProducer<String, String> kafkaProducer, String key,
                                             String value, String topic) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> send = kafkaProducer.send(record);
        RecordMetadata recordMetadata = send.get();
        return recordMetadata;
    }

    public static <K, V> RecordMetadata send(KafkaProducer<K, V> kafkaProducer,  K key,
                                    V value, String topic) throws ExecutionException, InterruptedException {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        Future<RecordMetadata> send = kafkaProducer.send(record);
        RecordMetadata recordMetadata = send.get();
        return recordMetadata;
    }

    public static <K, V> ConsumerRecords<K, V> consumer(KafkaConsumer<K, V> kafkaConsumer, String topic) throws Exception {

        try {
            kafkaConsumer.subscribe(Lists.newArrayList(topic));
            while (true) {
                ConsumerRecords<K, V> records = kafkaConsumer.poll(Duration.ofMillis(100).toMillis());
                return records;
            }
        } catch (Exception e) {
            throw e;
        } finally {
            kafkaConsumer.close();
        }
    }
}
