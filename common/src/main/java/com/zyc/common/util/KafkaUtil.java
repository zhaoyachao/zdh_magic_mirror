package com.zyc.common.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

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


    public void send(String message, String topic, String key){
        if(producer == null){
            synchronized (lock.intern()){
                if(producer == null){
                    init(this.props);
                }
            }
        }
        producer.send(new ProducerRecord<>(topic, key, message));
    }

    public void send(ProducerRecord producerRecord){
        if(producer == null){
            synchronized (lock.intern()){
                if(producer == null){
                    init(this.props);
                }
            }
        }
        producer.send(producerRecord);
    }

}
