package com.barclays.streaming.source.kafka;

import java.util.Map;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class KafkaWriter {
	private Producer<String,String> producer;
	
	public void initialize(Properties producerProps) {
        producerProps.put("request.required.acks", "1");
        ProducerConfig producerConfig = new ProducerConfig(producerProps);
        producer = new Producer<String,String>(producerConfig);
  }
	
	public void publishMesssage(String topicname,String key,String msg) throws Exception{
		KeyedMessage<String,String> data = new KeyedMessage<String,String>(topicname, key, msg);
        producer.send(data);
	}

}
