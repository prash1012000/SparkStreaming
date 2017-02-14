package com.barclays.streaming.source.kafka;

import java.util.Map;
import java.util.Set;

import com.barclays.streaming.source.ConsumerConfig;

import kafka.serializer.Decoder;

public class KafkaSparkConsumerConfig<K, V, KD extends Decoder<K>, VD extends Decoder<V>> extends ConsumerConfig<K, V, KD, VD> {
	
	private Set<String> topics;
    
	public Set<String> getTopics() {
		return topics;
	}
	public void setTopics(Set<String> topics) {
		this.topics = topics;
	}

}
