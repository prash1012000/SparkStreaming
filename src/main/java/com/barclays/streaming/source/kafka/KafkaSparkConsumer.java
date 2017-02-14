package com.barclays.streaming.source.kafka;

import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.barclays.streaming.source.Consumer;

import kafka.serializer.Decoder;

public class KafkaSparkConsumer<K extends Object, V extends Object, KD extends Decoder<K>, VD extends Decoder<V>> implements Consumer<K,V,KD,VD> {
	
	private KafkaSparkConsumerConfig<K, V, Decoder<K>, Decoder<V>> config;
	
	public KafkaSparkConsumer(Object config2) {
		this.config=(KafkaSparkConsumerConfig)config2;
	}
	public KafkaSparkConsumerConfig<K, V, Decoder<K>, Decoder<V>> getConfig() {
		return config;
	}
	public void setConfig(KafkaSparkConsumerConfig<K, V, Decoder<K>, Decoder<V>> config) {
		this.config = config;
	}
	
	@Override
	public JavaPairInputDStream<K, V> getDataStream(JavaStreamingContext jssc){
		return KafkaUtils.createDirectStream(jssc, config.getKeyClass(), config.getValueClass(), config.getKeyDecoderClass(), config.getValueDecoderClass(), config.getParams(), ((KafkaSparkConsumerConfig<K, V, KD, VD>)config).getTopics());
	}


}
