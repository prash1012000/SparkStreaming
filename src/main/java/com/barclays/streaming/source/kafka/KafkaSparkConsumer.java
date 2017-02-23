package com.barclays.streaming.source.kafka;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.barclays.streaming.common.ZookeeperOffsetStore;
import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.StreamConsumerConfig;

import kafka.common.TopicAndPartition;
import kafka.serializer.Decoder;

public class KafkaSparkConsumer<K extends Object, V extends Object, KD extends Decoder<K>, VD extends Decoder<V>,R> implements Consumer {
	
	private KafkaSparkConsumerConfig<K, V, Decoder<K>, Decoder<V>, R> config;
	
	public KafkaSparkConsumer(StreamConsumerConfig config2) {
		this.config=(KafkaSparkConsumerConfig)config2;
		config.createConsumerProperties();
	}
	public KafkaSparkConsumerConfig<K, V, Decoder<K>, Decoder<V>,R> getConfig() {
		return config;
	}
	
	@Override
	public JavaDStreamLike<?,?,?> getDataStream(JavaStreamingContext jssc){
		JavaDStreamLike<?,?,?> kafkaStream=null;
		ZookeeperOffsetStore store=new ZookeeperOffsetStore("localhost:2181","/offset/kafka",100000,100000);
		Set<String> topics= ((KafkaSparkConsumerConfig<K, V, KD, VD,R>)config).getTopics();
		String firstTopic=getFirstTopic(topics);
		Map<TopicAndPartition, Long> partitionOffsetMap=store.readOffsets(firstTopic);
		if(partitionOffsetMap.size()>0){
			kafkaStream=KafkaUtils.createDirectStream(jssc, config.getKeyClass(), config.getValueClass(), config.getKeyDecoderClass(), config.getValueDecoderClass(),config.getValueClass(), config.getParams(), partitionOffsetMap,mmd -> mmd.message());
		} else{
			Set<String> topicsSet = new HashSet<>(Arrays.asList(firstTopic));
			kafkaStream= KafkaUtils.createDirectStream(jssc, config.getKeyClass(), config.getValueClass(), config.getKeyDecoderClass(), config.getValueDecoderClass(), config.getParams(), topicsSet);
		}
		kafkaStream.foreachRDD(rdd -> {
			if(rdd!=null && rdd.rdd()!=null)
				store.saveOffsets(firstTopic, rdd.rdd());
			});
		return kafkaStream;
	}
	
	private String getFirstTopic(Set<String> topics) {
		Iterator<String> itr=topics.iterator();
		String topic=null;
		while(itr.hasNext()){
			topic=itr.next();
			break;
		}
		return topic;
	}


}
