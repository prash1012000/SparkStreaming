package com.barclays.streaming.processor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.barclays.streaming.processor.spark.kafka.KafkaSparkStreamProcessor;
import com.barclays.streaming.source.kafka.KafkaSparkConsumerConfig;

import kafka.serializer.StringDecoder;

public class StreamingDriver {
	
	public static void main(String args[]){
		System.out.println("Inside Main");
		String topics="streaming";
		KafkaSparkStreamProcessor streamProcess=new KafkaSparkStreamProcessor();
		KafkaSparkConsumerConfig<String, String, StringDecoder, StringDecoder> config=new KafkaSparkConsumerConfig<String, String, StringDecoder, StringDecoder>();
		config.setKeyClass(String.class);
		config.setValueClass(String.class);
		config.setKeyDecoderClass(StringDecoder.class);
		config.setValueDecoderClass(StringDecoder.class);
		Map<String, String> kafkaParams = new HashMap<>();
	    kafkaParams.put("metadata.broker.list", "localhost:9092");
	    config.setParams(kafkaParams);
	    Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    config.setTopics(topicsSet);
	    Map<String,Object> sparkParams=new HashMap<>();
	    sparkParams.put("sparkConfig",config);
	    streamProcess.process("test",sparkParams);
	}

}
