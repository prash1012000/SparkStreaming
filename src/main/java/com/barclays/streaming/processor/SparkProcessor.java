package com.barclays.streaming.processor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.barclays.streaming.source.kafka.KafkaWriter;


public class SparkProcessor<T, R> implements StreamProcessor<JavaDStream<T>,JavaDStream<String>> {

	public JavaDStream<String> process(JavaDStream<T> dstream) {
		Map<String,String> values=new HashMap<String, String>();
		JavaDStream<Long> messageCount=dstream.count();
		JavaDStream<Map<String, String>> r=messageCount.map( value->{values.put("msgcount",value.toString());
								 values.put("appname","sparkstreaming");
								 values.put("generator","processor");
								 return values;});
		
		r.foreachRDD(r1 -> r1.foreachPartition(itr -> {
			KafkaWriter writer = getKafkaWriter();
			while (itr.hasNext()) {
				Map<String, String> map = itr.next();
				if(Long.valueOf(map.get("msgcount"))>0){
					ObjectMapper mapperObj = new ObjectMapper();
					String jsonResp = null;
					try {
						jsonResp = mapperObj.writeValueAsString(map);
						System.out.println("JSON Response is" + jsonResp);
					} catch (IOException e) {
						e.printStackTrace();
					}
					writer.publishMesssage("monitor", null, jsonResp);
				}	
			}
		}));
		
		
		return null;
	
	}
	
	private KafkaWriter getKafkaWriter() {
		KafkaWriter writer = new KafkaWriter();
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", "localhost:9092");
		producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
		//producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		//producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProps.put("request.required.acks", "1");
		writer.initialize(producerProps);
		return writer;

	}
	
}
