package com.barclays.streaming.driver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.barclays.streaming.processor.SparkProcessor;
import com.barclays.streaming.processor.executor.SparkProcessorExecutor;
import com.barclays.streaming.source.kafka.KafkaSource;
import com.barclays.streaming.source.kafka.KafkaSparkConsumerConfig;

import kafka.serializer.StringDecoder;

public class StreamingDriver {
	
	public static <T> void main(String args[]){
		System.out.println("Inside Main");
		String topics="streaming";
		SparkProcessorExecutor streamProcess=new SparkProcessorExecutor();
		KafkaSparkConsumerConfig<String, String, StringDecoder, StringDecoder,String> config=new KafkaSparkConsumerConfig<String, String, StringDecoder, StringDecoder,String>();
		config.setKeyClass(String.class);
		config.setValueClass(String.class);
		config.setKeyDecoderClass(StringDecoder.class);
		config.setValueDecoderClass(StringDecoder.class);
		config.setGroupId("test");
		config.setBrokers("localhost:9092");
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
	    config.setTopics(topicsSet);
	    Map<String,Object> sparkParams=new HashMap<>();
	    sparkParams.put("sparkConfig",config);
	    sparkParams.put("master","local[2]");
	    sparkParams.put("BatchDuration","60");
	    KafkaSource source =new KafkaSource();
	    JavaInputDStream<T> dstream=source.getKafkaData("sparkstreaming", sparkParams);
	    streamProcess.execute(dstream,new SparkProcessor());
	    source.start();
		source.waitForTermination();
	}
	
	protected JavaStreamingContext getSparkStreamingContext(String appname, Map sparkParams) {
		SparkConf sparkConf = new SparkConf().setAppName(appname);
		sparkConf.setMaster((String)sparkParams.get("master"));
		sparkConf.set("es.index.auto.create", "true");
		sparkConf.set("es.nodes", "localhost");
		sparkConf.set("es.port", "9200");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		long batchDuration=Long.parseLong((String)sparkParams.get("BatchDuration"));
		JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(batchDuration));
	    return jssc;	
	}
	

	protected void start(JavaStreamingContext jssc) {
		jssc.start();
	}
	
	protected void waitForTermination(JavaStreamingContext jssc) {
		jssc.awaitTermination();
	}

}
