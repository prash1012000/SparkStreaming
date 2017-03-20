package com.barclays.streaming.source.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.barclays.streaming.source.Consumer;
import com.barclays.streaming.source.StreamConsumerConfig;


import scala.Tuple2;

public class KafkaSource<T, R> implements Serializable {
	private static JavaStreamingContext jssc=null;	
	
	public void createSparkStreamingContext(String appname, Map sparkParams) {
		if(jssc==null){
			SparkConf sparkConf = new SparkConf().setAppName(appname);
			sparkConf.setMaster((String)sparkParams.get("master"));
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
			long batchDuration=Long.parseLong((String)sparkParams.get("BatchDuration"));
			jssc = new JavaStreamingContext(sc, Durations.seconds(batchDuration));
		}
	  	
	}
	
	public void start() {		
		if(jssc!=null)
			jssc.start();
	}
	
	public void waitForTermination() {
		if(jssc!=null)
			jssc.awaitTermination();
	}
	
	public JavaInputDStream<T> getKafkaData(String appname,Map params){
		createSparkStreamingContext(appname, params);
		SQLContext sqlContext = new SQLContext(jssc.sparkContext());
		KafkaSparkConsumerConfig config=(KafkaSparkConsumerConfig)params.get("sparkConfig");
		Consumer consumer =  getInputConsumer(config);
		JavaInputDStream<T> messages=(JavaInputDStream<T>)consumer.getDataStream(jssc);
		messages.persist();
		Map<String,String> values=new HashMap<String, String>();
		JavaDStream<Long> messageCount=messages.count();
		JavaDStream<Map<String, String>> r=messageCount.map( value->{values.put("msgcount",value.toString());
								 values.put("appname",appname);
								 values.put("generator","source");
								 return values;});
		r.foreachRDD(r1 -> {
			JavaEsSpark.saveToEs(r1,"sparkdata/counter");
		});
		return messages;
		
	}
	
	public Consumer getInputConsumer(StreamConsumerConfig config) {
		return new KafkaSparkConsumer(config);
	}

}
